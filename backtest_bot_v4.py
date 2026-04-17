# ============================================================
# ALL-IN-ONE BOT v4 - BACKTEST + WEEKLY SCAN + BUILD LIST
# Lenh Telegram:
#   [MA]             : backtest 1 ma
#   /scanall         : backtest toan bo
#   /weeklyscan      : scan tin hieu tuan (RSI cat SMA)
#   /buildlist       : xay dung lai danh sach ma
#   /config          : xem tham so hien tai
#   /set vol [so]    : volume % MA20        (10-200, mac dinh 120)
#   /set trend [so]  : so phien xu huong    (1-10,  mac dinh 1)
#   /set stop [so]   : trailing stop %      (1-50,  mac dinh 10)
# Tu tat sau 30 phut khong hoat dong
# ============================================================

import os

API_KEY = 'vnstock_3519cd0014af8858dc6b96d189b8875e'
os.environ['VNSTOCK_API_KEY'] = API_KEY

import asyncio
import logging
import threading
import pandas as pd
import numpy as np
import time
import requests as _requests
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CommandHandler,
    filters, ContextTypes
)

TOKEN   = '8578016275:AAGvL6SoOO3Yifqner8EcynwKt7OKgwl_J0'
CHAT_ID = '7000478479'
VN_TZ   = timezone(timedelta(hours=7))

logging.basicConfig(level=logging.INFO)
SEP = '-' * 35

CONFIG = {
    'vol_pct' : 120,
    'trend_n' : 1,
    'stop_pct': 10,
}

# Nguong loc cho /buildlist
GIA_TOI_THIEU      = 2.0        # don vi nghin dong (2.0 = 2,000d)
VOL_TUAN_TOI_THIEU = 500_000

last_activity = [time.time()]

def update_activity():
    last_activity[0] = time.time()

def now_vn():
    return datetime.now(VN_TZ).strftime('%Y-%m-%d %H:%M')

# ============================================================
# RATE LIMITER - Token Bucket 170 req/phut (Bronze 180, de an toan)
# ============================================================
class RateLimiter:
    def __init__(self, max_calls: int = 170, period: float = 60.0):
        self.max_calls = max_calls
        self.period    = period
        self._lock     = threading.Lock()
        self._calls: list[float] = []

    def acquire(self):
        while True:
            with self._lock:
                now = time.time()
                self._calls = [t for t in self._calls if now - t < self.period]
                if len(self._calls) < self.max_calls:
                    self._calls.append(now)
                    return
                wait = self.period - (now - self._calls[0]) + 0.01
            time.sleep(max(wait, 0.05))

_rate_limiter = RateLimiter(max_calls=170, period=60.0)

# ============================================================
# VNSTOCK - Uu tien vnstock_data (Bronze), fallback sang vnstock
# ============================================================
def _get_vnstock_class():
    try:
        from vnstock_data import Vnstock as VnstockData
        return VnstockData
    except ImportError:
        pass
    from vnstock import Vnstock
    return Vnstock

_Vnstock      = None
_vnstock_lock = threading.Lock()

def get_vnstock_class():
    global _Vnstock
    if _Vnstock is None:
        with _vnstock_lock:
            if _Vnstock is None:
                _Vnstock = _get_vnstock_class()
    return _Vnstock

# ============================================================
# DOC / LOC DANH SACH MA
# ============================================================
def get_all_symbols(filename='vn_stocks_full.txt'):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            raw = [line.strip() for line in f if line.strip()]
        symbols = [s for s in raw if 2 <= len(s) <= 5 and s.isalpha()]
        exclude = {'E1VFVN30', 'FUEKIVFS', 'FUEMAV30', 'FUEMAVND',
                   'FUESSV30', 'FUESSVFL', 'FUETCC50', 'FUEVFVND', 'FUEVN100'}
        return [s for s in dict.fromkeys(symbols) if s not in exclude]
    except:
        return []

# ============================================================
# LAY DU LIEU NGAY + TUAN (dung cho backtest)
# ============================================================
def _fetch_df(symbol, source):
    Vnstock = get_vnstock_class()
    _rate_limiter.acquire()
    stock = Vnstock(show_log=False).stock(symbol=symbol, source=source)
    end   = datetime.now(VN_TZ).strftime('%Y-%m-%d')
    raw   = stock.quote.history(start='2022-01-01', end=end, interval='1D')

    if isinstance(raw, dict):
        df = pd.DataFrame(raw['data']) if 'data' in raw else None
    else:
        df = raw

    if df is None or (hasattr(df, 'empty') and df.empty):
        return None

    df.columns = [c.lower() for c in df.columns]
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])
        df = df.set_index('time')
    elif df.index.dtype != 'datetime64[ns]':
        df.index = pd.to_datetime(df.index)

    df = df.rename(columns={'close': 'Close', 'high': 'High',
                             'low': 'Low',   'volume': 'Volume'})
    df = df.sort_index().dropna(subset=['Close', 'High', 'Low', 'Volume'])
    return df if not df.empty else None

def get_data(symbol):
    last_errors = []
    for source in ('VCI', 'MSN', 'KBS'):
        try:
            df = _fetch_df(symbol, source)
            if df is not None:
                weekly = df.resample('W-FRI').agg(
                    {'Close': 'last', 'Volume': 'sum'}
                ).dropna()
                return df, weekly
            else:
                last_errors.append(source + ':empty')
        except Exception as e:
            err = str(e)
            logging.warning('[get_data] %s / %s: %s', symbol, source, err)
            last_errors.append(source + ':' + err[:120])
            if any(k in err.lower() for k in ['rate limit', '429', 'too many', 'exceeded']):
                logging.warning('[rate limit] sleeping 30s...')
                time.sleep(30)
                try:
                    df2 = _fetch_df(symbol, source)
                    if df2 is not None:
                        weekly = df2.resample('W-FRI').agg(
                            {'Close': 'last', 'Volume': 'sum'}
                        ).dropna()
                        return df2, weekly
                except Exception:
                    pass
            continue

    logging.warning('[get_data] %s failed: %s', symbol, ' | '.join(last_errors))
    return None, last_errors

# ============================================================
# CHI BAO KY THUAT
# ============================================================
def smma(series, period):
    """Smoothed Moving Average - chuan TradingView."""
    values = series.values.astype(float)
    result = np.full(len(values), np.nan)
    count, start = 0, -1
    for i, v in enumerate(values):
        if not np.isnan(v):
            count += 1
            if count == period:
                start = i
                break
        else:
            count = 0
    if start == -1:
        return pd.Series(result, index=series.index)
    result[start] = np.mean(values[start - period + 1: start + 1])
    for i in range(start + 1, len(values)):
        result[i] = (result[i-1] if np.isnan(values[i])
                     else (result[i-1] * (period-1) + values[i]) / period)
    return pd.Series(result, index=series.index)

def calc_weekly_indicators(weekly):
    df = weekly.copy()
    df['ma20_vol'] = df['Volume'].rolling(20).mean()
    delta         = df['Close'].diff()
    avg_gain      = smma(delta.where(delta > 0, 0.0), 14)
    avg_loss      = smma((-delta).where(delta < 0, 0.0), 14)
    df['rsi']     = 100 - (100 / (1 + avg_gain / avg_loss))
    df['sma_rsi'] = df['rsi'].rolling(14).mean()
    return df

def check_buy_signal(df_w, i, vol_pct, trend_n):
    if i < max(1, trend_n):
        return False
    row  = df_w.iloc[i]
    prev = df_w.iloc[i - 1]
    if any(pd.isna(row[c]) for c in ['Volume', 'ma20_vol', 'rsi', 'sma_rsi']):
        return False
    if pd.isna(prev['rsi']) or pd.isna(prev['sma_rsi']):
        return False
    dk1 = row['Volume'] > (vol_pct / 100) * row['ma20_vol']
    dk2 = prev['rsi'] <= prev['sma_rsi'] and row['rsi'] > row['sma_rsi']
    dk3 = all(
        not (pd.isna(df_w.iloc[i-k]['sma_rsi']) or pd.isna(df_w.iloc[i-k-1]['sma_rsi']) or
             df_w.iloc[i-k]['sma_rsi'] < df_w.iloc[i-k-1]['sma_rsi'])
        for k in range(trend_n) if i - k - 1 >= 0
    )
    return dk1 and dk2 and dk3

# ============================================================
# BACKTEST 1 MA
# ============================================================
def run_backtest(symbol, initial_capital=50_000_000,
                 vol_pct=None, trend_n=None, stop_pct=None):
    if vol_pct  is None: vol_pct  = CONFIG['vol_pct']
    if trend_n  is None: trend_n  = CONFIG['trend_n']
    if stop_pct is None: stop_pct = CONFIG['stop_pct']

    stop_mult = 1 - stop_pct / 100

    daily, weekly = get_data(symbol)
    if daily is None:
        err_detail = ' | '.join(weekly) if isinstance(weekly, list) else 'unknown'
        return {'error': 'Khong lay duoc du lieu cho ma ' + symbol + ' | ' + err_detail}

    df_w    = calc_weekly_indicators(weekly)
    df_w_bt = df_w[df_w.index >= '2023-01-01']
    if df_w_bt.empty:
        return {'error': 'Khong co du lieu tu 2023'}

    daily_bt   = daily[daily.index >= '2023-01-01'].copy()
    daily_list = list(daily_bt.iterrows())

    capital  = initial_capital
    trades   = []
    position = None
    day_idx  = 0

    def do_sell(buy_date, buy_price, sell_date, sell_price, peak, von_vao):
        pct = (sell_price - buy_price) / buy_price * 100
        cap = von_vao * (1 + pct / 100)
        return {
            'stt'      : len(trades) + 1,
            'loai'     : 'Ban',
            'ngay_mua' : buy_date.strftime('%Y-%m-%d'),
            'gia_mua'  : round(buy_price, 2),
            'ngay_ban' : sell_date.strftime('%Y-%m-%d'),
            'gia_ban'  : round(sell_price, 2),
            'gia_dinh' : round(peak, 2),
            'gia_stop' : round(peak * stop_mult, 2),
            'von_dau'  : round(von_vao, 0),
            'gia_tri'  : round(cap, 0),
            'pct'      : round(pct, 2),
            'lai_lo'   : round(cap - von_vao, 0),
            'von_sau'  : round(cap, 0),
            'dang_giu' : False,
        }, cap

    for wi, week_end in enumerate(df_w_bt.index.tolist()):
        global_wi = df_w.index.get_loc(week_end)

        if position is not None:
            sold = False
            while day_idx < len(daily_list):
                day_ts, day_row = daily_list[day_idx]
                if day_ts > week_end:
                    break
                if day_ts <= position['buy_date']:
                    day_idx += 1
                    continue
                stop_price = position['peak'] * stop_mult
                if day_row['Low'] <= stop_price:
                    t, capital = do_sell(
                        position['buy_date'], position['buy_price'],
                        day_ts, stop_price, position['peak'], position['cost']
                    )
                    trades.append(t)
                    position = None
                    day_idx += 1
                    sold = True
                    break
                if day_row['High'] > position['peak']:
                    position['peak'] = day_row['High']
                day_idx += 1

            if sold and check_buy_signal(df_w, global_wi, vol_pct, trend_n):
                bp = df_w_bt.iloc[wi]['Close']
                position = {'buy_date': week_end, 'buy_price': bp,
                            'shares': capital/bp, 'cost': capital, 'peak': bp}
            continue

        while day_idx < len(daily_list) and daily_list[day_idx][0] <= week_end:
            day_idx += 1
        if check_buy_signal(df_w, global_wi, vol_pct, trend_n):
            bp = df_w_bt.iloc[wi]['Close']
            position = {'buy_date': week_end, 'buy_price': bp,
                        'shares': capital/bp, 'cost': capital, 'peak': bp}

    if position is not None:
        while day_idx < len(daily_list):
            day_ts, day_row = daily_list[day_idx]
            if day_ts <= position['buy_date']:
                day_idx += 1
                continue
            stop_price = position['peak'] * stop_mult
            if day_row['Low'] <= stop_price:
                t, capital = do_sell(
                    position['buy_date'], position['buy_price'],
                    day_ts, stop_price, position['peak'], position['cost']
                )
                trades.append(t)
                position = None
                break
            if day_row['High'] > position['peak']:
                position['peak'] = day_row['High']
            day_idx += 1

        if position is not None:
            last_ts, last_row = daily_list[-1]
            lc      = last_row['Close']
            pct     = (lc - position['buy_price']) / position['buy_price'] * 100
            von_vao = position['cost']
            current = von_vao * (1 + pct / 100)
            capital = current
            trades.append({
                'stt': len(trades)+1, 'loai': 'Dang giu',
                'ngay_mua': position['buy_date'].strftime('%Y-%m-%d'),
                'gia_mua': round(position['buy_price'], 2),
                'ngay_ban': last_ts.strftime('%Y-%m-%d'),
                'gia_ban': round(lc, 2),
                'gia_dinh': round(position['peak'], 2),
                'gia_stop': round(position['peak'] * stop_mult, 2),
                'von_dau': round(von_vao, 0), 'gia_tri': round(current, 0),
                'pct': round(pct, 2), 'lai_lo': round(current - von_vao, 0),
                'von_sau': round(current, 0), 'dang_giu': True,
            })

    return {
        'symbol': symbol.upper(), 'von_ban_dau': initial_capital,
        'von_cuoi': round(capital, 0), 'lai_lo': round(capital - initial_capital, 0),
        'pct': round((capital / initial_capital - 1) * 100, 2),
        'so_gd': len(trades), 'trades': trades,
        'vol_pct': vol_pct, 'trend_n': trend_n, 'stop_pct': stop_pct,
    }

# ============================================================
# DINH DANG KET QUA BACKTEST
# ============================================================
def format_result(r):
    if 'error' in r:
        return ['Loi: ' + r['error']]
    msgs = []
    tong = (
        '<b>BACKTEST ' + r['symbol'] + '</b>\n'
        'Vol>' + str(r['vol_pct']) + '% | Trend ' + str(r['trend_n']) + 'p | Stop ' + str(r['stop_pct']) + '%\n' +
        SEP + '\n'
        'Von ban dau : ' + f"{r['von_ban_dau']:,.0f}" + 'd\n'
        'Von cuoi    : ' + f"{r['von_cuoi']:,.0f}" + 'd\n'
        'Loi nhuan   : ' + f"{r['lai_lo']:+,.0f}" + 'd (' + f"{r['pct']:+.2f}" + '%)\n'
        'So giao dich: ' + str(r['so_gd']) + '\n' + SEP
    )
    msgs.append(tong)
    chunk = []
    for t in r['trades']:
        status = 'DANG GIU' if t['dang_giu'] else 'BAN'
        label  = 'Hien tai' if t['dang_giu'] else 'Ban     '
        chunk.append(
            '<b>#' + str(t['stt']) + ' ' + status + '</b>\n'
            '  Mua     : ' + t['ngay_mua'] + ' @ ' + f"{t['gia_mua']:,}" + 'd\n'
            '  ' + label + ': ' + t['ngay_ban'] + ' @ ' + f"{t['gia_ban']:,}" + 'd\n'
            '  Dinh/Stop: ' + f"{t['gia_dinh']:,}" + 'd / ' + f"{t['gia_stop']:,}" + 'd\n'
            '  Von vao  : ' + f"{t['von_dau']:,.0f}" + 'd\n'
            '  Von sau  : ' + f"{t['von_sau']:,.0f}" + 'd\n'
            '  Lai/Lo   : ' + f"{t['lai_lo']:+,.0f}" + 'd (' + f"{t['pct']:+.2f}" + '%)'
        )
        if len(chunk) == 4:
            msgs.append('\n\n'.join(chunk))
            chunk = []
    if chunk:
        msgs.append('\n\n'.join(chunk))
    return msgs

# ============================================================
# WEEKLY SCAN - Logic tu weekly_scan_manual_v2.py
# ============================================================
def get_weekly_ohlcv(symbol, days=500):
    """Lay du lieu tuan cho weekly scan (khong gioi han nam bat dau)."""
    Vnstock = get_vnstock_class()
    for attempt in range(3):
        _rate_limiter.acquire()
        try:
            stock = Vnstock(show_log=False).stock(symbol=symbol, source='VCI')
            end   = datetime.now(VN_TZ).strftime('%Y-%m-%d')
            start = (datetime.now(VN_TZ) - timedelta(days=days)).strftime('%Y-%m-%d')
            raw   = stock.quote.history(start=start, end=end, interval='1D')

            if isinstance(raw, dict):
                df = pd.DataFrame(raw['data']) if 'data' in raw else None
            else:
                df = raw

            if df is None or (hasattr(df, 'empty') and df.empty):
                return None

            df.columns = [c.lower() for c in df.columns]
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                df = df.set_index('time')
            elif df.index.dtype != 'datetime64[ns]':
                df.index = pd.to_datetime(df.index)

            df = df.rename(columns={'close': 'Close', 'volume': 'Volume'})
            df = df.sort_index().dropna(subset=['Close', 'Volume'])

            weekly = df.resample('W-FRI').agg({'Close': 'last', 'Volume': 'sum'}).dropna()
            return weekly if len(weekly) >= 30 else None

        except Exception as e:
            err = str(e)
            logging.warning('[get_weekly] %s attempt %d: %s', symbol, attempt+1, err)
            if any(k in err.lower() for k in ['rate limit', '429', 'too many', 'exceeded']):
                time.sleep(15 * (attempt + 1))
                continue
            return None
    return None

def check_weekly_signal(symbol):
    """
    Kiem tra tin hieu tuan:
      1. Volume tuan > 500,000
      2. Volume > 120% MA20(Volume)
      3. RSI(14) cat len SMA(RSI, 14)
    Tra ve dict neu thoa, None neu khong.
    """
    weekly = get_weekly_ohlcv(symbol)
    if weekly is None:
        return None
    try:
        vol     = weekly['Volume'].iloc[-1]
        ma20vol = weekly['Volume'].rolling(20).mean().iloc[-1]
        if vol <= 500_000 or vol <= 1.2 * ma20vol:
            return None

        delta    = weekly['Close'].diff()
        avg_gain = smma(delta.where(delta > 0, 0.0), 14)
        avg_loss = smma((-delta).where(delta < 0, 0.0), 14)
        rsi      = 100 - (100 / (1 + avg_gain / avg_loss))
        signal   = rsi.rolling(14).mean()

        r1, r2 = rsi.iloc[-1], rsi.iloc[-2]
        s1, s2 = signal.iloc[-1], signal.iloc[-2]
        if any(np.isnan(v) for v in [r1, r2, s1, s2]):
            return None

        if r2 <= s2 and r1 > s1:
            return {
                'symbol'  : symbol,
                'week'    : weekly.index[-1].strftime('%Y-%m-%d'),
                'close'   : round(weekly['Close'].iloc[-1], 2),
                'volume'  : int(vol),
                'ma20_vol': int(ma20vol),
                'rsi'     : round(r1, 2),
                'sma_rsi' : round(s1, 2),
            }
    except:
        pass
    return None

# ============================================================
# BUILD SYMBOL LIST - Logic tu build_symbol_list.py
# ============================================================
def get_stock_stats_for_build(symbol, weeks=13):
    """
    Dung rieng cho build list vi can lay gia (don vi nghin dong).
    Tra ve (avg_weekly_vol, last_close) hoac None.
    """
    for attempt in range(3):
        _rate_limiter.acquire()
        try:
            Vnstock = get_vnstock_class()
            stock = Vnstock(show_log=False).stock(symbol=symbol, source='VCI')
            end   = datetime.now(VN_TZ).strftime('%Y-%m-%d')
            start = (datetime.now(VN_TZ) - timedelta(days=weeks * 7 + 14)).strftime('%Y-%m-%d')
            df = stock.quote.history(start=start, end=end, interval='1D')

            if df is None or (hasattr(df, 'empty') and df.empty):
                return None

            df.columns = [c.lower() for c in df.columns]
            if 'volume' not in df.columns or 'close' not in df.columns:
                return None
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                df = df.set_index('time')
            elif df.index.dtype != 'datetime64[ns]':
                df.index = pd.to_datetime(df.index)

            df = df.sort_index()
            last_close     = df['close'].iloc[-1]
            weekly_vol     = df['volume'].resample('W-FRI').sum().dropna()
            if len(weekly_vol) < 4:
                return None
            avg_weekly_vol = weekly_vol.tail(weeks).mean()
            return avg_weekly_vol, last_close
        except Exception as e:
            err = str(e).lower()
            if 'rate limit' in err or '429' in err or 'too many' in err:
                time.sleep(15 * (attempt + 1))
                continue
            return None
    return None

def _get_exchange_symbols():
    """Lay toan bo ma tu 3 san HOSE/HNX/UPCOM, loc <=3 ky tu."""
    from vnstock import Vnstock
    stock = Vnstock().stock(symbol='ACB', source='VCI')
    df    = stock.listing.symbols_by_exchange()
    df    = df[df['exchange'].isin(['HOSE', 'HNX', 'UPCOM'])].copy()
    all_s = df['symbol'].tolist()
    filtered = [s for s in all_s if len(s) <= 3]
    return all_s, filtered

# ============================================================
# HELPER - Pool dong bo
# ============================================================
def _run_pool_sync(fn, symbols, max_workers=20):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fn, sym): sym for sym in symbols}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error('[pool] unhandled: %s', e)

# ============================================================
# HANDLERS - TELEGRAM
# ============================================================

# ---------- /config ----------
async def handle_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    await update.message.reply_text(
        '<b>Tham so hien tai:</b>\n' + SEP + '\n'
        'Volume : > ' + str(CONFIG['vol_pct']) + '% MA20  (10-200)\n'
        'Trend  : ' + str(CONFIG['trend_n']) + ' phien        (1-10)\n'
        'Stop   : ' + str(CONFIG['stop_pct']) + '%            (1-50)\n\n'
        'Thay doi:\n'
        '  /set vol [so]   -> % volume\n'
        '  /set trend [so] -> so phien xu huong\n'
        '  /set stop [so]  -> % trailing stop',
        parse_mode='HTML'
    )

# ---------- /set ----------
async def handle_set(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    args = context.args
    if len(args) != 2:
        await update.message.reply_text(
            'Cu phap: /set [key] [gia tri]\n'
            '  /set vol 150   -> Volume > 150% MA20\n'
            '  /set trend 3   -> SMA tang trong 3 phien\n'
            '  /set stop 15   -> Trailing stop 15%'
        )
        return
    key = args[0].lower()
    try:
        val = float(args[1])
    except ValueError:
        await update.message.reply_text('Gia tri phai la so.')
        return

    if key == 'vol':
        if not (10 <= val <= 200):
            await update.message.reply_text('Vol phai tu 10 den 200 (%).')
            return
        CONFIG['vol_pct'] = int(val)
        await update.message.reply_text('Da cap nhat: Volume > ' + str(int(val)) + '% MA20')
    elif key == 'trend':
        if not (1 <= val <= 10):
            await update.message.reply_text('Trend phai tu 1 den 10.')
            return
        CONFIG['trend_n'] = int(val)
        await update.message.reply_text('Da cap nhat: Trend SMA ' + str(int(val)) + ' phien')
    elif key == 'stop':
        if not (1 <= val <= 50):
            await update.message.reply_text('Stop phai tu 1 den 50 (%).')
            return
        CONFIG['stop_pct'] = val
        await update.message.reply_text('Da cap nhat: Trailing stop ' + str(val) + '%')
    else:
        await update.message.reply_text('Key khong hop le. Dung: vol, trend, stop')

# ---------- Nhap ma co phieu ----------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    text = update.message.text.strip().upper()
    if not (2 <= len(text) <= 5 and text.isalpha()):
        await update.message.reply_text('Nhap ma co phieu (VD: VCB)\n/scanall /weeklyscan /buildlist /config /set')
        return
    await update.message.reply_text(
        '<b>Dang chay backtest ' + text + '...</b>\n'
        'Vol>' + str(CONFIG['vol_pct']) + '% | Trend ' + str(CONFIG['trend_n']) + 'p | Stop ' + str(CONFIG['stop_pct']) + '%',
        parse_mode='HTML'
    )
    result = run_backtest(text)
    for msg in format_result(result):
        await update.message.reply_text(msg, parse_mode='HTML')

# ---------- /scanall ----------
PROGRESS_INTERVAL = 50

async def handle_scanall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    symbols  = get_all_symbols()
    total    = len(symbols)
    chat_id  = update.effective_chat.id
    vol_pct  = CONFIG['vol_pct']
    trend_n  = CONFIG['trend_n']
    stop_pct = CONFIG['stop_pct']

    if total == 0:
        await context.bot.send_message(chat_id=chat_id,
            text='Khong tim thay file vn_stocks_full.txt hoac file rong.\nChay /buildlist truoc.')
        return

    start_time = time.time()
    await context.bot.send_message(
        chat_id=chat_id, parse_mode='HTML',
        text=(
            '<b>🔍 BAT DAU SCAN TOAN BO (BACKTEST)</b>\n' + SEP + '\n'
            'Tong so ma : <b>' + str(total) + '</b>\n'
            'Vol>' + str(vol_pct) + '% | Trend ' + str(trend_n) + 'p | Stop ' + str(stop_pct) + '%\n'
            'Workers    : 20 threads\n'
            'Rate limit : 170 req/phut (Bronze)\n' + SEP + '\n'
            'Cap nhat moi ' + str(PROGRESS_INTERVAL) + ' ma...'
        )
    )

    results        = []
    errors         = []
    done_cnt       = [0]
    lock           = threading.Lock()
    progress_queue = asyncio.Queue()

    def backtest_one(sym):
        r = run_backtest(sym, vol_pct=vol_pct, trend_n=trend_n, stop_pct=stop_pct)
        with lock:
            done_cnt[0] += 1
            n = done_cnt[0]
            if 'error' not in r:
                results.append({'symbol': sym, 'so_gd': r['so_gd'],
                                'pct': r['pct'], 'lai_lo': r['lai_lo']})
            else:
                errors.append(sym)

            if n % PROGRESS_INTERVAL == 0 or n == total:
                elapsed   = time.time() - start_time
                remaining = (elapsed / n) * (total - n) if n > 0 else 0
                speed     = n / elapsed * 60 if elapsed > 0 else 0
                msg = (
                    '📊 <b>TIEN TRINH BACKTEST</b>\n' + SEP + '\n'
                    'Da xong : ' + str(n) + '/' + str(total) +
                    ' (' + f"{n/total*100:.1f}" + '%)\n'
                    '✅ OK   : ' + str(len(results)) + '\n'
                    '❌ Loi  : ' + str(len(errors)) + '\n' + SEP + '\n'
                    '⏱ Da chay  : ' + f"{elapsed:.0f}" + 's\n'
                    '⏳ Con lai  : ~' + f"{remaining:.0f}" + 's\n'
                    '🚀 Toc do  : ' + f"{speed:.0f}" + ' ma/phut'
                )
                asyncio.run_coroutine_threadsafe(
                    progress_queue.put(msg), asyncio.get_event_loop()
                )

    loop = asyncio.get_event_loop()

    async def run_pool():
        await loop.run_in_executor(None, lambda: _run_pool_sync(backtest_one, symbols))
        await progress_queue.put(None)

    async def send_progress():
        while True:
            msg = await progress_queue.get()
            if msg is None:
                break
            try:
                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
            except Exception as e:
                logging.warning('[progress] %s', e)

    await asyncio.gather(run_pool(), send_progress())

    if not results:
        await context.bot.send_message(chat_id=chat_id, text='Khong co du lieu.')
        return

    df_r  = pd.DataFrame(results)
    df_gd = df_r[df_r['so_gd'] > 0]
    n_gd  = len(df_gd)

    if n_gd == 0:
        await context.bot.send_message(chat_id=chat_id, text='Khong co ma nao co giao dich.')
        return

    n_loi   = len(df_gd[df_gd['pct'] > 0])
    n_hoa   = len(df_gd[df_gd['pct'] == 0])
    n_lo    = len(df_gd[df_gd['pct'] < 0])
    n_ko_gd = len(df_r[df_r['so_gd'] == 0])
    tong_ll = df_gd['lai_lo'].sum()
    tb_pct  = df_gd['pct'].mean()
    tong_loi  = df_gd.loc[df_gd['lai_lo'] > 0, 'lai_lo'].sum()
    tong_lo_v = df_gd.loc[df_gd['lai_lo'] < 0, 'lai_lo'].sum()
    pf_str = (f"{tong_loi / abs(tong_lo_v):.2f}" if tong_lo_v < 0
              else 'N/A (khong co ma lo)')
    total_elapsed = time.time() - start_time

    await context.bot.send_message(chat_id=chat_id, parse_mode='HTML', text=(
        '<b>✅ KET QUA SCAN TOAN BO (BACKTEST)</b>\n'
        'Vol>' + str(vol_pct) + '% | Trend ' + str(trend_n) + 'p | Stop ' + str(stop_pct) + '%\n' + SEP + '\n'
        'Tong ma test : ' + str(len(df_r)) + '\n'
        'Co GD        : ' + str(n_gd) + '\n'
        'Khong co GD  : ' + str(n_ko_gd) + '\n'
        'Loi DL       : ' + str(len(errors)) + '\n' + SEP + '\n'
        'Trong ' + str(n_gd) + ' ma co GD:\n'
        '  Loi: ' + str(n_loi) + ' (' + str(round(n_loi/n_gd*100, 1)) + '%)\n'
        '  Hoa: ' + str(n_hoa) + ' (' + str(round(n_hoa/n_gd*100, 1)) + '%)\n'
        '  Lo : ' + str(n_lo)  + ' (' + str(round(n_lo /n_gd*100, 1)) + '%)\n' + SEP + '\n'
        'Tong lai/lo  : ' + f"{tong_ll:+,.0f}" + 'd\n'
        'TB/ma        : ' + f"{tong_ll/n_gd:+,.0f}" + 'd (' + f"{tb_pct:+.2f}" + '%)\n'
        'Profit Factor: ' + pf_str + '\n'
        '(Moi ma von 50tr)\n' + SEP + '\n'
        '⏱ Tong thoi gian: ' + f"{total_elapsed:.0f}" + 's\n'
        '🚀 Toc do TB    : ' + f"{len(df_r)/total_elapsed*60:.0f}" + ' ma/phut'
    ))

    top_loi = df_gd.nlargest(5, 'pct')[['symbol', 'pct', 'lai_lo', 'so_gd']]
    top_lo  = df_gd.nsmallest(5, 'pct')[['symbol', 'pct', 'lai_lo', 'so_gd']]

    msg_loi = '<b>🏆 TOP 5 LOI:</b>\n'
    for _, row in top_loi.iterrows():
        msg_loi += (row['symbol'] + ': ' + f"{row['pct']:+.2f}" + '% | '
                    + f"{row['lai_lo']:+,.0f}" + 'd | ' + str(int(row['so_gd'])) + ' GD\n')
    await context.bot.send_message(chat_id=chat_id, text=msg_loi, parse_mode='HTML')

    msg_lo = '<b>📉 TOP 5 LO:</b>\n'
    for _, row in top_lo.iterrows():
        msg_lo += (row['symbol'] + ': ' + f"{row['pct']:+.2f}" + '% | '
                   + f"{row['lai_lo']:+,.0f}" + 'd | ' + str(int(row['so_gd'])) + ' GD\n')
    await context.bot.send_message(chat_id=chat_id, text=msg_lo, parse_mode='HTML')

    timestamp = datetime.now(VN_TZ).strftime('%Y%m%d_%H%M')
    csv_name  = 'ket_qua_scanall_' + timestamp + '.csv'
    df_r.sort_values('pct', ascending=False).to_csv(csv_name, index=False)
    await context.bot.send_message(chat_id=chat_id,
        text='📁 Da luu CSV: ' + csv_name)

# ---------- /weeklyscan ----------
WEEKLY_PROGRESS_INTERVAL = 100

async def handle_weeklyscan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    symbols = get_all_symbols()
    total   = len(symbols)
    chat_id = update.effective_chat.id

    if total == 0:
        await context.bot.send_message(chat_id=chat_id,
            text='Khong tim thay file vn_stocks_full.txt hoac file rong.\nChay /buildlist truoc.')
        return

    start_time = time.time()
    await context.bot.send_message(
        chat_id=chat_id, parse_mode='HTML',
        text=(
            '<b>📅 BAT DAU WEEKLY SCAN</b>\n' + SEP + '\n'
            'Tong so ma : <b>' + str(total) + '</b>\n'
            'Dieu kien  :\n'
            '  1. Volume tuan &gt; 500,000\n'
            '  2. Volume &gt; 120% MA20\n'
            '  3. RSI(14) cat len SMA(RSI,14)\n'
            'Workers    : 20 threads\n'
            'Rate limit : 170 req/phut (Bronze)\n' + SEP + '\n'
            'Cap nhat moi ' + str(WEEKLY_PROGRESS_INTERVAL) + ' ma...'
        )
    )

    results        = []
    done_cnt       = [0]
    lock           = threading.Lock()
    progress_queue = asyncio.Queue()

    def scan_one(sym):
        res = check_weekly_signal(sym)
        with lock:
            done_cnt[0] += 1
            n = done_cnt[0]
            if res:
                results.append(res)
            if n % WEEKLY_PROGRESS_INTERVAL == 0 or n == total:
                elapsed  = time.time() - start_time
                remain   = (elapsed / n) * (total - n) if n > 0 else 0
                speed    = n / elapsed * 60 if elapsed > 0 else 0
                msg = (
                    '📊 <b>TIEN TRINH WEEKLY SCAN</b>\n' + SEP + '\n'
                    'Da xong : ' + str(n) + '/' + str(total) +
                    ' (' + f"{n/total*100:.1f}" + '%)\n'
                    '✅ Tim thay: ' + str(len(results)) + ' ma\n' + SEP + '\n'
                    '⏱ ' + f"{elapsed:.0f}" + 's | Con ~' + f"{remain:.0f}" + 's\n'
                    '🚀 ' + f"{speed:.0f}" + ' ma/phut'
                )
                asyncio.run_coroutine_threadsafe(
                    progress_queue.put(msg), asyncio.get_event_loop()
                )

    loop = asyncio.get_event_loop()

    async def run_pool_w():
        await loop.run_in_executor(None, lambda: _run_pool_sync(scan_one, symbols))
        await progress_queue.put(None)

    async def send_progress_w():
        while True:
            msg = await progress_queue.get()
            if msg is None:
                break
            try:
                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
            except Exception as e:
                logging.warning('[weekly_progress] %s', e)

    await asyncio.gather(run_pool_w(), send_progress_w())

    total_elapsed = time.time() - start_time
    results.sort(key=lambda x: x['symbol'])

    if results:
        msg = (
            '<b>📊 KET QUA WEEKLY SCAN</b>\n'
            '✅ Thoa dieu kien: <b>' + str(len(results)) + '/' + str(total) + '</b> ma\n'
            '⏱ ' + f"{total_elapsed:.0f}" + 's | ' + f"{total/total_elapsed*60:.0f}" + ' ma/phut\n'
            '🕐 ' + now_vn() + '\n\n'
        )
        for r in results[:20]:
            msg += (
                f"🔹 <b>{r['symbol']}</b> (tuan {r['week']}) — {r['close']:,}d"
                f" — Vol {r['volume']:,} (MA20: {r['ma20_vol']:,})"
                f" — RSI {r['rsi']} / SMA {r['sma_rsi']}\n"
            )
        if len(results) > 20:
            msg += f"\n...va {len(results)-20} ma khac (xem file CSV)"
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

        csv_name = 'ket_qua_weekly_' + datetime.now(VN_TZ).strftime('%Y%m%d_%H%M') + '.csv'
        pd.DataFrame(results).to_csv(csv_name, index=False)
        await context.bot.send_message(chat_id=chat_id,
            text='📁 Da luu CSV: ' + csv_name)
    else:
        await context.bot.send_message(
            chat_id=chat_id, parse_mode='HTML',
            text=(
                '😔 Khong tim thay ma nao thoa dieu kien.\n'
                'Tong quet: ' + str(total) + '\n'
                '⏱ ' + f"{total_elapsed:.0f}" + 's\n'
                '🕐 ' + now_vn()
            )
        )

# ---------- /buildlist ----------
BUILD_PROGRESS_INTERVAL = 60

async def handle_buildlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    chat_id = update.effective_chat.id

    await context.bot.send_message(
        chat_id=chat_id, parse_mode='HTML',
        text=(
            '<b>🔨 BAT DAU BUILD SYMBOL LIST</b>\n' + SEP + '\n'
            'Dang lay danh sach ma tu 3 san (HOSE/HNX/UPCOM)...\n'
            'Dieu kien loc:\n'
            '  - Gia dong cua >= ' + str(GIA_TOI_THIEU) + ' (>=' + str(int(GIA_TOI_THIEU*1000)) + 'd)\n'
            '  - Volume tuan trung binh >= ' + str(VOL_TUAN_TOI_THIEU) + '\n'
            'Uoc tinh: ~10-15 phut...'
        )
    )

    loop    = asyncio.get_event_loop()
    chat_id = chat_id

    # Lay danh sach ma (blocking) trong executor
    try:
        all_syms_raw, filtered = await loop.run_in_executor(None, _get_exchange_symbols)
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id,
            text='Loi khi lay danh sach ma: ' + str(e))
        return

    total = len(filtered)
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            'Tren san: ' + str(len(all_syms_raw)) + ' ma\n'
            'Sau loc <=3 ky tu: ' + str(total) + ' ma\n'
            'Bat dau kiem tra tung ma...'
        )
    )

    passed      = []
    removed_vol = []
    removed_gia = []
    no_data     = []
    done_cnt    = [0]
    lock        = threading.Lock()
    progress_queue = asyncio.Queue()
    start_time  = time.time()

    def check_one(sym):
        stats = get_stock_stats_for_build(sym)
        with lock:
            done_cnt[0] += 1
            n = done_cnt[0]
            if stats is None:
                no_data.append(sym)
                passed.append(sym)          # Giu lai neu khong co data
            else:
                avg_vol, last_close = stats
                if last_close < GIA_TOI_THIEU:
                    removed_gia.append(sym)
                elif avg_vol < VOL_TUAN_TOI_THIEU:
                    removed_vol.append(sym)
                else:
                    passed.append(sym)

            if n % BUILD_PROGRESS_INTERVAL == 0 or n == total:
                elapsed = time.time() - start_time
                remain  = (elapsed / n) * (total - n) if n > 0 else 0
                msg = (
                    '🔨 <b>TIEN TRINH BUILD LIST</b>\n' + SEP + '\n'
                    'Da kiem tra: ' + str(n) + '/' + str(total) +
                    ' (' + f"{n/total*100:.1f}" + '%)\n'
                    '✅ Giu: ' + str(len(passed)) + '\n'
                    '❌ Loai gia: ' + str(len(removed_gia)) + '\n'
                    '❌ Loai vol: ' + str(len(removed_vol)) + '\n' + SEP + '\n'
                    '⏱ ' + f"{elapsed:.0f}" + 's | Con ~' + f"{remain:.0f}" + 's'
                )
                asyncio.run_coroutine_threadsafe(
                    progress_queue.put(msg), asyncio.get_event_loop()
                )

    async def run_pool_b():
        await loop.run_in_executor(None, lambda: _run_pool_sync(check_one, filtered, max_workers=3))
        await progress_queue.put(None)

    async def send_progress_b():
        while True:
            msg = await progress_queue.get()
            if msg is None:
                break
            try:
                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
            except Exception as e:
                logging.warning('[build_progress] %s', e)

    await asyncio.gather(run_pool_b(), send_progress_b())

    passed.sort()
    with open('vn_stocks_full.txt', 'w', encoding='utf-8') as f:
        for sym in passed:
            f.write(sym + '\n')

    total_elapsed = time.time() - start_time
    await context.bot.send_message(
        chat_id=chat_id, parse_mode='HTML',
        text=(
            '<b>✅ BUILD LIST HOAN THANH</b>\n' + SEP + '\n'
            'Tong ma ban dau        : ' + str(total) + '\n'
            'Ma duoc giu            : <b>' + str(len(passed)) + '</b>\n'
            '  - Thoa ca 2 dieu kien: ' + str(len(passed) - len(no_data)) + '\n'
            '  - Khong co data (giu) : ' + str(len(no_data)) + '\n'
            'Loai gia < ' + str(int(GIA_TOI_THIEU*1000)) + 'd        : ' + str(len(removed_gia)) + '\n'
            'Loai vol tuan < 500K   : ' + str(len(removed_vol)) + '\n' + SEP + '\n'
            '⏱ Tong thoi gian: ' + f"{total_elapsed:.0f}" + 's\n'
            '🕐 ' + now_vn() + '\n'
            '💾 Da luu: vn_stocks_full.txt'
        )
    )

# ============================================================
# TU TAT
# ============================================================
async def watchdog(app):
    while True:
        await asyncio.sleep(60)
        if time.time() - last_activity[0] >= 1800:
            await app.bot.send_message(
                chat_id=CHAT_ID,
                text='Bot tu tat sau 30 phut khong hoat dong.'
            )
            await app.stop()
            break

async def post_init(app):
    update_activity()
    await app.bot.send_message(
        chat_id=CHAT_ID, parse_mode='HTML',
        text=(
            '<b>🤖 ALL-IN-ONE BOT san sang!</b>\n' + SEP + '\n'
            '<b>Lenh:</b>\n'
            '  [MA]          : backtest 1 ma\n'
            '  /scanall      : backtest toan bo\n'
            '  /weeklyscan   : scan tin hieu tuan\n'
            '  /buildlist    : xay dung lai danh sach ma\n'
            '  /config       : xem tham so hien tai\n\n'
            '<b>Chinh tham so backtest:</b>\n'
            '  /set vol [10-200]   : % volume\n'
            '  /set trend [1-10]  : so phien xu huong\n'
            '  /set stop [1-50]   : % trailing stop\n\n'
            'Mac dinh: Vol>' + str(CONFIG['vol_pct']) + '% | Trend ' +
            str(CONFIG['trend_n']) + 'p | Stop ' + str(CONFIG['stop_pct']) + '%\n'
            'Von backtest: 50tr/ma | Tu nam 2023\n'
            '⚡ Rate limit: 170 req/phut | 20 workers\n'
            '💤 Tu tat sau 30 phut khong hoat dong'
        )
    )
    asyncio.create_task(watchdog(app))

# ============================================================
# MAIN
# ============================================================
def main():
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler('scanall',     handle_scanall))
    app.add_handler(CommandHandler('weeklyscan',  handle_weeklyscan))
    app.add_handler(CommandHandler('buildlist',   handle_buildlist))
    app.add_handler(CommandHandler('config',      handle_config))
    app.add_handler(CommandHandler('set',         handle_set))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print('Bot dang chay...')
    app.run_polling()

if __name__ == '__main__':
    main()
