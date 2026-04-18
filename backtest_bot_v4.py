# ============================================================
# ALL-IN-ONE BOT v4 - BACKTEST + WEEKLY SCAN + BUILD LIST
# Telegram Commands:
#   [MÃ]             : backtest 1 mã
#   /scanall         : backtest toàn bộ
#   /weeklyscan      : scan tín hiệu tuần (RSI cắt SMA)
#   /buildlist       : xây dựng lại danh sách mã
#   /config          : xem tham số hiện tại
#   /set vol [số]    : volume % MA20        (10-200, mặc định 120)
#   /set trend [số]  : số phiên xu hướng    (1-10,  mặc định 1)
#   /set stop [số]   : trailing stop %      (1-50,  mặc định 10)
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
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters, ContextTypes

TOKEN   = '8578016275:AAGvL6SoOO3Yifqner8EcynwKt7OKgwl_J0'
CHAT_ID = '7000478479'
VN_TZ   = timezone(timedelta(hours=7))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
SEP = '─' * 35

CONFIG = {'vol_pct': 120, 'trend_n': 1, 'stop_pct': 10}
GIA_TOI_THIEU, VOL_TUAN_TOI_THIEU = 2.0, 500_000

now_vn = lambda: datetime.now(VN_TZ).strftime('%Y-%m-%d %H:%M')

# ============================================================
# RATE LIMITER - Token Bucket 170 req/phút
# ============================================================
class RateLimiter:
    def __init__(self, max_calls=170, period=60.0):
        self.max_calls, self.period = max_calls, period
        self._lock, self._calls = threading.Lock(), []

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

_rate_limiter = RateLimiter()

# ============================================================
# VNSTOCK - Ưu tiên vnstock_data, fallback sang vnstock
# ============================================================
_Vnstock, _vnstock_lock = None, threading.Lock()

def get_vnstock_class():
    global _Vnstock
    if _Vnstock is None:
        with _vnstock_lock:
            if _Vnstock is None:
                try:
                    from vnstock_data import Vnstock as VnstockData
                    _Vnstock = VnstockData
                except ImportError:
                    from vnstock import Vnstock
                    _Vnstock = Vnstock
    return _Vnstock

# ============================================================
# ĐỌC DANH SÁCH MÃ
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
# LẤY DỮ LIỆU - Với retry thông minh
# ============================================================
def _fetch_df(symbol, source, start_date='2022-01-01'):
    Vnstock = get_vnstock_class()
    _rate_limiter.acquire()
    stock = Vnstock(show_log=False).stock(symbol=symbol, source=source)
    end   = datetime.now(VN_TZ).strftime('%Y-%m-%d')
    raw   = stock.quote.history(start=start_date, end=end, interval='1D')

    df = pd.DataFrame(raw['data']) if isinstance(raw, dict) and 'data' in raw else raw
    if df is None or (hasattr(df, 'empty') and df.empty):
        return None

    df.columns = [c.lower() for c in df.columns]
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])
        df = df.set_index('time')
    elif df.index.dtype != 'datetime64[ns]':
        df.index = pd.to_datetime(df.index)

    rename_map = {'close': 'Close', 'high': 'High', 'low': 'Low', 'volume': 'Volume'}
    df = df.rename(columns=rename_map).sort_index()
    req_cols = ['Close', 'Volume']
    if not all(c in df.columns for c in req_cols):
        return None
    return df.dropna(subset=req_cols) if not df.empty else None

def get_data(symbol, start_date='2022-01-01'):
    """Lấy dữ liệu ngày + tuần cho backtest."""
    last_errors = []
    for source in ('VCI', 'MSN', 'KBS'):
        try:
            df = _fetch_df(symbol, source, start_date)
            if df is not None:
                weekly = df.resample('W-FRI').agg({'Close': 'last', 'Volume': 'sum'}).dropna()
                return df, weekly
            last_errors.append(f"{source}:empty")
        except Exception as e:
            err = str(e)
            logging.warning('[get_data] %s/%s: %s', symbol, source, err[:120])
            last_errors.append(f"{source}:{err[:120]}")
            if any(k in err.lower() for k in ['rate limit', '429', 'too many', 'exceeded']):
                time.sleep(30)
                try:
                    df2 = _fetch_df(symbol, source, start_date)
                    if df2 is not None:
                        weekly = df2.resample('W-FRI').agg({'Close': 'last', 'Volume': 'sum'}).dropna()
                        return df2, weekly
                except:
                    pass
    logging.warning('[get_data] %s failed: %s', symbol, ' | '.join(last_errors))
    return None, last_errors

# ============================================================
# CHỈ BÁO KỸ THUẬT
# ============================================================
def smma(series, period):
    """Smoothed Moving Average - chuẩn TradingView."""
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
        result[i] = result[i-1] if np.isnan(values[i]) else (result[i-1] * (period-1) + values[i]) / period
    return pd.Series(result, index=series.index)

def calc_indicators(weekly):
    """Tính RSI và SMA(RSI) trên khung tuần."""
    df = weekly.copy()
    df['ma20_vol'] = df['Volume'].rolling(20).mean()
    delta = df['Close'].diff()
    df['rsi'] = 100 - (100 / (1 + smma(delta.where(delta > 0, 0.0), 14) / 
                                    smma((-delta).where(delta < 0, 0.0), 14)))
    df['sma_rsi'] = df['rsi'].rolling(14).mean()
    return df

def check_buy_signal(df_w, i, vol_pct, trend_n):
    """Kiểm tra điều kiện mua tại cột tuần i."""
    if i < max(1, trend_n):
        return False
    row, prev = df_w.iloc[i], df_w.iloc[i - 1]
    req = ['Volume', 'ma20_vol', 'rsi', 'sma_rsi']
    if any(pd.isna(row[c]) for c in req) or pd.isna(prev['rsi']) or pd.isna(prev['sma_rsi']):
        return False
    
    dk1 = row['Volume'] > (vol_pct / 100) * row['ma20_vol']
    dk2 = prev['rsi'] <= prev['sma_rsi'] and row['rsi'] > row['sma_rsi']
    dk3 = all(not (pd.isna(df_w.iloc[i-k]['sma_rsi']) or pd.isna(df_w.iloc[i-k-1]['sma_rsi']) or
                   df_w.iloc[i-k]['sma_rsi'] < df_w.iloc[i-k-1]['sma_rsi'])
              for k in range(trend_n) if i - k - 1 >= 0)
    return dk1 and dk2 and dk3

# ============================================================
# BACKTEST 1 MÃ
# ============================================================
def run_backtest(symbol, initial_capital=50_000_000, vol_pct=None, trend_n=None, stop_pct=None):
    vol_pct  = vol_pct  or CONFIG['vol_pct']
    trend_n  = trend_n  or CONFIG['trend_n']
    stop_pct = stop_pct or CONFIG['stop_pct']
    stop_mult = 1 - stop_pct / 100

    daily, weekly = get_data(symbol)
    if daily is None:
        err = ' | '.join(weekly) if isinstance(weekly, list) else 'unknown'
        return {'error': f'Không lấy được dữ liệu cho mã {symbol} | {err}'}

    df_w = calc_indicators(weekly)
    df_w_bt = df_w[df_w.index >= '2023-01-01']
    if df_w_bt.empty:
        return {'error': 'Không có dữ liệu từ 2023'}

    daily_bt = daily[daily.index >= '2023-01-01'].copy()
    daily_list = list(daily_bt.iterrows())

    capital, trades, position, day_idx = initial_capital, [], None, 0

    def do_sell(buy_date, buy_price, sell_date, sell_price, peak, von_vao):
        pct = (sell_price - buy_price) / buy_price * 100
        cap = von_vao * (1 + pct / 100)
        return {
            'stt': len(trades) + 1, 'loai': 'Bán',
            'ngay_mua': buy_date.strftime('%Y-%m-%d'), 'gia_mua': round(buy_price, 2),
            'ngay_ban': sell_date.strftime('%Y-%m-%d'), 'gia_ban': round(sell_price, 2),
            'gia_dinh': round(peak, 2), 'gia_stop': round(peak * stop_mult, 2),
            'von_dau': round(von_vao, 0), 'gia_tri': round(cap, 0),
            'pct': round(pct, 2), 'lai_lo': round(cap - von_vao, 0),
            'von_sau': round(cap, 0), 'dang_giu': False
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
                    t, capital = do_sell(position['buy_date'], position['buy_price'],
                                        day_ts, stop_price, position['peak'], position['cost'])
                    trades.append(t)
                    position, sold = None, True
                    day_idx += 1
                    break
                if day_row['High'] > position['peak']:
                    position['peak'] = day_row['High']
                day_idx += 1

            if sold and check_buy_signal(df_w, global_wi, vol_pct, trend_n):
                bp = df_w_bt.iloc[wi]['Close']
                position = {'buy_date': week_end, 'buy_price': bp, 'shares': capital/bp,
                           'cost': capital, 'peak': bp}
            continue

        while day_idx < len(daily_list) and daily_list[day_idx][0] <= week_end:
            day_idx += 1
        if check_buy_signal(df_w, global_wi, vol_pct, trend_n):
            bp = df_w_bt.iloc[wi]['Close']
            position = {'buy_date': week_end, 'buy_price': bp, 'shares': capital/bp,
                       'cost': capital, 'peak': bp}

    if position is not None:
        while day_idx < len(daily_list):
            day_ts, day_row = daily_list[day_idx]
            if day_ts <= position['buy_date']:
                day_idx += 1
                continue
            stop_price = position['peak'] * stop_mult
            if day_row['Low'] <= stop_price:
                t, capital = do_sell(position['buy_date'], position['buy_price'],
                                    day_ts, stop_price, position['peak'], position['cost'])
                trades.append(t)
                position = None
                break
            if day_row['High'] > position['peak']:
                position['peak'] = day_row['High']
            day_idx += 1

        if position is not None:
            last_ts, last_row = daily_list[-1]
            lc = last_row['Close']
            pct = (lc - position['buy_price']) / position['buy_price'] * 100
            current = position['cost'] * (1 + pct / 100)
            capital = current
            trades.append({
                'stt': len(trades)+1, 'loai': 'Đang giữ',
                'ngay_mua': position['buy_date'].strftime('%Y-%m-%d'),
                'gia_mua': round(position['buy_price'], 2),
                'ngay_ban': last_ts.strftime('%Y-%m-%d'),
                'gia_ban': round(lc, 2),
                'gia_dinh': round(position['peak'], 2),
                'gia_stop': round(position['peak'] * stop_mult, 2),
                'von_dau': round(position['cost'], 0),
                'gia_tri': round(current, 0),
                'pct': round(pct, 2),
                'lai_lo': round(current - position['cost'], 0),
                'von_sau': round(current, 0),
                'dang_giu': True
            })

    return {
        'symbol': symbol.upper(), 'von_ban_dau': initial_capital,
        'von_cuoi': round(capital, 0), 'lai_lo': round(capital - initial_capital, 0),
        'pct': round((capital / initial_capital - 1) * 100, 2),
        'so_gd': len(trades), 'trades': trades,
        'vol_pct': vol_pct, 'trend_n': trend_n, 'stop_pct': stop_pct
    }

# ============================================================
# ĐỊNH DẠNG KÊT QUẢ
# ============================================================
def format_result(r):
    if 'error' in r:
        return [f"❌ Lỗi: {r['error']}"]
    
    msgs = [(
        f"<b>BACKTEST {r['symbol']}</b>\n"
        f"Vol>{r['vol_pct']}% | Trend {r['trend_n']}p | Stop {r['stop_pct']}%\n{SEP}\n"
        f"Vốn ban đầu : {r['von_ban_dau']:,.0f}đ\n"
        f"Vốn cuối    : {r['von_cuoi']:,.0f}đ\n"
        f"Lợi nhuận   : {r['lai_lo']:+,.0f}đ ({r['pct']:+.2f}%)\n"
        f"Số giao dịch: {r['so_gd']}\n{SEP}"
    )]
    
    chunk = []
    for t in r['trades']:
        status = 'ĐANG GIỮ' if t['dang_giu'] else 'BÁN'
        label = 'Hiện tại' if t['dang_giu'] else 'Bán     '
        chunk.append(
            f"<b>#{t['stt']} {status}</b>\n"
            f"  Mua     : {t['ngay_mua']} @ {t['gia_mua']:,}đ\n"
            f"  {label}: {t['ngay_ban']} @ {t['gia_ban']:,}đ\n"
            f"  Đỉnh/Stop: {t['gia_dinh']:,}đ / {t['gia_stop']:,}đ\n"
            f"  Vốn vào  : {t['von_dau']:,.0f}đ\n"
            f"  Vốn sau  : {t['von_sau']:,.0f}đ\n"
            f"  Lãi/Lỗ   : {t['lai_lo']:+,.0f}đ ({t['pct']:+.2f}%)"
        )
        if len(chunk) == 4:
            msgs.append('\n\n'.join(chunk))
            chunk = []
    if chunk:
        msgs.append('\n\n'.join(chunk))
    return msgs

# ============================================================
# WEEKLY SCAN - Kiểm tra tín hiệu tuần
# ============================================================
def check_weekly_signal(symbol):
    """Kiểm tra: Vol>500K, RSI cắt lên SMA."""
    try:
        start_date = (datetime.now(VN_TZ) - timedelta(days=500)).strftime('%Y-%m-%d')
        result = get_data(symbol, start_date=start_date)
        
        if result[0] is None:
            return None
        
        daily, weekly = result
        if weekly is None or len(weekly) < 30:
            return None
        
        vol = weekly['Volume'].iloc[-1]
        if vol <= 500_000:
            return None

        df_w = calc_indicators(weekly)
        r1, r2 = df_w['rsi'].iloc[-1], df_w['rsi'].iloc[-2]
        s1, s2 = df_w['sma_rsi'].iloc[-1], df_w['sma_rsi'].iloc[-2]
        
        if any(np.isnan(v) for v in [r1, r2, s1, s2]):
            return None
        
        if r2 <= s2 and r1 > s1:
            logging.info('✅ [%s] Vol=%.0fK | RSI: %.1f→%.1f | SMA: %.1f→%.1f', 
                        symbol, vol/1000, r2, r1, s2, s1)
            return {
                'symbol': symbol, 'week': weekly.index[-1].strftime('%Y-%m-%d'),
                'close': round(weekly['Close'].iloc[-1], 2), 'volume': int(vol),
                'rsi': round(r1, 2), 'sma_rsi': round(s1, 2)
            }
    except Exception as e:
        logging.warning('❌ [%s] %s', symbol, str(e)[:80])
    return None

# ============================================================
# BUILD SYMBOL LIST
# ============================================================
def get_stock_stats_for_build(symbol, weeks=13):
    """Lấy avg_weekly_vol và last_close để lọc mã."""
    for attempt in range(3):
        _rate_limiter.acquire()
        try:
            Vnstock = get_vnstock_class()
            stock = Vnstock(show_log=False).stock(symbol=symbol, source='VCI')
            end = datetime.now(VN_TZ).strftime('%Y-%m-%d')
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
            weekly_vol = df['volume'].resample('W-FRI').sum().dropna()
            if len(weekly_vol) < 4:
                return None
            return weekly_vol.tail(weeks).mean(), df['close'].iloc[-1]
        except Exception as e:
            if any(k in str(e).lower() for k in ['rate limit', '429', 'too many']):
                time.sleep(15 * (attempt + 1))
                continue
            return None
    return None

def get_exchange_symbols():
    """Lấy toàn bộ mã từ 3 sàn, lọc <=3 ký tự."""
    from vnstock import Vnstock
    stock = Vnstock().stock(symbol='ACB', source='VCI')
    df = stock.listing.symbols_by_exchange()
    df = df[df['exchange'].isin(['HOSE', 'HNX', 'UPCOM'])].copy()
    all_s = df['symbol'].tolist()
    return all_s, [s for s in all_s if len(s) <= 3]

# ============================================================
# HELPER - Pool đồng bộ
# ============================================================
def run_pool_sync(fn, symbols, max_workers=20):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fn, sym): sym for sym in symbols}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error('[pool] %s', e)

# ============================================================
# PROGRESS REPORTER - Dùng chung cho tất cả lệnh scan
# ============================================================
async def report_progress(chat_id, context, queue, task_name):
    """Nhận và gửi tin nhắn tiến trình từ queue."""
    while True:
        msg = await queue.get()
        if msg is None:
            break
        try:
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
        except Exception as e:
            logging.warning('[%s_progress] %s', task_name, e)

# ============================================================
# HANDLERS - TELEGRAM
# ============================================================
async def handle_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"<b>Tham số hiện tại:</b>\n{SEP}\n"
        f"Volume : > {CONFIG['vol_pct']}% MA20  (10-200)\n"
        f"Trend  : {CONFIG['trend_n']} phiên        (1-10)\n"
        f"Stop   : {CONFIG['stop_pct']}%            (1-50)\n\n"
        f"Thay đổi:\n"
        f"  /set vol [số]   → % volume\n"
        f"  /set trend [số] → số phiên xu hướng\n"
        f"  /set stop [số]  → % trailing stop",
        parse_mode='HTML'
    )

async def handle_set(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) != 2:
        await update.message.reply_text(
            'Cú pháp: /set [key] [giá trị]\n'
            '  /set vol 150   → Volume > 150% MA20\n'
            '  /set trend 3   → SMA tăng trong 3 phiên\n'
            '  /set stop 15   → Trailing stop 15%'
        )
        return
    
    key = args[0].lower()
    try:
        val = float(args[1])
    except ValueError:
        await update.message.reply_text('Giá trị phải là số.')
        return

    ranges = {'vol': (10, 200), 'trend': (1, 10), 'stop': (1, 50)}
    if key not in ranges:
        await update.message.reply_text('Key không hợp lệ. Dùng: vol, trend, stop')
        return
    
    if not (ranges[key][0] <= val <= ranges[key][1]):
        await update.message.reply_text(
            f'{key.capitalize()} phải từ {ranges[key][0]} đến {ranges[key][1]}.'
        )
        return
    
    CONFIG[f'{key}_pct' if key in ['vol', 'stop'] else f'{key}_n'] = int(val) if key != 'stop' else val
    msgs = {
        'vol': f'Đã cập nhật: Volume > {int(val)}% MA20',
        'trend': f'Đã cập nhật: Trend SMA {int(val)} phiên',
        'stop': f'Đã cập nhật: Trailing stop {val}%'
    }
    await update.message.reply_text(msgs[key])

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().upper()
    if not (2 <= len(text) <= 5 and text.isalpha()):
        await update.message.reply_text(
            'Nhập mã cổ phiếu (VD: VCB)\n/scanall /weeklyscan /buildlist /config /set'
        )
        return
    
    await update.message.reply_text(
        f"<b>Đang chạy backtest {text}...</b>\n"
        f"Vol>{CONFIG['vol_pct']}% | Trend {CONFIG['trend_n']}p | Stop {CONFIG['stop_pct']}%",
        parse_mode='HTML'
    )
    result = run_backtest(text)
    for msg in format_result(result):
        await update.message.reply_text(msg, parse_mode='HTML')

# ---------- /scanall ----------
PROGRESS_INTERVAL = 50

async def handle_scanall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    symbols = get_all_symbols()
    total = len(symbols)
    chat_id = update.effective_chat.id
    
    if total == 0:
        await context.bot.send_message(
            chat_id=chat_id,
            text='❌ Không tìm thấy file vn_stocks_full.txt hoặc file rỗng.\nChạy /buildlist trước.'
        )
        return

    vol_pct, trend_n, stop_pct = CONFIG['vol_pct'], CONFIG['trend_n'], CONFIG['stop_pct']
    start_time = time.time()
    
    await context.bot.send_message(
        chat_id=chat_id, parse_mode='HTML',
        text=(
            f"<b>🔍 BẮT ĐẦU SCAN TOÀN BỘ (BACKTEST)</b>\n{SEP}\n"
            f"Tổng số mã : <b>{total}</b>\n"
            f"Vol>{vol_pct}% | Trend {trend_n}p | Stop {stop_pct}%\n"
            f"Workers    : 20 threads\n"
            f"Rate limit : 170 req/phút (Bronze)\n{SEP}\n"
            f"Cập nhật mỗi {PROGRESS_INTERVAL} mã..."
        )
    )

    results, errors, done_cnt = [], [], [0]
    lock, progress_queue = threading.Lock(), asyncio.Queue()

    def backtest_one(sym):
        r = run_backtest(sym, vol_pct=vol_pct, trend_n=trend_n, stop_pct=stop_pct)
        with lock:
            done_cnt[0] += 1
            n = done_cnt[0]
            if 'error' not in r:
                results.append({'symbol': sym, 'so_gd': r['so_gd'], 'pct': r['pct'], 'lai_lo': r['lai_lo']})
            else:
                errors.append(sym)

            if n % PROGRESS_INTERVAL == 0 or n == total:
                elapsed = time.time() - start_time
                remaining = (elapsed / n) * (total - n) if n > 0 else 0
                speed = n / elapsed * 60 if elapsed > 0 else 0
                msg = (
                    f"📊 <b>TIẾN TRÌNH BACKTEST</b>\n{SEP}\n"
                    f"Đã xong : {n}/{total} ({n/total*100:.1f}%)\n"
                    f"✅ OK   : {len(results)}\n"
                    f"❌ Lỗi  : {len(errors)}\n{SEP}\n"
                    f"⏱ Đã chạy  : {elapsed:.0f}s\n"
                    f"⏳ Còn lại  : ~{remaining:.0f}s\n"
                    f"🚀 Tốc độ  : {speed:.0f} mã/phút"
                )
                asyncio.run_coroutine_threadsafe(progress_queue.put(msg), asyncio.get_event_loop())

    loop = asyncio.get_event_loop()
    await asyncio.gather(
        loop.run_in_executor(None, lambda: run_pool_sync(backtest_one, symbols)),
        report_progress(chat_id, context, progress_queue, 'backtest')
    )
    await progress_queue.put(None)

    if not results:
        await context.bot.send_message(chat_id=chat_id, text='❌ Không có dữ liệu.')
        return

    df_r = pd.DataFrame(results)
    df_gd = df_r[df_r['so_gd'] > 0]
    n_gd = len(df_gd)

    if n_gd == 0:
        await context.bot.send_message(chat_id=chat_id, text='❌ Không có mã nào có giao dịch.')
        return

    n_loi, n_hoa, n_lo = len(df_gd[df_gd['pct'] > 0]), len(df_gd[df_gd['pct'] == 0]), len(df_gd[df_gd['pct'] < 0])
    tong_ll, tb_pct = df_gd['lai_lo'].sum(), df_gd['pct'].mean()
    tong_loi = df_gd.loc[df_gd['lai_lo'] > 0, 'lai_lo'].sum()
    tong_lo_v = df_gd.loc[df_gd['lai_lo'] < 0, 'lai_lo'].sum()
    pf_str = f"{tong_loi / abs(tong_lo_v):.2f}" if tong_lo_v < 0 else 'N/A (không có mã lỗ)'
    total_elapsed = time.time() - start_time

    await context.bot.send_message(
        chat_id=chat_id, parse_mode='HTML',
        text=(
            f"<b>✅ KẾT QUẢ SCAN TOÀN BỘ (BACKTEST)</b>\n"
            f"Vol>{vol_pct}% | Trend {trend_n}p | Stop {stop_pct}%\n{SEP}\n"
            f"Tổng mã test : {len(df_r)}\n"
            f"Có GD        : {n_gd}\n"
            f"Không có GD  : {len(df_r[df_r['so_gd'] == 0])}\n"
            f"Lỗi DL       : {len(errors)}\n{SEP}\n"
            f"Trong {n_gd} mã có GD:\n"
            f"  Lời: {n_loi} ({round(n_loi/n_gd*100, 1)}%)\n"
            f"  Hòa: {n_hoa} ({round(n_hoa/n_gd*100, 1)}%)\n"
            f"  Lỗ : {n_lo} ({round(n_lo/n_gd*100, 1)}%)\n{SEP}\n"
            f"Tổng lãi/lỗ  : {tong_ll:+,.0f}đ\n"
            f"TB/mã        : {tong_ll/n_gd:+,.0f}đ ({tb_pct:+.2f}%)\n"
            f"Profit Factor: {pf_str}\n"
            f"(Mỗi mã vốn 50tr)\n{SEP}\n"
            f"⏱ Tổng thời gian: {total_elapsed:.0f}s\n"
            f"🚀 Tốc độ TB    : {len(df_r)/total_elapsed*60:.0f} mã/phút"
        )
    )

    top_loi = df_gd.nlargest(5, 'pct')[['symbol', 'pct', 'lai_lo', 'so_gd']]
    top_lo = df_gd.nsmallest(5, 'pct')[['symbol', 'pct', 'lai_lo', 'so_gd']]

    msg_loi = '<b>🏆 TOP 5 LỜI:</b>\n'
    for _, row in top_loi.iterrows():
        msg_loi += f"{row['symbol']}: {row['pct']:+.2f}% | {row['lai_lo']:+,.0f}đ | {int(row['so_gd'])} GD\n"
    await context.bot.send_message(chat_id=chat_id, text=msg_loi, parse_mode='HTML')

    msg_lo = '<b>📉 TOP 5 LỖ:</b>\n'
    for _, row in top_lo.iterrows():
        msg_lo += f"{row['symbol']}: {row['pct']:+.2f}% | {row['lai_lo']:+,.0f}đ | {int(row['so_gd'])} GD\n"
    await context.bot.send_message(chat_id=chat_id, text=msg_lo, parse_mode='HTML')

    csv_name = f"ket_qua_scanall_{datetime.now(VN_TZ).strftime('%Y%m%d_%H%M')}.csv"
    df_r.sort_values('pct', ascending=False).to_csv(csv_name, index=False)
    await context.bot.send_message(chat_id=chat_id, text=f'📁 Đã lưu CSV: {csv_name}')

# ---------- /weeklyscan ----------
WEEKLY_PROGRESS = 100

async def handle_weeklyscan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    symbols = get_all_symbols()
    total = len(symbols)
    chat_id = update.effective_chat.id

    if total == 0:
        await context.bot.send_message(
            chat_id=chat_id,
            text='❌ Không tìm thấy file vn_stocks_full.txt hoặc file rỗng.\nChạy /buildlist trước.'
        )
        return

    start_time = time.time()
    await context.bot.send_message(
        chat_id=chat_id, parse_mode='HTML',
        text=(
            f"<b>📅 BẮT ĐẦU WEEKLY SCAN</b>\n{SEP}\n"
            f"Tổng số mã : <b>{total}</b>\n"
            f"Điều kiện  :\n"
            f"  1. Volume tuần &gt; 500,000\n"
            f"  2. RSI(14) cắt lên SMA(RSI,14)\n"
            f"Workers    : 20 threads\n"
            f"Rate limit : 170 req/phút (Bronze)\n{SEP}\n"
            f"Cập nhật mỗi {WEEKLY_PROGRESS} mã..."
        )
    )

    results, done_cnt = [], [0]
    lock, progress_queue = threading.Lock(), asyncio.Queue()

    def scan_one(sym):
        res = check_weekly_signal(sym)
        with lock:
            done_cnt[0] += 1
            n = done_cnt[0]
            if res:
                results.append(res)
            if n % WEEKLY_PROGRESS == 0 or n == total:
                elapsed = time.time() - start_time
                remain = (elapsed / n) * (total - n) if n > 0 else 0
                speed = n / elapsed * 60 if elapsed > 0 else 0
                msg = (
                    f"📊 <b>TIẾN TRÌNH WEEKLY SCAN</b>\n{SEP}\n"
                    f"Đã xong : {n}/{total} ({n/total*100:.1f}%)\n"
                    f"✅ Tìm thấy: {len(results)} mã\n{SEP}\n"
                    f"⏱ {elapsed:.0f}s | Còn ~{remain:.0f}s\n"
                    f"🚀 {speed:.0f} mã/phút"
                )
                asyncio.run_coroutine_threadsafe(progress_queue.put(msg), asyncio.get_event_loop())

    loop = asyncio.get_event_loop()
    await asyncio.gather(
        loop.run_in_executor(None, lambda: run_pool_sync(scan_one, symbols)),
        report_progress(chat_id, context, progress_queue, 'weekly')
    )
    await progress_queue.put(None)

    total_elapsed = time.time() - start_time
    results.sort(key=lambda x: x['symbol'])
    
    # Log summary statistics
    logging.info('─' * 50)
    logging.info('WEEKLY SCAN SUMMARY:')
    logging.info('  Total scanned: %d', total)
    logging.info('  Signals found: %d', len(results))
    logging.info('  Time elapsed: %.0fs (%.0f symbols/min)', total_elapsed, total/total_elapsed*60)
    logging.info('─' * 50)

    if results:
        msg = (
            f"<b>📊 KẾT QUẢ WEEKLY SCAN</b>\n"
            f"✅ Thỏa điều kiện: <b>{len(results)}/{total}</b> mã\n"
            f"⏱ {total_elapsed:.0f}s | {total/total_elapsed*60:.0f} mã/phút\n"
            f"🕐 {now_vn()}\n\n"
        )
        for r in results[:20]:
            msg += (
                f"🔹 <b>{r['symbol']}</b> (tuần {r['week']}) — {r['close']:,}đ"
                f" — Vol {r['volume']:,}"
                f" — RSI {r['rsi']} / SMA {r['sma_rsi']}\n"
            )
        if len(results) > 20:
            msg += f"\n...và {len(results)-20} mã khác (xem file CSV)"
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

        csv_name = f"ket_qua_weekly_{datetime.now(VN_TZ).strftime('%Y%m%d_%H%M')}.csv"
        pd.DataFrame(results).to_csv(csv_name, index=False)
        await context.bot.send_message(chat_id=chat_id, text=f'📁 Đã lưu CSV: {csv_name}')
    else:
        await context.bot.send_message(
            chat_id=chat_id, parse_mode='HTML',
            text=(
                f"😔 Không tìm thấy mã nào thỏa điều kiện.\n"
                f"Tổng quét: {total}\n"
                f"⏱ {total_elapsed:.0f}s\n"
                f"🕐 {now_vn()}\n\n"
                f"💡 <i>Kiểm tra log để xem chi tiết</i>"
            )
        )

# ---------- /buildlist ----------
BUILD_PROGRESS = 60

async def handle_buildlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    await context.bot.send_message(
        chat_id=chat_id, parse_mode='HTML',
        text=(
            f"<b>🔨 BẮT ĐẦU BUILD SYMBOL LIST</b>\n{SEP}\n"
            f"Đang lấy danh sách mã từ 3 sàn (HOSE/HNX/UPCOM)...\n"
            f"Điều kiện lọc:\n"
            f"  - Giá đóng cửa >= {GIA_TOI_THIEU} (>={int(GIA_TOI_THIEU*1000)}đ)\n"
            f"  - Volume tuần trung bình >= {VOL_TUAN_TOI_THIEU}\n"
            f"Ước tính: ~10-15 phút..."
        )
    )

    loop = asyncio.get_event_loop()
    try:
        all_syms_raw, filtered = await loop.run_in_executor(None, get_exchange_symbols)
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f'❌ Lỗi khi lấy danh sách mã: {str(e)}')
        return

    total = len(filtered)
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"Trên sàn: {len(all_syms_raw)} mã\n"
            f"Sau lọc ≤3 ký tự: {total} mã\n"
            f"Bắt đầu kiểm tra từng mã..."
        )
    )

    passed, removed_vol, removed_gia, no_data = [], [], [], []
    done_cnt, lock = [0], threading.Lock()
    progress_queue = asyncio.Queue()
    start_time = time.time()

    def check_one(sym):
        stats = get_stock_stats_for_build(sym)
        with lock:
            done_cnt[0] += 1
            n = done_cnt[0]
            if stats is None:
                no_data.append(sym)
                passed.append(sym)
            else:
                avg_vol, last_close = stats
                if last_close < GIA_TOI_THIEU:
                    removed_gia.append(sym)
                elif avg_vol < VOL_TUAN_TOI_THIEU:
                    removed_vol.append(sym)
                else:
                    passed.append(sym)

            if n % BUILD_PROGRESS == 0 or n == total:
                elapsed = time.time() - start_time
                remain = (elapsed / n) * (total - n) if n > 0 else 0
                msg = (
                    f"🔨 <b>TIẾN TRÌNH BUILD LIST</b>\n{SEP}\n"
                    f"Đã kiểm tra: {n}/{total} ({n/total*100:.1f}%)\n"
                    f"✅ Giữ: {len(passed)}\n"
                    f"❌ Loại giá: {len(removed_gia)}\n"
                    f"❌ Loại vol: {len(removed_vol)}\n{SEP}\n"
                    f"⏱ {elapsed:.0f}s | Còn ~{remain:.0f}s"
                )
                asyncio.run_coroutine_threadsafe(progress_queue.put(msg), asyncio.get_event_loop())

    await asyncio.gather(
        loop.run_in_executor(None, lambda: run_pool_sync(check_one, filtered, max_workers=3)),
        report_progress(chat_id, context, progress_queue, 'build')
    )
    await progress_queue.put(None)

    passed.sort()
    with open('vn_stocks_full.txt', 'w', encoding='utf-8') as f:
        for sym in passed:
            f.write(f"{sym}\n")

    total_elapsed = time.time() - start_time
    await context.bot.send_message(
        chat_id=chat_id, parse_mode='HTML',
        text=(
            f"<b>✅ BUILD LIST HOÀN THÀNH</b>\n{SEP}\n"
            f"Tổng mã ban đầu        : {total}\n"
            f"Mã được giữ            : <b>{len(passed)}</b>\n"
            f"  - Thỏa cả 2 điều kiện: {len(passed) - len(no_data)}\n"
            f"  - Không có data (giữ) : {len(no_data)}\n"
            f"Loại giá < {int(GIA_TOI_THIEU*1000)}đ        : {len(removed_gia)}\n"
            f"Loại vol tuần < 500K   : {len(removed_vol)}\n{SEP}\n"
            f"⏱ Tổng thời gian: {total_elapsed:.0f}s\n"
            f"🕐 {now_vn()}\n"
            f"💾 Đã lưu: vn_stocks_full.txt"
        )
    )

# ============================================================
# INIT & MAIN
# ============================================================
async def post_init(app):
    await app.bot.send_message(
        chat_id=CHAT_ID, parse_mode='HTML',
        text=(
            f"<b>🤖 ALL-IN-ONE BOT sẵn sàng!</b>\n{SEP}\n"
            f"<b>Lệnh:</b>\n"
            f"  [MÃ]          : backtest 1 mã\n"
            f"  /scanall      : backtest toàn bộ\n"
            f"  /weeklyscan   : scan tín hiệu tuần\n"
            f"  /buildlist    : xây dựng lại danh sách mã\n"
            f"  /config       : xem tham số hiện tại\n\n"
            f"<b>Chỉnh tham số backtest:</b>\n"
            f"  /set vol [10-200]  : % volume\n"
            f"  /set trend [1-10]  : số phiên xu hướng\n"
            f"  /set stop [1-50]   : % trailing stop\n\n"
            f"Mặc định: Vol>{CONFIG['vol_pct']}% | Trend {CONFIG['trend_n']}p | Stop {CONFIG['stop_pct']}%\n"
            f"Vốn backtest: 50tr/mã | Từ năm 2023\n"
            f"⚡ Rate limit: 170 req/phút | 20 workers"
        )
    )

def main():
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler('scanall', handle_scanall))
    app.add_handler(CommandHandler('weeklyscan', handle_weeklyscan))
    app.add_handler(CommandHandler('buildlist', handle_buildlist))
    app.add_handler(CommandHandler('config', handle_config))
    app.add_handler(CommandHandler('set', handle_set))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print('Bot đang chạy...')
    app.run_polling()

if __name__ == '__main__':
    main()