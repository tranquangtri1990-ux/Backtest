# ============================================================
# BACKTEST BOT - TELEGRAM
# Lenh:
#   [MA]            : backtest 1 ma
#   /scanall        : quet toan bo (song song)
#   /config         : xem tham so
#   /set vol [so]   : volume % MA20     (10-200, mac dinh 120)
#   /set trend [so] : so phien xu huong (1-10,  mac dinh 1)
#   /set stop [so]  : trailing stop %   (1-50,  mac dinh 10)
# Tu tat sau 30 phut khong hoat dong
# ============================================================

import os

# ---- SET API KEY TRUOC KHI IMPORT VNSTOCK ----
API_KEY = 'vnstock_b89d601e86a29649640f94ab0634433e'
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
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CommandHandler,
    filters, ContextTypes
)

TOKEN   = '8578016275:AAGvL6SoOO3Yifqner8EcynwKt7OKgwl_J0'
CHAT_ID = '7000478479'
VN_TZ   = timezone(timedelta(hours=7))

logging.basicConfig(level=logging.INFO)

SEP = '-' * 35

# -------------------- Tham so --------------------
CONFIG = {
    'vol_pct' : 120,
    'trend_n' : 1,
    'stop_pct': 10,
}

last_activity = [time.time()]

def update_activity():
    last_activity[0] = time.time()

# -------------------- Rate limit (Token Bucket - Bronze 180 req/phut) --------------------
_RATE_LIMIT   = 160       # de du 160/phut (an toan duoi muc 180)
_MIN_INTERVAL = 60.0 / _RATE_LIMIT   # ~0.375 giay/request
_last_call    = [0.0]
_lock         = threading.Lock()

def rate_limited_sleep():
    """Token bucket: toi da 160 req/phut, an toan cho goi Bronze 180/phut"""
    with _lock:
        now  = time.time()
        wait = _MIN_INTERVAL - (now - _last_call[0])
        if wait > 0:
            time.sleep(wait)
        _last_call[0] = time.time()

# -------------------- Doc danh sach ma --------------------
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

# -------------------- Lay du lieu --------------------
def _fetch_df(symbol, source):
    """Lay raw daily DataFrame tu mot source cu the. Tra ve None neu that bai."""
    from vnstock import Vnstock
    stock = Vnstock(api_key=API_KEY, show_log=False).stock(symbol=symbol, source=source)
    end   = datetime.now(VN_TZ).strftime('%Y-%m-%d')
    raw = stock.quote.history(start='2022-01-01', end=end, interval='1D')

    # vnstock moi co the tra ve dict {'data': [...]} hoac DataFrame
    if isinstance(raw, dict):
        if 'data' in raw:
            df = pd.DataFrame(raw['data'])
        else:
            return None
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
    df = df.rename(columns={'close': 'Close', 'high': 'High', 'low': 'Low', 'volume': 'Volume'})
    df = df.sort_index().dropna(subset=['Close', 'High', 'Low', 'Volume'])
    return df if not df.empty else None

def get_data(symbol):
    last_errors = []
    for source in ('VCI', 'MSN', 'KBS'):
        rate_limited_sleep()
        try:
            df = _fetch_df(symbol, source)
            if df is not None:
                weekly = df.resample('W-FRI').agg({'Close': 'last', 'Volume': 'sum'}).dropna()
                return df, weekly
            else:
                last_errors.append(source + ':empty')
        except Exception as e:
            err = str(e)
            logging.warning('[get_data] %s / %s: %s', symbol, source, err)
            last_errors.append(source + ':' + err[:120])
            if any(k in err.lower() for k in ['rate limit', '429', 'too many', 'exceeded']):
                logging.warning('[rate limit] sleeping 60s...')
                time.sleep(60)
                # Retry chinh source nay them 1 lan
                try:
                    df2 = _fetch_df(symbol, source)
                    if df2 is not None:
                        weekly = df2.resample('W-FRI').agg({'Close': 'last', 'Volume': 'sum'}).dropna()
                        return df2, weekly
                except Exception:
                    pass
            continue
    logging.warning('[get_data] %s failed: %s', symbol, ' | '.join(last_errors))
    return None, last_errors

# -------------------- Chi bao --------------------
def smma(series, period):
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

def calc_weekly_indicators(weekly):
    df = weekly.copy()
    df['ma20_vol'] = df['Volume'].rolling(20).mean()
    delta          = df['Close'].diff()
    avg_gain       = smma(delta.where(delta > 0, 0.0), 14)
    avg_loss       = smma((-delta).where(delta < 0, 0.0), 14)
    df['rsi']      = 100 - (100 / (1 + avg_gain / avg_loss))
    df['sma_rsi']  = df['rsi'].rolling(14).mean()
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

# -------------------- Backtest 1 ma --------------------
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

# -------------------- Dinh dang ket qua --------------------
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

# -------------------- Handlers --------------------
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

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    text = update.message.text.strip().upper()
    if not (2 <= len(text) <= 5 and text.isalpha()):
        await update.message.reply_text('Nhap ma co phieu (VD: VCB)\n/scanall /config /set')
        return
    await update.message.reply_text(
        '<b>Dang chay backtest ' + text + '...</b>\n'
        'Vol>' + str(CONFIG['vol_pct']) + '% | Trend ' + str(CONFIG['trend_n']) + 'p | Stop ' + str(CONFIG['stop_pct']) + '%',
        parse_mode='HTML'
    )
    result = run_backtest(text)
    for msg in format_result(result):
        await update.message.reply_text(msg, parse_mode='HTML')

async def handle_scanall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    symbols  = get_all_symbols()
    total    = len(symbols)
    chat_id  = update.effective_chat.id
    vol_pct  = CONFIG['vol_pct']
    trend_n  = CONFIG['trend_n']
    stop_pct = CONFIG['stop_pct']

    results = []
    errors  = []
    lock    = threading.Lock()

    def backtest_one(sym):
        r = run_backtest(sym, vol_pct=vol_pct, trend_n=trend_n, stop_pct=stop_pct)
        with lock:
            if 'error' not in r:
                results.append({'symbol': sym, 'so_gd': r['so_gd'],
                                 'pct': r['pct'], 'lai_lo': r['lai_lo']})
            else:
                errors.append(sym)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(backtest_one, sym): sym for sym in symbols}
        for future in as_completed(futures):
            future.result()

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

    # Profit Factor = tong loi nhuan / tong lo (theo gia tri tuyet doi)
    tong_loi = df_gd.loc[df_gd['lai_lo'] > 0, 'lai_lo'].sum()
    tong_lo  = df_gd.loc[df_gd['lai_lo'] < 0, 'lai_lo'].sum()
    if tong_lo < 0:
        profit_factor = tong_loi / abs(tong_lo)
        pf_str = f"{profit_factor:.2f}"
    else:
        pf_str = 'N/A (khong co ma lo)'

    await context.bot.send_message(chat_id=chat_id, parse_mode='HTML', text=(
        '<b>KET QUA SCAN TOAN BO</b>\n'
        'Vol>' + str(vol_pct) + '% | Trend ' + str(trend_n) + 'p | Stop ' + str(stop_pct) + '%\n' +
        SEP + '\n'
        'Tong ma test : ' + str(len(df_r)) + '\n'
        'Co GD        : ' + str(n_gd) + '\n'
        'Khong co GD  : ' + str(n_ko_gd) + '\n'
        'Loi DL       : ' + str(len(errors)) + '\n' +
        SEP + '\n'
        'Trong ' + str(n_gd) + ' ma co GD:\n'
        '  Loi: ' + str(n_loi) + ' (' + str(round(n_loi/n_gd*100,1)) + '%)\n'
        '  Hoa: ' + str(n_hoa) + ' (' + str(round(n_hoa/n_gd*100,1)) + '%)\n'
        '  Lo : ' + str(n_lo)  + ' (' + str(round(n_lo /n_gd*100,1)) + '%)\n' +
        SEP + '\n'
        'Tong lai/lo  : ' + f"{tong_ll:+,.0f}" + 'd\n'
        'TB/ma        : ' + f"{tong_ll/n_gd:+,.0f}" + 'd (' + f"{tb_pct:+.2f}" + '%)\n'
        'Profit Factor: ' + pf_str + '\n'
        '(Moi ma von 50tr)\n' + SEP
    ))

    top_loi = df_gd.nlargest(5, 'pct')[['symbol', 'pct', 'lai_lo', 'so_gd']]
    top_lo  = df_gd.nsmallest(5, 'pct')[['symbol', 'pct', 'lai_lo', 'so_gd']]

    msg_loi = '<b>TOP 5 LOI:</b>\n'
    for _, row in top_loi.iterrows():
        msg_loi += row['symbol'] + ': ' + f"{row['pct']:+.2f}" + '% | ' + f"{row['lai_lo']:+,.0f}" + 'd | ' + str(int(row['so_gd'])) + ' GD\n'
    await context.bot.send_message(chat_id=chat_id, text=msg_loi, parse_mode='HTML')

    msg_lo = '<b>TOP 5 LO:</b>\n'
    for _, row in top_lo.iterrows():
        msg_lo += row['symbol'] + ': ' + f"{row['pct']:+.2f}" + '% | ' + f"{row['lai_lo']:+,.0f}" + 'd | ' + str(int(row['so_gd'])) + ' GD\n'
    await context.bot.send_message(chat_id=chat_id, text=msg_lo, parse_mode='HTML')

    timestamp = datetime.now(VN_TZ).strftime('%Y%m%d_%H%M')
    csv_name  = 'ket_qua_scanall_' + timestamp + '.csv'
    csv_path  = os.path.abspath(csv_name)
    df_r.sort_values('pct', ascending=False).to_csv(csv_name, index=False)

    await context.bot.send_message(
        chat_id=chat_id,
        text='Da luu file CSV:\n' + csv_name + '\n' + csv_path
    )

# -------------------- Tu tat --------------------
async def watchdog(app):
    while True:
        await asyncio.sleep(60)
        if time.time() - last_activity[0] >= 1800:
            await app.bot.send_message(chat_id=CHAT_ID, text='Bot tu tat sau 30 phut khong hoat dong.')
            await app.stop()
            break

async def post_init(app):
    update_activity()
    await app.bot.send_message(
        chat_id=CHAT_ID, parse_mode='HTML',
        text=(
            '<b>Bot Backtest san sang!</b>\n' + SEP + '\n'
            'Lenh:\n'
            '  [MA]     : backtest 1 ma\n'
            '  /scanall : quet toan bo\n'
            '  /config  : xem tham so\n\n'
            'Chinh tham so:\n'
            '  /set vol [10-200]  : % volume\n'
            '  /set trend [1-10]  : so phien SMA\n'
            '  /set stop [1-50]   : % trailing stop\n\n'
            'Mac dinh: Vol>' + str(CONFIG['vol_pct']) + '% | Trend ' +
            str(CONFIG['trend_n']) + 'p | Stop ' + str(CONFIG['stop_pct']) + '%\n'
            'Von: 50tr/ma | Tu 2023 | Tu tat sau 30p.'
        )
    )
    asyncio.create_task(watchdog(app))

def main():
    app = (ApplicationBuilder().token(TOKEN).post_init(post_init).build())
    app.add_handler(CommandHandler('scanall', handle_scanall))
    app.add_handler(CommandHandler('config',  handle_config))
    app.add_handler(CommandHandler('set',     handle_set))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print('Bot dang chay...')
    app.run_polling()

if __name__ == '__main__':
    main()
