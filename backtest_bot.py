# ============================================================
# BACKTEST BOT - TELEGRAM
# Lenh:
#   [MA]      : backtest 1 ma voi tham so hien tai
#   /scanall  : quet toan bo
#   /config   : xem tham so hien tai
#   /set      : chinh tham so
#                 /set vol 150     -> volume > 150% MA20
#                 /set trend 3     -> SMA(RSI) tang trong 3 phien
# Tu tat sau 30 phut khong hoat dong
# ============================================================

import os
import asyncio
import logging
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, timezone
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CommandHandler,
    filters, ContextTypes
)

TOKEN   = '8578016275:AAGvL6SoOO3Yifqner8EcynwKt7OKgwl_J0'
CHAT_ID = '7000478479'
API_KEY = 'vnstock_f9fb6ea7e9ef42cf8472ee293eb9c16a'
VN_TZ   = timezone(timedelta(hours=7))

os.environ['VNSTOCK_API_KEY'] = API_KEY
logging.basicConfig(level=logging.INFO)

SEP = '-' * 35

# -------------------- Tham so toan cuc --------------------
# vol_pct : volume phai > vol_pct% cua MA20
# trend_n : so phien lien tiep SMA(RSI) phai tang hoac di ngang
CONFIG = {
    'vol_pct' : 120,   # %
    'trend_n' : 1,     # 1 = chi can phien hien tai >= phien truoc
}

# Thoi gian hoat dong cuoi cung (de tu tat)
last_activity = [time.time()]

def update_activity():
    last_activity[0] = time.time()

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
def get_data(symbol):
    try:
        from vnstock import Vnstock
        stock = Vnstock().stock(symbol=symbol, source='VCI')
        end   = datetime.now(VN_TZ).strftime('%Y-%m-%d')
        df = stock.quote.history(start='2022-01-01', end=end, interval='1D')
        if df is None or df.empty:
            return None, None
        df.columns = [c.lower() for c in df.columns]
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
            df = df.set_index('time')
        elif df.index.dtype != 'datetime64[ns]':
            df.index = pd.to_datetime(df.index)
        df = df.rename(columns={'close': 'Close', 'high': 'High', 'low': 'Low', 'volume': 'Volume'})
        df = df.sort_index().dropna(subset=['Close', 'High', 'Low', 'Volume'])
        weekly = df.resample('W-FRI').agg({'Close': 'last', 'Volume': 'sum'}).dropna()
        return df, weekly
    except:
        return None, None

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
    df['ma20_vol']     = df['Volume'].rolling(20).mean()
    delta              = df['Close'].diff()
    avg_gain           = smma(delta.where(delta > 0, 0.0), 14)
    avg_loss           = smma((-delta).where(delta < 0, 0.0), 14)
    df['rsi']          = 100 - (100 / (1 + avg_gain / avg_loss))
    df['sma_rsi']      = df['rsi'].rolling(14).mean()
    return df

def check_buy_signal(df_w, i, vol_pct, trend_n):
    """
    vol_pct : volume > vol_pct% cua MA20
    trend_n : SMA(RSI) phai tang lien tiep trong trend_n phien
    """
    if i < max(1, trend_n):
        return False
    row  = df_w.iloc[i]
    prev = df_w.iloc[i - 1]

    # Kiem tra NaN
    if any(pd.isna(row[c]) for c in ['Volume', 'ma20_vol', 'rsi', 'sma_rsi']):
        return False
    if pd.isna(prev['rsi']) or pd.isna(prev['sma_rsi']):
        return False

    # DK1: Volume
    dk1 = row['Volume'] > (vol_pct / 100) * row['ma20_vol']

    # DK2: RSI cat len SMA(RSI)
    dk2 = prev['rsi'] <= prev['sma_rsi'] and row['rsi'] > row['sma_rsi']

    # DK3: SMA(RSI) tang lien tiep trend_n phien
    dk3 = True
    for k in range(trend_n):
        idx_cur  = i - k
        idx_prev = i - k - 1
        if idx_prev < 0:
            dk3 = False
            break
        s_cur  = df_w.iloc[idx_cur]['sma_rsi']
        s_prev = df_w.iloc[idx_prev]['sma_rsi']
        if pd.isna(s_cur) or pd.isna(s_prev):
            dk3 = False
            break
        if s_cur < s_prev:  # Cho phep di ngang (>=)
            dk3 = False
            break

    return dk1 and dk2 and dk3

# -------------------- Backtest 1 ma --------------------
def run_backtest(symbol, initial_capital=50_000_000, vol_pct=None, trend_n=None):
    if vol_pct  is None: vol_pct  = CONFIG['vol_pct']
    if trend_n  is None: trend_n  = CONFIG['trend_n']

    daily, weekly = get_data(symbol)
    if daily is None or weekly is None:
        return {'error': 'Khong lay duoc du lieu cho ma ' + symbol}

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
                stop_price = position['peak'] * 0.90
                if day_row['Low'] <= stop_price:
                    gia_ban   = stop_price
                    gia_mua   = position['buy_price']
                    pct_trade = (gia_ban - gia_mua) / gia_mua * 100
                    von_vao   = position['cost']
                    capital   = von_vao * (1 + pct_trade / 100)
                    trades.append({
                        'stt'      : len(trades) + 1,
                        'loai'     : 'Ban',
                        'ngay_mua' : position['buy_date'].strftime('%Y-%m-%d'),
                        'gia_mua'  : round(gia_mua, 2),
                        'ngay_ban' : day_ts.strftime('%Y-%m-%d'),
                        'gia_ban'  : round(gia_ban, 2),
                        'gia_dinh' : round(position['peak'], 2),
                        'gia_stop' : round(stop_price, 2),
                        'von_dau'  : round(von_vao, 0),
                        'gia_tri'  : round(capital, 0),
                        'pct'      : round(pct_trade, 2),
                        'lai_lo'   : round(capital - von_vao, 0),
                        'von_sau'  : round(capital, 0),
                        'dang_giu' : False,
                    })
                    position = None
                    day_idx += 1
                    sold = True
                    break
                if day_row['High'] > position['peak']:
                    position['peak'] = day_row['High']
                day_idx += 1

            if sold and check_buy_signal(df_w, global_wi, vol_pct, trend_n):
                buy_price = df_w_bt.iloc[wi]['Close']
                position  = {
                    'buy_date' : week_end, 'buy_price': buy_price,
                    'shares'   : capital / buy_price,
                    'cost'     : capital, 'peak': buy_price,
                }
            continue

        while day_idx < len(daily_list) and daily_list[day_idx][0] <= week_end:
            day_idx += 1
        if check_buy_signal(df_w, global_wi, vol_pct, trend_n):
            buy_price = df_w_bt.iloc[wi]['Close']
            position  = {
                'buy_date' : week_end, 'buy_price': buy_price,
                'shares'   : capital / buy_price,
                'cost'     : capital, 'peak': buy_price,
            }

    # Xu ly lenh con lai
    if position is not None:
        while day_idx < len(daily_list):
            day_ts, day_row = daily_list[day_idx]
            if day_ts <= position['buy_date']:
                day_idx += 1
                continue
            stop_price = position['peak'] * 0.90
            if day_row['Low'] <= stop_price:
                gia_ban   = stop_price
                gia_mua   = position['buy_price']
                pct_trade = (gia_ban - gia_mua) / gia_mua * 100
                von_vao   = position['cost']
                capital   = von_vao * (1 + pct_trade / 100)
                trades.append({
                    'stt': len(trades)+1, 'loai': 'Ban',
                    'ngay_mua': position['buy_date'].strftime('%Y-%m-%d'),
                    'gia_mua': round(gia_mua, 2),
                    'ngay_ban': day_ts.strftime('%Y-%m-%d'),
                    'gia_ban': round(gia_ban, 2),
                    'gia_dinh': round(position['peak'], 2),
                    'gia_stop': round(stop_price, 2),
                    'von_dau': round(von_vao, 0),
                    'gia_tri': round(capital, 0),
                    'pct': round(pct_trade, 2),
                    'lai_lo': round(capital - von_vao, 0),
                    'von_sau': round(capital, 0),
                    'dang_giu': False,
                })
                position = None
                break
            if day_row['High'] > position['peak']:
                position['peak'] = day_row['High']
            day_idx += 1

        if position is not None:
            last_ts, last_row = daily_list[-1]
            last_close = last_row['Close']
            gia_mua    = position['buy_price']
            pct_trade  = (last_close - gia_mua) / gia_mua * 100
            von_vao    = position['cost']
            current    = von_vao * (1 + pct_trade / 100)
            capital    = current
            trades.append({
                'stt': len(trades)+1, 'loai': 'Dang giu',
                'ngay_mua': position['buy_date'].strftime('%Y-%m-%d'),
                'gia_mua': round(gia_mua, 2),
                'ngay_ban': last_ts.strftime('%Y-%m-%d'),
                'gia_ban': round(last_close, 2),
                'gia_dinh': round(position['peak'], 2),
                'gia_stop': round(position['peak'] * 0.90, 2),
                'von_dau': round(von_vao, 0),
                'gia_tri': round(current, 0),
                'pct': round(pct_trade, 2),
                'lai_lo': round(current - von_vao, 0),
                'von_sau': round(current, 0),
                'dang_giu': True,
            })

    return {
        'symbol'     : symbol.upper(),
        'von_ban_dau': initial_capital,
        'von_cuoi'   : round(capital, 0),
        'lai_lo'     : round(capital - initial_capital, 0),
        'pct'        : round((capital / initial_capital - 1) * 100, 2),
        'so_gd'      : len(trades),
        'trades'     : trades,
        'vol_pct'    : vol_pct,
        'trend_n'    : trend_n,
    }

# -------------------- Dinh dang ket qua --------------------
def format_result(r):
    if 'error' in r:
        return ['Loi: ' + r['error']]
    msgs = []
    tong = (
        '<b>BACKTEST ' + r['symbol'] + '</b>\n'
        'Vol > ' + str(r['vol_pct']) + '% MA20 | '
        'Trend SMA ' + str(r['trend_n']) + ' phien\n' +
        SEP + '\n'
        'Von ban dau : ' + f"{r['von_ban_dau']:,.0f}" + 'd\n'
        'Von cuoi    : ' + f"{r['von_cuoi']:,.0f}" + 'd\n'
        'Loi nhuan   : ' + f"{r['lai_lo']:+,.0f}" + 'd (' + f"{r['pct']:+.2f}" + '%)\n'
        'So giao dich: ' + str(r['so_gd']) + '\n' +
        SEP
    )
    msgs.append(tong)
    chunk = []
    for t in r['trades']:
        status = 'DANG GIU' if t['dang_giu'] else 'BAN'
        label  = 'Hien tai' if t['dang_giu'] else 'Ban     '
        dong = (
            '<b>#' + str(t['stt']) + ' ' + status + '</b>\n'
            '  Mua      : ' + t['ngay_mua'] + ' @ ' + f"{t['gia_mua']:,}" + 'd\n'
            '  ' + label + ' : ' + t['ngay_ban'] + ' @ ' + f"{t['gia_ban']:,}" + 'd\n'
            '  Dinh/Stop : ' + f"{t['gia_dinh']:,}" + 'd / ' + f"{t['gia_stop']:,}" + 'd\n'
            '  Von vao   : ' + f"{t['von_dau']:,.0f}" + 'd\n'
            '  Von sau   : ' + f"{t['von_sau']:,.0f}" + 'd\n'
            '  Lai/Lo    : ' + f"{t['lai_lo']:+,.0f}" + 'd (' + f"{t['pct']:+.2f}" + '%)'
        )
        chunk.append(dong)
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
        '<b>Tham so hien tai:</b>\n' +
        SEP + '\n'
        'Volume    : > ' + str(CONFIG['vol_pct']) + '% MA20\n'
        'Trend SMA : tang/ngang ' + str(CONFIG['trend_n']) + ' phien lien tiep\n\n'
        'Thay doi:\n'
        '  /set vol 150   -> Volume > 150% MA20\n'
        '  /set trend 3   -> SMA tang trong 3 phien',
        parse_mode='HTML'
    )

async def handle_set(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    args = context.args
    if len(args) != 2:
        await update.message.reply_text(
            'Cu phap: /set [tham so] [gia tri]\n'
            'Vi du  : /set vol 150\n'
            '         /set trend 3'
        )
        return

    key, val_str = args[0].lower(), args[1]
    try:
        val = int(val_str)
    except ValueError:
        await update.message.reply_text('Gia tri phai la so nguyen.')
        return

    if key == 'vol':
        if val < 100 or val > 500:
            await update.message.reply_text('Vol phai tu 100 den 500 (%).')
            return
        CONFIG['vol_pct'] = val
        await update.message.reply_text(
            'Da cap nhat: Volume > ' + str(val) + '% MA20',
            parse_mode='HTML'
        )
    elif key == 'trend':
        if val < 1 or val > 10:
            await update.message.reply_text('Trend phai tu 1 den 10 (phien).')
            return
        CONFIG['trend_n'] = val
        await update.message.reply_text(
            'Da cap nhat: SMA(RSI) tang/ngang ' + str(val) + ' phien lien tiep',
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text(
            'Tham so khong hop le. Dung: vol hoac trend'
        )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    text = update.message.text.strip().upper()
    if not (2 <= len(text) <= 5 and text.isalpha()):
        await update.message.reply_text(
            'Nhap ma co phieu (VD: VCB)\n'
            'Hoac dung lenh:\n'
            '  /scanall : quet toan bo\n'
            '  /config  : xem tham so\n'
            '  /set     : chinh tham so'
        )
        return
    await update.message.reply_text(
        '<b>Dang chay backtest cho ' + text + '...</b>\n'
        'Vol > ' + str(CONFIG['vol_pct']) + '% MA20 | '
        'Trend ' + str(CONFIG['trend_n']) + ' phien',
        parse_mode='HTML'
    )
    result = run_backtest(text)
    for msg in format_result(result):
        await update.message.reply_text(msg, parse_mode='HTML')

async def handle_scanall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    symbols = get_all_symbols()
    total   = len(symbols)
    chat_id = update.effective_chat.id

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            '<b>Bat dau scan ' + str(total) + ' ma...</b>\n'
            'Vol > ' + str(CONFIG['vol_pct']) + '% MA20 | '
            'Trend ' + str(CONFIG['trend_n']) + ' phien\n'
            'Uoc tinh 15-30 phut, se bao cao khi xong.'
        ),
        parse_mode='HTML'
    )

    results, errors, count = [], [], 0
    for sym in symbols:
        try:
            r = run_backtest(sym)
            if 'error' not in r:
                results.append({
                    'symbol': sym,
                    'so_gd' : r['so_gd'],
                    'pct'   : r['pct'],
                    'lai_lo': r['lai_lo'],
                })
            else:
                errors.append(sym)
        except:
            errors.append(sym)
        count += 1
        if count % 50 == 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text='Tien do: ' + str(count) + '/' + str(total) + ' ma...'
            )
        time.sleep(1.1)

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

    msg_tk = (
        '<b>KET QUA SCAN TOAN BO</b>\n'
        'Vol > ' + str(CONFIG['vol_pct']) + '% MA20 | Trend ' + str(CONFIG['trend_n']) + ' phien\n' +
        SEP + '\n'
        'Tong ma test    : ' + str(len(df_r)) + '\n'
        'Co giao dich    : ' + str(n_gd) + '\n'
        'Khong co GD     : ' + str(n_ko_gd) + '\n'
        'Loi DL          : ' + str(len(errors)) + '\n' +
        SEP + '\n'
        'Trong ' + str(n_gd) + ' ma co GD:\n'
        '  Loi : ' + str(n_loi) + ' ma (' + str(round(n_loi/n_gd*100, 1)) + '%)\n'
        '  Hoa : ' + str(n_hoa) + ' ma (' + str(round(n_hoa/n_gd*100, 1)) + '%)\n'
        '  Lo  : ' + str(n_lo)  + ' ma (' + str(round(n_lo /n_gd*100, 1)) + '%)\n' +
        SEP + '\n'
        'Tong lai/lo    : ' + f"{tong_ll:+,.0f}" + 'd\n'
        'TB lai/lo/ma   : ' + f"{tong_ll/n_gd:+,.0f}" + 'd\n'
        'TB % moi ma    : ' + f"{tb_pct:+.2f}" + '%\n'
        '(Moi ma von 50,000,000d)\n' +
        SEP
    )
    await context.bot.send_message(chat_id=chat_id, text=msg_tk, parse_mode='HTML')

    top_loi = df_gd.nlargest(5, 'pct')[['symbol', 'pct', 'lai_lo', 'so_gd']]
    top_lo  = df_gd.nsmallest(5, 'pct')[['symbol', 'pct', 'lai_lo', 'so_gd']]

    msg_loi = '<b>TOP 5 LOI NHAT:</b>\n'
    for _, row in top_loi.iterrows():
        msg_loi += (row['symbol'] + ': ' + f"{row['pct']:+.2f}" + '% | '
                    + f"{row['lai_lo']:+,.0f}" + 'd | ' + str(int(row['so_gd'])) + ' GD\n')
    await context.bot.send_message(chat_id=chat_id, text=msg_loi, parse_mode='HTML')

    msg_lo = '<b>TOP 5 LO NHAT:</b>\n'
    for _, row in top_lo.iterrows():
        msg_lo += (row['symbol'] + ': ' + f"{row['pct']:+.2f}" + '% | '
                   + f"{row['lai_lo']:+,.0f}" + 'd | ' + str(int(row['so_gd'])) + ' GD\n')
    await context.bot.send_message(chat_id=chat_id, text=msg_lo, parse_mode='HTML')

    df_r.sort_values('pct', ascending=False).to_csv('ket_qua_scanall.csv', index=False)
    await context.bot.send_message(chat_id=chat_id, text='Da luu: ket_qua_scanall.csv')

# -------------------- Tu tat sau 30 phut --------------------
async def watchdog(app):
    """Kiem tra moi 60 giay, tu tat neu khong hoat dong 30 phut"""
    timeout = 30 * 60  # 30 phut
    while True:
        await asyncio.sleep(60)
        idle = time.time() - last_activity[0]
        if idle >= timeout:
            await app.bot.send_message(
                chat_id=CHAT_ID,
                text='Bot tu tat sau 30 phut khong hoat dong.'
            )
            await app.stop()
            break

async def post_init(app):
    update_activity()
    await app.bot.send_message(
        chat_id=CHAT_ID,
        text=(
            '<b>Bot Backtest san sang!</b>\n' +
            SEP + '\n'
            'Lenh:\n'
            '  [MA]     : backtest 1 ma (VD: VCB)\n'
            '  /scanall : quet toan bo\n'
            '  /config  : xem tham so hien tai\n'
            '  /set vol [so]   : doi nguong volume\n'
            '  /set trend [so] : doi so phien xu huong\n\n'
            'Tham so mac dinh:\n'
            '  Vol > ' + str(CONFIG['vol_pct']) + '% MA20\n'
            '  Trend SMA: ' + str(CONFIG['trend_n']) + ' phien\n'
            '  Von: 50,000,000d | Tu 2023\n\n'
            'Tu tat sau 30 phut khong hoat dong.'
        ),
        parse_mode='HTML'
    )
    asyncio.create_task(watchdog(app))

def main():
    app = (ApplicationBuilder()
           .token(TOKEN)
           .post_init(post_init)
           .build())
    app.add_handler(CommandHandler('scanall', handle_scanall))
    app.add_handler(CommandHandler('config',  handle_config))
    app.add_handler(CommandHandler('set',     handle_set))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print('Bot dang chay...')
    app.run_polling()

if __name__ == '__main__':
    main()