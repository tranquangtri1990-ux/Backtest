# ============================================================
# BACKTEST BOT - TELEGRAM
# 2 chế độ:
#   - Nhap ma cu the (VD: VCB) -> backtest 1 ma
#   - Nhap lenh /scanall        -> backtest toan bo ma trong file
# ============================================================

import os
import logging
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, timezone
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters, ContextTypes

TOKEN   = '8578016275:AAGvL6SoOO3Yifqner8EcynwKt7OKgwl_J0'
CHAT_ID = '7000478479'
API_KEY = 'vnstock_f9fb6ea7e9ef42cf8472ee293eb9c16a'
VN_TZ   = timezone(timedelta(hours=7))

os.environ['VNSTOCK_API_KEY'] = API_KEY
logging.basicConfig(level=logging.INFO)

SEP = '-' * 35

# -------------------- Doc danh sach ma --------------------
def get_all_symbols(filename='vn_stocks_full.txt'):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            raw = [line.strip() for line in f if line.strip()]
        # Chi giu ma co phieu thuan tuy: 2-5 ky tu chu cai
        symbols = [s for s in raw if 2 <= len(s) <= 5 and s.isalpha()]
        # Loai bo ETF va quy
        exclude = {'E1VFVN30', 'FUEKIVFS', 'FUEMAV30', 'FUEMAVND',
                   'FUESSV30', 'FUESSVFL', 'FUETCC50', 'FUEVFVND', 'FUEVN100'}
        symbols = [s for s in symbols if s not in exclude]
        return list(dict.fromkeys(symbols))  # Giu thu tu, loai trung lap
    except:
        return []

# -------------------- Lay du lieu --------------------
def get_data(symbol):
    try:
        from vnstock import Vnstock
        stock = Vnstock().stock(symbol=symbol, source='VCI')
        end   = datetime.now(VN_TZ).strftime('%Y-%m-%d')
        start = '2022-01-01'
        df = stock.quote.history(start=start, end=end, interval='1D')
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

# -------------------- Chi bao tuan --------------------
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
    df['sma_rsi_prev'] = df['sma_rsi'].shift(1)
    return df

def check_buy_signal(df_w, i):
    if i < 1:
        return False
    row  = df_w.iloc[i]
    prev = df_w.iloc[i - 1]
    cols = ['Volume', 'ma20_vol', 'rsi', 'sma_rsi', 'sma_rsi_prev']
    if any(pd.isna(row[c]) for c in cols):
        return False
    if pd.isna(prev['rsi']) or pd.isna(prev['sma_rsi']):
        return False
    dk1 = row['Volume'] > 1.2 * row['ma20_vol']
    dk2 = prev['rsi'] <= prev['sma_rsi'] and row['rsi'] > row['sma_rsi']
    dk3 = row['sma_rsi'] >= row['sma_rsi_prev']
    return dk1 and dk2 and dk3

# -------------------- Backtest 1 ma --------------------
def run_backtest(symbol, initial_capital=50_000_000):
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

    week_dates = df_w_bt.index.tolist()

    for wi, week_end in enumerate(week_dates):
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
                high_today = day_row['High']
                low_today  = day_row['Low']
                stop_price = position['peak'] * 0.90
                if low_today <= stop_price:
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
                if high_today > position['peak']:
                    position['peak'] = high_today
                day_idx += 1
            if sold:
                if position is None and check_buy_signal(df_w, global_wi):
                    buy_price = df_w_bt.iloc[wi]['Close']
                    position  = {
                        'buy_date' : week_end,
                        'buy_price': buy_price,
                        'shares'   : capital / buy_price,
                        'cost'     : capital,
                        'peak'     : buy_price,
                    }
            continue

        while day_idx < len(daily_list) and daily_list[day_idx][0] <= week_end:
            day_idx += 1
        if check_buy_signal(df_w, global_wi):
            buy_price = df_w_bt.iloc[wi]['Close']
            position  = {
                'buy_date' : week_end,
                'buy_price': buy_price,
                'shares'   : capital / buy_price,
                'cost'     : capital,
                'peak'     : buy_price,
            }

    # Lenh con lai
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
    }

# -------------------- Dinh dang 1 ma --------------------
def format_result(r):
    if 'error' in r:
        return ['Loi: ' + r['error']]
    msgs = []
    tong = (
        '<b>BACKTEST ' + r['symbol'] + '</b>\n'
        '(Tin hieu tuan | Trailing stop ngay 10%)\n' +
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

# -------------------- Scan toan bo --------------------
async def run_scanall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    symbols = get_all_symbols()
    total   = len(symbols)
    chat_id = update.effective_chat.id

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            '<b>Bat dau scan toan bo ' + str(total) + ' ma...</b>\n'
            'Von moi ma: 50,000,000d | Tu 2023\n'
            'Qua trinh co the mat 30-60 phut, vui long cho.\n'
            'Se bao cao khi hoan tat.'
        ),
        parse_mode='HTML'
    )

    results     = []
    errors      = []
    count       = 0

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
        except Exception as e:
            errors.append(sym)

        count += 1
        # Bao cao tien do moi 50 ma
        if count % 50 == 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text='Tien do: ' + str(count) + '/' + str(total) + ' ma...',
                parse_mode='HTML'
            )
        time.sleep(1.1)  # Tranh rate limit

    # ---- Thong ke ----
    if not results:
        await context.bot.send_message(chat_id=chat_id, text='Khong co du lieu de thong ke.')
        return

    df_r = pd.DataFrame(results)

    # Chi tinh ma co giao dich
    df_gd  = df_r[df_r['so_gd'] > 0]
    n_gd   = len(df_gd)
    n_loi  = len(df_gd[df_gd['pct'] > 0])
    n_hoa  = len(df_gd[df_gd['pct'] == 0])
    n_lo   = len(df_gd[df_gd['pct'] < 0])
    n_ko_gd = len(df_r[df_r['so_gd'] == 0])

    tong_lai_lo    = df_gd['lai_lo'].sum()
    tb_pct         = df_gd['pct'].mean()
    tong_ma_test   = len(df_r)

    # Top 5 loi nhat
    top_loi = df_gd.nlargest(5, 'pct')[['symbol', 'pct', 'lai_lo', 'so_gd']]
    # Top 5 lo nhat
    top_lo  = df_gd.nsmallest(5, 'pct')[['symbol', 'pct', 'lai_lo', 'so_gd']]

    msg_tk = (
        '<b>KET QUA SCAN TOAN BO</b>\n' +
        SEP + '\n'
        'Tong ma test      : ' + str(tong_ma_test) + '\n'
        'Co giao dich      : ' + str(n_gd) + '\n'
        'Khong co GD       : ' + str(n_ko_gd) + '\n'
        'Khong lay duoc DL : ' + str(len(errors)) + '\n' +
        SEP + '\n'
        'Ma CO GD (' + str(n_gd) + ' ma):\n'
        '  Loi  : ' + str(n_loi) + ' ma (' + str(round(n_loi/n_gd*100, 1)) + '%)\n'
        '  Hoa  : ' + str(n_hoa) + ' ma (' + str(round(n_hoa/n_gd*100, 1)) + '%)\n'
        '  Lo   : ' + str(n_lo) + ' ma (' + str(round(n_lo/n_gd*100, 1)) + '%)\n' +
        SEP + '\n'
        'Tong lai/lo tat ca: ' + f"{tong_lai_lo:+,.0f}" + 'd\n'
        'TB lai/lo moi ma  : ' + f"{tong_lai_lo/n_gd:+,.0f}" + 'd\n'
        'TB % moi ma       : ' + f"{tb_pct:+.2f}" + '%\n'
        '(Moi ma dau 50,000,000d)\n' +
        SEP
    )
    await context.bot.send_message(chat_id=chat_id, text=msg_tk, parse_mode='HTML')

    # Top loi
    msg_top_loi = '<b>TOP 5 LOI NHAT:</b>\n'
    for _, row in top_loi.iterrows():
        msg_top_loi += (row['symbol'] + ': ' + f"{row['pct']:+.2f}" + '% | '
                        + f"{row['lai_lo']:+,.0f}" + 'd | '
                        + str(int(row['so_gd'])) + ' GD\n')
    await context.bot.send_message(chat_id=chat_id, text=msg_top_loi, parse_mode='HTML')

    # Top lo
    msg_top_lo = '<b>TOP 5 LO NHAT:</b>\n'
    for _, row in top_lo.iterrows():
        msg_top_lo += (row['symbol'] + ': ' + f"{row['pct']:+.2f}" + '% | '
                       + f"{row['lai_lo']:+,.0f}" + 'd | '
                       + str(int(row['so_gd'])) + ' GD\n')
    await context.bot.send_message(chat_id=chat_id, text=msg_top_lo, parse_mode='HTML')

    # Luu CSV
    df_r.sort_values('pct', ascending=False).to_csv('ket_qua_scanall.csv', index=False)
    await context.bot.send_message(
        chat_id=chat_id,
        text='Da luu chi tiet vao ket_qua_scanall.csv',
        parse_mode='HTML'
    )

# -------------------- Telegram handlers --------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().upper()
    if not (2 <= len(text) <= 5 and text.isalpha()):
        await update.message.reply_text('Nhap ma co phieu (VD: VCB) hoac /scanall de quet toan bo.')
        return
    await update.message.reply_text(
        '<b>Dang chay backtest cho ' + text + '...</b>\n'
        'Von: 50,000,000d | Tin hieu W | Stop D 10%',
        parse_mode='HTML'
    )
    result = run_backtest(text)
    for msg in format_result(result):
        await update.message.reply_text(msg, parse_mode='HTML')

async def handle_scanall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run_scanall(update, context)

async def post_init(app):
    await app.bot.send_message(
        chat_id=CHAT_ID,
        text=(
            '<b>Bot Backtest san sang!</b>\n' +
            SEP + '\n'
            'Lenh:\n'
            '  [MA]     : backtest 1 ma (VD: VCB)\n'
            '  /scanall : quet + thong ke toan bo\n\n'
            'Thong so:\n'
            '  Von: 50,000,000d | Tu 2023\n'
            '  Tin hieu W | Trailing stop D 10%'
        ),
        parse_mode='HTML'
    )

def main():
    app = (ApplicationBuilder()
           .token(TOKEN)
           .post_init(post_init)
           .build())
    app.add_handler(CommandHandler('scanall', handle_scanall))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print('Bot dang chay...')
    app.run_polling()

if __name__ == '__main__':
    main()
