# ============================================================
# BACKTEST BOT - TELEGRAM
# Nhap ma co phieu -> chay backtest khung tuan tu 2023
# Dieu kien mua:
#   1. Volume > 120% MA20(Volume)
#   2. RSI(14) cat len SMA(RSI,14)
#   3. SMA(RSI,14) di ngang hoac tang (so voi tuan truoc)
# Trailing stop: 10% tu dinh cao nhat sau khi mua
# ============================================================

import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes

TOKEN   = '8578016275:AAGvL6SoOO3Yifqner8EcynwKt7OKgwl_J0'
CHAT_ID = '7000478479'
API_KEY = 'vnstock_f9fb6ea7e9ef42cf8472ee293eb9c16a'
VN_TZ   = timezone(timedelta(hours=7))

os.environ['VNSTOCK_API_KEY'] = API_KEY
logging.basicConfig(level=logging.INFO)

SEP = '-' * 35

# -------------------- Lay du lieu --------------------
def get_weekly_data(symbol):
    try:
        from vnstock import Vnstock
        stock = Vnstock().stock(symbol=symbol, source='VCI')
        end   = datetime.now(VN_TZ).strftime('%Y-%m-%d')
        start = '2022-01-01'
        df = stock.quote.history(start=start, end=end, interval='1D')
        if df is None or df.empty:
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
        return weekly
    except Exception as e:
        logging.error('get_weekly_data loi: ' + str(e))
        return None

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

def calc_indicators(weekly):
    df = weekly.copy()
    df['ma20_vol']     = df['Volume'].rolling(20).mean()
    delta              = df['Close'].diff()
    avg_gain           = smma(delta.where(delta > 0, 0.0), 14)
    avg_loss           = smma((-delta).where(delta < 0, 0.0), 14)
    df['rsi']          = 100 - (100 / (1 + avg_gain / avg_loss))
    df['sma_rsi']      = df['rsi'].rolling(14).mean()
    df['sma_rsi_prev'] = df['sma_rsi'].shift(1)
    return df

# -------------------- Dieu kien mua --------------------
def check_buy(df, i):
    if i < 1:
        return False
    row  = df.iloc[i]
    prev = df.iloc[i - 1]
    cols = ['Volume', 'ma20_vol', 'rsi', 'sma_rsi', 'sma_rsi_prev']
    if any(pd.isna(row[c]) for c in cols):
        return False
    if pd.isna(prev['rsi']) or pd.isna(prev['sma_rsi']):
        return False
    dk1 = row['Volume'] > 1.2 * row['ma20_vol']
    dk2 = prev['rsi'] <= prev['sma_rsi'] and row['rsi'] > row['sma_rsi']
    dk3 = row['sma_rsi'] >= row['sma_rsi_prev']
    return dk1 and dk2 and dk3

# -------------------- Backtest --------------------
def run_backtest(symbol, initial_capital=50_000_000):
    weekly = get_weekly_data(symbol)
    if weekly is None or weekly.empty:
        return {'error': 'Khong lay duoc du lieu cho ma ' + symbol}

    df = calc_indicators(weekly)
    df_bt = df[df.index >= '2023-01-01'].copy()
    if df_bt.empty:
        return {'error': 'Khong co du lieu tu 2023'}

    capital  = initial_capital
    trades   = []
    position = None

    for i in range(1, len(df_bt)):
        row      = df_bt.iloc[i]
        week_str = df_bt.index[i].strftime('%Y-%m-%d')
        price    = row['Close']

        if position is not None:
            if price > position['peak']:
                position['peak'] = price
            stop_price = position['peak'] * 0.90
            if price <= stop_price:
                sell_value = position['shares'] * price
                profit     = sell_value - position['cost']
                capital    = sell_value
                trades.append({
                    'stt'      : len(trades) + 1,
                    'loai'     : 'Ban',
                    'tuan_mua' : position['buy_week'],
                    'gia_mua'  : round(position['buy_price'], 2),
                    'tuan_ban' : week_str,
                    'gia_ban'  : round(price, 2),
                    'gia_dinh' : round(position['peak'], 2),
                    'gia_stop' : round(stop_price, 2),
                    'von_dau'  : round(position['cost'], 0),
                    'gia_tri'  : round(sell_value, 0),
                    'lai_lo'   : round(profit, 0),
                    'von_sau'  : round(capital, 0),
                    'dang_giu' : False,
                })
                position = None
            continue

        global_i = df.index.get_loc(df_bt.index[i])
        if check_buy(df, global_i):
            position = {
                'buy_week' : week_str,
                'buy_price': price,
                'shares'   : capital / price,
                'cost'     : capital,
                'peak'     : price,
            }

    # Lenh dang giu chua ban
    if position is not None:
        last_price  = df_bt.iloc[-1]['Close']
        last_week   = df_bt.index[-1].strftime('%Y-%m-%d')
        current_val = position['shares'] * last_price
        profit      = current_val - position['cost']
        trades.append({
            'stt'      : len(trades) + 1,
            'loai'     : 'Dang giu',
            'tuan_mua' : position['buy_week'],
            'gia_mua'  : round(position['buy_price'], 2),
            'tuan_ban' : last_week,
            'gia_ban'  : round(last_price, 2),
            'gia_dinh' : round(position['peak'], 2),
            'gia_stop' : round(position['peak'] * 0.90, 2),
            'von_dau'  : round(position['cost'], 0),
            'gia_tri'  : round(current_val, 0),
            'lai_lo'   : round(profit, 0),
            'von_sau'  : round(current_val, 0),
            'dang_giu' : True,
        })
        capital = current_val

    return {
        'symbol'     : symbol.upper(),
        'von_ban_dau': initial_capital,
        'von_cuoi'   : round(capital, 0),
        'lai_lo'     : round(capital - initial_capital, 0),
        'pct'        : round((capital / initial_capital - 1) * 100, 2),
        'so_gd'      : len(trades),
        'trades'     : trades,
    }

# -------------------- Dinh dang ket qua --------------------
def format_result(r):
    if 'error' in r:
        return ['Loi: ' + r['error']]

    msgs = []
    icon = 'Tang' if r['lai_lo'] >= 0 else 'Giam'

    # Tong quan
    tong = (
        '<b>BACKTEST ' + r['symbol'] + ' (Khung tuan, tu 2023)</b>\n' +
        SEP + '\n'
        'Von ban dau : ' + f"{r['von_ban_dau']:,.0f}" + 'd\n'
        'Von cuoi    : ' + f"{r['von_cuoi']:,.0f}" + 'd\n'
        'Loi nhuan   : ' + f"{r['lai_lo']:+,.0f}" + 'd (' + f"{r['pct']:+.2f}" + '%)\n'
        'So giao dich: ' + str(r['so_gd']) + '\n' +
        SEP
    )
    msgs.append(tong)

    # Chi tiet tung giao dich, moi 4 GD 1 tin
    chunk = []
    for t in r['trades']:
        if t['dang_giu']:
            icon_t  = 'DANG GIU'
            tuan_ky = 'Hien tai'
        else:
            icon_t  = 'BAN'
            tuan_ky = 'Ban'
        lai_str = f"{t['lai_lo']:+,.0f}" + 'd'
        pct_t   = round((t['lai_lo'] / t['von_dau']) * 100, 2)

        dong = (
            '<b>#' + str(t['stt']) + ' ' + icon_t + '</b>\n'
            '  Mua  : tuan ' + t['tuan_mua'] + ' @ ' + f"{t['gia_mua']:,}" + 'd\n'
            '  ' + tuan_ky + ': tuan ' + t['tuan_ban'] + ' @ ' + f"{t['gia_ban']:,}" + 'd\n'
            '  Dinh / Stop: ' + f"{t['gia_dinh']:,}" + 'd / ' + f"{t['gia_stop']:,}" + 'd\n'
            '  Von vao : ' + f"{t['von_dau']:,.0f}" + 'd\n'
            '  Gia tri : ' + f"{t['gia_tri']:,.0f}" + 'd\n'
            '  Lai/Lo  : ' + lai_str + ' (' + f"{pct_t:+.2f}" + '%)\n'
            '  Von sau : ' + f"{t['von_sau']:,.0f}" + 'd'
        )
        chunk.append(dong)
        if len(chunk) == 4:
            msgs.append('\n\n'.join(chunk))
            chunk = []

    if chunk:
        msgs.append('\n\n'.join(chunk))

    return msgs

# -------------------- Telegram handler --------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().upper()

    if not (2 <= len(text) <= 5 and text.isalpha()):
        await update.message.reply_text(
            'Vui long nhap ma co phieu (VD: VCB, HPG, FPT...)',
            parse_mode='HTML'
        )
        return

    await update.message.reply_text(
        '<b>Dang chay backtest cho ' + text + '...</b>\n'
        'Von: 50,000,000d | Khung W | Tu 2023\n'
        'Vui long cho...',
        parse_mode='HTML'
    )

    result = run_backtest(text)
    msgs   = format_result(result)

    for msg in msgs:
        await update.message.reply_text(msg, parse_mode='HTML')

# -------------------- Main --------------------
def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print('Bot dang chay... Nhan ma co phieu vao Telegram de backtest.')
    app.run_polling()

if __name__ == '__main__':
    main()
