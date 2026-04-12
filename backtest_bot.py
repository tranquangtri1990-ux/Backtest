# ============================================================
# BACKTEST BOT - TELEGRAM
# - Tin hieu mua: dong tuan thoa 3 dieu kien
# - Gia mua    : gia dong cua tuan tin hieu
# - Trailing stop theo ngay (khong nhin truoc):
#     + Moi ngay: cap nhat peak TRUOC, roi moi check stop
#     + Ban khi: gia dong cua ngay <= peak_hom_qua * 90%
#       (peak_hom_qua = peak toi da cua tat ca ngay truoc do)
# - Loi nhuan: (gia ban - gia mua) / gia mua * 100%
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
        df = df.sort_index().dropna(subset=['Close', 'Volume'])
        weekly = df.resample('W-FRI').agg({'Close': 'last', 'Volume': 'sum'}).dropna()
        return df, weekly
    except Exception as e:
        logging.error('get_data loi: ' + str(e))
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

# -------------------- Backtest --------------------
def run_backtest(symbol, initial_capital=50_000_000):
    daily, weekly = get_data(symbol)
    if daily is None or weekly is None:
        return {'error': 'Khong lay duoc du lieu cho ma ' + symbol}

    df_w    = calc_weekly_indicators(weekly)
    df_w_bt = df_w[df_w.index >= '2023-01-01']
    if df_w_bt.empty:
        return {'error': 'Khong co du lieu tu 2023'}

    # Lay toan bo du lieu ngay tu 2023 tro di sap xep theo thoi gian
    daily_bt = daily[daily.index >= '2023-01-01'].copy()
    daily_list = list(daily_bt.iterrows())  # [(timestamp, row), ...]

    capital  = initial_capital
    trades   = []
    position = None
    day_idx  = 0  # Con tro ngay, chi chay tien, khong quay lui

    week_dates = df_w_bt.index.tolist()

    for wi, week_end in enumerate(week_dates):
        global_wi = df_w.index.get_loc(week_end)

        # ---- Dang giu lenh: duyet tung ngay trong tuan nay ----
        if position is not None:
            # Tim ngay bat dau cua tuan nay (sau ngay mua)
            sold = False

            while day_idx < len(daily_list):
                day_ts, day_row = daily_list[day_idx]

                # Chi xu ly ngay nam trong tuan nay va sau ngay mua
                if day_ts > week_end:
                    break  # Chua den tuan nay, thoat vong while
                if day_ts <= position['buy_date']:
                    day_idx += 1
                    continue  # Bo qua ngay mua va truoc do

                close = day_row['Close']

                # Stop gia dua tren peak TRUOC khi cap nhat
                # (peak la dinh cua tat ca ngay truoc ngay hom nay)
                stop_price = position['peak'] * 0.90

                # Kiem tra dieu kien ban TRUOC khi cap nhat peak
                if close <= stop_price:
                    gia_mua   = position['buy_price']
                    pct_trade = (close - gia_mua) / gia_mua * 100
                    von_vao   = position['cost']
                    capital   = von_vao * (1 + pct_trade / 100)

                    trades.append({
                        'stt'      : len(trades) + 1,
                        'loai'     : 'Ban',
                        'ngay_mua' : position['buy_date'].strftime('%Y-%m-%d'),
                        'gia_mua'  : round(gia_mua, 2),
                        'ngay_ban' : day_ts.strftime('%Y-%m-%d'),
                        'gia_ban'  : round(close, 2),
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

                # Cap nhat peak SAU khi da check stop
                if close > position['peak']:
                    position['peak'] = close

                day_idx += 1

            if sold:
                # Sau khi ban, kiem tra tin hieu mua moi cuoi tuan nay
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

        # ---- Chua co lenh: kiem tra tin hieu mua cuoi tuan ----
        # Truoc het cap nhat con tro ngay qua het tuan nay
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

    # ---- Lenh dang giu chua ban: duyet het cac ngay con lai ----
    if position is not None:
        while day_idx < len(daily_list):
            day_ts, day_row = daily_list[day_idx]
            if day_ts <= position['buy_date']:
                day_idx += 1
                continue

            close      = day_row['Close']
            stop_price = position['peak'] * 0.90

            if close <= stop_price:
                # Bi stop o ngay cuoi - coi nhu da ban
                gia_mua   = position['buy_price']
                pct_trade = (close - gia_mua) / gia_mua * 100
                von_vao   = position['cost']
                capital   = von_vao * (1 + pct_trade / 100)
                trades.append({
                    'stt'      : len(trades) + 1,
                    'loai'     : 'Ban',
                    'ngay_mua' : position['buy_date'].strftime('%Y-%m-%d'),
                    'gia_mua'  : round(gia_mua, 2),
                    'ngay_ban' : day_ts.strftime('%Y-%m-%d'),
                    'gia_ban'  : round(close, 2),
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
                break

            if close > position['peak']:
                position['peak'] = close
            day_idx += 1

        # Van dang giu den hien tai
        if position is not None:
            last_ts, last_row = daily_list[-1]
            last_close        = last_row['Close']
            gia_mua           = position['buy_price']
            pct_trade         = (last_close - gia_mua) / gia_mua * 100
            von_vao           = position['cost']
            current           = von_vao * (1 + pct_trade / 100)
            capital           = current
            trades.append({
                'stt'      : len(trades) + 1,
                'loai'     : 'Dang giu',
                'ngay_mua' : position['buy_date'].strftime('%Y-%m-%d'),
                'gia_mua'  : round(gia_mua, 2),
                'ngay_ban' : last_ts.strftime('%Y-%m-%d'),
                'gia_ban'  : round(last_close, 2),
                'gia_dinh' : round(position['peak'], 2),
                'gia_stop' : round(position['peak'] * 0.90, 2),
                'von_dau'  : round(von_vao, 0),
                'gia_tri'  : round(current, 0),
                'pct'      : round(pct_trade, 2),
                'lai_lo'   : round(current - von_vao, 0),
                'von_sau'  : round(current, 0),
                'dang_giu' : True,
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

# -------------------- Dinh dang ket qua --------------------
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
            '  Mua     : ' + t['ngay_mua'] + ' @ ' + f"{t['gia_mua']:,}" + 'd\n'
            '  ' + label + ': ' + t['ngay_ban'] + ' @ ' + f"{t['gia_ban']:,}" + 'd\n'
            '  Dinh/Stop: ' + f"{t['gia_dinh']:,}" + 'd / ' + f"{t['gia_stop']:,}" + 'd\n'
            '  Von vao  : ' + f"{t['von_dau']:,.0f}" + 'd\n'
            '  Von sau  : ' + f"{t['von_sau']:,.0f}" + 'd\n'
            '  Lai/Lo   : ' + f"{t['lai_lo']:+,.0f}" + 'd (' + f"{t['pct']:+.2f}" + '%)'
        )
        chunk.append(dong)
        if len(chunk) == 4:
            msgs.append('\n\n'.join(chunk))
            chunk = []
    if chunk:
        msgs.append('\n\n'.join(chunk))

    return msgs

# -------------------- Telegram --------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().upper()
    if not (2 <= len(text) <= 5 and text.isalpha()):
        await update.message.reply_text('Vui long nhap ma co phieu (VD: VCB, HPG, CDC...)')
        return
    await update.message.reply_text(
        '<b>Dang chay backtest cho ' + text + '...</b>\n'
        'Von: 50,000,000d | Tin hieu W | Stop D 10%\n'
        'Vui long cho...',
        parse_mode='HTML'
    )
    result = run_backtest(text)
    for msg in format_result(result):
        await update.message.reply_text(msg, parse_mode='HTML')

async def post_init(app):
    await app.bot.send_message(
        chat_id=CHAT_ID,
        text=(
            '<b>Bot Backtest da san sang!</b>\n' +
            SEP + '\n'
            'Nhap ma co phieu de chay backtest.\n'
            'Vi du: VCB, HPG, FPT, CDC...\n\n'
            'Thong so:\n'
            '  Von ban dau : 50,000,000d\n'
            '  Tin hieu mua: dong tuan\n'
            '  Trailing stop: theo ngay, 10%\n'
            '  Du lieu tu  : 2023 den hien tai'
        ),
        parse_mode='HTML'
    )

def main():
    app = (ApplicationBuilder()
           .token(TOKEN)
           .post_init(post_init)
           .build())
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print('Bot dang chay...')
    app.run_polling()

if __name__ == '__main__':
    main()