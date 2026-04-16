# ============================================================
# BACKTEST BOT - TELEGRAM (FINAL BRONZE FIX)
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

# Kiem tra thu vien
try:
    from vnstock3 import Vnstock
    V_VER = 3
except ImportError:
    from vnstock import Vnstock
    V_VER = 1

TOKEN   = '8578016275:AAGvL6SoOO3Yifqner8EcynwKt7OKgwl_J0'
CHAT_ID = '7000478479'
API_KEY = 'vnstock_92c86f761ec105508ba230ede06850c7'
VN_TZ   = timezone(timedelta(hours=7))

logging.basicConfig(level=logging.INFO)
SEP = '-' * 35

CONFIG = {'vol_pct': 120, 'trend_n': 1, 'stop_pct': 10}
last_activity = [time.time()]

def update_activity():
    last_activity[0] = time.time()

# Khoi tao engine Bronze
try:
    stock_engine = Vnstock().config(api_key=API_KEY) if V_VER == 3 else Vnstock()
except:
    stock_engine = None

# --- HAM LAY DU LIEU FIX TRIET DE ---
def get_data(symbol):
    try:
        end_date = datetime.now(VN_TZ).strftime('%Y-%m-%d')
        start_date = '2022-01-01' # Lay du de tinh MA20 tuan
        
        df = None
        if V_VER == 3:
            # Dung ham moi nhat cua vnstock3 cho goi Bronze
            df = stock_engine.stock_historical_data(symbol=symbol, start_date=start_date, end_date=end_date, resolution='1D', type='stock')
        else:
            from vnstock import Vnstock as Vs
            df = Vs().stock(symbol=symbol, source='TCBS').quote.history(start=start_date, end=end_date)
        
        if df is None or df.empty: return None, None

        # Chuan hoa ten cot ve chu thuong
        df.columns = [c.lower() for c in df.columns]
        
        # Chuyen cot thoi gian sang Index
        for col in ['time', 'date', 'datetime']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                df = df.set_index(col)
                break
            
        # Mapping lai ten cot chuan cho logic
        df = df.rename(columns={'close': 'Close', 'high': 'High', 'low': 'Low', 'volume': 'Volume', 'open': 'Open'})
        df = df.sort_index().dropna(subset=['Close', 'Volume'])
        
        # Tinh toan nen tuan (Quan trong: RESAMPLE)
        weekly = df.resample('W-FRI').agg({
            'Close': 'last', 
            'Volume': 'sum', 
            'High': 'max', 
            'Low': 'min'
        }).dropna()
        
        if len(weekly) < 20: return None, None
            
        return df, weekly
    except Exception as e:
        logging.error(f"Loi {symbol}: {e}")
        return None, None

# --- CHI BAO ---
def smma(series, period):
    values = series.values.astype(float)
    result = np.full(len(values), np.nan)
    count, start = 0, -1
    for i, v in enumerate(values):
        if not np.isnan(v):
            count += 1
            if count == period: (start := i); break
    if start == -1: return pd.Series(result, index=series.index)
    result[start] = np.mean(values[start - period + 1: start + 1])
    for i in range(start + 1, len(values)):
        result[i] = (result[i-1] * (period-1) + values[i]) / period
    return pd.Series(result, index=series.index)

def calc_indicators(weekly):
    df = weekly.copy()
    df['ma20_vol'] = df['Volume'].rolling(20).mean()
    delta = df['Close'].diff()
    avg_g = smma(delta.where(delta > 0, 0.0), 14)
    avg_l = smma((-delta).where(delta < 0, 0.0), 14).replace(0, 0.0001)
    df['rsi'] = 100 - (100 / (1 + avg_g / avg_l))
    df['sma_rsi'] = df['rsi'].rolling(14).mean()
    return df

# --- BACKTEST ---
def run_backtest(symbol):
    daily, weekly = get_data(symbol)
    if daily is None or weekly is None: return {'error': f'Khong lay duoc DL ma {symbol}'}

    df_w = calc_indicators(weekly)
    df_w_bt = df_w[df_w.index >= '2023-01-01']
    if df_w_bt.empty: return {'error': 'Dữ liệu quá ngắn'}

    cap = 50_000_000
    trades = []
    pos = None
    stop_m = 1 - CONFIG['stop_pct'] / 100
    daily_list = list(daily[daily.index >= '2023-01-01'].iterrows())
    d_idx = 0

    for wi, w_date in enumerate(df_w_bt.index):
        g_idx = df_w.index.get_loc(w_date)
        if pos:
            while d_idx < len(daily_list):
                dt, row = daily_list[d_idx]
                if dt > w_date: break
                if dt <= pos['d']: (d_idx := d_idx + 1); continue
                if row['Low'] <= pos['peak'] * stop_m:
                    price = pos['peak'] * stop_m
                    pct = (price - pos['p']) / pos['p'] * 100
                    cap = pos['c'] * (1 + pct/100)
                    trades.append({'stt': len(trades)+1, 'type': 'Ban', 'd1': pos['d'], 'p1': pos['p'], 'd2': dt.strftime('%Y-%m-%d'), 'p2': price, 'pct': pct, 'v2': cap})
                    pos = None; break
                if row['High'] > pos['peak']: pos['peak'] = row['High']
                d_idx += 1
            if not pos: continue

        if not pos:
            while d_idx < len(daily_list) and daily_list[d_idx][0] <= w_date: d_idx += 1
            # Check Buy
            if g_idx >= 2:
                row_w = df_w.iloc[g_idx]
                prev_w = df_w.iloc[g_idx-1]
                if not any(pd.isna(x) for x in [row_w['Volume'], row_w['ma20_vol'], row_w['rsi'], row_w['sma_rsi']]):
                    if row_w['Volume'] > (CONFIG['vol_pct']/100)*row_w['ma20_vol'] and prev_w['rsi'] <= prev_w['sma_rsi'] and row_w['rsi'] > row_w['sma_rsi']:
                        pos = {'d': w_date, 'p': row_w['Close'], 'peak': row_w['Close'], 'c': cap}

    if pos:
        last_dt, last_row = daily_list[-1]
        pct = (last_row['Close'] - pos['p']) / pos['p'] * 100
        trades.append({'stt': len(trades)+1, 'type': 'Giu', 'd1': pos['d'], 'p1': pos['p'], 'd2': last_dt.strftime('%Y-%m-%d'), 'p2': last_row['Close'], 'pct': pct, 'v2': pos['c']*(1+pct/100)})
        cap = pos['c']*(1+pct/100)

    return {'symbol': symbol, 'cap': cap, 'pct': (cap/50_000_000-1)*100, 'trades': trades}

# --- HANDLERS ---
async def handle_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    ticker = update.message.text.strip().upper()
    if not (2 <= len(ticker) <= 5): return
    
    msg_wait = await update.message.reply_text(f"⏳ Dang backtest {ticker}...")
    res = run_backtest(ticker)
    await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=msg_wait.message_id)
    
    if 'error' in res:
        await update.message.reply_text(f"⚠️ {res['error']}")
        return
        
    m = f"<b>📊 {res['symbol']}</b>\nLợi nhuận: {res['pct']:+.2f}%\nVốn cuối: {res['cap']:,.0f}đ\n{SEP}\n"
    for t in res['trades']:
        icon = "🟢" if t['type'] == 'Giu' else "🔴"
        m += f"{icon} #{t['stt']}: {t['pct']:+.1f}% ({t['p1']:,.0f} -> {t['p2']:,.0f})\n"
    await update.message.reply_text(m, parse_mode='HTML')

async def post_init(app):
    await app.bot.send_message(chat_id=CHAT_ID, text="🚀 <b>Bot san sang (Bronze)!</b>\nNhap ma CP (ACB, FPT...) de test.", parse_mode='HTML')

def main():
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))
    app.run_polling()

if __name__ == '__main__': main()