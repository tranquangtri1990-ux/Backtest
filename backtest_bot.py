# ============================================================
# BACKTEST BOT - TELEGRAM (FIXED FOR GITHUB ACTIONS & BRONZE)
# ============================================================

import os
import asyncio
import logging
import threading
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, timezone
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CommandHandler,
    filters, ContextTypes
)

# --- KIỂM TRA THƯ VIỆN (SỬA LỖI MODULE NOT FOUND) ---
try:
    from vnstock3 import Vnstock
    V_VERSION = 3
    print("Su dung vnstock phien ban 3")
except ImportError:
    try:
        from vnstock import Vnstock
        V_VERSION = 1
        print("Su dung vnstock phien ban cu")
    except ImportError:
        V_VERSION = 0
        print("CANH BAO: Chua cai dat thu vien vnstock!")

TOKEN   = '8578016275:AAGvL6SoOO3Yifqner8EcynwKt7OKgwl_J0'
CHAT_ID = '7000478479'
API_KEY = 'vnstock_92c86f761ec105508ba230ede06850c7'
VN_TZ   = timezone(timedelta(hours=7))

logging.basicConfig(level=logging.INFO)
SEP = '-' * 35

# -------------------- Tham số --------------------
CONFIG = {
    'vol_pct' : 120,
    'trend_n' : 1,
    'stop_pct': 10,
}

last_activity = [time.time()]

def update_activity():
    last_activity[0] = time.time()

# -------------------- Khởi tạo Engine --------------------
stock_engine = None
if V_VERSION == 3:
    try:
        stock_engine = Vnstock().config(api_key=API_KEY)
    except:
        stock_engine = Vnstock()
elif V_VERSION == 1:
    # Ban cu khong can config api_key theo cach nay
    pass

# -------------------- Hàm lấy dữ liệu (Phần quan trọng nhất) --------------------
def get_data(symbol):
    try:
        end_date = datetime.now(VN_TZ).strftime('%Y-%m-%d')
        start_date = '2022-01-01'
        
        df = None
        if V_VERSION == 3:
            # Cach lay cua vnstock3
            df = stock_engine.stock_historical_data(
                symbol=symbol, 
                start_date=start_date, 
                end_date=end_date,
                resolution='1D', 
                type='stock'
            )
        else:
            # Cach lay cua vnstock cu (fallback)
            from vnstock import Vnstock as Vs
            # Thu lay tu nhieu nguon neu 1 nguon loi
            for src in ['VCI', 'TCBS', 'SSI']:
                try:
                    df = Vs().stock(symbol=symbol, source=src).quote.history(start=start_date, end=end_date)
                    if df is not None and not df.empty: break
                except: continue
        
        if df is None or df.empty:
            return None, None

        # Chuan hoa cot ve chu thuong
        df.columns = [c.lower() for c in df.columns]
        
        # Xu ly index thoi gian
        t_col = 'time' if 'time' in df.columns else 'date'
        if t_col in df.columns:
            df[t_col] = pd.to_datetime(df[t_col])
            df = df.set_index(t_col)
            
        df = df.rename(columns={'close': 'Close', 'high': 'High', 'low': 'Low', 'volume': 'Volume'})
        df = df.sort_index().dropna(subset=['Close', 'Volume'])
        
        # Tao nen tuan
        weekly = df.resample('W-FRI').agg({
            'Close': 'last', 'Volume': 'sum', 'High': 'max', 'Low': 'min'
        }).dropna()
        
        return df, weekly
    except Exception as e:
        logging.error(f"Loi lay du lieu {symbol}: {e}")
        return None, None

# -------------------- Chi bao ky thuat (SMMA cho RSI) --------------------
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
    if start == -1: return pd.Series(result, index=series.index)
    result[start] = np.mean(values[start - period + 1: start + 1])
    for i in range(start + 1, len(values)):
        result[i] = (result[i-1] * (period-1) + values[i]) / period
    return pd.Series(result, index=series.index)

def calc_indicators(weekly):
    df = weekly.copy()
    df['ma20_vol'] = df['Volume'].rolling(20).mean()
    delta = df['Close'].diff()
    avg_gain = smma(delta.where(delta > 0, 0.0), 14)
    avg_loss = smma((-delta).where(delta < 0, 0.0), 14).replace(0, 0.0001)
    df['rsi'] = 100 - (100 / (1 + avg_gain / avg_loss))
    df['sma_rsi'] = df['rsi'].rolling(14).mean()
    return df

def check_buy(df_w, i):
    if i < CONFIG['trend_n'] + 1: return False
    row = df_w.iloc[i]
    prev = df_w.iloc[i-1]
    
    if any(pd.isna(x) for x in [row['Volume'], row['ma20_vol'], row['rsi'], row['sma_rsi']]):
        return False
        
    vol_ok = row['Volume'] > (CONFIG['vol_pct'] / 100) * row['ma20_vol']
    rsi_cross = prev['rsi'] <= prev['sma_rsi'] and row['rsi'] > row['sma_rsi']
    
    trend_ok = True
    for k in range(CONFIG['trend_n']):
        if df_w.iloc[i-k]['sma_rsi'] < df_w.iloc[i-k-1]['sma_rsi']:
            trend_ok = False; break
            
    return vol_ok and rsi_cross and trend_ok

# -------------------- Backtest --------------------
def run_backtest(symbol):
    daily, weekly = get_data(symbol)
    if daily is None or weekly is None or len(daily) < 20:
        return {'error': f'DL cho ma {symbol} khong du hoac loi API'}

    df_w = calc_indicators(weekly)
    df_w_bt = df_w[df_w.index >= '2023-01-01']
    if df_w_bt.empty: return {'error': 'Ma moi niem yet, DL tu 2023 rong'}

    capital = 50_000_000
    trades = []
    pos = None
    stop_mult = 1 - CONFIG['stop_pct'] / 100
    daily_list = list(daily[daily.index >= '2023-01-01'].iterrows())
    d_idx = 0

    for wi, w_date in enumerate(df_w_bt.index):
        g_idx = df_w.index.get_loc(w_date)
        
        if pos:
            while d_idx < len(daily_list):
                dt, row = daily_list[d_idx]
                if dt > w_date: break
                if dt <= pos['d']: (d_idx := d_idx + 1); continue
                
                if row['Low'] <= pos['peak'] * stop_mult:
                    price = pos['peak'] * stop_mult
                    pct = (price - pos['p']) / pos['p'] * 100
                    capital = pos['c'] * (1 + pct/100)
                    trades.append({'stt': len(trades)+1, 'type': 'Ban', 'd1': pos['d'], 'p1': pos['p'], 'd2': dt.strftime('%Y-%m-%d'), 'p2': price, 'pct': pct, 'v2': capital})
                    pos = None; break
                if row['High'] > pos['peak']: pos['peak'] = row['High']
                d_idx += 1
            if not pos: continue

        if not pos:
            while d_idx < len(daily_list) and daily_list[d_idx][0] <= w_date: d_idx += 1
            if check_buy(df_w, g_idx):
                p = df_w_bt.iloc[wi]['Close']
                pos = {'d': w_date, 'p': p, 'peak': p, 'c': capital}

    if pos:
        last_dt, last_row = daily_list[-1]
        pct = (last_row['Close'] - pos['p']) / pos['p'] * 100
        trades.append({'stt': len(trades)+1, 'type': 'Giu', 'd1': pos['d'], 'p1': pos['p'], 'd2': last_dt.strftime('%Y-%m-%d'), 'p2': last_row['Close'], 'pct': pct, 'v2': pos['c']*(1+pct/100)})
        capital = pos['c']*(1+pct/100)

    return {'symbol': symbol, 'cap': capital, 'pct': (capital/50_000_000-1)*100, 'trades': trades}

# -------------------- Telegram Handlers --------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    ticker = update.message.text.strip().upper()
    if not (2 <= len(ticker) <= 5): return
    
    await update.message.reply_text(f"🔍 Dang phan tich {ticker}...")
    res = run_backtest(ticker)
    
    if 'error' in res:
        await update.message.reply_text(f"⚠️ {res['error']}")
        return
        
    msg = f"<b>📊 {res['symbol']} (Bronze Ready)</b>\nLai/Lo: {res['pct']:+.2f}%\nVon cuoi: {res['cap']:,.0f}đ\n{SEP}\n"
    for t in res['trades']:
        icon = "🟢" if t['type'] == 'Giu' else "🔴"
        msg += f"{icon} #{t['stt']}: {t['pct']:+.1f}% ({t['p1']:,.0f} -> {t['p2']:,.0f})\n"
    await update.message.reply_text(msg, parse_mode='HTML')

async def handle_set(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    try:
        k, v = context.args[0].lower(), float(context.args[1])
        if k == 'vol': CONFIG['vol_pct'] = int(v)
        elif k == 'trend': CONFIG['trend_n'] = int(v)
        elif k == 'stop': CONFIG['stop_pct'] = v
        await update.message.reply_text(f"✅ Da set {k} = {v}")
    except:
        await update.message.reply_text("Sai cu phap. VD: /set vol 150")

async def post_init(app):
    status = "Bronze" if V_VERSION == 3 else "Basic"
    await app.bot.send_message(chat_id=CHAT_ID, text=f"🚀 <b>Bot san sang ({status})!</b>\nNí nhap ma CP (ACB, FPT...) de backtest.", parse_mode='HTML')

def main():
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler('set', handle_set))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.run_polling()

if __name__ == '__main__':
    main()