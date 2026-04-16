# ============================================================
# BACKTEST BOT - TELEGRAM (FIXED FOR BRONZE & VNSTOCK3)
# ============================================================

import os
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

# Thử import vnstock3
try:
    from vnstock3 import Vnstock
except ImportError:
    print("Vui long chay: pip install vnstock3")
    raise

TOKEN   = '8578016275:AAGvL6SoOO3Yifqner8EcynwKt7OKgwl_J0'
CHAT_ID = '7000478479'
API_KEY = 'vnstock_92c86f761ec105508ba230ede06850c7'
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

# -------------------- Khoi tao Vnstock Bronze --------------------
try:
    # Khoi tao engine duy nhat dung chung cho toan bot
    vstock = Vnstock().config(api_key=API_KEY)
except Exception as e:
    logging.error(f"Loi cau hinh API Key: {e}")
    vstock = Vnstock()

# -------------------- Rate limit cho goi Bronze --------------------
_last_call = [0.0]
_lock = threading.Lock()

def rate_limited_sleep():
    with _lock:
        now  = time.time()
        # 0.25s = 4 req/s = 240 req/min (An toan cho Bronze)
        wait = 0.25 - (now - _last_call[0])
        if wait > 0:
            time.sleep(wait)
        _last_call[0] = time.time()

# -------------------- Lay danh sach ma --------------------
def get_all_symbols(filename='vn_stocks_full.txt'):
    try:
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                raw = [line.strip() for line in f if line.strip()]
            symbols = [s for s in raw if 2 <= len(s) <= 5 and s.isalpha()]
            return [s for s in dict.fromkeys(symbols)]
        else:
            # Neu ko co file thi lay tu san chung khoan
            df = vstock.stock_listing()
            return df['ticker'].tolist()
    except:
        return ['ACB', 'SSI', 'FPT', 'TCB', 'VND', 'MWG', 'HPG', 'VIC']

# -------------------- Lay du lieu va Xu ly --------------------
def get_data(symbol):
    rate_limited_sleep()
    try:
        end_date = datetime.now(VN_TZ).strftime('%Y-%m-%d')
        start_date = '2022-01-01'
        
        # Dung ham price.history cua vnstock3
        df = vstock.stock_historical_data(
            symbol=symbol, 
            start_date=start_date, 
            end_date=end_date,
            resolution='1D',
            type='stock'
        )
        
        if df is None or df.empty:
            logging.warning(f"API tra ve rong cho ma: {symbol}")
            return None, None

        # Chuan hoa ten cot (vnstock3 thuong tra ve chu thuong)
        df.columns = [c.lower() for c in df.columns]
        
        # Xu ly index thoi gian
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
            df = df.set_index('time')
        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            
        # Mapping cot de logic backtest chay dung
        df = df.rename(columns={
            'close': 'Close', 'high': 'High', 'low': 'Low', 
            'volume': 'Volume', 'open': 'Open'
        })
        
        df = df.sort_index().dropna(subset=['Close', 'Volume'])
        
        # Tao nen tuan
        weekly = df.resample('W-FRI').agg({
            'Close': 'last', 
            'Volume': 'sum',
            'High': 'max',
            'Low': 'min'
        }).dropna()
        
        if len(weekly) < 25: # Can it nhat 20 phien cho MA20 tuan
            return None, None
            
        return df, weekly
    except Exception as e:
        logging.error(f"Loi truy xuat {symbol}: {e}")
        return None, None

# -------------------- Chi bao ky thuat --------------------
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
    if start == -1: return pd.Series(result, index=series.index)
    result[start] = np.mean(values[start - period + 1: start + 1])
    for i in range(start + 1, len(values)):
        result[i] = (result[i-1] * (period-1) + values[i]) / period
    return pd.Series(result, index=series.index)

def calc_weekly_indicators(weekly):
    df = weekly.copy()
    df['ma20_vol'] = df['Volume'].rolling(20).mean()
    delta          = df['Close'].diff()
    avg_gain       = smma(delta.where(delta > 0, 0.0), 14)
    avg_loss       = smma((-delta).where(delta < 0, 0.0), 14).replace(0, 0.0001)
    df['rsi']      = 100 - (100 / (1 + avg_gain / avg_loss))
    df['sma_rsi']  = df['rsi'].rolling(14).mean()
    return df

def check_buy_signal(df_w, i, vol_pct, trend_n):
    if i < max(1, trend_n + 1): return False
    row  = df_w.iloc[i]
    prev = df_w.iloc[i - 1]
    
    if any(pd.isna(x) for x in [row['Volume'], row['ma20_vol'], row['rsi'], row['sma_rsi']]):
        return False
        
    dk1 = row['Volume'] > (vol_pct / 100) * row['ma20_vol']
    dk2 = prev['rsi'] <= prev['sma_rsi'] and row['rsi'] > row['sma_rsi']
    
    # Kiem tra xu huong SMA RSI tang dan
    dk3 = True
    for k in range(trend_n):
        idx = i - k
        if df_w.iloc[idx]['sma_rsi'] < df_w.iloc[idx-1]['sma_rsi']:
            dk3 = False
            break
            
    return dk1 and dk2 and dk3

# -------------------- Backtest logic --------------------
def run_backtest(symbol, initial_capital=50_000_000, vol_pct=None, trend_n=None, stop_pct=None):
    if vol_pct is None: vol_pct = CONFIG['vol_pct']
    if trend_n is None: trend_n = CONFIG['trend_n']
    if stop_pct is None: stop_pct = CONFIG['stop_pct']

    daily, weekly = get_data(symbol)
    if daily is None or weekly is None:
        return {'error': f'Khong lay duoc du lieu cho ma {symbol}'}

    df_w = calc_weekly_indicators(weekly)
    df_w_bt = df_w[df_w.index >= '2023-01-01']
    if df_w_bt.empty:
        return {'error': 'Du lieu tu 2023 rong'}

    stop_mult = 1 - stop_pct / 100
    daily_list = list(daily[daily.index >= '2023-01-01'].iterrows())
    
    capital = initial_capital
    trades = []
    position = None
    day_idx = 0

    for wi, week_end in enumerate(df_w_bt.index):
        global_wi = df_w.index.get_loc(week_end)
        
        # Neu dang giu hang: Kiem tra trailing stop
        if position:
            sold = False
            while day_idx < len(daily_list):
                d_ts, d_row = daily_list[day_idx]
                if d_ts > week_end: break
                if d_ts <= position['buy_date']: 
                    day_idx += 1
                    continue
                
                # Check stop loss
                if d_row['Low'] <= position['peak'] * stop_mult:
                    exit_price = position['peak'] * stop_mult
                    pct = (exit_price - position['buy_price']) / position['buy_price'] * 100
                    capital = position['cost'] * (1 + pct / 100)
                    trades.append({
                        'stt': len(trades)+1, 'loai': 'Ban', 'ngay_mua': position['buy_date'],
                        'gia_mua': position['buy_price'], 'ngay_ban': d_ts.strftime('%Y-%m-%d'),
                        'gia_ban': exit_price, 'pct': pct, 'von_sau': capital, 'dang_giu': False,
                        'gia_dinh': position['peak'], 'von_dau': position['cost']
                    })
                    position = None
                    sold = True
                    break
                
                if d_row['High'] > position['peak']: position['peak'] = d_row['High']
                day_idx += 1
            
            if sold: continue

        # Check mua
        if not position:
            while day_idx < len(daily_list) and daily_list[day_idx][0] <= week_end:
                day_idx += 1
                
            if check_buy_signal(df_w, global_wi, vol_pct, trend_n):
                bp = df_w_bt.iloc[wi]['Close']
                position = {'buy_date': week_end, 'buy_price': bp, 'peak': bp, 'cost': capital}

    # Chot trang thai cuoi cung
    if position:
        last_ts, last_row = daily_list[-1]
        pct = (last_row['Close'] - position['buy_price']) / position['buy_price'] * 100
        val = position['cost'] * (1 + pct/100)
        trades.append({
            'stt': len(trades)+1, 'loai': 'Dang giu', 'ngay_mua': position['buy_date'],
            'gia_mua': position['buy_price'], 'ngay_ban': last_ts.strftime('%Y-%m-%d'),
            'gia_ban': last_row['Close'], 'pct': pct, 'von_sau': val, 'dang_giu': True,
            'gia_dinh': position['peak'], 'von_dau': position['cost']
        })
        capital = val

    return {
        'symbol': symbol, 'von_ban_dau': initial_capital, 'von_cuoi': capital,
        'pct': (capital/initial_capital - 1)*100, 'trades': trades, 'so_gd': len(trades),
        'vol_pct': vol_pct, 'trend_n': trend_n, 'stop_pct': stop_pct
    }

# -------------------- Telegram Giao dien --------------------
def format_result(r):
    if 'error' in r: return [f"⚠️ {r['error']}"]
    
    header = (
        f"<b>📊 BACKTEST: {r['symbol']}</b>\n"
        f"<i>Cài đặt: Vol > {r['vol_pct']}% | Trend {r['trend_n']}p | Stop {r['stop_pct']}%</i>\n{SEP}\n"
        f"Vốn đầu: {r['von_ban_dau']:,.0f}đ\n"
        f"Vốn cuối: {r['von_cuoi']:,.0f}đ\n"
        f"Lợi nhuận: {r['pct']:+.2f}%\n"
        f"Số lệnh: {r['so_gd']}\n{SEP}"
    )
    
    msgs = [header]
    for t in r['trades']:
        st = "🟢 MUA" if t['dang_giu'] else "🔴 BÁN"
        m = (
            f"<b>#{t['stt']} {st}</b>\n"
            f"Mua: {t['ngay_mua'].strftime('%Y-%m-%d')} @ {t['gia_mua']:,.0f}\n"
            f"Kết: {t['ngay_ban']} @ {t['gia_ban']:,.0f}\n"
            f"Lãi: {t['pct']:+.2f}% | Vốn: {t['von_sau']:,.0f}đ"
        )
        msgs.append(m)
    return msgs

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    ticker = update.message.text.strip().upper()
    if not (2 <= len(ticker) <= 5): return
    
    msg_wait = await update.message.reply_text(f"⏳ Đang tính toán cho {ticker}...")
    res = run_backtest(ticker)
    
    await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=msg_wait.message_id)
    for part in format_result(res):
        await update.message.reply_text(part, parse_mode='HTML')

async def handle_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    text = (
        f"⚙️ <b>THÔNG SỐ HIỆN TẠI</b>\n{SEP}\n"
        f"1. Volume: >{CONFIG['vol_pct']}% MA20\n"
        f"2. Trend: {CONFIG['trend_n']} phiên tăng\n"
        f"3. Trailing Stop: {CONFIG['stop_pct']}%\n\n"
        f"Dùng <code>/set [key] [giá trị]</code> để chỉnh."
    )
    await update.message.reply_text(text, parse_mode='HTML')

async def handle_set(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    try:
        key, val = context.args[0].lower(), float(context.args[1])
        if key == 'vol': CONFIG['vol_pct'] = int(val)
        elif key == 'trend': CONFIG['trend_n'] = int(val)
        elif key == 'stop': CONFIG['stop_pct'] = val
        await update.message.reply_text(f"✅ Đã cập nhật {key} = {val}")
    except:
        await update.message.reply_text("Lỗi cú pháp. VD: /set vol 150")

async def post_init(app):
    await app.bot.send_message(chat_id=CHAT_ID, text="🚀 <b>Bot Bronze Sẵn Sàng!</b>\nNhập mã CP để bắt đầu.", parse_mode='HTML')

def main():
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler('config', handle_config))
    app.add_handler(CommandHandler('set', handle_set))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.run_polling()

if __name__ == '__main__':
    main()