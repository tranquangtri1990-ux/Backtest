# ============================================================
# BACKTEST BOT - TELEGRAM (OPTIMIZED FOR BRONZE & VNSTOCK3)
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

# Import thư viện vnstock3
try:
    from vnstock3 import Vnstock
except ImportError:
    # Fallback dự phòng
    from vnstock import Vnstock

TOKEN   = '8578016275:AAGvL6SoOO3Yifqner8EcynwKt7OKgwl_J0'
CHAT_ID = '7000478479'
API_KEY = 'vnstock_92c86f761ec105508ba230ede06850c7'
VN_TZ   = timezone(timedelta(hours=7))

logging.basicConfig(level=logging.INFO)
SEP = '-' * 35

# -------------------- Tham số mặc định --------------------
CONFIG = {
    'vol_pct' : 120,
    'trend_n' : 1,
    'stop_pct': 10,
}

last_activity = [time.time()]

def update_activity():
    last_activity[0] = time.time()

# -------------------- Khởi tạo Vnstock Bronze --------------------
try:
    # Sử dụng vnstock3 config cho Bronze
    vstock = Vnstock().config(api_key=API_KEY)
except:
    vstock = Vnstock()

# -------------------- Hàm lấy dữ liệu (Sửa lỗi triệt để) --------------------
def get_data(symbol):
    try:
        # Gói Bronze hỗ trợ lấy dữ liệu từ lâu, ta lấy từ 2022 để tính MA20 tuần
        end_date = datetime.now(VN_TZ).strftime('%Y-%m-%d')
        start_date = '2022-01-01'
        
        # Sử dụng hàm chuẩn của vnstock3
        df = vstock.stock_historical_data(
            symbol=symbol, 
            start_date=start_date, 
            end_date=end_date,
            resolution='1D',
            type='stock'
        )
        
        if df is None or df.empty:
            return None, None

        # Chuẩn hóa cột về chữ thường để tránh lỗi giữa các phiên bản vnstock
        df.columns = [c.lower() for c in df.columns]
        
        # Xử lý cột thời gian thành Index
        time_col = 'time' if 'time' in df.columns else 'date'
        if time_col in df.columns:
            df[time_col] = pd.to_datetime(df[time_col])
            df = df.set_index(time_col)
            
        # Mapping lại các cột cần thiết cho logic backtest
        df = df.rename(columns={
            'close': 'Close', 
            'high': 'High', 
            'low': 'Low', 
            'volume': 'Volume'
        })
        
        df = df.sort_index().dropna(subset=['Close', 'Volume'])
        
        # Tạo nến tuần (W-FRI: Kết tuần vào thứ 6)
        weekly = df.resample('W-FRI').agg({
            'Close': 'last', 
            'Volume': 'sum', 
            'High': 'max', 
            'Low': 'min'
        }).dropna()
        
        if len(weekly) < 25: # Cần tối thiểu dữ liệu để tính RSI/MA20
            return None, None
            
        return df, weekly
    except Exception as e:
        logging.error(f"Lỗi truy xuất {symbol}: {e}")
        return None, None

# -------------------- Chỉ báo kỹ thuật --------------------
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
    # MA20 Volume tuần
    df['ma20_vol'] = df['Volume'].rolling(20).mean()
    # RSI 14 tuần
    delta = df['Close'].diff()
    avg_gain = smma(delta.where(delta > 0, 0.0), 14)
    avg_loss = smma((-delta).where(delta < 0, 0.0), 14).replace(0, 0.0001)
    df['rsi'] = 100 - (100 / (1 + avg_gain / avg_loss))
    # SMA 14 của RSI
    df['sma_rsi'] = df['rsi'].rolling(14).mean()
    return df

def check_buy(df_w, i):
    if i < CONFIG['trend_n'] + 1: return False
    row = df_w.iloc[i]
    prev = df_w.iloc[i-1]
    
    if any(pd.isna(x) for x in [row['Volume'], row['ma20_vol'], row['rsi'], row['sma_rsi']]):
        return False
        
    # Điều kiện 1: Volume vượt % so với trung bình
    vol_ok = row['Volume'] > (CONFIG['vol_pct'] / 100) * row['ma20_vol']
    # Điều kiện 2: RSI cắt lên SMA RSI
    rsi_cross = prev['rsi'] <= prev['sma_rsi'] and row['rsi'] > row['sma_rsi']
    # Điều kiện 3: Xu hướng SMA RSI đang tăng (Trend)
    trend_ok = True
    for k in range(CONFIG['trend_n']):
        if df_w.iloc[i-k]['sma_rsi'] < df_w.iloc[i-k-1]['sma_rsi']:
            trend_ok = False
            break
            
    return vol_ok and rsi_cross and trend_ok

# -------------------- Backtest Logic --------------------
def run_backtest(symbol):
    daily, weekly = get_data(symbol)
    if daily is None or weekly is None:
        return {'error': f'Không lấy được dữ liệu cho mã {symbol}'}

    df_w = calc_indicators(weekly)
    # Chỉ backtest từ năm 2023
    df_w_bt = df_w[df_w.index >= '2023-01-01']
    if df_w_bt.empty: return {'error': 'Dữ liệu từ 2023 trống'}

    capital = 50_000_000
    trades = []
    pos = None
    stop_mult = 1 - CONFIG['stop_pct'] / 100
    daily_list = list(daily[daily.index >= '2023-01-01'].iterrows())
    d_idx = 0

    for wi, w_date in enumerate(df_w_bt.index):
        g_idx = df_w.index.get_loc(w_date)
        
        if pos: # Đang giữ hàng
            while d_idx < len(daily_list):
                dt, row = daily_list[d_idx]
                if dt > w_date: break
                if dt <= pos['d']: d_idx += 1; continue
                
                # Check Trailing Stop
                if row['Low'] <= pos['peak'] * stop_mult:
                    price = pos['peak'] * stop_mult
                    pct = (price - pos['p']) / pos['p'] * 100
                    capital = pos['c'] * (1 + pct/100)
                    trades.append({
                        'stt': len(trades)+1, 'type': 'Bán', 
                        'd1': pos['d'].strftime('%Y-%m-%d'), 'p1': pos['p'], 
                        'd2': dt.strftime('%Y-%m-%d'), 'p2': price, 
                        'pct': pct, 'v2': capital
                    })
                    pos = None; break
                if row['High'] > pos['peak']: pos['peak'] = row['High']
                d_idx += 1
            if not pos: continue

        if not pos: # Tìm điểm mua
            while d_idx < len(daily_list) and daily_list[d_idx][0] <= w_date: d_idx += 1
            if check_buy(df_w, g_idx):
                p = df_w_bt.iloc[wi]['Close']
                pos = {'d': w_date, 'p': p, 'peak': p, 'c': capital}

    # Nếu cuối kỳ vẫn đang giữ hàng
    if pos:
        last_dt, last_row = daily_list[-1]
        pct = (last_row['Close'] - pos['p']) / pos['p'] * 100
        capital_final = pos['c']*(1+pct/100)
        trades.append({
            'stt': len(trades)+1, 'type': 'Giữ', 
            'd1': pos['d'].strftime('%Y-%m-%d'), 'p1': pos['p'], 
            'd2': last_dt.strftime('%Y-%m-%d'), 'p2': last_row['Close'], 
            'pct': pct, 'v2': capital_final
        })
        capital = capital_final

    return {
        'symbol': symbol.upper(), 
        'cap': capital, 
        'pct': (capital/50_000_000-1)*100, 
        'trades': trades
    }

# -------------------- Telegram Handlers --------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    ticker = update.message.text.strip().upper()
    if not (2 <= len(ticker) <= 5): return
    
    await update.message.reply_text(f"⏳ Đang chạy backtest cho {ticker}...")
    res = run_backtest(ticker)
    
    if 'error' in res:
        await update.message.reply_text(f"⚠️ {res['error']}")
        return
        
    msg = (
        f"<b>📊 KẾT QUẢ: {res['symbol']}</b>\n"
        f"Lợi nhuận: {res['pct']:+.2f}%\n"
        f"Vốn cuối: {res['cap']:,.0f}đ\n"
        f"Cài đặt: Vol >{CONFIG['vol_pct']}% | Trend {CONFIG['trend_n']}p\n{SEP}\n"
    )
    for t in res['trades']:
        icon = "🟢" if t['type'] == 'Giữ' else "🔴"
        msg += f"{icon} #{t['stt']}: {t['pct']:+.1f}% ({t['p1']:,.0f} -> {t['p2']:,.0f})\n"
        msg += f"   <i>({t['d1']} đến {t['d2']})</i>\n\n"
        
    await update.message.reply_text(msg, parse_mode='HTML')

async def handle_set(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_activity()
    try:
        k, v = context.args[0].lower(), float(context.args[1])
        if k == 'vol': CONFIG['vol_pct'] = int(v)
        elif k == 'trend': CONFIG['trend_n'] = int(v)
        elif k == 'stop': CONFIG['stop_pct'] = v
        await update.message.reply_text(f"✅ Đã cập nhật {k} = {v}")
    except:
        await update.message.reply_text("Sai cú pháp. VD: /set vol 150")

async def post_init(app):
    await app.bot.send_message(chat_id=CHAT_ID, text="🚀 <b>Bot Backtest Bronze Sẵn Sàng!</b>\nNhập mã CP để xem kết quả.", parse_mode='HTML')

def main():
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler('set', handle_set))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.run_polling()

if __name__ == '__main__':
    main()