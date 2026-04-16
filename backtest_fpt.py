import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, timezone
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO)

# Kiểm tra thư viện vnstock3
try:
    from vnstock3 import Vnstock
except ImportError:
    print("ERR: Chua cai dat vnstock3")
    exit()

API_KEY = 'vnstock_92c86f761ec105508ba230ede06850c7'
CONFIG = {'vol_pct': 120, 'trend_n': 1, 'stop_pct': 10}

def get_data_fpt():
    try:
        vstock = Vnstock().config(api_key=API_KEY)
        symbol = "FPT"
        # Lấy từ 2022 để đủ dữ liệu tính SMA nến tuần
        df = vstock.stock_historical_data(symbol=symbol, start_date='2022-01-01', end_date=datetime.now().strftime('%Y-%m-%d'), resolution='1D', type='stock')
        
        if df is None or df.empty:
            return None, None

        # 1. CHUẨN HÓA TÊN CỘT (Sửa lỗi chí tử cho gói Bronze)
        df.columns = [c.lower() for c in df.columns]
        
        # 2. Xử lý thời gian
        t_col = 'time' if 'time' in df.columns else 'date'
        df[t_col] = pd.to_datetime(df[t_col])
        df = df.set_index(t_col)
        
        # 3. Đổi tên cột để code tính toán hiểu được
        df = df.rename(columns={'close': 'Close', 'volume': 'Volume', 'high': 'High', 'low': 'Low'})
        df = df.sort_index().dropna(subset=['Close', 'Volume'])
        
        # 4. Tạo nến tuần
        weekly = df.resample('W-FRI').agg({
            'Close': 'last', 'Volume': 'sum', 'High': 'max', 'Low': 'min'
        }).dropna()
        
        return df, weekly
    except Exception as e:
        logging.error(f"Loi lay du lieu: {e}")
        return None, None

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

def backtest_fpt():
    daily, weekly = get_data_fpt()
    if daily is None or weekly is None:
        print("❌ Khong lay duoc du lieu FPT")
        return

    # Tính chỉ báo
    df_w = weekly.copy()
    df_w['ma20_vol'] = df_w['Volume'].rolling(20).mean()
    delta = df_w['Close'].diff()
    avg_g = smma(delta.where(delta > 0, 0.0), 14)
    avg_l = smma((-delta).where(delta < 0, 0.0), 14).replace(0, 0.0001)
    df_w['rsi'] = 100 - (100 / (1 + avg_g / avg_l))
    df_w['sma_rsi'] = df_w['rsi'].rolling(14).mean()

    # Backtest từ 2023
    df_w_bt = df_w[df_w.index >= '2023-01-01']
    cap = 50_000_000
    pos = None
    trades = []
    stop_mult = 1 - CONFIG['stop_pct'] / 100
    daily_list = list(daily[daily.index >= '2023-01-01'].iterrows())
    d_idx = 0

    print(f"🚀 Bat dau Backtest FPT với vốn {cap:,.0f}đ")

    for w_date in df_w_bt.index:
        idx = df_w.index.get_loc(w_date)
        
        if pos:
            while d_idx < len(daily_list):
                dt, row = daily_list[d_idx]
                if dt > w_date: break
                if dt <= pos['d']: (d_idx := d_idx + 1); continue
                if row['Low'] <= pos['peak'] * stop_mult:
                    price = pos['peak'] * stop_mult
                    pct = (price - pos['p']) / pos['p'] * 100
                    cap = pos['c'] * (1 + pct/100)
                    trades.append(f"🔴 Bán {dt.date()} | Lãi: {pct:+.1f}% | Vốn: {cap:,.0f}")
                    pos = None; break
                if row['High'] > pos['peak']: pos['peak'] = row['High']
                d_idx += 1
            if not pos: continue

        if not pos and idx > 1:
            row_w = df_w.iloc[idx]
            prev_w = df_w.iloc[idx-1]
            # Điều kiện mua: Vol > 120% MA20 và RSI cắt lên SMA_RSI
            if row_w['Volume'] > (CONFIG['vol_pct']/100)*row_w['ma20_vol'] and prev_w['rsi'] <= prev_w['sma_rsi'] and row_w['rsi'] > row_w['sma_rsi']:
                pos = {'d': w_date, 'p': row_w['Close'], 'peak': row_w['Close'], 'c': cap}
                trades.append(f"🟢 Mua {w_date.date()} | Giá: {pos['p']:,.0f}")

    print("\n--- KET QUA GIAO DICH ---")
    for t in trades: print(t)
    print(f"\n✅ Tong ket: {cap:,.0f}đ ({(cap/50_000_000-1)*100:+.2f}%)")

if __name__ == "__main__":
    backtest_fpt()
