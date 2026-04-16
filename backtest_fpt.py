# ============================================================
# BACKTEST FPT - CẬP NHẬT VNSTOCK3
# ============================================================
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

# --- CẤU HÌNH ---
API_KEY = 'vnstock_92c86f761ec105508ba230ede06850c7'
SYMBOL = 'FPT'
VN_TZ = timezone(timedelta(hours=7))

# Thiết lập API Key cho vnstock3
os.environ['VNSTOCK_API_KEY'] = API_KEY

def get_data_fpt(symbol):
    try:
        from vnstock3 import Vnstock
        # Khởi tạo Vnstock
        vstock = Vnstock().config(api_key=API_KEY)
        
        # Lấy dữ liệu lịch sử
        # Chú ý: resolution trong vnstock3 dùng '1D'
        df = vstock.stock_historical_data(
            symbol=symbol, 
            start_date='2022-01-01', 
            end_date=datetime.now(VN_TZ).strftime('%Y-%m-%d'), 
            resolution='1D', 
            type='stock'
        )
        
        if df is None or df.empty:
            print(f"❌ Không lấy được dữ liệu cho {symbol}")
            return None
        
        # Chuẩn hóa cột dữ liệu (vnstock3 thường trả về chữ thường)
        df.columns = [c.lower() for c in df.columns]
        t_col = 'time' if 'time' in df.columns else 'date'
        
        df[t_col] = pd.to_datetime(df[t_col])
        df = df.set_index(t_col).sort_index()
        
        # Đổi tên cột để dùng trong tính toán bên dưới
        df = df.rename(columns={
            'open': 'Open', 'high': 'High', 'low': 'Low', 
            'close': 'Close', 'volume': 'Volume'
        })
        return df
    except Exception as e:
        print(f"❌ Lỗi truy vấn dữ liệu: {e}")
        return None

def smma(series, period):
    """Tính đường SMMA cho RSI"""
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

def calculate_rsi(df, period=14):
    delta = df['Close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = smma(gain, period)
    avg_loss = smma(loss, period).replace(0, 0.0001)
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def run_fpt_backtest():
    print(f"🚀 Bắt đầu Backtest mã: {SYMBOL}")
    print("-" * 40)
    
    df = get_data_fpt(SYMBOL)
    if df is None: return

    # Tính toán chỉ báo
    df['RSI'] = calculate_rsi(df)
    df['MA_RSI'] = df['RSI'].rolling(14).mean()
    
    # Khởi tạo vốn và trạng thái
    initial_cap = 50_000_000
    cap = initial_cap
    pos = None # Lưu thông tin lệnh đang mở
    
    # Chỉ backtest dữ liệu từ năm 2023 đến nay
    df_bt = df[df.index >= '2023-01-01'].copy()

    for i in range(1, len(df_bt)):
        current_date = df_bt.index[i]
        row = df_bt.iloc[i]
        prev_row = df_bt.iloc[i-1]

        # Tín hiệu MUA
        if not pos:
            if prev_row['RSI'] <= prev_row['MA_RSI'] and row['RSI'] > row['MA_RSI']:
                pos = {'date': current_date, 'price': row['Close']}
                print(f"🟢 MUA  ngày {current_date.date()} | Giá: {row['Close']:,}")
        
        # Tín hiệu BÁN
        else:
            if prev_row['RSI'] >= prev_row['MA_RSI'] and row['RSI'] < row['MA_RSI']:
                profit_pct = (row['Close'] - pos['price']) / pos['price'] * 100
                cap *= (1 + profit_pct/100)
                print(f"🔴 BÁN  ngày {current_date.date()} | Giá: {row['Close']:,} | Lãi: {profit_pct:.2f}%")
                pos = None

    print("-" * 40)
    final_profit = (cap / initial_cap - 1) * 100
    print(f"✅ Tổng lợi nhuận từ 2023: {final_profit:.2f}%")
    print(f"💰 Vốn cuối cùng: {cap:,.0f} VNĐ")

if __name__ == "__main__":
    run_fpt_backtest()