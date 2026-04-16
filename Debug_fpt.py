# ============================================================
# SCRIPT DEBUG RIÊNG CHO MÃ FPT - KIỂM TRA LỖI DỮ LIỆU
# ============================================================

import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Thiết lập log để xem chi tiết
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

API_KEY = 'vnstock_92c86f761ec105508ba230ede06850c7'

def debug_fpt():
    print("🚀 Bắt đầu quá trình Debug mã FPT...")
    
    # 1. Kiểm tra thư viện
    try:
        from vnstock3 import Vnstock
        print("✅ Đã tìm thấy thư viện vnstock3")
    except ImportError:
        print("❌ Lỗi: Chưa cài đặt vnstock3. Chạy 'pip install vnstock3'")
        return

    # 2. Khởi tạo và lấy dữ liệu
    try:
        vstock = Vnstock().config(api_key=API_KEY)
        symbol = "FPT"
        start_date = "2022-01-01"
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"📡 Đang gọi API lấy dữ liệu {symbol} từ {start_date}...")
        df = vstock.stock_historical_data(symbol=symbol, start_date=start_date, end_date=end_date, resolution='1D', type='stock')
        
        if df is None or df.empty:
            print("❌ Lỗi: API trả về rỗng (Empty DataFrame)")
            return
        
        print(f"✅ Lấy dữ liệu thành công: {len(df)} dòng.")
        print("📊 Các cột hiện có:", df.columns.tolist())
        
        # 3. Chuẩn hóa dữ liệu (Lỗi hay nằm ở đây)
        df.columns = [c.lower() for c in df.columns]
        
        # Xử lý thời gian
        t_col = 'time' if 'time' in df.columns else 'date'
        df[t_col] = pd.to_datetime(df[t_col])
        df = df.set_index(t_col)
        
        # Mapping cột
        df = df.rename(columns={'close': 'Close', 'volume': 'Volume', 'high': 'High', 'low': 'Low'})
        
        # Kiểm tra dữ liệu sau khi lọc NaN
        initial_len = len(df)
        df = df.dropna(subset=['Close', 'Volume'])
        if len(df) < initial_len:
            print(f"⚠️ Cảnh báo: Đã xóa {initial_len - len(df)} dòng bị thiếu giá hoặc khối lượng.")

        # 4. Gộp nến tuần
        print("⏳ Đang gộp dữ liệu sang nến tuần...")
        weekly = df.resample('W-FRI').agg({
            'Close': 'last', 
            'Volume': 'sum', 
            'High': 'max', 
            'Low': 'min'
        }).dropna()
        
        print(f"✅ Số lượng nến tuần: {len(weekly)}")
        if len(weekly) < 20:
            print("❌ Lỗi: Quá ít nến tuần để tính MA20 (Yêu cầu > 20).")
            return

        # 5. Tính toán chỉ báo (RSI, MA20)
        print("📈 Đang tính toán chỉ báo RSI và MA20 Vol...")
        weekly['ma20_vol'] = weekly['Volume'].rolling(20).mean()
        
        # Tính RSI đơn giản để debug
        delta = weekly['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        weekly['rsi'] = 100 - (100 / (1 + rs))
        weekly['sma_rsi'] = weekly['rsi'].rolling(14).mean()
        
        last_row = weekly.iloc[-1]
        print(f"📝 Kết quả hàng cuối cùng ({weekly.index[-1].date()}):")
        print(f"   - Giá đóng cửa: {last_row['Close']:,.0f}")
        print(f"   - Khối lượng tuần: {last_row['Volume']:,.0f}")
        print(f"   - MA20 Vol tuần: {last_row['ma20_vol']:,.0f}")
        print(f"   - RSI: {last_row['rsi']:.2f}")
        
        # 6. Kiểm tra tín hiệu mua gần nhất
        print(f"{'-'*30}\n🔎 Kiểm tra tín hiệu mua gần nhất:")
        found_signal = False
        for i in range(20, len(weekly)):
            row = weekly.iloc[i]
            prev = weekly.iloc[i-1]
            
            # Logic: Vol > 120% MA20 và RSI cắt lên SMA_RSI
            cond1 = row['Volume'] > 1.2 * row['ma20_vol']
            cond2 = prev['rsi'] <= prev['sma_rsi'] and row['rsi'] > row['sma_rsi']
            
            if cond1 and cond2:
                print(f"✨ Tìm thấy điểm mua tại ngày: {weekly.index[i].date()} | Giá: {row['Close']:,.0f}")
                found_signal = True
        
        if not found_signal:
            print("ℹ️ Không tìm thấy tín hiệu mua nào theo bộ lọc hiện tại.")

    except Exception as e:
        print(f"🔥 Lỗi hệ thống: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_fpt()
