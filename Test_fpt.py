import pandas as pd
import numpy as np
from datetime import datetime
import sys

# Thử import cả 2 phiên bản vnstock để đảm bảo không lỗi Module
try:
    from vnstock3 import Vnstock
    vn_version = 3
    print("--- He thong dang dung vnstock3 ---")
except ImportError:
    try:
        from vnstock import Vnstock
        vn_version = 1
        print("--- He thong dang dung vnstock (old) ---")
    except ImportError:
        print("ERR: Khong tim thay thu vien vnstock. Hay kiem tra file workflows!")
        sys.exit(1)

API_KEY = 'vnstock_92c86f761ec105508ba230ede06850c7'

def run_test():
    symbol = "FPT"
    print(f"--- Bat dau kiem tra ma: {symbol} ---")
    
    try:
        # 1. Lay du lieu
        if vn_version == 3:
            vstock = Vnstock().config(api_key=API_KEY)
            df = vstock.stock_historical_data(symbol=symbol, start_date='2023-01-01', end_date=datetime.now().strftime('%Y-%m-%d'))
        else:
            # Dung source mac dinh la TCBS cho ban cu
            from vnstock import Vnstock as Vs
            df = Vs().stock(symbol=symbol, source='TCBS').quote.history(start='2023-01-01', end=datetime.now().strftime('%Y-%m-%d'))

        if df is None or df.empty:
            print("ERR: API tra ve du lieu trong!")
            return

        # 2. Chuan hoa ten cot (Day la buoc hay bi loi nhat)
        df.columns = [c.lower() for c in df.columns]
        print(f"Cac cot tim thay: {df.columns.tolist()}")

        # Mapping de code hieu duoc
        df = df.rename(columns={'close': 'Close', 'volume': 'Volume', 'time': 'Date', 'date': 'Date'})
        
        # 3. Kiem tra hien thi 5 dong cuoi
        print("\n--- 5 dong du lieu moi nhat ---")
        print(df[['Close', 'Volume']].tail())
        
        # 4. Kiem tra gop nen tuan
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date')
        
        weekly = df.resample('W-FRI').agg({'Close': 'last', 'Volume': 'sum'})
        print(f"\n--- Gop thanh cong {len(weekly)} nen tuan ---")
        print(weekly.tail(3))
        
        print("\n=> KET LUAN: DU LIEU OK, CO THE CHAY BOT BACKTEST.")

    except Exception as e:
        print(f"ERR: Co loi xay ra: {str(e)}")

if __name__ == "__main__":
    run_test()
