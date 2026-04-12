# ============================================================
# BACKTEST BOT - TELEGRAM
# Nhập mã cổ phiếu → chạy backtest khung tuần từ 2023
# Điều kiện mua:
#   1. Volume > 120% MA20(Volume)
#   2. RSI(14) cắt lên SMA(RSI,14)
#   3. SMA(RSI,14) đi ngang hoặc tăng (so với SMA 14 tuần trước)
# Trailing stop: 10% từ đỉnh cao nhất sau khi mua
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

# -------------------- Lấy dữ liệu --------------------
def get_weekly_data(symbol: str) -> pd.DataFrame | None:
    try:
        from vnstock import Vnstock
        stock = Vnstock().stock(symbol=symbol, source='VCI')
        end   = datetime.now(VN_TZ).strftime('%Y-%m-%d')
        start = '2022-01-01'  # Lấy thêm data trước 2023 để warmup chỉ báo
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
        logging.error(f'get_weekly_data lỗi: {e}')
        return None

# -------------------- Chỉ báo --------------------
def smma(series: pd.Series, period: int) -> pd.Series:
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

def calc_indicators(weekly: pd.DataFrame) -> pd.DataFrame:
    df = weekly.copy()
    # Volume MA20
    df['ma20_vol'] = df['Volume'].rolling(20).mean()
    # RSI dùng SMMA (chuẩn TradingView)
    delta    = df['Close'].diff()
    avg_gain = smma(delta.where(delta > 0, 0.0), 14)
    avg_loss = smma((-delta).where(delta < 0, 0.0), 14)
    df['rsi']    = 100 - (100 / (1 + avg_gain / avg_loss))
    # SMA(RSI, 14)
    df['sma_rsi'] = df['rsi'].rolling(14).mean()
    # SMA(RSI) 14 tuần trước để xác định xu hướng SMA
    df['sma_rsi_prev'] = df['sma_rsi'].shift(1)
    return df

# -------------------- Điều kiện mua --------------------
def check_buy(df: pd.DataFrame, i: int) -> bool:
    """Kiểm tra điều kiện mua tại vị trí i (so sánh i và i-1)"""
    if i < 1:
        return False
    row  = df.iloc[i]
    prev = df.iloc[i - 1]

    # Kiểm tra NaN
    cols = ['Volume', 'ma20_vol', 'rsi', 'sma_rsi', 'sma_rsi_prev']
    if any(pd.isna(row[c]) for c in cols):
        return False
    if pd.isna(prev['rsi']) or pd.isna(prev['sma_rsi']):
        return False

    dk1 = row['Volume'] > 1.2 * row['ma20_vol']
    dk2 = prev['rsi'] <= prev['sma_rsi'] and row['rsi'] > row['sma_rsi']  # RSI cắt lên SMA
    dk3 = row['sma_rsi'] >= row['sma_rsi_prev']  # SMA(RSI) đi ngang hoặc tăng

    return dk1 and dk2 and dk3

# -------------------- Backtest --------------------
def run_backtest(symbol: str, initial_capital: float = 50_000_000) -> dict:
    weekly = get_weekly_data(symbol)
    if weekly is None or weekly.empty:
        return {'error': f'Không lấy được dữ liệu cho mã {symbol}'}

    df = calc_indicators(weekly)

    # Chỉ backtest từ tuần đầu 2023
    df_bt = df[df.index >= '2023-01-01'].copy()
    if df_bt.empty:
        return {'error': 'Không có dữ liệu từ 2023'}

    capital    = initial_capital
    trades     = []
    position   = None  # {'buy_week', 'buy_price', 'shares', 'cost', 'peak'}

    for i in range(1, len(df_bt)):
        row      = df_bt.iloc[i]
        week_str = df_bt.index[i].strftime('%Y-%m-%d')
        price    = row['Close']

        # Đang giữ lệnh → kiểm tra trailing stop
        if position is not None:
            # Cập nhật đỉnh
            if price > position['peak']:
                position['peak'] = price

            stop_price = position['peak'] * 0.90  # Trailing stop 10%

            if price <= stop_price:
                # Bán
                sell_value  = position['shares'] * price
                profit      = sell_value - position['cost']
                capital     = sell_value
                trades.append({
                    'stt'        : len(trades) + 1,
                    'loai'       : 'Bán',
                    'tuan_mua'   : position['buy_week'],
                    'gia_mua'    : round(position['buy_price'], 2),
                    'tuan_ban'   : week_str,
                    'gia_ban'    : round(price, 2),
                    'gia_stop'   : round(stop_price, 2),
                    'gia_dinh'   : round(position['peak'], 2),
                    'von_dau'    : round(position['cost'], 0),
                    'gia_tri_ban': round(sell_value, 0),
                    'lai_lo'     : round(profit, 0),
                    'von_sau'    : round(capital, 0),
                    'dang_giu'   : False,
                })
                position = None
            continue  # Không mua khi đang giữ lệnh

        # Chưa có lệnh → kiểm tra điều kiện mua
        # Tìm vị trí i trong df gốc để check điều kiện
        global_i = df.index.get_loc(df_bt.index[i])
        if check_buy(df, global_i):
            shares          = capital / price
            position = {
                'buy_week' : week_str,
                'buy_price': price,
                'shares'   : shares,
                'cost'     : capital,
                'peak'     : price,
            }

    # Cuối cùng: nếu còn đang giữ lệnh
    if position is not None:
        last_price  = df_bt.iloc[-1]['Close']
        last_week   = df_bt.index[-1].strftime('%Y-%m-%d')
        current_val = position['shares'] * last_price
        profit      = current_val - position['cost']
        trades.append({
            'stt'        : len(trades) + 1,
            'loai'       : 'Đang giữ',
            'tuan_mua'   : position['buy_week'],
            'gia_mua'    : round(position['buy_price'], 2),
            'tuan_ban'   : last_week,
            'gia_ban'    : round(last_price, 2),
            'gia_stop'   : round(position['peak'] * 0.90, 2),
            'gia_dinh'   : round(position['peak'], 2),
            'von_dau'    : round(position['cost'], 0),
            'gia_tri_ban': round(current_val, 0),
            'lai_lo'     : round(profit, 0),
            'von_sau'    : round(current_val, 0),
            'dang_giu'   : True,
        })
        capital = current_val

    return {
        'symbol'         : symbol.upper(),
        'von_ban_dau'    : initial_capital,
        'von_cuoi'       : round(capital, 0),
        'tong_loi_nhuan' : round(capital - initial_capital, 0),
        'pct_return'     : round((capital / initial_capital - 1) * 100, 2),
        'so_giao_dich'   : len(trades),
        'trades'         : trades,
    }

# -------------------- Định dạng kết quả --------------------
def format_result(result: dict) -> list[str]:
    if 'error' in result:
        return [f'❌ Lỗi: {result["error"]}']

    symbol  = result['symbol']
    von_bd  = result['von_ban_dau']
    von_c   = result['von_cuoi']
    loi_nhuan = result['tong_loi_nhuan']
    pct     = result['pct_return']
    trades  = result['trades']
    icon    = '📈' if loi_nhuan >= 0 else '📉'

    msgs = []

    # Tin nhắn tổng quan
    tong = (f'<b>📊 BACKTEST — {symbol} (Khung tuần, từ 2023)</b>\n'
            f'Vốn ban đầu : {von_bd:,.0f}đ\n'
            f'Vốn cuối    : {von_c:,.0f}đ\n'
            f'{icon} Lợi nhuận   : {loi_nhuan:+,.0f}đ ({pct:+.2f}%)\n'
            f'Số giao dịch: {result["so_giao_dich"]}\n'
            f'{'─'*35}')
    msgs.append(tong)

    # Chi tiết từng giao dịch (mỗi 5 giao dịch 1 tin)
    chunk = []
    for t in trades:
        icon_t = '🟡' if t['dang_giu'] else ('🟢' if t['lai_lo'] >= 0 else '🔴')
        status = '(đang giữ)' if t['dang_giu'] else ''
        lai_lo_str = f"{t['lai_lo']:+,.0f}đ"

        chunk.append(
            f"{icon_t} <b>#{t['stt']} {t['loai']} {status}</b>\n"
            f"  Mua : tuần {t['tuan_mua']} @ {t['gia_mua']:,}đ\n"
            f"  {'Hiện tại' if t['dang_giu'] else 'Bán'}: tuần {t['tuan_ban']} @ {t['gia_ban']:,}đ\n"
            f"  {'Stop hiện tại' if t['dang_giu'] else 'Đỉnh / Stop'}: {t['gia_dinh']:,}đ / {t['gia_stop']:,}đ\n"
            f"  Vốn vào : {t['von_dau']:,.0f}đ\n"
            f"  Giá trị : {t['gia_tri_ban']:,.0f}đ\n"
            f"  Lãi/Lỗ : {lai_lo_str}\n"
            f"  Vốn sau: {t['von_sau']:,.0f}đ"
        )

        if len(chunk) == 5:
            msgs.append('\n\n'.join(chunk))
            chunk = []

    if chunk:
        msgs.append('\n\n'.join(chunk))

    return msgs

# -------------------- Telegram handler --------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().upper()

    # Chỉ xử lý mã hợp lệ (2-5 ký tự chữ cái)
    if not (2 <= len(text) <= 5 and text.isalpha()):
        await update.message.reply_text(
            'Vui lòng nhập mã cổ phiếu (VD: VCB, HPG, FPT...)'
        )
        return

    await update.message.reply_text(
        f'⏳ Đang chạy backtest cho <b>{text}</b>...\n'
        f'Vốn ban đầu: 50,000,000đ | Khung W | Từ 2023',
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
    print('✅ Bot đang chạy... Nhập mã cổ phiếu vào Telegram để backtest.')
    app.run_polling()

if __name__ == '__main__':
    main()
