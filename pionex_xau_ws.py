import csv
import os
import json
import websocket
import time
from datetime import datetime

WS_URL = "wss://ws.pionex.com/wsPub"

# ========== CONFIGURATION ==========
SYMBOL_A = "PAXG_USDT_PERP"  # The one you'll SHORT (sell)
SYMBOL_B = "XAUT_USDT_PERP"  # The one you'll LONG (buy)
CSV_FILE = "spread_data.csv"  # 输出的 CSV 文件名
# ===================================

def write_to_csv(timestamp, entry_spread, exit_spread):
    """将数据写入 CSV 文件"""
    file_exists = os.path.exists(CSV_FILE)
    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, delimiter=";")
        if not file_exists:  # 如果文件不存在，写入表头
            writer.writerow(["datetime", "open", "close"])
        # 对 entry_spread 和 exit_spread 进行四舍五入
        writer.writerow([timestamp, round(entry_spread, 2), round(exit_spread, 2)])

def on_open(ws):
    print("✅ Connected to Pionex WebSocket")
    symbols = [SYMBOL_A, SYMBOL_B]
    for symbol in symbols:
        sub_msg = {
            "op": "SUBSCRIBE",
            "topic": "DEPTH",
            "symbol": symbol,
            "limit": 5
        }
        ws.send(json.dumps(sub_msg))
    print(f"📡 Subscribed to {SYMBOL_A} and {SYMBOL_B} order book depth")

prices = {
    SYMBOL_A: {"bid": None, "ask": None},
    SYMBOL_B: {"bid": None, "ask": None}
}
last_diff = None
max_entry_spread = {"value": float("-inf"), "time": None}
min_exit_spread = {"value": float("inf"), "time": None}

def on_message(ws, message):
    global prices, last_diff
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        print(f"⚠️ Failed to decode message: {message}")
        return

    if data.get("op") == "PING":
        pong_msg = {
            "op": "PONG",
            "timestamp": int(time.time() * 1000)
        }
        ws.send(json.dumps(pong_msg))
        return

    if data.get("topic") == "DEPTH":
        symbol = data.get("symbol")
        depth_data = data.get("data")
        if not depth_data or symbol not in prices:
            return

        bids = depth_data.get("bids", [])
        asks = depth_data.get("asks", [])
        if not bids or not asks:
            return

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        prices[symbol]["bid"] = best_bid
        prices[symbol]["ask"] = best_ask

        if (prices[SYMBOL_A]["bid"] is not None and 
            prices[SYMBOL_A]["ask"] is not None and
            prices[SYMBOL_B]["bid"] is not None and 
            prices[SYMBOL_B]["ask"] is not None):
            
            entry_spread = prices[SYMBOL_A]["bid"] - prices[SYMBOL_B]["ask"]
            exit_spread = prices[SYMBOL_A]["ask"] - prices[SYMBOL_B]["bid"]

            if entry_spread != last_diff:
                last_diff = entry_spread
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                if entry_spread > max_entry_spread["value"]:
                    max_entry_spread["value"] = entry_spread
                    max_entry_spread["time"] = timestamp

                if exit_spread < min_exit_spread["value"]:
                    min_exit_spread["value"] = exit_spread
                    min_exit_spread["time"] = timestamp

                print(
                    f"⏰ {timestamp} | "
                    f"-: {entry_spread:.2f} | "
                    f"+: {exit_spread:.2f} | "
                    f"Max Entry: {max_entry_spread['value']:.2f} ({max_entry_spread['time']}) | "
                    f"Min Exit: {min_exit_spread['value']:.2f} ({min_exit_spread['time']})"
                )

                write_to_csv(timestamp, entry_spread, exit_spread)

def on_error(ws, error):
    print("❌ Error:", error)

def on_close(ws, close_status_code, close_msg):
    print(f"🔌 WebSocket closed | code={close_status_code} msg={close_msg}")

if __name__ == "__main__":
    websocket.enableTrace(False)

    def run_websocket():
        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever()

    while True:
        try:
            print("🔄 Starting WebSocket connection...")
            run_websocket()
            print("⚠️ Connection closed. Reconnecting in 3 seconds...")
            time.sleep(3)
        except KeyboardInterrupt:
            print("\n👋 Shutting down gracefully...")
            break
        except Exception as e:
            print(f"⚠️ Unexpected error: {e}. Reconnecting in 3 seconds...")
            time.sleep(3)
