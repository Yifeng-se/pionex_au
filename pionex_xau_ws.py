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

def write_to_csv(timestamp, p_bid, p_ask, x_bid, x_ask, diff_short_p, diff_long_p):
    """将数据写入 CSV 文件 (timestamp;PAXG_bid;PAXG_ask;XAUT_bid;XAUT_ask;diff_short_P;diff_long_P)"""
    file_exists = os.path.exists(CSV_FILE)
    has_data = file_exists and os.path.getsize(CSV_FILE) > 0
    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, delimiter=";")
        if not has_data:  # 如果文件不存在或为空，写入表头
            writer.writerow([
                "timestamp", "PAXG_bid", "PAXG_ask", "XAUT_bid", "XAUT_ask", 
                "diff_short_P", "diff_long_P"
            ])
        writer.writerow([
            timestamp, 
            p_bid, p_ask, 
            x_bid, x_ask, 
            round(diff_short_p, 2), 
            round(diff_long_p, 2)
        ])

def on_open(ws):
    print("✅ Connected to Pionex WebSocket", flush=True)
    symbols = [SYMBOL_A, SYMBOL_B]
    for symbol in symbols:
        sub_msg = {
            "op": "SUBSCRIBE",
            "topic": "DEPTH",
            "symbol": symbol,
            "limit": 5
        }
        ws.send(json.dumps(sub_msg))
    print(f"📡 Subscribed to {SYMBOL_A} and {SYMBOL_B} order book depth", flush=True)

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
        print(f"⚠️ Failed to decode message: {message}", flush=True)
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
            
            p_bid = prices[SYMBOL_A]["bid"]
            p_ask = prices[SYMBOL_A]["ask"]
            x_bid = prices[SYMBOL_B]["bid"]
            x_ask = prices[SYMBOL_B]["ask"]

            diff_short_p = p_bid - x_ask
            diff_long_p = p_ask - x_bid

            if diff_short_p != last_diff:
                last_diff = diff_short_p
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                if diff_short_p > max_entry_spread["value"]:
                    max_entry_spread["value"] = diff_short_p
                    max_entry_spread["time"] = timestamp

                if diff_long_p < min_exit_spread["value"]:
                    min_exit_spread["value"] = diff_long_p
                    min_exit_spread["time"] = timestamp

                print(
                    f"⏰ {timestamp} | "
                    f"SP: {diff_short_p:.2f} | "
                    f"LP: {diff_long_p:.2f} | "
                    f"Max SP: {max_entry_spread['value']:.2f} ({max_entry_spread['time']}) | "
                    f"Min LP: {min_exit_spread['value']:.2f} ({min_exit_spread['time']})",
                    flush=True
                )

                write_to_csv(timestamp, p_bid, p_ask, x_bid, x_ask, diff_short_p, diff_long_p)

def on_error(ws, error):
    print("❌ Error:", error, flush=True)

def on_close(ws, close_status_code, close_msg):
    print(f"🔌 WebSocket closed | code={close_status_code} msg={close_msg}", flush=True)

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
            print("🔄 Starting WebSocket connection...", flush=True)
            run_websocket()
            print("⚠️ Connection closed. Reconnecting in 3 seconds...", flush=True)
            time.sleep(3)
        except KeyboardInterrupt:
            print("\n👋 Shutting down gracefully...", flush=True)
            break
        except Exception as e:
            print(f"⚠️ Unexpected error: {e}. Reconnecting in 3 seconds...", flush=True)
            time.sleep(3)
