import json
import websocket
import time
from datetime import datetime

WS_URL = "wss://ws.pionex.com/wsPub"

# ========== CONFIGURATION ==========
# Change these symbols to test different pairs
SYMBOL_A = "PAXG_USDT_PERP"  # The one you'll SHORT (sell)
SYMBOL_B = "XAUT_USDT_PERP"  # The one you'll LONG (buy)

# Example pairs to test:
# SYMBOL_A = "PAXG_USDT_PERP"
# SYMBOL_B = "XAUT_USDT_PERP"
# ===================================

def on_open(ws):
    print("✅ Connected to Pionex WebSocket")

    # Subscribe to order book depth for both symbols
    symbols = [SYMBOL_A, SYMBOL_B]
    for symbol in symbols:
        sub_msg = {
            "op": "SUBSCRIBE",
            "topic": "DEPTH",
            "symbol": symbol,
            "limit": 5  # Get top 5 levels of order book
        }
        ws.send(json.dumps(sub_msg))
    print(f"📡 Subscribed to {SYMBOL_A} and {SYMBOL_B} order book depth")

# 存储最新的bid和ask价格
prices = {
    SYMBOL_A: {"bid": None, "ask": None},
    SYMBOL_B: {"bid": None, "ask": None}
}
last_diff = None  # 上一次的差价

# 存储最大和最小的 spread 值及其时间
max_entry_spread = {"value": float("-inf"), "time": None}
min_exit_spread = {"value": float("inf"), "time": None}

def on_message(ws, message):
    global prices, last_diff
    try:
        data = json.loads(message)
        # Uncomment for debugging: print(f"📥 DEBUG: {data}")
    except json.JSONDecodeError:
        print(f"⚠️ Failed to decode message: {message}")
        return

    # Handle PING from server - must respond with PONG!
    if data.get("op") == "PING":
        pong_msg = {
            "op": "PONG",
            "timestamp": int(time.time() * 1000)  # Current timestamp in milliseconds
        }
        ws.send(json.dumps(pong_msg))
        # print(f"💓 Sent PONG response")  # Uncomment to debug heartbeat
        return

    # 检查是否是 DEPTH 数据
    if data.get("topic") == "DEPTH":
        symbol = data.get("symbol")
        depth_data = data.get("data")
        
        if not depth_data or symbol not in prices:
            return

        # 获取最佳bid和ask价格
        bids = depth_data.get("bids", [])
        asks = depth_data.get("asks", [])
        
        if not bids or not asks:
            return
        
        # Best bid is the first element in bids array [price, size]
        # Best ask is the first element in asks array [price, size]
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        
        # 更新价格
        prices[symbol]["bid"] = best_bid
        prices[symbol]["ask"] = best_ask

        # 检查两个价格都有了
        if (prices[SYMBOL_A]["bid"] is not None and 
            prices[SYMBOL_A]["ask"] is not None and
            prices[SYMBOL_B]["bid"] is not None and 
            prices[SYMBOL_B]["ask"] is not None):
            
            # Entry: Short SYMBOL_A (sell at bid), Long SYMBOL_B (buy at ask)
            entry_spread = prices[SYMBOL_A]["bid"] - prices[SYMBOL_B]["ask"]
            
            # Exit: Close short SYMBOL_A (buy at ask), Close long SYMBOL_B (sell at bid)
            exit_spread = prices[SYMBOL_A]["ask"] - prices[SYMBOL_B]["bid"]

            # 如果差价有变化，打印时间和差价
            if entry_spread != last_diff:
                last_diff = entry_spread
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # 更新最大 entry_spread
                if entry_spread > max_entry_spread["value"]:
                    max_entry_spread["value"] = entry_spread
                    max_entry_spread["time"] = timestamp

                # 更新最小 exit_spread
                if exit_spread < min_exit_spread["value"]:
                    min_exit_spread["value"] = exit_spread
                    min_exit_spread["time"] = timestamp

                # 打印当前差价和最大/最小记录
                print(
                    f"⏰ {timestamp} | "
                    f"-: {entry_spread:.2f} | "
                    f"+: {exit_spread:.2f} | "
                    f"Max Entry: {max_entry_spread['value']:.2f} ({max_entry_spread['time']}) | "
                    f"Min Exit: {min_exit_spread['value']:.2f} ({min_exit_spread['time']})"
                )


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

        # No need for ping_interval/ping_timeout since Pionex handles heartbeat via JSON messages
        ws.run_forever()

    # Auto-reconnect loop
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
