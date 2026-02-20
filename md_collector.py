#!/usr/bin/env python3
"""
Standalone Bybit Linear market data collector.
Collects quotes (orderbook.1) and trades (publicTrade) and saves them
as gzip CSV files.

Output structure:
    {output_dir}/bybit-linear/{YYYY-MM-DD}/{symbol}/quotes.gz
    {output_dir}/bybit-linear/{YYYY-MM-DD}/{symbol}/trades.gz
    (symbol in lowercase, e.g. ethusdt)

Usage:
    python md_collector.py BTCUSDT ETHUSDT
    python md_collector.py --output-dir ./data BTCUSDT

Requires:
    pip install websockets
"""

import asyncio
import csv
import gzip
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

try:
    import websockets
except ImportError:
    print("Missing dependency: pip install websockets")
    print(f"(running with: {sys.executable})")
    sys.exit(1)


WS_URL = "wss://stream.bybit.com/v5/public/linear"
FLUSH_INTERVAL_SEC = 5

TRADE_COLS = [
    "exchange", "symbol", "timestamp", "event_timestamp", "local_timestamp",
    "trade_id", "side", "price", "amount",
]

QUOTE_COLS = [
    "exchange", "symbol", "timestamp", "event_timestamp", "local_timestamp",
    "is_snapshot", "ask_amount", "ask_price", "bid_price", "bid_amount",
]

# In-memory write buffer: {filepath_str: [row_dicts]}
_buf: dict[str, list] = defaultdict(list)
# Column headers per file
_cols: dict[str, list] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def local_ts_us() -> int:
    return time.time_ns() // 1_000


def ms_to_us(ms) -> int:
    return int(ms) * 1_000


def date_from_us(ts_us: int) -> str:
    return datetime.fromtimestamp(ts_us / 1_000_000, tz=timezone.utc).strftime("%Y-%m-%d")


def file_path(output_dir: str, symbol: str, filename: str, ts_us: int) -> Path:
    date = date_from_us(ts_us)
    return Path(output_dir) / "bybit-linear" / date / symbol.lower() / filename


# ---------------------------------------------------------------------------
# Buffering
# ---------------------------------------------------------------------------

def enqueue(path: Path, cols: list, row: dict):
    key = str(path)
    _cols[key] = cols
    _buf[key].append(row)


def flush_all():
    for key, rows in list(_buf.items()):
        if not rows:
            continue
        path = Path(key)
        cols = _cols[key]
        path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = path.exists()
        with gzip.open(path, "at", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=cols)
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)
        _buf[key].clear()


async def periodic_flush():
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SEC)
        flush_all()


# ---------------------------------------------------------------------------
# Message handlers
# ---------------------------------------------------------------------------

def on_trade(output_dir: str, symbol: str, msg: dict):
    local_ts = local_ts_us()
    event_ts = ms_to_us(msg["ts"])
    path = file_path(output_dir, symbol, "trades.gz", local_ts)

    for t in msg.get("data", []):
        enqueue(path, TRADE_COLS, {
            "exchange":        "bybit-linear",
            "symbol":          symbol,
            "timestamp":       ms_to_us(t["T"]),
            "event_timestamp": event_ts,
            "local_timestamp": local_ts,
            "trade_id":        t["i"],
            "side":            "buy" if t["S"] == "Buy" else "sell",
            "price":           t["p"],
            "amount":          t["v"],
        })


def on_quote(output_dir: str, symbol: str, msg: dict):
    local_ts = local_ts_us()
    event_ts = ms_to_us(msg["ts"])
    # cts = cross-matching engine timestamp (root-level field)
    cts = msg.get("cts")
    ts = ms_to_us(cts) if cts else event_ts

    data = msg.get("data", {})
    bids = data.get("b", [])
    asks = data.get("a", [])
    if not bids or not asks:
        return

    path = file_path(output_dir, symbol, "quotes.gz", local_ts)
    enqueue(path, QUOTE_COLS, {
        "exchange":        "bybit-linear",
        "symbol":          symbol,
        "timestamp":       ts,
        "event_timestamp": event_ts,
        "local_timestamp": local_ts,
        "is_snapshot":     msg.get("type") == "snapshot",
        "ask_price":       asks[0][0],
        "ask_amount":      asks[0][1],
        "bid_price":       bids[0][0],
        "bid_amount":      bids[0][1],
    })


# ---------------------------------------------------------------------------
# WebSocket loop
# ---------------------------------------------------------------------------

async def collect(symbols: list[str], output_dir: str):
    topics = []
    for sym in symbols:
        topics.append(f"publicTrade.{sym}")
        topics.append(f"orderbook.1.{sym}")

    sub_msg = json.dumps({"op": "subscribe", "args": topics})

    asyncio.create_task(periodic_flush())

    print(f"Symbols  : {symbols}")
    print(f"Output   : {output_dir}/bybit-linear/")
    print(f"Flushing every {FLUSH_INTERVAL_SEC}s")

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
                await ws.send(sub_msg)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Connected and subscribed.")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    topic = msg.get("topic", "")
                    if not topic:
                        continue

                    if topic.startswith("publicTrade."):
                        on_trade(output_dir, topic[12:], msg)
                    elif topic.startswith("orderbook.1."):
                        on_quote(output_dir, topic[12:], msg)

        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error: {e} — reconnecting in 5s")
            await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    import argparse
    p = argparse.ArgumentParser(description="Bybit linear MD collector (quotes + trades)")
    p.add_argument("symbols", nargs="+", help="Symbols, e.g. BTCUSDT ETHUSDT")
    p.add_argument("--output-dir", default="./data", help="Root output directory (default: ./data)")
    args = p.parse_args()

    symbols = [s.upper() for s in args.symbols]
    try:
        asyncio.run(collect(symbols, args.output_dir))
    except KeyboardInterrupt:
        print("\nFlushing remaining data...")
        flush_all()
        print("Done.")


if __name__ == "__main__":
    main()
