# ingest.py — fetch historical bars and write per-symbol parquet
import os
import argparse
from datetime import datetime, timedelta
import pandas as pd
from alpaca_trade_api.rest import REST
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

def fetch_and_save(symbols, start, end, timeframe, out_dir):
    client = REST(API_KEY, API_SECRET, base_url=BASE_URL, api_version="v2")
    os.makedirs(out_dir, exist_ok=True)
    all_dfs = []
    for symbol in symbols:
        print(f"Fetching {symbol} from {start} to {end} @ {timeframe}")
        bars = client.get_bars(symbol, timeframe, start=start, end=end).df
        if bars.empty:
            print(f"❗ no data for {symbol}")
            continue
        bars = bars.reset_index()
        bars["symbol"] = symbol
        path = os.path.join(out_dir, f"{symbol}.parquet")
        bars.to_parquet(path)
        all_dfs.append(path)
    return all_dfs

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", nargs="+", required=True,
                   help="List of symbols, e.g. --symbols AAPL MSFT GOOG")
    p.add_argument("--start", type=str,
                   help="Start ISO datetime, e.g. 2025-06-01T09:30:00Z")
    p.add_argument("--end", type=str,
                   help="End ISO datetime, e.g. 2025-06-10T16:00:00Z")
    p.add_argument("--timeframe", default="1Min",
                   help="Bar timeframe, e.g. 1Min, 5Min, 1Day")
    p.add_argument("--data_dir", default="/inputs/stocks_data",
                   help="Where to write parquet files")
    args = p.parse_args()

    # default last 2 weeks if not provided
    if not args.end:
        args.end = datetime.utcnow().isoformat()
    if not args.start:
        start_dt = datetime.fromisoformat(args.end.rstrip("Z")) - timedelta(days=14)
        args.start = start_dt.isoformat() + "Z"

    fetch_and_save(args.symbols, args.start, args.end, args.timeframe, args.data_dir)
