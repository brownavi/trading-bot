# ingest.py

import os
import argparse
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from alpaca_trade_api.rest import REST, TimeFrame, URL

def fetch_and_save(symbol: str, client: REST, out_dir: Path):
    print(f"→ fetching bars for {symbol}")
    bars = client.get_bars(
        symbol,
        TimeFrame.Minute,
        start="2020-01-01",
        end=None,               # up to now
        adjustment="raw"
    ).df
    # if you want a flat column for timestamp:
    # bars = bars.reset_index().rename(columns={"timestamp": "time"})
    path = out_dir / f"{symbol}.parquet"
    bars.to_parquet(path)
    print(f"✓ saved {path}")

def main(data_dir: str, symbols: list[str]):
    load_dotenv()  # reads .env in cwd
    api_key = os.getenv("APCA_API_KEY_ID")
    api_secret = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not api_secret:
        raise RuntimeError("Missing APCA_API_KEY_ID / APCA_API_SECRET_KEY in .env")

    # instantiate Alpaca REST client (paper trading/data)
    client = REST(
        api_key,
        api_secret,
        base_url=URL.PAPER,   # for paper-trading & data
        api_version="v2"
    )

    out_dir = Path(data_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for sym in symbols:
        fetch_and_save(sym, client, out_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest historical bars from Alpaca into Parquet files"
    )
    parser.add_argument(
        "--data_dir",
        required=True,
        help="Path where parquet files will be written (e.g. /inputs/stocks_data)"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["AAPL"],
        help="One or more ticker symbols to pull"
    )
    args = parser.parse_args()
    main(args.data_dir, args.symbols)
