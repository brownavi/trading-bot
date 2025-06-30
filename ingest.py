# ingest.py
import os
import argparse
from alpaca_trade_api.rest import REST, TimeFrame
import pandas as pd
from dotenv import load_dotenv

load_dotenv()  # so you can keep API keys in a .env file

def fetch_and_save(symbols, data_dir, api_key, api_secret, base_url):
    client = REST(api_key, api_secret, base_url)
    os.makedirs(data_dir, exist_ok=True)
    for sym in symbols:
        # fetch last 100 daily bars
        bars = client.get_bars(sym, TimeFrame.Day, limit=100).df
        bars.index.name = 'timestamp'
        file_path = os.path.join(data_dir, f"{sym}.parquet")
        bars.to_parquet(file_path)
        print(f"Saved {sym} → {file_path}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data_dir", required=True,
                   help="Directory where to write parquet files")
    args = p.parse_args()

    symbols = os.getenv("SYMBOLS", "AAPL,MSFT,GOOG").split(",")
    api_key = os.getenv("APCA_API_KEY_ID")
    api_secret = os.getenv("APCA_API_SECRET_KEY")
    base_url = os.getenv(
        "APCA_API_BASE_URL", "https://paper-api.alpaca.markets"
    )

    fetch_and_save(symbols, args.data_dir, api_key, api_secret, base_url)

if __name__ == "__main__":
    main()

