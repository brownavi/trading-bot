import os
import argparse
from dotenv import load_dotenv
from alpaca_trade_api.rest import REST
import pandas as pd

def fetch_data(symbols, data_dir):
    load_dotenv()
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    client = REST(api_key, secret_key, base_url="https://paper-api.alpaca.markets")
    os.makedirs(data_dir, exist_ok=True)
    for symbol in symbols:
        bars = client.get_bars(symbol, "1D", start="2025-06-01", end="2025-06-29").df
        bars.to_parquet(f"{data_dir}/{symbol}.parquet")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", required=True, help="Path to write parquet files")
    args = parser.parse_args()

    # list your symbols (or load from file)
    symbols = ["AAPL", "MSFT", "GOOG"]
    fetch_data(symbols, args.data_dir)
