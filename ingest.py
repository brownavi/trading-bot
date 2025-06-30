# ingest.py
import os
import argparse
import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame

def fetch_symbol(symbol, start, end, out_folder):
    api_key = os.getenv("PKL6RK0JSV4MT4E2JXRN")
    secret  = os.getenv("gDC073gAxsB1S1Rcj5Q0c6EzFWuIXyHMBQDCpSDL")
    client = REST(api_key, secret, paper=True)

    bars = client.get_bars(symbol,
                           TimeFrame.Day,
                           start,
                           end,
                           adjustment="raw").df
    path = os.path.join(out_folder, f"{symbol}.parquet")
    bars.to_parquet(path)
    print(f"Written {symbol} to {path}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--data_dir", required=True)
    args = p.parse_args()

    from datetime import date, timedelta
    end   = date.today().isoformat()
    start = (date.today() - timedelta(days=365)).isoformat()

    # example — add as many tickers as you like here
    for sym in ["AAPL", "MSFT", "GOOG"]:
        fetch_symbol(sym, start, end, args.data_dir)
