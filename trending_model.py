# trending_model.py
import os
import argparse
import pandas as pd

def load_data(data_dir):
    files = [f for f in os.listdir(data_dir) if f.endswith(".parquet")]
    if not files:
        print(f"No parquet files found in {data_dir}.  Did ingestion run?")
        return None

    dfs = []
    for fn in files:
        df = pd.read_parquet(os.path.join(data_dir, fn))
        df["symbol"] = fn.replace(".parquet", "")
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def backtest(df):
    # your existing backtest logic here
    print("Running backtest on", df["symbol"].unique())

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data_dir", required=True)
    args = p.parse_args()

    df = load_data(args.data_dir)
    if df is None:
        return

    backtest(df)

if __name__ == "__main__":
    main()
