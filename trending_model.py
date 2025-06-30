# trending_model.py — load each symbol’s parquet and backtest
import os
import argparse
import pandas as pd
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import numpy as np

class SMAStrategy(Strategy):
    n1 = 10
    n2 = 30

    def init(self):
        price = self.data.Close
        self.sma1 = self.I(pd.Series.rolling, price, self.n1).mean()
        self.sma2 = self.I(pd.Series.rolling, price, self.n2).mean()

    def next(self):
        if crossover(self.sma1, self.sma2):
            self.buy()
        elif crossover(self.sma2, self.sma1):
            self.sell()

def backtest_file(path, cash=10_000):
    df = pd.read_parquet(path).set_index("timestamp")
    bt = Backtest(df, SMAStrategy, cash=cash, commission=0.001)
    stats = bt.run()
    print(f"=== {os.path.basename(path)} ===")
    print(stats[["Return [%]", "Sharpe Ratio", "Max Drawdown [%]"]])
    # Optionally save equity curve
    # stats._equity.to_csv(f"{path}.equity.csv")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--data_dir", required=True,
                   help="Directory of parquet files")
    args = p.parse_args()

    files = [
        os.path.join(args.data_dir, f)
        for f in os.listdir(args.data_dir)
        if f.endswith(".parquet")
    ]
    if not files:
        raise ValueError("No parquet files found in " + args.data_dir)

    for f in files:
        backtest_file(f)
