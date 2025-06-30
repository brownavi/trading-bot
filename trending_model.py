# trending_model.py
import os, glob, argparse, pandas as pd
from backtesting import Backtest, Strategy

class SmaCross(Strategy):
    n1, n2 = 50, 200
    def init(self):
        price = self.data.Close
        self.sma1 = self.I(lambda x: x.rolling(self.n1).mean(), price)
        self.sma2 = self.I(lambda x: x.rolling(self.n2).mean(), price)
    def next(self):
        if self.sma1 > self.sma2: self.buy()
        elif self.sma1 < self.sma2: self.sell()

def run_backtests(data_dir):
    files = glob.glob(os.path.join(data_dir, "*.parquet"))
    if not files:
        print("⚠️ No parquet files found, skipping backtest.")
        return

    for fp in files:
        df = pd.read_parquet(fp)
        df.index = pd.to_datetime(df.index)
        df = df[["open","high","low","close","volume"]]
        print(f"\n--- {os.path.basename(fp)} ---")
        bt = Backtest(df, SmaCross, cash=10_000,
                      commission=0.002,
                      trade_on_close=True,
                      exclusive_orders=True)
        print(bt.run())

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--data_dir", required=True)
    args = p.parse_args()
    run_backtests(args.data_dir)
