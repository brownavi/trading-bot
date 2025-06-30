import os
import argparse
import pandas as pd
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from backtesting.test import SMA

def load_data(data_dir):
    files = [f for f in os.listdir(data_dir) if f.endswith('.parquet')]
    if not files:
        raise ValueError("No parquet files found in data_dir")
    dfs = [pd.read_parquet(os.path.join(data_dir, f)) for f in files]
    # assume each DF has Date, Open, High, Low, Close, Volume
    return pd.concat(dfs, ignore_index=True)

class SmaCross(Strategy):
    def init(self):
        self.sma1 = self.I(SMA, self.data.Close, 10)
        self.sma2 = self.I(SMA, self.data.Close, 20)

    def next(self):
        if crossover(self.sma1, self.sma2):
            self.buy()
        elif crossover(self.sma2, self.sma1):
            self.sell()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", required=True)
    args = parser.parse_args()

    df = load_data(args.data_dir)
    bt = Backtest(df, SmaCross, cash=10000, commission=.002)
    stats = bt.run()
    print(stats)
