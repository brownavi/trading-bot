# trending_model.py

import argparse
from pathlib import Path

import pandas as pd
from backtesting import Backtest, Strategy
from backtesting.lib import crossover

class SMACross(Strategy):
    # fast & slow lookback windows
    n1 = 10
    n2 = 60

    def init(self):
        price = self.data.Close
        # compute short & long SMAs
        self.sma1 = self.I(lambda x: x.rolling(self.n1).mean(), price)
        self.sma2 = self.I(lambda x: x.rolling(self.n2).mean(), price)

    def next(self):
        # if short crosses above long → go long
        if crossover(self.sma1, self.sma2):
            self.buy()
        # if long crosses above short → go short/exit
        elif crossover(self.sma2, self.sma1):
            self.sell()

def run_backtest(file_path: Path):
    symbol = file_path.stem
    print(f"\n→ Backtesting {symbol}")

    # load data
    df = pd.read_parquet(file_path)
    # ensure DateTimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    # select and rename columns for backtesting.py
    df = (
        df[["open","high","low","close","volume"]]
        .rename(columns=str.title)
    )

    bt = Backtest(
        df,
        SMACross,
        cash=10_000,
        commission=0.001,
        exclusive_orders=True
    )
    stats = bt.run()
    print(f"  Return [%]:    {stats['Return [%]']:.2f}")
    print(f"  Win rate [%]:  {stats['Win rate [%]']:.1f}")
    print(f"  Sharpe Ratio:  {stats['Sharpe Ratio']:.2f}")
    return stats

def main(data_dir: str):
    data_path = Path(data_dir)
    files = sorted(data_path.glob("*.parquet"))
    if not files:
        print(f"No parquet files found in {data_dir}")
        return

    all_stats = []
    for f in files:
        stats = run_backtest(f)
        all_stats.append((f.stem, stats))

    # optional summary table
    print("\n=== SUMMARY ===")
    for sym, s in all_stats:
        print(
            f"{sym:6}  Return {s['Return [%]']:.1f}%   "
            f"Win {s['Win rate [%]']:.1f}%   Sharpe {s['Sharpe Ratio']:.2f}"
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run SMA‐crossover backtests on all Parquet files in a folder"
    )
    parser.add_argument(
        "--data_dir",
        required=True,
        help="Folder where your Parquet bars were written (e.g. /inputs/stocks_data)"
    )
    args = parser.parse_args()
    main(args.data_dir)
