#!/usr/bin/env python3
import argparse, json, glob
from datetime import datetime
import numpy as np
import pandas as pd

def compute_trending(df, window_mins=15):
    df = df[['close']].resample('1T').last().ffill()
    ret = df['close'].pct_change(window_mins)
    return ret.iloc[-1] * 100

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--start',   required=True)
    p.add_argument('--end',     required=True)
    p.add_argument('--data-dir', default='data/history')
    p.add_argument('--out',     default='signals.json')
    args = p.parse_args()

    signals = []
    for fp in glob.glob(f"{args.data_dir}/*.parquet"):
        sym = fp.rsplit('/',1)[-1].replace('.parquet','')
        df = pd.read_parquet(fp)
        if isinstance(df.index, pd.MultiIndex):
            df.index = df.index.get_level_values(-1)
        df.index = pd.to_datetime(df.index)
        mask = (
            (df.index.date >= datetime.fromisoformat(args.start).date()) &
            (df.index.date <= datetime.fromisoformat(args.end).date())
        )
        today = df.loc[mask]
        if len(today) < 16:
            continue
        mom = compute_trending(today, window_mins=15)
        signals.append({'symbol': sym, 'momentum_15m[%]': round(mom,2)})

    if not signals:
        print("No signals for today.")
        return

    df = pd.DataFrame(signals)
    cutoff = np.percentile(df['momentum_15m[%]'], 95)
    top = df[df['momentum_15m[%]'] >= cutoff]\
            .sort_values('momentum_15m[%]', ascending=False)

    out = {
      'timestamp': datetime.utcnow().isoformat(),
      'top_movers': top.to_dict(orient='records')
    }
    with open(args.out,'w') as f:
        json.dump(out, f, indent=2)
    print(f"Wrote {len(top)} signals to {args.out}")

if __name__=='__main__':
    main()
