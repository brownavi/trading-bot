import os
import glob
import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from joblib import dump

DATA_FOLDER = 'data/history'
MODEL_PATH = 'models/trend_model.joblib'
os.makedirs('models', exist_ok=True)

def load_data():
    files = glob.glob(f"{DATA_FOLDER}/*.parquet")
    dfs = []
    for fp in files:
        df = pd.read_parquet(fp)
        if len(df) >= 100:
            df = df[['open','high','low','close','volume']].copy()
            df['symbol'] = os.path.splitext(os.path.basename(fp))[0]
            # simple feature + target: close > open
            df['target'] = (df['close'] > df['open']).astype(int)
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

if __name__ == '__main__':
    df = load_data()
    X = df[['open','high','low','close','volume']]
    y = df['target']
    pipe = make_pipeline(StandardScaler(), SGDClassifier(max_iter=1000, tol=1e-3))
    pipe.fit(X, y)
    dump(pipe, MODEL_PATH)
    print(f"Trained model saved to {MODEL_PATH}")
