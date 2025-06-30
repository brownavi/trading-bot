import os
import pandas as pd
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import date

API_KEY = os.getenv('APCA_API_KEY_ID')
API_SECRET = os.getenv('APCA_API_SECRET_KEY')
DATA_FOLDER = 'data/history'

# ensure folder exists
os.makedirs(DATA_FOLDER, exist_ok=True)

client = StockHistoricalDataClient(API_KEY, API_SECRET)

def get_symbols():
    sp500 = pd.read_html(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
        flavor='bs4'
    )[0]['Symbol'].astype(str).tolist()
    dow = next(
        df for df in pd.read_html(
            'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average',
            flavor='bs4'
        ) if 'Symbol' in df.columns
    )['Symbol'].astype(str).tolist()
    nasdaq = pd.read_csv(
        'https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv'
    )['Symbol'].dropna().astype(str).tolist()
    syms = sorted({s.strip() for s in sp500 + dow + nasdaq if s.strip()})
    return syms

def fetch_and_store(symbols, start, end):
    from tqdm import tqdm
    for symbol in tqdm(symbols, desc='Downloading OHLCV'):
        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Day,
            start=start.isoformat(),
            end=end.isoformat()
        )
        df = client.get_stock_bars(req).df
        if not df.empty:
            df.to_parquet(f"{DATA_FOLDER}/{symbol}.parquet")

if __name__ == '__main__':
    symbols = get_symbols()
    print(f"Total symbols: {len(symbols)}")
    start = date.today().replace(year=date.today().year - 1)
    end = date.today()
    fetch_and_store(symbols, start, end)
    print("Ingestion complete.")
