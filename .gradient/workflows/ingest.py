#!/usr/bin/env python3
import os
from datetime import date, timedelta
import pandas as pd
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# Load credentials
API_KEY = os.getenv('APCA_API_KEY_ID')
API_SECRET = os.getenv('APCA_API_SECRET_KEY')
if not API_KEY or not API_SECRET:
    raise ValueError("Set APCA_API_KEY_ID and APCA_API_SECRET_KEY in env")

# Initialize client
client = StockHistoricalDataClient(API_KEY, API_SECRET)

# Build symbol list
sp500 = pd.read_html(
    'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
    flavor='bs4'
)[0]['Symbol'].astype(str).tolist()

dow = [
    df for df in pd.read_html(
        'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average',
        flavor='bs4'
    ) if 'Symbol' in df.columns
][0]['Symbol'].astype(str).tolist()

nasdaq = pd.read_csv(
    'https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv'
)['Symbol'].dropna().astype(str).tolist()

symbols = sorted({s for s in sp500 + dow + nasdaq if s.strip()})

# Create data folder
DATA_FOLDER = 'data/history'
os.makedirs(DATA_FOLDER, exist_ok=True)

# Fetch last 5 years
end = date.today()
start = end - timedelta(days=5*365 + 30)
for sym in symbols:
    bars = client.get_stock_bars(
        StockBarsRequest(
            symbol_or_symbols=sym,
            timeframe=TimeFrame.Day,
            start=start.isoformat(),
            end=end.isoformat(),
        )
    ).df
    if not bars.empty:
        bars.to_parquet(f"{DATA_FOLDER}/{sym}.parquet")

print("Ingest complete.")
