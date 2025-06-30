import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

class BacktestResult(BaseModel):
    stats: dict

# Dummy in-memory store (swap for real DB or file)
RESULTS = {}

@app.post("/backtest/{symbol}")
async def backtest(symbol: str):
    key = symbol.upper()
    if key not in RESULTS:
        raise HTTPException(status_code=404, detail="No backtest for this symbol")
    return BacktestResult(stats=RESULTS[key])

@app.get("/health")
async def health():
    return {"status": "ok"}
