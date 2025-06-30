# app.py — minimal FastAPI server to serve your signals
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Trading Bot Signals")

class Signal(BaseModel):
    symbol: str
    position: str  # "long" or "short"

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/signals", response_model=List[Signal])
def get_signals(symbols: List[str]):
    # Placeholder: return dummy flat signals
    return [Signal(symbol=s, position="flat") for s in symbols]

