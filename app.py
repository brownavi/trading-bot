import os
from fastapi import FastAPI
from pydantic import BaseModel

class TradeRequest(BaseModel):
    symbol: str
    side: str
    qty: int

app = FastAPI()

@app.get("/")
def read_root():
    return {"status": "up"}

@app.post("/trade")
def place_trade(req: TradeRequest):
    # placeholder
    return {"received": req.dict()}
