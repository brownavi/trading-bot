from fastapi import FastAPI, HTTPException
import json, os

app = FastAPI()

@app.get("/signals")
def get_signals():
    fn = "signals.json"
    if not os.path.exists(fn):
        raise HTTPException(status_code=404, detail="signals.json not found")
    with open(fn) as f:
        return json.load(f)
