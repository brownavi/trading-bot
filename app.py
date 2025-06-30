from flask import Flask, jsonify, request
from joblib import load
import pandas as pd

MODEL_PATH = 'models/trend_model.joblib'
app = Flask(__name__)
model = load(MODEL_PATH)

@app.route('/predict', methods=['POST'])
def predict():
    payload = request.json  # expect { "open":.., "high":.., "low":.., "close":.., "volume":.. }
    df = pd.DataFrame([payload])
    pred = model.predict(df)[0]
    return jsonify({'symbol': payload.get('symbol'), 'trend_up': bool(pred)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
