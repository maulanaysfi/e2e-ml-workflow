from typing import Any, Dict, List

import joblib
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel

model_path = "/mnt/models/lightgbm.pkl"

model = joblib.load(model_path)

features = [
    "Invoices",
    "ItemSold",
    "Year",
    "Month",
    "Week",
    "Date",
    "Quantity",
    "Sales",
    "DayNo",
    "IsMonthStart",
    "IsMonthEnd",
    "Season",
    "Qty_lag1",
    "Qty_lag7",
    "Qty_lag30",
    "Qty_RolSum7",
    "Qty_RolSum30",
    "Qty_RolMean7",
    "Qty_RolMean30",
    "Qty_RolStd7",
    "Qty_RolStd30",
    "Qty_EMA7",
    "Sls_lag1",
    "Sls_lag7",
    "Sls_lag30",
    "Sls_RolSum7",
    "Sls_RolSum30",
    "Sls_RolMean7",
    "Sls_RolMean30",
    "Sls_RolStd7",
    "Sls_RolStd30",
    "Sls_EMA7",
]

app = FastAPI(
    title="LightGBM Direct Forecaster - Sales prediction",
    description="Predict sales from an online retail performance using LightGBM Regressor model.",
    version="1.0.0",
)


class PredictionRequest(BaseModel):
    data: List[Dict[str, Any]]


@app.get("/")
def get_root():
    return {"status": "OK", "message": "Model server is running."}


@app.get("/predict")
def get_predict():
    return {"status": "OK", "message": "Use POST method to get predictions :)"}


@app.post("/predict")
def post_predict(request: PredictionRequest):
    if not request.data:
        return {"Error": "Empty input data!"}

    input_df = pd.DataFrame(request.data)
    input_df = input_df[features]
    preds = model.predict(input_df)

    return {"input_rows": len(request.data), "predictions": preds.tolist()}
