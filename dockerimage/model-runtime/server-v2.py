from typing import Any, Dict, List, Optional

import joblib, os, logging
import pandas as pd
from fastapi import FastAPI, File, HTTPException
from pydantic import BaseModel

# -------- CONFIG --------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# MODEL_PATH = "/mnt/models/lightgbm.pkl"
# MODEL_PATH = "./lightgbm.pkl"
MODEL_PATH = os.getenv("MODEL_PATH")

if MODEL_PATH == "":
    logger.error(f"Model path not found!")
    raise SystemExit(f"No value is set on $MODEL_PATH env variable!")
else:
    logger.info(f"Model path is set to {MODEL_PATH}")

EXOG_COLUMNS = [
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

# -------- LOAD MODEL --------
try:
    model = joblib.load(MODEL_PATH)
    print(model)
    DEFAULT_STEPS = int(model.max_step)
    logger.info(f"Model's max step: {DEFAULT_STEPS}")
    LAST_INDEX = int(model.training_range_.max())
    logger.info(f"Model's last train index: {LAST_INDEX}")
except Exception as e:
    raise SystemExit(f"Failed to load model from {MODEL_PATH}: {e}")

app = FastAPI(title="ForecasterDirect (LightGBM) serving", version="1.0")

class PredictRequest(BaseModel):
    # exog: list of rows (each row is a mapping column->value) OR a 2D list/array
    exog: List[Dict[str, Any]]
    # optionally specify steps (int). If omitted, server uses model.steps (must exist).
    steps: Optional[int] = DEFAULT_STEPS

def _validate_and_prepare_exog(exog_df: pd.DataFrame, steps: int) -> pd.DataFrame:
    """
    Ensure exog_df has enough rows and expected columns.
    Returns a dataframe with exactly `steps` rows (first steps rows).
    """
    # If EXOG_COLUMNS configured by user, enforce order
    if EXOG_COLUMNS is not None:
        missing = [c for c in EXOG_COLUMNS if c not in exog_df.columns]
        if missing:
            logger.warning(f"Missing exog columns: {missing}")
            raise HTTPException( status_code=400, detail=f"Missing exog columns: {missing}" ) 
        exog_df = exog_df[EXOG_COLUMNS]

    exog_df = exog_df[EXOG_COLUMNS]

    if len(exog_df) < steps:
        logger.warning(f"Not enough exog rows: got {len(exog_df)}, need {steps}")
        raise HTTPException(status_code=400, detail=f"Not enough exog rows: got {len(exog_df)}, need {steps}")
    exog_slice = exog_df[:steps]
    exog_slice.index = range(LAST_INDEX + 1, LAST_INDEX + 1 + len(exog_slice))
    return exog_slice

@app.get("/")
def health():
    return {
        "status": "API server is running.",
    }

@app.post("/predict")
def predict(req: PredictRequest):
    # determine steps
    steps = req.steps if req.steps is not None else DEFAULT_STEPS
    # steps = DEFAULT_STEPS
    if steps is None:
        logger.warning("No steps provided and model has no default 'steps' attribute.")
        raise HTTPException(status_code=400,detail="No steps provided and model has no default 'steps' attribute.")

    # convert exog list-of-dicts to DataFrame
    try:
        exog_df = pd.DataFrame(req.exog)
    except Exception as e:
        logger.warning(f"Invalid exog format: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid exog format: {e}")

    exog_for_pred = _validate_and_prepare_exog(exog_df, steps)

    # call ForecasterDirect.predict(steps=..., exog=...)
    try:
        preds = model.predict(steps=steps, exog=exog_for_pred)
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")

    # preds is typically a pandas Series or DataFrame (depending on forecaster config)
    # normalize to list-of-values or dict
    if isinstance(preds, pd.Series):
        out = preds.tolist()
    elif isinstance(preds, (pd.DataFrame,)):
        out = preds.to_dict(orient="list")
    elif isinstance(preds, (list, tuple)):
        out = list(preds)
    else:
        # fallback: try to convert to list
        try:
            out = list(preds)
        except Exception:
            out = str(preds)

    return {"used_steps": steps, "used_rows": len(exog_for_pred), "predictions": out}