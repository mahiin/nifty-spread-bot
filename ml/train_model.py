"""
ML Spread Prediction – Training Script
=======================================
Trains a RandomForest model to predict whether curve_diff will
narrow in the next N bars (making our butterfly trade profitable).

Output:
  - data/ml/spread_model.pkl  (loaded by Lambda from S3)

Usage:
    python train_model.py
    python train_model.py --lookahead 3 --model_type rf
"""

import os
import pickle
import argparse
import logging

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

DATA_FILE  = os.path.join(os.path.dirname(__file__), "..", "data", "nifty_futures_aligned.csv")
MODEL_DIR  = os.path.join(os.path.dirname(__file__), "..", "data", "ml")
os.makedirs(MODEL_DIR, exist_ok=True)


def build_features(df: pd.DataFrame, lookback: int = 20) -> pd.DataFrame:
    df = df.copy()

    df["spread1"]    = df["next_close"] - df["near_close"]
    df["spread2"]    = df["far_close"]  - df["next_close"]
    df["curve_diff"] = df["spread1"]    - df["spread2"]

    roll = df["curve_diff"].rolling(lookback)
    df["mean"]   = roll.mean()
    df["std"]    = roll.std()
    df["zscore"] = (df["curve_diff"] - df["mean"]) / df["std"].replace(0, np.nan)

    # Lag features
    for lag in [1, 2, 3]:
        df[f"curve_diff_lag{lag}"] = df["curve_diff"].shift(lag)
        df[f"zscore_lag{lag}"]     = df["zscore"].shift(lag)

    # Momentum
    df["spread1_change"] = df["spread1"].diff()
    df["spread2_change"] = df["spread2"].diff()
    df["cd_momentum"]    = df["curve_diff"].diff(3)

    # Days-to-expiry proxy (approximate from near_expiry if available)
    if "near_expiry" in df.columns:
        df["near_expiry"] = pd.to_datetime(df["near_expiry"])
        df["date_dt"]     = pd.to_datetime(df["date"])
        df["dte"]         = (df["near_expiry"] - df["date_dt"]).dt.days.clip(0, 30)
    else:
        df["dte"] = 15  # default mid-month

    return df.dropna()


def build_target(df: pd.DataFrame, lookahead: int = 2) -> pd.Series:
    """
    Binary label:
      1 = curve_diff narrows (profitable for our butterfly)
      0 = curve_diff widens  (loss)
    """
    future_diff = df["curve_diff"].shift(-lookahead)
    return (future_diff < df["curve_diff"]).astype(int)


FEATURE_COLS = [
    "spread1", "spread2", "curve_diff", "zscore",
    "curve_diff_lag1", "curve_diff_lag2", "curve_diff_lag3",
    "zscore_lag1", "zscore_lag2", "zscore_lag3",
    "spread1_change", "spread2_change", "cd_momentum", "dte",
]


def train(data_file: str, lookahead: int, model_type: str):
    df = pd.read_csv(data_file)
    log.info(f"Loaded {len(df)} rows")

    df = build_features(df)
    y  = build_target(df, lookahead)

    # Align
    valid = y.notna()
    df = df[valid]
    y  = y[valid]

    X = df[FEATURE_COLS]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=False
    )

    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s  = scaler.transform(X_test)

    if model_type == "gb":
        model = GradientBoostingClassifier(n_estimators=200, max_depth=4, learning_rate=0.05)
    else:
        model = RandomForestClassifier(n_estimators=200, max_depth=6, random_state=42, n_jobs=-1)

    model.fit(X_train_s, y_train)
    preds = model.predict(X_test_s)

    log.info("\n" + classification_report(y_test, preds, target_names=["WIDEN", "NARROW"]))

    # Save
    bundle = {"model": model, "scaler": scaler, "features": FEATURE_COLS}
    out = os.path.join(MODEL_DIR, "spread_model.pkl")
    with open(out, "wb") as f:
        pickle.dump(bundle, f)
    log.info(f"Model saved to {out}")

    # Feature importance
    if hasattr(model, "feature_importances_"):
        fi = pd.Series(model.feature_importances_, index=FEATURE_COLS)
        fi = fi.sort_values(ascending=False)
        log.info(f"\nTop features:\n{fi.head(8).to_string()}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookahead",  type=int, default=2,  help="Bars ahead to predict")
    ap.add_argument("--model_type", default="rf",         help="rf | gb")
    args = ap.parse_args()

    if not os.path.exists(DATA_FILE):
        log.error(f"Data not found: {DATA_FILE}. Run backtest/data_downloader.py first.")
    else:
        train(DATA_FILE, args.lookahead, args.model_type)
