"""
NSE Historical Futures Data Downloader
=======================================
Downloads NSE Bhavcopy (daily OHLC) for NIFTY futures from NSE directly.
Builds a merged dataset with Near / Next / Far aligned by date.

Usage:
    python data_downloader.py --start 2020-01-01 --end 2025-01-01

Output:
    data/nifty_futures_aligned.csv
"""

import os
import time
import argparse
import logging
from datetime import datetime, timedelta, date
from io import StringIO

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(OUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.nseindia.com",
}


# ─── NSE Bhavcopy URL builder ───────────────────────────────────────────────

def bhavcopy_url(dt: date) -> str:
    dd  = dt.strftime("%d")
    mmm = dt.strftime("%b").upper()
    yyyy = dt.strftime("%Y")
    fname = f"fo{dd}{mmm}{yyyy}bhav.csv"
    return f"https://archives.nseindia.com/content/historical/DERIVATIVES/{yyyy}/{mmm}/{fname}"


def download_bhavcopy(dt: date, session: requests.Session) -> pd.DataFrame | None:
    url = bhavcopy_url(dt)
    try:
        r = session.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return None
        df = pd.read_csv(StringIO(r.text))
        df.columns = df.columns.str.strip()
        return df
    except Exception as exc:
        log.debug(f"Failed {dt}: {exc}")
        return None


# ─── Extract NIFTY futures from bhavcopy ───────────────────────────────────

def extract_nifty_futures(df: pd.DataFrame) -> pd.DataFrame:
    fut = df[
        (df["SYMBOL"] == "NIFTY") & (df["INSTRUMENT"] == "FUTSTK") |
        (df["SYMBOL"] == "NIFTY") & (df["INSTRUMENT"] == "FUTIDX")
    ].copy()
    if fut.empty:
        return fut
    fut["EXPIRY_DT"] = pd.to_datetime(fut["EXPIRY_DT"], dayfirst=True)
    fut = fut[["EXPIRY_DT", "CLOSE"]].rename(columns={"CLOSE": "close"})
    return fut.sort_values("EXPIRY_DT")


# ─── Align near / next / far by trade date ─────────────────────────────────

def build_aligned_dataset(start: date, end: date) -> pd.DataFrame:
    session = requests.Session()
    rows = []

    current = start
    while current <= end:
        if current.weekday() >= 5:      # skip weekends
            current += timedelta(days=1)
            continue

        df_raw = download_bhavcopy(current, session)
        if df_raw is not None:
            fut = extract_nifty_futures(df_raw)
            if len(fut) >= 3:
                row = {
                    "date":        current.isoformat(),
                    "near_expiry": fut.iloc[0]["EXPIRY_DT"].date().isoformat(),
                    "next_expiry": fut.iloc[1]["EXPIRY_DT"].date().isoformat(),
                    "far_expiry":  fut.iloc[2]["EXPIRY_DT"].date().isoformat(),
                    "near_close":  float(fut.iloc[0]["close"]),
                    "next_close":  float(fut.iloc[1]["close"]),
                    "far_close":   float(fut.iloc[2]["close"]),
                }
                rows.append(row)
                log.info(f"{current} – near={row['near_close']} next={row['next_close']} far={row['far_close']}")

        current += timedelta(days=1)
        time.sleep(0.3)   # be polite to NSE servers

    if not rows:
        log.warning("No data downloaded")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    out = os.path.join(OUT_DIR, "nifty_futures_aligned.csv")
    df.to_csv(out, index=False)
    log.info(f"Saved {len(df)} rows to {out}")
    return df


# ─── CLI ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download NSE NIFTY futures historical data")
    parser.add_argument("--start", default="2020-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   default=date.today().isoformat(), help="End date YYYY-MM-DD")
    args = parser.parse_args()

    build_aligned_dataset(
        date.fromisoformat(args.start),
        date.fromisoformat(args.end),
    )
