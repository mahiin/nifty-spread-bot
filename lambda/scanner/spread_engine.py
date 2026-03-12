"""
Core spread engine.
Handles:
  - Auto-detecting near/next/far futures
  - Spread and Z-score calculation
  - OI divergence & volume imbalance signals
  - Expiry compression model
  - Signal strength scoring
  - Quantity sizing with strict risk parameters
"""
import os
from datetime import date, datetime
from typing import Optional
import pandas as pd
import numpy as np
import pytz

UNDERLYING = os.environ.get("UNDERLYING", "NIFTY")
LOT_SIZE = int(os.environ.get("LOT_SIZE", "75"))
IST = pytz.timezone("Asia/Kolkata")

# ─── Futures auto-detection ────────────────────────────────────────────────

def get_active_futures(broker, underlying: str = UNDERLYING) -> dict:
    """
    Fetch NFO instrument dump and return the 3 nearest expiry futures.
    Auto-rolls every month – no manual symbol entry needed.
    """
    df = broker.get_instruments("NFO")
    fut = df[(df["name"] == underlying) & (df["instrumenttype"].isin(["FUT", "FUTIDX", "FUTSTK"]))].copy()
    fut["expiry"] = pd.to_datetime(fut["expiry"])
    fut = fut[fut["expiry"] >= pd.Timestamp.now()].sort_values("expiry").reset_index(drop=True)

    if len(fut) < 3:
        raise RuntimeError(f"Only {len(fut)} active futures found for {underlying}")

    return {
        "near":        fut.iloc[0]["symbol"],
        "near_token":  fut.iloc[0]["token"],
        "next":        fut.iloc[1]["symbol"],
        "next_token":  fut.iloc[1]["token"],
        "far":         fut.iloc[2]["symbol"],
        "far_token":   fut.iloc[2]["token"],
        "near_expiry": fut.iloc[0]["expiry"],
        "next_expiry": fut.iloc[1]["expiry"],
        "far_expiry":  fut.iloc[2]["expiry"],
    }


# ─── Spread maths ──────────────────────────────────────────────────────────

def compute_spreads(near: float, nxt: float, far: float) -> dict:
    spread1 = nxt - near          # APR – MAR
    spread2 = far - nxt           # MAY – APR
    curve_diff = spread1 - spread2
    return {"spread1": spread1, "spread2": spread2, "curve_diff": curve_diff}


def compute_zscore(current: float, history: list) -> float:
    if len(history) < 10:
        return 0.0
    arr = np.array(history, dtype=float)
    mean, std = arr.mean(), arr.std()
    return 0.0 if std == 0 else (current - mean) / std


# ─── OI divergence ─────────────────────────────────────────────────────────

def check_oi_divergence(quote_data: dict, near_sym: str, next_sym: str) -> dict:
    """
    Rising OI in next + falling OI in near → institutional rollover.
    Adds conviction to butterfly signal.
    """
    near_oi = float(quote_data.get(f"NFO:{near_sym}", {}).get("opnInterest", 0) or 0)
    next_oi = float(quote_data.get(f"NFO:{next_sym}", {}).get("opnInterest", 0) or 0)
    oi_diff = next_oi - near_oi
    rollover_signal = oi_diff > 50_000
    return {
        "near_oi":         near_oi,
        "next_oi":         next_oi,
        "oi_diff":         oi_diff,
        "rollover_signal": rollover_signal,
    }


# ─── Volume imbalance ──────────────────────────────────────────────────────

def check_volume_imbalance(quote_data: dict, near_sym: str, next_sym: str) -> dict:
    """
    Heavy volume in next vs near → institutional repositioning.
    """
    near_vol = float(quote_data.get(f"NFO:{near_sym}", {}).get("tradeVolume", 1) or 1)
    next_vol = float(quote_data.get(f"NFO:{next_sym}", {}).get("tradeVolume", 1) or 1)
    vol_ratio = next_vol / max(near_vol, 1)
    return {
        "near_volume":  near_vol,
        "next_volume":  next_vol,
        "vol_ratio":    round(vol_ratio, 3),
        "volume_signal": vol_ratio > 3,
    }


# ─── Expiry compression model ──────────────────────────────────────────────

def days_to_expiry(expiry_date) -> int:
    today = date.today()
    delta = (pd.Timestamp(expiry_date).date() - today).days
    return max(0, delta)


def expiry_bias(dte: int) -> str:
    """
    Near expiry the near-month spread collapses.
    Bias: SHORT_SPREAD = sell the spread (near will converge faster).
    """
    if dte <= 3:
        return "STRONG_SHORT_SPREAD"
    if dte <= 7:
        return "SHORT_SPREAD"
    return "NORMAL"


# ─── Signal strength scoring ───────────────────────────────────────────────

def signal_strength(zscore: float, oi_signal: bool,
                    vol_signal: bool, exp_bias: str) -> float:
    """
    Composite score 0–5.  Execute only if >= min_signal_strength (2.5).
    """
    score = min(abs(zscore), 3.0)         # Z-score cap at 3
    if oi_signal:
        score += 0.5
    if vol_signal:
        score += 0.5
    if exp_bias == "STRONG_SHORT_SPREAD":
        score += 1.0
    elif exp_bias == "SHORT_SPREAD":
        score += 0.5
    return round(min(score, 5.0), 2)


# ─── Position sizing ───────────────────────────────────────────────────────

def recommended_qty(strength: float, capital: float,
                    lot_size: int = LOT_SIZE,
                    margin_per_set: float = 100_000) -> int:
    """
    Conservative sizing: never risk more than capital allows.
    Returns total contracts (already multiplied by lot_size).

    1 butterfly set = 1 near + 2 next + 1 far = 4 legs.
    Approx margin per set ≈ ₹1L (hedged position benefit).
    """
    max_sets = max(1, int(capital // margin_per_set))

    if strength >= 4.0:
        multiplier = 1.0
    elif strength >= 3.0:
        multiplier = 0.7
    elif strength >= 2.5:
        multiplier = 0.5
    else:
        multiplier = 0.3

    sets = max(1, int(max_sets * multiplier))
    return sets * lot_size


# ─── Stoploss / target levels ──────────────────────────────────────────────

def compute_levels(signal: str, entry_curve_diff: float,
                   sl_pts: float = 20, tgt_pts: float = 15) -> dict:
    """
    For SELL butterfly (next overpriced): expect curve_diff to fall.
    For BUY butterfly (next underpriced): expect curve_diff to rise.
    """
    if "SELL" in signal or "OVERPRICED" in signal:
        stoploss = entry_curve_diff + sl_pts   # exits if spread expands further
        target   = entry_curve_diff - tgt_pts  # profit when spread narrows
    else:
        stoploss = entry_curve_diff - sl_pts
        target   = entry_curve_diff + tgt_pts
    return {"stoploss_diff": round(stoploss, 2), "target_diff": round(target, 2)}


# ─── NSE Holidays (updated annually) ──────────────────────────────────────
# Official BSE/NSE trading holidays. Saturdays/Sundays are already excluded
# by weekday check. Update this list each year from:
# https://www.nseindia.com/global/content/market_data/equities/equities/market_timings_holidays.htm

NSE_HOLIDAYS_DEFAULT = {
    # 2025
    "2025-02-26",  # Mahashivratri
    "2025-03-14",  # Holi
    "2025-03-31",  # Id-Ul-Fitr (Eid)
    "2025-04-14",  # Dr. B.R. Ambedkar Jayanti
    "2025-04-18",  # Good Friday
    "2025-05-01",  # Maharashtra Day
    "2025-08-15",  # Independence Day
    "2025-08-27",  # Ganesh Chaturthi
    "2025-10-02",  # Mahatma Gandhi Jayanti
    "2025-10-24",  # Diwali – Laxmi Pujan
    "2025-11-05",  # Guru Nanak Jayanti
    "2025-12-25",  # Christmas

    # 2026
    "2026-01-26",  # Republic Day
    "2026-03-03",  # Holi
    "2026-03-20",  # Id-Ul-Fitr (Eid) – approximate
    "2026-04-03",  # Good Friday
    "2026-04-14",  # Dr. B.R. Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-08-15",  # Independence Day
    "2026-09-14",  # Ganesh Chaturthi – approximate
    "2026-10-02",  # Mahatma Gandhi Jayanti
    "2026-11-12",  # Diwali – Laxmi Pujan – approximate
    "2026-11-23",  # Guru Nanak Jayanti – approximate
    "2026-12-25",  # Christmas
}


# ─── Market hours check ────────────────────────────────────────────────────

def is_market_open(nse_holidays: list = None) -> bool:
    """
    Returns True only if:
      - It is a weekday (Mon–Fri)
      - It is NOT an NSE holiday
      - Current IST time is between 09:15 and 15:30

    nse_holidays: list of 'YYYY-MM-DD' strings loaded from DynamoDB config.
                  Falls back to NSE_HOLIDAYS_DEFAULT when not provided.
    """
    now = datetime.now(IST)

    # Weekend check
    if now.weekday() >= 5:
        return False

    # Holiday check
    today = now.strftime("%Y-%m-%d")
    holidays = set(nse_holidays) if nse_holidays else NSE_HOLIDAYS_DEFAULT
    if today in holidays:
        return False

    # Market hours check (09:15 – 15:30 IST)
    open_  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    close_ = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return open_ <= now <= close_


def is_nse_holiday(nse_holidays: list = None) -> bool:
    """Returns True if today is a weekend or NSE holiday."""
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return True
    today = now.strftime("%Y-%m-%d")
    holidays = set(nse_holidays) if nse_holidays else NSE_HOLIDAYS_DEFAULT
    return today in holidays


def past_entry_cutoff(cutoff: str = "14:30") -> bool:
    now = datetime.now(IST)
    h, m = map(int, cutoff.split(":"))
    cutoff_dt = now.replace(hour=h, minute=m, second=0, microsecond=0)
    return now >= cutoff_dt
