"""
Option Chain Intelligence Engine
==================================
Fetches the NIFTY option chain from NSE India and computes actionable metrics:

  1. PCR  (Put-Call Ratio)
       Total put OI / total call OI across all strikes for the nearest expiry.
       PCR > 1.3  → Contrarian BULLISH  (excess put writing = market not expected to fall)
       PCR < 0.7  → Contrarian BEARISH  (excess call writing = market not expected to rally)
       PCR 0.7–1.3 → NEUTRAL

  2. Max Pain
       The strike price at which the total intrinsic value payable by option writers
       is minimised — i.e. where option BUYERS collectively lose the most.
       NIFTY has a statistical tendency to close near max pain on expiry (Thursday).

  3. OI Walls
       Strikes with the highest call OI → key resistance.
       Strikes with the highest put OI  → key support.

  4. Iron Condor Strikes
       Optimal short/long strikes derived from VIX-based 1-sigma range.
       short_offset = 0.45 × sigma  (rounded to nearest 50)
       long_offset  = short_offset + 200 (protection wing)

All data is cached in DynamoDB (nifty_config) for CACHE_MINUTES.
The engine is fully defensive — any failure returns neutral empty output.

Usage
-----
  from option_chain_engine import fetch_option_chain, get_iron_condor_strikes

  chain = fetch_option_chain()          # cached; safe to call every scan
  pcr   = chain["pcr"]
  mp    = chain["max_pain"]
  ic    = get_iron_condor_strikes(spot=23000, vix=18.0, dte=4)
"""

from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime
from typing import Optional

import boto3
import pytz
import requests

# ── Config ────────────────────────────────────────────────────────────────────
IST            = pytz.timezone("Asia/Kolkata")
REGION         = os.environ.get("AWS_REGION_NAME", "ap-south-1")
CFG_TABLE      = os.environ.get("CONFIG_TABLE",    "nifty_config")
CACHE_KEY      = "OPTION_CHAIN_CACHE"
CACHE_MINUTES  = 15          # refresh option chain every 15 minutes
UNDERLYING     = os.environ.get("UNDERLYING", "NIFTY")

_ddb = boto3.resource("dynamodb", region_name=REGION)


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_option_chain() -> dict:
    """
    Return option chain metrics for NIFTY near-expiry.
    Uses DynamoDB cache; refreshes from NSE if older than CACHE_MINUTES.

    Returns dict with:
      pcr           – Put-Call Ratio (float)
      pcr_signal    – PCR_BULLISH | PCR_BEARISH | PCR_NEUTRAL
      max_pain      – int strike
      call_wall     – highest call-OI strike (resistance)
      put_wall      – highest put-OI strike  (support)
      atm_iv        – average ATM IV %
      total_call_oi – int
      total_put_oi  – int
      expiry        – expiry date string used
      fetched_at    – ISO timestamp
      source        – NSE_LIVE | CACHE | FALLBACK
    """
    cached = _load_cache()
    if cached and _is_fresh(cached):
        return cached | {"source": "CACHE"}

    live = _fetch_from_nse()
    if live:
        _save_cache(live)
        return live

    if cached:
        print("[option_chain] Using stale cache (NSE unavailable)")
        return cached | {"source": "STALE_CACHE"}
    return _empty()


def get_iron_condor_strikes(spot: float, vix: float, dte: int) -> dict:
    """
    Compute optimal Iron Condor strike levels using VIX-based 1-sigma range.

    short_offset = 0.45 × sigma  (≈ 1 std dev at 45% confidence, OTM enough
                                   to have low delta ~0.20–0.25)
    long_offset  = short_offset + 200 pts (protection wing)

    All strikes rounded to nearest 50 (NIFTY option strike step).

    Returns:
      atm         – int ATM strike
      short_call  – int short call strike
      long_call   – int long call strike (wing)
      short_put   – int short put strike
      long_put    – int long put strike  (wing)
      short_offset – pts OTM for short strikes
      sigma_pts   – calculated 1-sigma move in points
      wing_width  – pts between short and long (protection)
    """
    atm          = int(round(spot / 50) * 50)
    # 1-sigma move = VIX% × spot × sqrt(dte/365)
    sigma_pts    = (vix / 100.0) * spot * math.sqrt(max(dte, 1) / 365.0)
    short_offset = max(150, int(round(0.45 * sigma_pts / 50) * 50))
    long_offset  = short_offset + 200

    return {
        "atm":          atm,
        "short_call":   atm + short_offset,
        "long_call":    atm + long_offset,
        "short_put":    atm - short_offset,
        "long_put":     atm - long_offset,
        "short_offset": short_offset,
        "sigma_pts":    round(sigma_pts, 0),
        "wing_width":   200,
    }


# ── NSE Fetch ─────────────────────────────────────────────────────────────────

def _nse_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer":         "https://www.nseindia.com",
        "Connection":      "keep-alive",
    })
    try:
        session.get("https://www.nseindia.com", timeout=8)
        time.sleep(0.3)
    except Exception as exc:
        print(f"[option_chain] NSE seed failed: {exc}")
    return session


def _fetch_from_nse() -> Optional[dict]:
    """Fetch option chain from NSE and compute all metrics. Returns None on failure."""
    try:
        session = _nse_session()
        url     = (
            f"https://www.nseindia.com/api/option-chain-indices"
            f"?symbol={UNDERLYING}"
        )
        resp = session.get(url, timeout=12)
        resp.raise_for_status()
        data = resp.json()

        records   = data.get("records", {})
        filtered  = data.get("filtered", {})
        all_data  = records.get("data", [])
        expiries  = records.get("expiryDates", [])

        if not all_data or not expiries:
            print("[option_chain] Empty response from NSE")
            return None

        near_expiry = expiries[0]   # nearest expiry

        # ── Filter to near expiry ──────────────────────────────────────────
        near_data = [r for r in all_data if r.get("expiryDate") == near_expiry]

        if not near_data:
            return None

        # ── Build strike → OI map ─────────────────────────────────────────
        strike_map: dict[int, dict] = {}
        for row in near_data:
            k          = int(row.get("strikePrice", 0))
            call_entry = row.get("CE", {}) or {}
            put_entry  = row.get("PE",  {}) or {}
            strike_map[k] = {
                "call_oi": int(call_entry.get("openInterest", 0) or 0),
                "put_oi":  int(put_entry.get("openInterest",  0) or 0),
                "call_iv": float(call_entry.get("impliedVolatility", 0) or 0),
                "put_iv":  float(put_entry.get("impliedVolatility",  0) or 0),
                "call_ltp": float(call_entry.get("lastPrice", 0) or 0),
                "put_ltp":  float(put_entry.get("lastPrice",  0) or 0),
            }

        strikes = sorted(strike_map.keys())
        if not strikes:
            return None

        # ── PCR ───────────────────────────────────────────────────────────
        total_call_oi = sum(v["call_oi"] for v in strike_map.values())
        total_put_oi  = sum(v["put_oi"]  for v in strike_map.values())
        pcr = round(total_put_oi / total_call_oi, 3) if total_call_oi > 0 else 1.0

        # ── Max Pain ──────────────────────────────────────────────────────
        max_pain = _compute_max_pain(strike_map, strikes)

        # ── OI Walls ──────────────────────────────────────────────────────
        call_wall = max(strikes, key=lambda k: strike_map[k]["call_oi"])
        put_wall  = max(strikes, key=lambda k: strike_map[k]["put_oi"])

        # ── ATM IV + IV Skew (put IV vs call IV near ATM) ────────────────
        underlying = float(records.get("underlyingValue", 0) or
                           filtered.get("underlyingValue", 0) or 0)
        atm_iv, call_iv_atm, put_iv_atm = _compute_atm_iv(strike_map, strikes, underlying)
        # Skew: positive = puts more expensive → bearish sentiment
        iv_skew = round(put_iv_atm - call_iv_atm, 2)
        iv_skew_signal = (
            "PUT_SKEW_HIGH"  if iv_skew >  3.0 else   # bearish fear
            "CALL_SKEW_HIGH" if iv_skew < -3.0 else   # bullish complacency
            "SKEW_NEUTRAL"
        )

        result = {
            "pcr":            pcr,
            "pcr_signal":     _pcr_signal(pcr),
            "max_pain":       max_pain,
            "call_wall":      call_wall,
            "put_wall":       put_wall,
            "atm_iv":         atm_iv,
            "call_iv_atm":    call_iv_atm,
            "put_iv_atm":     put_iv_atm,
            "iv_skew":        iv_skew,
            "iv_skew_signal": iv_skew_signal,
            "total_call_oi":  total_call_oi,
            "total_put_oi":   total_put_oi,
            "underlying":     underlying,
            "expiry":         near_expiry,
            "fetched_at":     datetime.now(IST).isoformat(),
            "source":         "NSE_LIVE",
        }
        print(
            f"[option_chain] PCR={pcr:.2f} ({result['pcr_signal']})  "
            f"MaxPain={max_pain}  CallWall={call_wall}  PutWall={put_wall}  "
            f"ATM_IV={atm_iv:.1f}%  Skew={iv_skew:+.1f} ({iv_skew_signal})"
        )
        return result

    except Exception as exc:
        print(f"[option_chain] Fetch error: {exc}")
        return None


# ── Metric calculations ───────────────────────────────────────────────────────

def _compute_max_pain(strike_map: dict, strikes: list) -> int:
    """
    Max pain = the expiry price where total payoff to option buyers is minimised
    (= where option writers keep the most premium).

    For each candidate expiry price S:
      pain = sum over all call strikes K<S of (S-K)*call_OI
           + sum over all put  strikes K>S of (K-S)*put_OI
    Return the S with minimum pain.
    """
    min_pain = float("inf")
    result   = strikes[len(strikes) // 2]   # default = middle strike

    for s in strikes:
        pain = 0
        for k, v in strike_map.items():
            if s > k:                           # in-the-money call
                pain += (s - k) * v["call_oi"]
            if s < k:                           # in-the-money put
                pain += (k - s) * v["put_oi"]
        if pain < min_pain:
            min_pain = pain
            result   = s

    return result


def _compute_atm_iv(strike_map: dict, strikes: list, underlying: float) -> tuple:
    """
    Average IV of the 3 strikes nearest to current spot.
    Returns (avg_iv, avg_call_iv, avg_put_iv).
    """
    if not underlying:
        return 0.0, 0.0, 0.0
    near      = sorted(strikes, key=lambda k: abs(k - underlying))[:3]
    call_ivs  = []
    put_ivs   = []
    for k in near:
        v = strike_map[k]
        if v["call_iv"] > 0:
            call_ivs.append(v["call_iv"])
        if v["put_iv"] > 0:
            put_ivs.append(v["put_iv"])
    avg_call = round(sum(call_ivs) / len(call_ivs), 2) if call_ivs else 0.0
    avg_put  = round(sum(put_ivs)  / len(put_ivs),  2) if put_ivs  else 0.0
    all_ivs  = call_ivs + put_ivs
    avg_all  = round(sum(all_ivs) / len(all_ivs), 2) if all_ivs else 0.0
    return avg_all, avg_call, avg_put


def _pcr_signal(pcr: float) -> str:
    if pcr >= 1.3:
        return "PCR_BULLISH"
    if pcr <= 0.7:
        return "PCR_BEARISH"
    return "PCR_NEUTRAL"


# ── DynamoDB cache ────────────────────────────────────────────────────────────

def _load_cache() -> Optional[dict]:
    try:
        resp = _ddb.Table(CFG_TABLE).get_item(Key={"config_key": CACHE_KEY})
        raw  = resp.get("Item", {}).get("config_value", "")
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _save_cache(data: dict) -> None:
    try:
        _ddb.Table(CFG_TABLE).put_item(Item={
            "config_key":   CACHE_KEY,
            "config_value": json.dumps(data),
        })
    except Exception as exc:
        print(f"[option_chain] Cache write failed: {exc}")


def _is_fresh(data: dict) -> bool:
    try:
        fetched = datetime.fromisoformat(data["fetched_at"])
        if fetched.tzinfo is None:
            fetched = IST.localize(fetched)
        age_min = (datetime.now(IST) - fetched).total_seconds() / 60
        return age_min < CACHE_MINUTES
    except Exception:
        return False


def _empty() -> dict:
    return {
        "pcr":            1.0,
        "pcr_signal":     "PCR_NEUTRAL",
        "max_pain":       0,
        "call_wall":      0,
        "put_wall":       0,
        "atm_iv":         0.0,
        "call_iv_atm":    0.0,
        "put_iv_atm":     0.0,
        "iv_skew":        0.0,
        "iv_skew_signal": "SKEW_NEUTRAL",
        "total_call_oi":  0,
        "total_put_oi":   0,
        "underlying":     0.0,
        "expiry":         "",
        "fetched_at":     datetime.now(IST).isoformat(),
        "source":         "FALLBACK",
    }
