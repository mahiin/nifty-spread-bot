"""
FII/DII Institutional Flow Engine
==================================
Two data sources, both public NSE APIs, cached in DynamoDB.

1. Cash-market flow  (fetch_fii_dii)
   ─────────────────────────────────
   Source : NSE fiidiiTradeReact API  (published daily ~6 PM)
   Returns: FII/DII net buy/sell in cash market (Crores)
   Signals: FII_BULLISH / FII_BEARISH / FII_NEUTRAL (threshold ±500 Cr)

2. F&O Participant OI  (fetch_fii_futures)
   ────────────────────────────────────────
   Source : nsearchives.nseindia.com  fao_participant_oi_{DDMMYYYY}.csv
   Returns: FII index futures net (long − short contracts)
            FII options OI PCR (FII put OI / FII call OI)
   Signals: FII_FUT_LONG / FII_FUT_SHORT / FII_FUT_NEUTRAL (threshold ±5000 contracts)

   This is the "gold standard" — FII index futures positioning tells you
   whether institutions are structurally long or short the market.

   FII net long  futures > +5000 contracts → FII_FUT_LONG  (bullish tail)
   FII net short futures < -5000 contracts → FII_FUT_SHORT (bearish tail)

Usage
-----
  from fii_dii_engine import (
      fetch_fii_dii, get_fii_dii_signal,
      fetch_fii_futures, get_fii_futures_signal,
  )

  cash    = fetch_fii_dii()          # cash-market flow
  futures = fetch_fii_futures()      # F&O positioning (the gold standard)
  signal  = get_fii_futures_signal(futures)  # FII_FUT_LONG / SHORT / NEUTRAL
"""

from __future__ import annotations

import io
import json
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import boto3
import pytz
import requests

# ── Config ───────────────────────────────────────────────────────────────────
IST               = pytz.timezone("Asia/Kolkata")
REGION            = os.environ.get("AWS_REGION_NAME", "ap-south-1")
CFG_TABLE         = os.environ.get("CONFIG_TABLE",    "nifty_config")
CACHE_KEY         = "FII_DII_LATEST"
CACHE_KEY_FUT     = "FII_FUTURES_LATEST"
CACHE_HOURS       = 12          # use cached data for up to 12 hours
FII_BULL_THRESH   = 500.0       # Crores — net buy above this = bullish
FII_BEAR_THRESH   = -500.0      # Crores — net sell below this = bearish
FII_FUT_LONG_THR  = 5000        # contracts — FII net long above this = bullish
FII_FUT_SHORT_THR = -5000       # contracts — FII net short below this = bearish

_ddb = boto3.resource("dynamodb", region_name=REGION)


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_fii_dii() -> dict:
    """
    Return FII/DII net positions. Uses DynamoDB cache; refreshes from NSE if stale.

    Returns dict with keys:
      fii_net_cr  – FII net (positive = buying, negative = selling)
      dii_net_cr  – DII net
      signal      – FII_BULLISH | FII_BEARISH | FII_NEUTRAL
      data_date   – date string of the data (usually previous trading day)
      fetched_at  – ISO timestamp of last NSE fetch
      source      – NSE_LIVE | CACHE | FALLBACK
    """
    cached = _load_cache()
    if cached and _is_fresh(cached):
        return cached | {"source": "CACHE"}

    live = _fetch_from_nse()
    if live:
        _save_cache(live)
        return live

    # NSE unreachable — return cached (even if stale) or empty neutral
    if cached:
        print("[fii_dii] Using stale cache (NSE unavailable)")
        return cached | {"source": "STALE_CACHE"}
    return _empty()


def get_fii_dii_signal(data: dict) -> str:
    """Extract signal string from fetch_fii_dii() result."""
    return data.get("signal", "FII_NEUTRAL")


# ── NSE Fetch ─────────────────────────────────────────────────────────────────

def _nse_session() -> requests.Session:
    """
    Build a requests.Session with NSE-compatible headers and cookies.
    NSE requires an initial GET to nseindia.com to set session cookies
    before API calls will succeed.
    """
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
        # Seed cookies
        session.get("https://www.nseindia.com", timeout=8)
        time.sleep(0.3)
    except Exception as exc:
        print(f"[fii_dii] NSE seed request failed: {exc}")
    return session


def _fetch_from_nse() -> Optional[dict]:
    """Fetch FII/DII data from NSE API. Returns None on any failure."""
    try:
        session = _nse_session()
        resp = session.get(
            "https://www.nseindia.com/api/fiidiiTradeReact",
            timeout=10,
        )
        resp.raise_for_status()
        rows = resp.json()

        fii_net   = 0.0
        fii_buy   = 0.0
        fii_sell  = 0.0
        dii_net   = 0.0
        dii_buy   = 0.0
        dii_sell  = 0.0
        data_date = datetime.now(IST).strftime("%d-%b-%Y")

        for row in rows:
            name = str(row.get("name", "")).upper()
            net  = float(row.get("netValue",   0) or 0)
            buy  = float(row.get("buyValue",   0) or 0)
            sell = float(row.get("sellValue",  0) or 0)
            d    = row.get("date", "")
            if "FII" in name or "FPI" in name:
                fii_net   = net
                fii_buy   = buy
                fii_sell  = sell
                data_date = d or data_date
            elif "DII" in name:
                dii_net  = net
                dii_buy  = buy
                dii_sell = sell

        result = {
            "fii_net_cr":  round(fii_net,  2),
            "fii_buy_cr":  round(fii_buy,  2),
            "fii_sell_cr": round(fii_sell, 2),
            "dii_net_cr":  round(dii_net,  2),
            "dii_buy_cr":  round(dii_buy,  2),
            "dii_sell_cr": round(dii_sell, 2),
            "signal":      _derive_signal(fii_net),
            "data_date":   data_date,
            "fetched_at":  datetime.now(IST).isoformat(),
            "source":      "NSE_LIVE",
        }
        print(
            f"[fii_dii] FII net={fii_net:+.0f}Cr (buy={fii_buy:.0f} sell={fii_sell:.0f})  "
            f"DII net={dii_net:+.0f}Cr  signal={result['signal']}"
        )
        return result

    except Exception as exc:
        print(f"[fii_dii] NSE fetch error: {exc}")
        return None


# ── Signal derivation ─────────────────────────────────────────────────────────

def _derive_signal(fii_net: float) -> str:
    if fii_net >= FII_BULL_THRESH:
        return "FII_BULLISH"
    if fii_net <= FII_BEAR_THRESH:
        return "FII_BEARISH"
    return "FII_NEUTRAL"


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
        print(f"[fii_dii] Cache write failed: {exc}")


def _is_fresh(data: dict) -> bool:
    try:
        fetched = datetime.fromisoformat(data["fetched_at"])
        if fetched.tzinfo is None:
            fetched = IST.localize(fetched)
        age_h = (datetime.now(IST) - fetched).total_seconds() / 3600
        return age_h < CACHE_HOURS
    except Exception:
        return False


def _empty() -> dict:
    return {
        "fii_net_cr":  0.0,
        "fii_buy_cr":  0.0,
        "fii_sell_cr": 0.0,
        "dii_net_cr":  0.0,
        "dii_buy_cr":  0.0,
        "dii_sell_cr": 0.0,
        "signal":      "FII_NEUTRAL",
        "data_date":   datetime.now(IST).strftime("%d-%b-%Y"),
        "fetched_at":  datetime.now(IST).isoformat(),
        "source":      "FALLBACK",
    }


# ══════════════════════════════════════════════════════════════════════════════
# FII F&O Participant Open Interest  (the "gold standard" signal)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_fii_futures() -> dict:
    """
    Return FII index futures positioning from NSE participant-wise OI CSV.
    Published daily at ~6–7 PM IST. Cached 12 h in DynamoDB.

    Returns dict with keys:
      fii_fut_long    – FII index futures long contracts
      fii_fut_short   – FII index futures short contracts
      fii_fut_net     – net (long − short); positive = net long = bullish
      fii_opt_call_oi – FII index options call OI (long + short)
      fii_opt_put_oi  – FII index options put OI  (long + short)
      fii_opt_pcr     – FII options PCR (put_oi / call_oi)
      fut_signal      – FII_FUT_LONG | FII_FUT_SHORT | FII_FUT_NEUTRAL
      data_date       – date string of the report
      fetched_at      – ISO timestamp
      source          – NSE_LIVE | CACHE | STALE_CACHE | FALLBACK
    """
    cached = _load_cache_futures()
    if cached and _is_fresh(cached):
        return cached | {"source": "CACHE"}

    live = _fetch_futures_from_nse()
    if live:
        _save_cache_futures(live)
        return live

    if cached:
        print("[fii_futures] Using stale cache (NSE unavailable)")
        return cached | {"source": "STALE_CACHE"}
    return _empty_futures()


def get_fii_futures_signal(data: dict) -> str:
    """Extract futures signal string from fetch_fii_futures() result."""
    return data.get("fut_signal", "FII_FUT_NEUTRAL")


# ── NSE CSV fetch ─────────────────────────────────────────────────────────────

def _futures_csv_url(date_ist: datetime) -> str:
    """Build NSE archive URL for participant OI CSV for a given date."""
    return (
        "https://nsearchives.nseindia.com/content/nsccl/"
        f"fao_participant_oi_{date_ist.strftime('%d%m%Y')}.csv"
    )


def _fetch_futures_from_nse() -> Optional[dict]:
    """
    Download participant-wise OI CSV from NSE archives and parse FII row.

    The CSV has one row per participant (FII/FPI, DII, CLIENT, PRO).
    Columns of interest:
      Client Type | Future Index Long | Future Index Short |
      Option Index Call Long | Option Index Call Short |
      Option Index Put Long  | Option Index Put Short  | ...

    NSE publishes the file for today after market close (~6-7 PM).
    If today's file is not yet available, falls back to previous trading day.
    """
    now = datetime.now(IST)
    # Try today first, then fall back up to 3 previous trading days
    for days_back in range(4):
        attempt_date = now - timedelta(days=days_back)
        if attempt_date.weekday() >= 5:   # skip weekends
            continue
        url = _futures_csv_url(attempt_date)
        try:
            session = _nse_session()
            resp    = session.get(url, timeout=15)
            if resp.status_code == 404:
                print(f"[fii_futures] {attempt_date.strftime('%d-%m-%Y')} not yet published — trying previous day")
                continue
            resp.raise_for_status()
            return _parse_participant_oi_csv(resp.text, attempt_date.strftime("%d-%b-%Y"))
        except Exception as exc:
            print(f"[fii_futures] Fetch error for {attempt_date.strftime('%d-%m-%Y')}: {exc}")
            continue
    print("[fii_futures] Could not fetch participant OI from NSE")
    return None


def _parse_participant_oi_csv(csv_text: str, data_date: str) -> Optional[dict]:
    """
    Parse NSE fao_participant_oi CSV.

    Expected header row (may vary slightly by NSE format):
    Client Type, Future Index Long, Future Index Short, Future Index OI,
    Option Index Call Long, Option Index Call Short, Option Index Call OI,
    Option Index Put Long, Option Index Put Short, Option Index Put OI, ...

    Returns None if parsing fails.
    """
    try:
        lines = [l.strip() for l in csv_text.splitlines() if l.strip()]

        # Find header row — it contains "Client Type" or "Future Index Long"
        header_idx = None
        for i, line in enumerate(lines):
            if "Future Index Long" in line or "Client Type" in line:
                header_idx = i
                break
        if header_idx is None:
            print("[fii_futures] CSV header not found")
            return None

        headers = [h.strip() for h in lines[header_idx].split(",")]

        # Find FII/FPI row
        fii_row = None
        for line in lines[header_idx + 1:]:
            cells = [c.strip() for c in line.split(",")]
            if cells and ("FII" in cells[0].upper() or "FPI" in cells[0].upper()):
                fii_row = cells
                break
        if fii_row is None:
            print("[fii_futures] FII row not found in CSV")
            return None

        def _col(name: str) -> int:
            """Get integer value from column matching name."""
            for i, h in enumerate(headers):
                if name.lower() in h.lower() and i < len(fii_row):
                    try:
                        return int(str(fii_row[i]).replace(",", "").strip() or "0")
                    except ValueError:
                        return 0
            return 0

        fut_long  = _col("Future Index Long")
        fut_short = _col("Future Index Short")
        fut_net   = fut_long - fut_short

        # Options OI: long + short combined per side (market-wide FII option positioning)
        call_long  = _col("Option Index Call Long")
        call_short = _col("Option Index Call Short")
        put_long   = _col("Option Index Put Long")
        put_short  = _col("Option Index Put Short")
        call_oi    = call_long + call_short
        put_oi     = put_long  + put_short
        opt_pcr    = round(put_oi / call_oi, 3) if call_oi > 0 else 1.0

        result = {
            "fii_fut_long":    fut_long,
            "fii_fut_short":   fut_short,
            "fii_fut_net":     fut_net,
            "fii_opt_call_oi": call_oi,
            "fii_opt_put_oi":  put_oi,
            "fii_opt_pcr":     opt_pcr,
            "fut_signal":      _derive_futures_signal(fut_net),
            "data_date":       data_date,
            "fetched_at":      datetime.now(IST).isoformat(),
            "source":          "NSE_LIVE",
        }
        print(
            f"[fii_futures] FII fut_net={fut_net:+,d} (long={fut_long:,} short={fut_short:,})  "
            f"opt_PCR={opt_pcr:.2f}  signal={result['fut_signal']}"
        )
        return result

    except Exception as exc:
        print(f"[fii_futures] Parse error: {exc}")
        return None


def _derive_futures_signal(fut_net: int) -> str:
    if fut_net >= FII_FUT_LONG_THR:
        return "FII_FUT_LONG"
    if fut_net <= FII_FUT_SHORT_THR:
        return "FII_FUT_SHORT"
    return "FII_FUT_NEUTRAL"


# ── DynamoDB cache for futures data ──────────────────────────────────────────

def _load_cache_futures() -> Optional[dict]:
    try:
        resp = _ddb.Table(CFG_TABLE).get_item(Key={"config_key": CACHE_KEY_FUT})
        raw  = resp.get("Item", {}).get("config_value", "")
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _save_cache_futures(data: dict) -> None:
    try:
        _ddb.Table(CFG_TABLE).put_item(Item={
            "config_key":   CACHE_KEY_FUT,
            "config_value": json.dumps(data),
        })
    except Exception as exc:
        print(f"[fii_futures] Cache write failed: {exc}")


def _empty_futures() -> dict:
    return {
        "fii_fut_long":    0,
        "fii_fut_short":   0,
        "fii_fut_net":     0,
        "fii_opt_call_oi": 0,
        "fii_opt_put_oi":  0,
        "fii_opt_pcr":     1.0,
        "fut_signal":      "FII_FUT_NEUTRAL",
        "data_date":       datetime.now(IST).strftime("%d-%b-%Y"),
        "fetched_at":      datetime.now(IST).isoformat(),
        "source":          "FALLBACK",
    }


# ══════════════════════════════════════════════════════════════════════════════
# FII Daily History & 3-Day Trend Analysis
# ══════════════════════════════════════════════════════════════════════════════
#
# Strategy:
#   • store_daily_fii_snapshot() — saves today's FII data to nifty_fii_history
#     (keyed by date; idempotent). Call once per Lambda invocation (non-fatal).
#   • get_fii_trend()            — reads 5 trading days of history, computes:
#       - trend_direction: BULLISH / BEARISH / NEUTRAL
#       - ls_ratio_trend : RISING / FALLING / FLAT
#       - consecutive_days: how many days FII has been consistently one-directional
#   • get_cash_futures_divergence() — TRUE_EXIT vs HEDGING detection
#   • get_fii_composite_signal()    — single combined direction + confidence
#
# Table: nifty_fii_history (primary key: date YYYY-MM-DD)
# ══════════════════════════════════════════════════════════════════════════════

FII_HISTORY_TABLE = os.environ.get("FII_HISTORY_TABLE", "nifty_fii_history")
_TREND_DAYS       = 5


def store_daily_fii_snapshot(cash_data: dict, futures_data: dict) -> None:
    """
    Save today's FII data point to nifty_fii_history for trend tracking.
    Idempotent — same date key overwrites on repeated calls.
    Call this once per Lambda run; errors are non-fatal.
    """
    today = datetime.now(IST).strftime("%Y-%m-%d")
    try:
        # Compute L/S ratio — stored as string (DynamoDB Decimal-safe)
        fut_long  = int(futures_data.get("fii_fut_long",  0))
        fut_short = int(futures_data.get("fii_fut_short", 0))
        ls_ratio  = round(fut_long / fut_short, 3) if fut_short > 0 else 0.0
        _ddb.Table(FII_HISTORY_TABLE).put_item(Item={
            "date":          today,
            "fii_fut_long":  str(fut_long),
            "fii_fut_short": str(fut_short),
            "fii_fut_net":   str(int(futures_data.get("fii_fut_net", 0))),
            "fii_ls_ratio":  str(ls_ratio),
            "fii_opt_pcr":   str(round(float(futures_data.get("fii_opt_pcr", 1.0)), 3)),
            "fii_net_cr":    str(round(float(cash_data.get("fii_net_cr",  0.0)), 2)),
            "fii_signal":    cash_data.get("signal",     "FII_NEUTRAL"),
            "fut_signal":    futures_data.get("fut_signal", "FII_FUT_NEUTRAL"),
            "timestamp":     datetime.now(IST).isoformat(),
        })
        print(f"[fii_history] Snapshot saved for {today}: fut_net={futures_data.get('fii_fut_net',0):+,d} ls_ratio={ls_ratio:.3f}")
    except Exception as exc:
        print(f"[fii_history] Snapshot write failed (non-fatal): {exc}")


def get_fii_trend(days: int = _TREND_DAYS) -> dict:
    """
    Read the last `days` records from nifty_fii_history and compute:

    Returns:
      trend_direction  – BULLISH / BEARISH / NEUTRAL  (net futures direction)
      ls_ratio_trend   – RISING / FALLING / FLAT       (L/S ratio momentum)
      net_change_5d    – total futures contracts added (positive = net buying)
      consecutive_days – how many days in a row same direction
      score            – raw directional score (-5 … +5)
      history          – list of daily snapshot dicts (newest first)
      confidence       – HIGH (3+ consecutive) / MEDIUM (trend visible) / LOW
    """
    history = _load_fii_history(days)

    empty = {
        "trend_direction":  "NEUTRAL",
        "ls_ratio_trend":   "FLAT",
        "net_change_5d":    0,
        "consecutive_days": 0,
        "score":            0,
        "confidence":       "LOW",
        "history":          history,
    }

    if len(history) < 2:
        return empty

    # Newest record first; sort descending by date
    history_sorted = sorted(history, key=lambda x: x.get("date", ""), reverse=True)

    # ── Net futures direction each day ────────────────────────────────────────
    direction_days = []   # +1 = bullish, -1 = bearish, 0 = neutral
    ls_ratios      = []

    for rec in history_sorted:
        fut_net  = int(rec.get("fii_fut_net", 0))
        ls_ratio = float(rec.get("fii_ls_ratio", 1.0))
        ls_ratios.append(ls_ratio)
        if fut_net >= FII_FUT_LONG_THR:
            direction_days.append(1)
        elif fut_net <= FII_FUT_SHORT_THR:
            direction_days.append(-1)
        else:
            direction_days.append(0)

    # ── Consecutive streak (from most recent backwards) ─────────────────────
    streak_dir  = direction_days[0]   # today's direction
    consecutive = 1
    if streak_dir != 0:
        for d in direction_days[1:]:
            if d == streak_dir:
                consecutive += 1
            else:
                break

    # ── 5-day cumulative net change ──────────────────────────────────────────
    net_change_5d = sum(int(r.get("fii_fut_net", 0)) for r in history_sorted)

    # ── L/S ratio trend: is ratio rising or falling? ─────────────────────────
    if len(ls_ratios) >= 3:
        recent_avg = sum(ls_ratios[:2]) / 2          # avg of last 2 days
        older_avg  = sum(ls_ratios[2:]) / len(ls_ratios[2:])  # avg of earlier
        if recent_avg > older_avg + 0.05:
            ls_ratio_trend = "RISING"
        elif recent_avg < older_avg - 0.05:
            ls_ratio_trend = "FALLING"
        else:
            ls_ratio_trend = "FLAT"
    else:
        ls_ratio_trend = "FLAT"

    # ── Overall directional score ────────────────────────────────────────────
    score = sum(direction_days)   # range: -5 to +5

    # ── Trend direction ──────────────────────────────────────────────────────
    if score >= 2:
        trend_direction = "BULLISH"
    elif score <= -2:
        trend_direction = "BEARISH"
    else:
        trend_direction = "NEUTRAL"

    # ── Confidence ───────────────────────────────────────────────────────────
    if consecutive >= 3:
        confidence = "HIGH"
    elif consecutive >= 2 or abs(score) >= 3:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return {
        "trend_direction":  trend_direction,
        "ls_ratio_trend":   ls_ratio_trend,
        "net_change_5d":    net_change_5d,
        "consecutive_days": consecutive,
        "score":            score,
        "confidence":       confidence,
        "history":          history_sorted,
    }


def get_cash_futures_divergence(cash_data: dict, futures_data: dict) -> dict:
    """
    Detect whether FII is HEDGING (short futures / neutral cash) vs
    TRUE EXIT (short futures + cash selling simultaneously).

    This is critical: FII routinely short index futures to hedge their large
    equity portfolio. That is NOT a bearish signal. Only when BOTH cash and
    futures move together does it become a true directional signal.

    Returns:
      divergence – TRUE_EXIT | TRUE_ACCUMULATION | HEDGING | NONE
      signal     – STRONG_BEARISH | STRONG_BULLISH | NEUTRAL_OR_MILD_BEARISH | NEUTRAL
      explanation – human-readable string
    """
    cash_sig   = cash_data.get("signal",     "FII_NEUTRAL")
    fut_sig    = futures_data.get("fut_signal", "FII_FUT_NEUTRAL")
    fii_net_cr = float(cash_data.get("fii_net_cr", 0))
    fut_net    = int(futures_data.get("fii_fut_net", 0))

    # TRUE EXIT: cash selling AND futures short
    if cash_sig == "FII_BEARISH" and fut_sig == "FII_FUT_SHORT":
        return {
            "divergence":  "TRUE_EXIT",
            "signal":      "STRONG_BEARISH",
            "explanation": (
                f"FII selling both cash (₹{fii_net_cr:+,.0f}Cr) AND "
                f"index futures (net {fut_net:+,d} contracts). "
                "True institutional exit — strong bearish signal."
            ),
        }

    # TRUE ACCUMULATION: cash buying AND futures long
    if cash_sig == "FII_BULLISH" and fut_sig == "FII_FUT_LONG":
        return {
            "divergence":  "TRUE_ACCUMULATION",
            "signal":      "STRONG_BULLISH",
            "explanation": (
                f"FII buying both cash (₹{fii_net_cr:+,.0f}Cr) AND "
                f"holding index futures long (net {fut_net:+,d} contracts). "
                "True accumulation — strong bullish signal."
            ),
        }

    # HEDGING: cash selling but futures neutral/long → just protecting equity book
    if cash_sig == "FII_BEARISH" and fut_sig in ("FII_FUT_NEUTRAL", "FII_FUT_LONG"):
        return {
            "divergence":  "HEDGING",
            "signal":      "NEUTRAL_OR_MILD_BEARISH",
            "explanation": (
                f"FII selling cash (₹{fii_net_cr:+,.0f}Cr) but "
                f"futures net {fut_net:+,d} contracts (not short). "
                "Likely hedging equity portfolio — NOT a true bearish bet."
            ),
        }

    # PARTIAL BULLISH: cash neutral but futures long
    if cash_sig != "FII_BEARISH" and fut_sig == "FII_FUT_LONG":
        return {
            "divergence":  "NONE",
            "signal":      "MILD_BULLISH",
            "explanation": (
                f"FII net long index futures ({fut_net:+,d} contracts) "
                f"with neutral cash flow (₹{fii_net_cr:+,.0f}Cr). "
                "Mild bullish bias via futures positioning."
            ),
        }

    return {
        "divergence":  "NONE",
        "signal":      "NEUTRAL",
        "explanation": (
            f"Cash: {cash_sig} (₹{fii_net_cr:+,.0f}Cr)  "
            f"Futures: {fut_sig} ({fut_net:+,d} contracts). "
            "No clear institutional directional conviction."
        ),
    }


def get_fii_composite_signal(
    cash_data:    dict,
    futures_data: dict,
    trend:        dict,
) -> dict:
    """
    Single composite FII signal combining:
      1. Single-day futures positioning (gold standard)
      2. 3–5 day rolling trend direction + consecutive streak
      3. Cash vs futures divergence (TRUE_EXIT vs HEDGING)
      4. FII options PCR

    Returns:
      direction   – BULLISH | BEARISH | NEUTRAL
      confidence  – HIGH | MEDIUM | LOW
      score       – int (-10 … +10): positive = bullish
      action      – what strategy bias to apply
      explanation – human-readable
    """
    score = 0

    # ── 1. Single-day futures signal ──────────────────────────────────────────
    fut_sig = futures_data.get("fut_signal", "FII_FUT_NEUTRAL")
    if fut_sig == "FII_FUT_LONG":
        score += 3
    elif fut_sig == "FII_FUT_SHORT":
        score -= 3

    # ── 2. 3-5 day trend ──────────────────────────────────────────────────────
    trend_dir  = trend.get("trend_direction", "NEUTRAL")
    consec     = int(trend.get("consecutive_days", 0))
    ls_trend   = trend.get("ls_ratio_trend", "FLAT")

    if trend_dir == "BULLISH":
        score += min(consec, 3)          # up to +3 for streak
    elif trend_dir == "BEARISH":
        score -= min(consec, 3)          # up to -3 for streak

    if ls_trend == "RISING":
        score += 1
    elif ls_trend == "FALLING":
        score -= 1

    # ── 3. Cash vs futures divergence ────────────────────────────────────────
    div   = get_cash_futures_divergence(cash_data, futures_data)
    dsig  = div.get("signal", "NEUTRAL")
    if dsig == "STRONG_BULLISH":
        score += 2
    elif dsig == "STRONG_BEARISH":
        score -= 2
    elif dsig == "MILD_BULLISH":
        score += 1
    elif dsig == "NEUTRAL_OR_MILD_BEARISH":
        score -= 1

    # ── 4. FII options PCR (put/call ratio) ──────────────────────────────────
    opt_pcr = float(futures_data.get("fii_opt_pcr", 1.0))
    if opt_pcr > 1.3:
        score -= 1   # FII buying more puts = hedging/bearish
    elif opt_pcr < 0.7:
        score += 1   # FII buying more calls = bullish

    # ── Decision ──────────────────────────────────────────────────────────────
    if score >= 5:
        direction  = "BULLISH"
        confidence = "HIGH"
        action     = "BULL_PUT_SPREAD or BULL_CALL_SPREAD"
    elif score >= 2:
        direction  = "BULLISH"
        confidence = "MEDIUM"
        action     = "BULL_PUT_SPREAD (reduce size 50% if MEDIUM)"
    elif score <= -5:
        direction  = "BEARISH"
        confidence = "HIGH"
        action     = "BEAR_CALL_SPREAD or BUY_STRANGLE if VIX rising"
    elif score <= -2:
        direction  = "BEARISH"
        confidence = "MEDIUM"
        action     = "BEAR_CALL_SPREAD (reduce size 50%)"
    else:
        direction  = "NEUTRAL"
        confidence = "LOW"
        action     = "IRON_CONDOR or WAIT — no clear FII directional conviction"

    return {
        "direction":     direction,
        "confidence":    confidence,
        "score":         score,
        "action":        action,
        "divergence":    div.get("divergence", "NONE"),
        "divergence_explanation": div.get("explanation", ""),
        "fut_signal":    fut_sig,
        "trend_direction": trend_dir,
        "consecutive_days": consec,
        "ls_ratio_trend": ls_trend,
        "opt_pcr":       opt_pcr,
    }


# ── History DynamoDB helpers ──────────────────────────────────────────────────

def _load_fii_history(days: int) -> list:
    """Fetch last `days` records from nifty_fii_history table."""
    try:
        # Scan with date filter: last N calendar days (covers weekends too)
        from datetime import timedelta
        cutoff = (datetime.now(IST) - timedelta(days=days + 3)).strftime("%Y-%m-%d")
        resp = _ddb.Table(FII_HISTORY_TABLE).scan(
            FilterExpression="#d >= :c",
            ExpressionAttributeNames={"#d": "date"},
            ExpressionAttributeValues={":c": cutoff},
        )
        items = resp.get("Items", [])
        # Sort descending by date, return latest `days` entries
        items.sort(key=lambda x: x.get("date", ""), reverse=True)
        return items[:days]
    except Exception as exc:
        print(f"[fii_history] Load failed (non-fatal): {exc}")
        return []
