"""
Iron Condor Strategy Builder
==============================
Constructs a complete, ready-to-trade Iron Condor plan for NIFTY weekly options.

An Iron Condor = Bear Call Spread + Bull Put Spread
  SELL short_call  +  BUY long_call   (bear call spread — caps upside loss)
  SELL short_put   +  BUY long_put    (bull put spread  — caps downside loss)

This gives a defined-risk, defined-reward structure ideal for range-bound markets.

P&L Profile (per lot)
---------------------
  Max profit  = net premium collected × lot_size
  Max loss    = (wing_width − net_premium) × lot_size
  Breakeven upper = short_call + net_premium
  Breakeven lower = short_put  − net_premium
  Target          = 50% of max profit (close when premium decays 50%)
  Stop-loss       = 100% of max profit (close if position doubles against you)

Entry conditions (ALL must be true)
------------------------------------
  - Day of week: Monday or Tuesday (best for weekly theta capture)
  - VIX: 14 ≤ VIX ≤ 22 (too low = no premium; too high = gap risk)
  - PCR: 0.75 ≤ PCR ≤ 1.35 (neutral market, no strong directional bias)
  - DTE: ≥ 3 (need at least 3 days for theta to work)
  - No event dates today

Usage
-----
  from iron_condor import build_iron_condor_plan, ic_entry_allowed

  if ic_entry_allowed(vix, pcr, dte, day_of_week, event_dates):
      plan = build_iron_condor_plan(broker, spot, vix, dte, lot_size)
"""

from __future__ import annotations

import os
from datetime import date, datetime
from typing import Optional

import pandas as pd
import pytz

IST        = pytz.timezone("Asia/Kolkata")
UNDERLYING = os.environ.get("UNDERLYING", "NIFTY")
LOT_SIZE   = int(os.environ.get("LOT_SIZE", "75"))


# ── Entry gate ────────────────────────────────────────────────────────────────

def ic_entry_allowed(
    vix:         float,
    pcr:         float,
    dte:         int,
    day_of_week: int,    # 0=Mon … 6=Sun
    event_dates: list[str] = [],
) -> tuple[bool, str]:
    """
    Check all Iron Condor entry conditions.
    Returns (allowed: bool, reason: str).
    """
    today_str = date.today().isoformat()

    if day_of_week == 3:
        return False, "Thursday (expiry day) — no new Iron Condor entries"
    if day_of_week == 4:
        return False, "Friday — skip IC; wait for Monday weekly setup"
    if day_of_week >= 5:
        return False, "Weekend"
    if day_of_week == 2:
        return False, "Wednesday — too close to expiry for new IC entry"
    if vix < 13:
        return False, f"VIX {vix:.1f} too low — insufficient premium to collect"
    if vix > 22:
        return False, f"VIX {vix:.1f} too high — gap risk makes IC dangerous"
    if dte < 3:
        return False, f"DTE={dte} — less than 3 days to expiry, IC not viable"
    if pcr < 0.70:
        return False, f"PCR={pcr:.2f} bearish — market has directional pressure, skip IC"
    if pcr > 1.40:
        return False, f"PCR={pcr:.2f} very bullish — market has directional pressure, skip IC"
    if today_str in event_dates:
        return False, "Event date today — skip IC to avoid event-driven gap"

    return True, "All IC entry conditions met"


# ── Plan builder ──────────────────────────────────────────────────────────────

def build_iron_condor_plan(
    broker,
    spot:       float,
    vix:        float,
    dte:        int,
    expiry_date,               # near futures expiry (used to find option symbols)
    pcr:        float  = 1.0,
    max_pain:   int    = 0,
    lot_size:   int    = LOT_SIZE,
) -> dict:
    """
    Build a complete Iron Condor plan.

    Returns dict with:
      strategy, legs (4), strikes, premium estimates,
      max_profit_inr, max_loss_inr, target_inr, sl_inr,
      breakeven_upper, breakeven_lower, capital_needed
    """
    from option_chain_engine import get_iron_condor_strikes

    strikes = get_iron_condor_strikes(spot, vix, dte)
    atm          = strikes["atm"]
    short_call_k = strikes["short_call"]
    long_call_k  = strikes["long_call"]
    short_put_k  = strikes["short_put"]
    long_put_k   = strikes["long_put"]
    wing_width   = strikes["wing_width"]      # 200 pts
    sigma_pts    = strikes["sigma_pts"]

    # Adjust short strikes toward max pain if provided (improves probability)
    if max_pain and abs(max_pain - atm) <= 100:
        shift       = max_pain - atm
        short_call_k = int(round((short_call_k + shift) / 50) * 50)
        short_put_k  = int(round((short_put_k  + shift) / 50) * 50)
        long_call_k  = short_call_k + wing_width
        long_put_k   = short_put_k  - wing_width

    # Fetch actual option symbols from broker instruments
    symbols = _fetch_option_symbols(
        broker, expiry_date,
        [short_call_k, long_call_k, short_put_k, long_put_k],
    )

    # Get live premiums where available, else estimate
    sc_sym, sc_px = symbols.get(f"{short_call_k}CE", (f"{UNDERLYING}CE{short_call_k}", 0.0))
    lc_sym, lc_px = symbols.get(f"{long_call_k}CE",  (f"{UNDERLYING}CE{long_call_k}",  0.0))
    sp_sym, sp_px = symbols.get(f"{short_put_k}PE",  (f"{UNDERLYING}PE{short_put_k}",  0.0))
    lp_sym, lp_px = symbols.get(f"{long_put_k}PE",   (f"{UNDERLYING}PE{long_put_k}",   0.0))

    # If premiums not fetched, estimate from VIX/sigma (rough)
    if sc_px == 0:
        sc_px = _estimate_premium(spot, short_call_k, vix, dte, "CE")
    if lc_px == 0:
        lc_px = _estimate_premium(spot, long_call_k,  vix, dte, "CE")
    if sp_px == 0:
        sp_px = _estimate_premium(spot, short_put_k,  vix, dte, "PE")
    if lp_px == 0:
        lp_px = _estimate_premium(spot, long_put_k,   vix, dte, "PE")

    # Net premium (what we collect)
    call_spread_credit = max(0.0, round(sc_px - lc_px, 2))
    put_spread_credit  = max(0.0, round(sp_px - lp_px, 2))
    net_premium        = round(call_spread_credit + put_spread_credit, 2)

    # P&L in INR (per 1 lot)
    max_profit_inr = round(net_premium    * lot_size, 0)
    max_loss_inr   = round((wing_width - net_premium) * lot_size, 0)
    target_inr     = round(max_profit_inr * 0.50, 0)   # close at 50% profit
    sl_inr         = round(max_profit_inr * 1.00, 0)   # SL = 100% of credit

    breakeven_upper = short_call_k + net_premium
    breakeven_lower = short_put_k  - net_premium

    # Capital required (margin for short strikes, roughly wing_width × lot_size per spread)
    capital_needed = round(wing_width * lot_size * 2 * 1.2, 0)   # ×1.2 buffer

    legs = [
        _leg("SELL", lot_size, sc_sym, "CE", short_call_k, round(sc_px, 2)),
        _leg("BUY",  lot_size, lc_sym, "CE", long_call_k,  round(lc_px, 2)),
        _leg("SELL", lot_size, sp_sym, "PE", short_put_k,  round(sp_px, 2)),
        _leg("BUY",  lot_size, lp_sym, "PE", long_put_k,   round(lp_px, 2)),
    ]

    return {
        "strategy":          "IRON_CONDOR",
        "emoji":             "🎯",
        "atm":               atm,
        "short_call":        short_call_k,
        "long_call":         long_call_k,
        "short_put":         short_put_k,
        "long_put":          long_put_k,
        "net_premium":       net_premium,
        "call_spread_credit":call_spread_credit,
        "put_spread_credit": put_spread_credit,
        "max_profit_inr":    max_profit_inr,
        "max_loss_inr":      max_loss_inr,
        "target_inr":        target_inr,
        "sl_inr":            sl_inr,
        "target_pct":        0.50,
        "sl_pct":            1.00,
        "breakeven_upper":   round(breakeven_upper, 0),
        "breakeven_lower":   round(breakeven_lower, 0),
        "capital_needed":    capital_needed,
        "wing_width":        wing_width,
        "sigma_pts":         sigma_pts,
        "dte":               dte,
        "vix":               vix,
        "pcr":               pcr,
        "legs":              legs,
        "legs_text":         "\n".join(_fmt_leg(l) for l in legs),
        "sl_note":           f"Close if position loss > ₹{sl_inr:,.0f} (≈ premium doubles)",
        "target_note":       f"Close at 50% profit ≈ ₹{target_inr:,.0f}",
        "hard_exit":         "13:30",
        "entry_window":      "9:30–10:00 AM",
        "generated_at":      datetime.now(IST).isoformat(),
        "date":              date.today().isoformat(),
    }


# ── Symbol lookup ─────────────────────────────────────────────────────────────

def _fetch_option_symbols(broker, expiry_date, strikes: list[int]) -> dict:
    """
    Look up tradeable option symbols from broker instrument list.
    Returns dict: "23150CE" → (symbol_string, ltp_float)
    """
    result: dict = {}
    try:
        df         = broker.get_instruments("NFO")
        expiry_str = pd.Timestamp(expiry_date).strftime("%d%b%Y").upper()

        for strike in strikes:
            strike_val = float(strike * 100)
            for opt_type in ("CE", "PE"):
                suffix = "CE" if opt_type == "CE" else "PE"
                rows = df[
                    (df["name"]           == UNDERLYING)
                    & (df["expiry"]         == expiry_str)
                    & (df["strike"].astype(float) == strike_val)
                    & (df["instrumenttype"].isin(["OPTIDX", "OPTSTK"]))
                    & (df["symbol"].str.endswith(suffix))
                ][["symbol", "token"]].values

                if len(rows):
                    sym   = rows[0][0]
                    token = rows[0][1]
                    # Fetch LTP via broker quote
                    ltp = 0.0
                    try:
                        q   = broker.get_quote([f"NFO:{token}"])
                        ltp = float(q.get(f"NFO:{sym}", {}).get("ltp", 0) or 0)
                    except Exception:
                        pass
                    result[f"{strike}{opt_type}"] = (sym, ltp)
    except Exception as exc:
        print(f"[iron_condor] Symbol fetch error: {exc}")
    return result


# ── Premium estimator (fallback when live prices unavailable) ─────────────────

def _estimate_premium(
    spot: float, strike: float, vix: float, dte: int, opt_type: str
) -> float:
    """
    Black-Scholes approximation using erf for norm_cdf.
    Returns estimated option premium (not a substitute for live prices).
    """
    import math
    try:
        S, K, T = spot, strike, max(dte, 1) / 365.0
        sigma   = vix / 100.0
        r       = 0.065          # approx Indian risk-free rate
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)

        def _ncdf(x):
            return 0.5 * (1 + math.erf(x / math.sqrt(2)))

        if opt_type == "CE":
            price = S * _ncdf(d1) - K * math.exp(-r * T) * _ncdf(d2)
        else:
            price = K * math.exp(-r * T) * _ncdf(-d2) - S * _ncdf(-d1)
        return max(0.5, round(price, 2))
    except Exception:
        return 0.0


# ── Helpers ───────────────────────────────────────────────────────────────────

def _leg(
    action: str, qty: int, symbol: str,
    opt_type: str, strike: int, ltp: float,
) -> dict:
    action_icon = "✅ BUY" if action == "BUY" else "🔴 SELL"
    return {
        "action":    action,
        "qty":       qty,
        "symbol":    symbol,
        "inst_type": opt_type,
        "strike":    strike,
        "ltp":       ltp,
        "label":     f"{strike} {opt_type}",
        "order_type":"MARKET",
    }


def _fmt_leg(leg: dict) -> str:
    icon = "✅ BUY " if leg["action"] == "BUY" else "🔴 SELL"
    return (
        f"  {icon} {leg['qty']}× {leg['symbol']}"
        f" ({leg['label']}) @ ≈{leg['ltp']}"
    )
