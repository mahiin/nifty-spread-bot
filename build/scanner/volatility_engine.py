"""
Volatility Engine
=================
Computes implied volatility (IV) from ATM option prices using Black-Scholes bisection.
No scipy required — pure stdlib math.

Generates signals based on IV vs India VIX divergence:

  IV – VIX Spread > 4   → Options OVERPRICED  → SELL_STRADDLE (collect premium)
  IV – VIX Spread < -3  → Options CHEAP        → BUY_STRADDLE (buy volatility)
  VIX ≥ 25             → Tail-risk spike       → BUY_STRANGLE (protect capital)
  Otherwise            → NONE

ATM call and put prices are passed in directly from the arbitrage engine's
already-fetched quotes — no extra broker API calls needed.
"""

import math
from typing import Optional

# ─── Thresholds ────────────────────────────────────────────────────────────
IV_SELL_THRESHOLD  =  4.0   # IV-VIX spread > this → sell premium
IV_BUY_THRESHOLD   = -3.0   # IV-VIX spread < this → buy straddle
VIX_SPIKE_THRESH   = 25.0   # Hard VIX spike → buy strangle for protection
RISK_FREE_RATE     = 0.065  # ~6.5% Indian risk-free rate

# ─── Black-Scholes ─────────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erf. Pure stdlib — no scipy."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _bs_call(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes call option price. T in years."""
    if T <= 0:
        return max(0.0, S - K)
    if sigma <= 0:
        return max(0.0, S - K * math.exp(-r * T))
    sqrtT = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)


def _bs_put(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes put price via put-call parity."""
    call = _bs_call(S, K, T, r, sigma)
    return call - S + K * math.exp(-r * T)


def compute_iv_bisection(
    market_price: float,
    S: float,
    K: float,
    dte: int,
    option_type: str = "call",
    r: float = RISK_FREE_RATE,
    lo: float = 0.001,
    hi: float = 5.0,
    max_iter: int = 100,
    tol: float = 1e-5,
) -> float:
    """
    Compute implied volatility via bisection.

    Returns annualised IV as a decimal (0.20 = 20%).
    Returns 0.0 if price is zero, non-positive, or convergence fails.
    """
    if market_price <= 0 or S <= 0 or K <= 0:
        return 0.0
    # Use trading days (≈ calendar × 5/7) annualised at 252 trading days/year.
    # This is more accurate than calendar days / 365, especially near expiry.
    trading_dte = max(dte * 5 / 7, 0.5)
    T = trading_dte / 252.0
    bs_fn = _bs_call if option_type == "call" else _bs_put

    # Quick sanity: market price must be between lo and hi bounds
    if bs_fn(S, K, T, r, hi) < market_price:
        return hi      # price implies very high vol
    if bs_fn(S, K, T, r, lo) > market_price:
        return 0.0     # too cheap — likely deep OTM with no extrinsic

    for _ in range(max_iter):
        mid   = (lo + hi) / 2.0
        price = bs_fn(S, K, T, r, mid)
        if abs(price - market_price) < tol:
            break
        if price > market_price:
            hi = mid
        else:
            lo = mid

    iv = (lo + hi) / 2.0
    return round(iv, 6) if iv > 0.001 else 0.0


# ─── Public API ────────────────────────────────────────────────────────────

def compute_volatility_signal(
    spot_price:  float,
    call_price:  float,
    put_price:   float,
    strike:      int,
    dte:         int,
    vix:         float,
    call_symbol: str,
    put_symbol:  str,
    lot_size:    int = 75,
    risk_free_rate: float = RISK_FREE_RATE,
) -> dict:
    """
    Analyse ATM option pricing relative to India VIX and generate a volatility signal.

    Parameters
    ----------
    spot_price   : NIFTY 50 spot
    call_price   : ATM call last price
    put_price    : ATM put last price
    strike       : ATM strike (integer, nearest 50)
    dte          : Days to (near-month) expiry
    vix          : India VIX live value
    call_symbol  : NSE tradingsymbol for ATM call
    put_symbol   : NSE tradingsymbol for ATM put
    lot_size     : NIFTY lot size (default 75)

    Returns
    -------
    {
      call_iv              : float  – annualised decimal (0.19 = 19%)
      put_iv               : float
      avg_iv               : float
      call_iv_pct          : float  – call_iv * 100 for display
      put_iv_pct           : float
      avg_iv_pct           : float
      iv_vix_spread        : float  – avg_iv_pct - vix  (both in % units)
      vol_signal           : str    – SELL_STRADDLE | BUY_STRADDLE | BUY_STRANGLE | NONE
      vol_signal_reason    : str    – one-liner human explanation
      straddle_premium     : float  – call_price + put_price (points)
      straddle_cost_inr    : float  – straddle_premium * lot_size
      breakeven_upper      : float  – strike + straddle_premium
      breakeven_lower      : float  – strike - straddle_premium
      breakeven_move_pct   : float  – straddle_premium / spot * 100
      strangle_call_symbol : str    – strike + 200 CE symbol (display only)
      strangle_put_symbol  : str    – strike - 200 PE symbol (display only)
      call_symbol          : str
      put_symbol           : str
      strike               : int
      dte                  : int
      capital_needed       : float  – ₹ estimate for 1 lot-set
    }
    """
    result: dict = {
        "call_iv": 0.0, "put_iv": 0.0, "avg_iv": 0.0,
        "call_iv_pct": 0.0, "put_iv_pct": 0.0, "avg_iv_pct": 0.0,
        "iv_vix_spread": 0.0,
        "vol_signal": "NONE", "vol_signal_reason": "IV data unavailable",
        "straddle_premium": 0.0, "straddle_cost_inr": 0.0,
        "breakeven_upper": 0.0, "breakeven_lower": 0.0, "breakeven_move_pct": 0.0,
        "strangle_call_symbol": "", "strangle_put_symbol": "",
        "call_symbol": call_symbol, "put_symbol": put_symbol,
        "strike": strike, "dte": dte, "capital_needed": 0.0,
    }

    if call_price <= 0 or put_price <= 0 or strike <= 0:
        return result

    # ── Compute IVs ──────────────────────────────────────────────────────
    call_iv = compute_iv_bisection(call_price, spot_price, strike, dte, "call", risk_free_rate)
    put_iv  = compute_iv_bisection(put_price,  spot_price, strike, dte, "put",  risk_free_rate)
    avg_iv  = (call_iv + put_iv) / 2.0 if (call_iv > 0 and put_iv > 0) else max(call_iv, put_iv)

    call_iv_pct = round(call_iv * 100, 2)
    put_iv_pct  = round(put_iv  * 100, 2)
    avg_iv_pct  = round(avg_iv  * 100, 2)
    iv_vix_spread = round(avg_iv_pct - vix, 2) if vix > 0 else 0.0

    # ── Straddle metrics ─────────────────────────────────────────────────
    straddle_premium   = round(call_price + put_price, 2)
    straddle_cost_inr  = round(straddle_premium * lot_size, 2)
    breakeven_upper    = round(strike + straddle_premium, 2)
    breakeven_lower    = round(strike - straddle_premium, 2)
    breakeven_move_pct = round(straddle_premium / spot_price * 100, 2) if spot_price > 0 else 0.0

    # ── Strangle symbol stubs (OTM ±200 pts) ─────────────────────────────
    strangle_strike_ce = strike + 200
    strangle_strike_pe = strike - 200
    strangle_call_sym  = call_symbol.replace(str(strike), str(strangle_strike_ce)) if call_symbol else ""
    strangle_put_sym   = put_symbol.replace(str(strike),  str(strangle_strike_pe)) if put_symbol else ""

    # ── Signal logic ─────────────────────────────────────────────────────
    vol_signal = "NONE"
    vol_reason = f"IV {avg_iv_pct:.1f}% | VIX {vix:.1f} | Spread {iv_vix_spread:+.2f}"

    if vix >= VIX_SPIKE_THRESH:
        vol_signal = "BUY_STRANGLE"
        vol_reason = (
            f"VIX spike at {vix:.1f} (≥ {VIX_SPIKE_THRESH}). "
            f"Buy OTM strangle for tail-risk protection. "
            f"Cheaper than straddle — catches large directional moves."
        )
        capital_needed = round(straddle_premium * 0.6 * lot_size, 0)  # OTM cheaper estimate

    elif iv_vix_spread > IV_SELL_THRESHOLD:
        vol_signal = "SELL_STRADDLE"
        vol_reason = (
            f"IV–VIX spread = +{iv_vix_spread:.1f} (> {IV_SELL_THRESHOLD}). "
            f"Options are OVERPRICED vs historical volatility. "
            f"Sell ATM straddle to collect premium as IV reverts to VIX level."
        )
        capital_needed = round(spot_price * 0.1 * lot_size * 2, 0)  # rough SPAN margin

    elif iv_vix_spread < IV_BUY_THRESHOLD:
        vol_signal = "BUY_STRADDLE"
        vol_reason = (
            f"IV–VIX spread = {iv_vix_spread:.1f} (< {IV_BUY_THRESHOLD}). "
            f"Options are CHEAP vs expected volatility. "
            f"Buy ATM straddle — profit if market makes a large move in either direction."
        )
        capital_needed = round(straddle_cost_inr, 0)

    else:
        vol_signal = "NONE"
        vol_reason = (
            f"IV = {avg_iv_pct:.1f}%, VIX = {vix:.1f}, spread = {iv_vix_spread:+.2f}. "
            f"Options are fairly priced. No strong volatility edge."
        )
        capital_needed = straddle_cost_inr  # for display

    result.update({
        "call_iv": call_iv, "put_iv": put_iv, "avg_iv": avg_iv,
        "call_iv_pct": call_iv_pct, "put_iv_pct": put_iv_pct, "avg_iv_pct": avg_iv_pct,
        "iv_vix_spread":        iv_vix_spread,
        "vol_signal":           vol_signal,
        "vol_signal_reason":    vol_reason,
        "straddle_premium":     straddle_premium,
        "straddle_cost_inr":    straddle_cost_inr,
        "breakeven_upper":      breakeven_upper,
        "breakeven_lower":      breakeven_lower,
        "breakeven_move_pct":   breakeven_move_pct,
        "strangle_call_symbol": strangle_call_sym,
        "strangle_put_symbol":  strangle_put_sym,
        "capital_needed":       capital_needed,
    })
    return result
