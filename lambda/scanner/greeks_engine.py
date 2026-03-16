"""
Greeks Engine
=============
Black-Scholes Greeks for options positions.

Uses math.erf for norm_cdf — pure stdlib, no scipy dependency.

Functions
---------
compute_greeks(S, K, dte, sigma, r, option_type) -> dict
    Per-contract delta, gamma, theta, vega.

compute_position_greeks(positions, spot) -> dict
    Aggregate Greeks across all open positions.
"""

import math
from typing import Optional


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def compute_greeks(
    S:           float,
    K:           float,
    dte:         int,
    sigma:       float,   # annual IV as decimal (e.g. 0.15 for 15%)
    r:           float = 0.065,
    option_type: str   = "CE",
) -> dict:
    """
    Black-Scholes Greeks for one contract.

    Returns
    -------
    delta : float  – sensitivity to underlying move (CE: 0–1, PE: -1–0)
    gamma : float  – rate of change of delta per 1 pt move
    theta : float  – daily time decay in points (negative = decay)
    vega  : float  – change in value per 1% IV change
    """
    if dte <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}

    T  = dte / 365.0
    sq = math.sqrt(T)

    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sq)
    d2 = d1 - sigma * sq

    pdf_d1 = _norm_pdf(d1)

    if option_type == "CE":
        delta = _norm_cdf(d1)
        theta = (
            -(S * sigma * pdf_d1) / (2 * sq)
            - r * K * math.exp(-r * T) * _norm_cdf(d2)
        ) / 365.0
    else:  # PE
        delta = _norm_cdf(d1) - 1.0
        theta = (
            -(S * sigma * pdf_d1) / (2 * sq)
            + r * K * math.exp(-r * T) * _norm_cdf(-d2)
        ) / 365.0

    gamma = pdf_d1 / (S * sigma * sq)
    vega  = S * sq * pdf_d1 / 100.0   # per 1% IV point

    return {
        "delta": round(delta, 4),
        "gamma": round(gamma, 6),
        "theta": round(theta, 4),
        "vega":  round(vega,  4),
    }


def compute_position_greeks(positions: list, spot: float, lot_size: int = 75) -> dict:
    """
    Aggregate Black-Scholes Greeks for all open positions.

    Each position dict may contain:
        trade_type, call_symbol, put_symbol, qty, atm_strike,
        entry_premium, call_iv_pct (optional), put_iv_pct (optional)

    Returns total_delta, total_gamma, total_theta_inr, total_vega_inr,
    and per_lot_delta for the portfolio.
    """
    total_delta = 0.0
    total_gamma = 0.0
    total_theta = 0.0
    total_vega  = 0.0
    total_lots  = 0

    for pos in positions:
        trade_type = pos.get("trade_type", "")
        try:
            qty  = int(pos.get("qty", lot_size))
            lots = qty // lot_size if lot_size > 0 else 1
            # Try to get strike from position; fall back to nearest 50
            try:
                K = float(pos.get("atm_strike") or round(spot / 50) * 50)
            except Exception:
                K = round(spot / 50) * 50

            # IV — use stored avg_iv_pct if available, else assume 15%
            sigma = float(pos.get("avg_iv_pct") or 15.0) / 100.0

            # Approximate DTE from position timestamp
            from datetime import datetime, date
            try:
                pos_date = datetime.fromisoformat(pos.get("timestamp", "")).date()
                dte = max(1, (date.today() - pos_date).days + 7)  # rough estimate
            except Exception:
                dte = 7

            is_sell = "SELL" in trade_type

            if trade_type in ("SELL_STRADDLE", "BUY_STRADDLE"):
                for opt_type in ("CE", "PE"):
                    g = compute_greeks(spot, K, dte, sigma, option_type=opt_type)
                    sign = -1 if is_sell else 1
                    total_delta += sign * g["delta"] * qty
                    total_gamma += sign * g["gamma"] * qty
                    total_theta += g["theta"] * qty   # theta always benefits seller
                    total_vega  += sign * g["vega"]   * qty
                    total_lots  += lots

            elif trade_type in ("SELL_STRANGLE", "BUY_STRANGLE"):
                for opt_type in ("CE", "PE"):
                    K_adj = K + 200 if opt_type == "CE" else K - 200
                    g = compute_greeks(spot, K_adj, dte, sigma, option_type=opt_type)
                    sign = -1 if is_sell else 1
                    total_delta += sign * g["delta"] * qty
                    total_gamma += sign * g["gamma"] * qty
                    total_theta += g["theta"] * qty
                    total_vega  += sign * g["vega"]   * qty
                    total_lots  += lots

            elif trade_type in ("BEAR_CALL_SPREAD",):
                # Short ATM CE + Long OTM+200 CE
                g_short = compute_greeks(spot, K,       dte, sigma, option_type="CE")
                g_long  = compute_greeks(spot, K + 200, dte, sigma, option_type="CE")
                net_delta = (-g_short["delta"] + g_long["delta"]) * qty
                total_delta += net_delta
                total_lots  += lots

            elif trade_type in ("BULL_PUT_SPREAD",):
                # Short ATM PE + Long OTM-200 PE
                g_short = compute_greeks(spot, K,       dte, sigma, option_type="PE")
                g_long  = compute_greeks(spot, K - 200, dte, sigma, option_type="PE")
                net_delta = (-g_short["delta"] + g_long["delta"]) * qty
                total_delta += net_delta
                total_lots  += lots

            elif trade_type in ("BULL_CALL_SPREAD",):
                g_long  = compute_greeks(spot, K,       dte, sigma, option_type="CE")
                g_short = compute_greeks(spot, K + 200, dte, sigma, option_type="CE")
                net_delta = (g_long["delta"] - g_short["delta"]) * qty
                total_delta += net_delta
                total_lots  += lots

            elif trade_type in ("BEAR_PUT_SPREAD",):
                g_long  = compute_greeks(spot, K,       dte, sigma, option_type="PE")
                g_short = compute_greeks(spot, K - 200, dte, sigma, option_type="PE")
                net_delta = (g_long["delta"] - g_short["delta"]) * qty
                total_delta += net_delta
                total_lots  += lots

        except Exception as e:
            print(f"[greeks_engine] Skipping position {pos.get('position_id','?')}: {e}")
            continue

    per_lot_delta = round(total_delta / total_lots, 4) if total_lots > 0 else 0.0

    return {
        "total_delta":     round(total_delta, 4),
        "total_gamma":     round(total_gamma, 6),
        "total_theta_inr": round(total_theta, 2),  # per day in points (× lot_size for INR)
        "total_vega_inr":  round(total_vega,  2),  # per 1% IV change
        "per_lot_delta":   per_lot_delta,
        "position_count":  len(positions),
    }
