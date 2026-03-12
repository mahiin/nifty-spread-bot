"""
Regime Detector
===============
Classifies the current market environment into one of three trading regimes:

  VOLATILITY  – VIX elevated or large intraday range. Mean-reversion strategies unreliable.
  TREND       – Market moving strongly in one direction. Spread value unclear.
  SPREAD      – Calm, range-bound. Optimal for calendar spreads and synthetic arbitrage.

All inputs are derived from data already fetched in the scanner — no extra API calls.

Priority order: VOLATILITY > TREND > SPREAD
"""

from typing import Optional

# ─── Thresholds ────────────────────────────────────────────────────────────
VIX_VOL_THRESHOLD      = 20.0   # VIX ≥ 20 → VOLATILITY regime
RANGE_VOL_THRESHOLD    = 2.0    # Intraday range ≥ 2% of low → VOLATILITY
SLOPE_TREND_THRESHOLD  = 1.0    # Spot moved ≥ 1% over last N scans → TREND
SLOPE_WINDOW           = 30     # Number of historical scans for slope


class TradingRegime:
    VOLATILITY = "VOLATILITY"
    TREND      = "TREND"
    SPREAD     = "SPREAD"


# ─── Public API ────────────────────────────────────────────────────────────

def detect_regime(
    vix: float,
    spot_price: float,
    intraday_high: float,
    intraday_low: float,
    spot_history: Optional[list] = None,
) -> dict:
    """
    Classify the current market regime.

    Parameters
    ----------
    vix            : India VIX live value (0 if unavailable)
    spot_price     : Current NIFTY 50 spot price
    intraday_high  : Today's intraday high (from quote OHLC)
    intraday_low   : Today's intraday low  (from quote OHLC)
    spot_history   : List of recent spot_price values from DynamoDB (oldest first)

    Returns
    -------
    {
      regime             : str   – VOLATILITY | TREND | SPREAD
      intraday_range_pct : float – (high-low)/low * 100
      spot_slope_pct     : float – (last-first)/first * 100 over SLOPE_WINDOW scans
      vix                : float
      reasons            : list  – human-readable explanation bullets
    }
    """
    if spot_history is None:
        spot_history = []

    range_pct = _compute_range_pct(intraday_high, intraday_low)
    slope_pct = _compute_slope_pct(spot_history)

    reasons = []

    # ── 1. Volatility regime ─────────────────────────────────────────────
    if vix >= VIX_VOL_THRESHOLD:
        reasons.append(
            f"India VIX = {vix:.1f} (≥ {VIX_VOL_THRESHOLD:.0f}). "
            f"Volatility is elevated — calendar spreads are unsafe."
        )
        return _result(TradingRegime.VOLATILITY, range_pct, slope_pct, vix, reasons)

    if range_pct >= RANGE_VOL_THRESHOLD:
        reasons.append(
            f"Intraday range = {range_pct:.1f}% (≥ {RANGE_VOL_THRESHOLD:.0f}%). "
            f"Large intraday move — spread liquidity impaired."
        )
        return _result(TradingRegime.VOLATILITY, range_pct, slope_pct, vix, reasons)

    # ── 2. Trend regime ───────────────────────────────────────────────────
    if abs(slope_pct) >= SLOPE_TREND_THRESHOLD:
        direction = "UP" if slope_pct > 0 else "DOWN"
        reasons.append(
            f"Spot price trending {direction} {abs(slope_pct):.2f}% "
            f"over last {SLOPE_WINDOW} scans. "
            f"Calendar spreads less predictable during directional moves."
        )
        return _result(TradingRegime.TREND, range_pct, slope_pct, vix, reasons)

    # ── 3. Spread regime (default) ────────────────────────────────────────
    reasons.append(
        f"VIX = {vix:.1f} (below {VIX_VOL_THRESHOLD:.0f}), "
        f"range = {range_pct:.1f}%, slope = {slope_pct:.2f}%. "
        f"Market is calm and range-bound — ideal conditions for spread trading."
    )
    return _result(TradingRegime.SPREAD, range_pct, slope_pct, vix, reasons)


# ─── Helpers ───────────────────────────────────────────────────────────────

def _compute_range_pct(high: float, low: float) -> float:
    if low <= 0:
        return 0.0
    return round((high - low) / low * 100, 3)


def _compute_slope_pct(history: list, window: int = SLOPE_WINDOW) -> float:
    h = [x for x in history if x and float(x) > 0]
    h = h[-window:]
    if len(h) < 2:
        return 0.0
    first, last = float(h[0]), float(h[-1])
    if first <= 0:
        return 0.0
    return round((last - first) / first * 100, 3)


def _result(regime: str, range_pct: float, slope_pct: float,
            vix: float, reasons: list) -> dict:
    return {
        "regime":             regime,
        "intraday_range_pct": range_pct,
        "spot_slope_pct":     slope_pct,
        "vix":                vix,
        "reasons":            reasons,
    }
