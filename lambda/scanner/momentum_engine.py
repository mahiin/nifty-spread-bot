"""
Momentum Engine — VWAP + EMA(20) + RSI(14) triple-confirmation signal
======================================================================
Uses 5-minute NIFTY futures candles (NOT spot index) to compute intraday
momentum signals for NIFTY F&O direction trades.

Signal logic (TRIPLE CONFIRMATION):
  MOMENTUM_LONG:  close > VWAP  AND  close > EMA(20)  AND  RSI(14) > 60
  MOMENTUM_SHORT: close < VWAP  AND  close < EMA(20)  AND  RSI(14) < 40
  NEUTRAL: otherwise

Guards:
  - Minimum 16 candles required (RSI(14) needs 15 seed + 1 live → valid ~10:35 AM)
  - VIX gate: skip if VIX ≥ 22 (use volatility strategies instead)
  - IV-VIX gate: skip if |iv_vix_spread| > 5 (extreme IV events)
  - Returns NEUTRAL if broker call fails (non-fatal)

Confidence:
  HIGH:   RSI > 65 (long) or < 35 (short) + all 3 conditions align
  MEDIUM: RSI 60-65 (long) or 35-40 (short) + all 3 conditions align
  LOW:    only 2 of 3 conditions met (display only, not traded)

Pure Python — no numpy/scipy required.
"""
from __future__ import annotations
import math


# ─── Core calculations ──────────────────────────────────────────────────────

def compute_vwap(candles: list) -> float:
    """
    Cumulative intraday VWAP from market open.
    candles: [[ts, open, high, low, close, volume], ...]
    Returns 0.0 if candles is empty or volumes are all zero.
    """
    cum_tp_vol = 0.0
    cum_vol    = 0.0
    for c in candles:
        try:
            high   = float(c[2])
            low    = float(c[3])
            close  = float(c[4])
            volume = float(c[5])
        except (IndexError, TypeError, ValueError):
            continue
        if volume <= 0:
            continue
        typical_price = (high + low + close) / 3.0
        cum_tp_vol   += typical_price * volume
        cum_vol      += volume
    return cum_tp_vol / cum_vol if cum_vol > 0 else 0.0


def compute_ema(closes: list, n: int) -> float:
    """
    Exponential Moving Average using standard multiplier k = 2/(n+1).
    Seed = simple average of first n values.
    Returns 0.0 if not enough data.
    """
    if len(closes) < n:
        return 0.0
    k   = 2.0 / (n + 1)
    ema = sum(closes[:n]) / n      # SMA seed
    for price in closes[n:]:
        ema = price * k + ema * (1 - k)
    return ema


def compute_rsi(closes: list, n: int = 14) -> float:
    """
    RSI using Wilder's smoothing (RMA).
    Requires at least n+1 values.
    Returns 50.0 (neutral) if not enough data.
    """
    if len(closes) < n + 1:
        return 50.0

    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    # Initial Wilder average (SMA of first n)
    avg_gain = sum(gains[:n]) / n
    avg_loss = sum(losses[:n]) / n

    # Wilder smoothing for remaining bars
    for i in range(n, len(gains)):
        avg_gain = (avg_gain * (n - 1) + gains[i]) / n
        avg_loss = (avg_loss * (n - 1) + losses[i]) / n

    if avg_loss == 0:
        return 100.0
    rs  = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


# ─── Public API ─────────────────────────────────────────────────────────────

def compute_momentum_signal(
    broker,
    near_token:    str,
    spot_price:    float,
    vix:           float   = 0.0,
    iv_vix_spread: float   = 0.0,
    lot_size:      int     = 75,
) -> dict:
    """
    Fetch 5-min NIFTY futures candles and compute VWAP + EMA(20) + RSI(14).

    Returns a dict with keys:
      signal       : "MOMENTUM_LONG" | "MOMENTUM_SHORT" | "NEUTRAL"
      rsi          : float
      ema20        : float
      vwap         : float
      candle_count : int
      confidence   : "HIGH" | "MEDIUM" | "LOW"
      reason       : str
    """
    _neutral = {
        "signal":       "NEUTRAL",
        "rsi":          50.0,
        "ema20":        0.0,
        "vwap":         0.0,
        "candle_count": 0,
        "confidence":   "LOW",
        "reason":       "",
    }

    # ── VIX gate: high VIX → use volatility strategies, not momentum ────────
    if vix >= 22.0:
        _neutral["reason"] = f"VIX={vix:.1f} ≥ 22 — skipping momentum (use volatility strategy)"
        return _neutral

    # ── IV-VIX extreme gate ─────────────────────────────────────────────────
    if abs(iv_vix_spread) > 5.0:
        _neutral["reason"] = (
            f"IV-VIX spread={iv_vix_spread:.1f} (extreme) — "
            f"skipping momentum (use straddle/strangle)"
        )
        return _neutral

    # ── Fetch candles ────────────────────────────────────────────────────────
    try:
        candles = broker.get_candles(
            token    = str(near_token),
            exchange = "NFO",
            interval = "FIVE_MINUTE",
        )
    except Exception as e:
        _neutral["reason"] = f"Candle fetch failed: {e}"
        return _neutral

    if not candles or len(candles) < 16:
        _neutral["reason"] = (
            f"Insufficient candle data ({len(candles) if candles else 0} candles, "
            f"need ≥16)"
        )
        return _neutral

    # ── Extract close prices ─────────────────────────────────────────────────
    closes = []
    for c in candles:
        try:
            closes.append(float(c[4]))
        except (IndexError, TypeError, ValueError):
            pass

    if len(closes) < 16:
        _neutral["reason"] = f"Insufficient valid close prices ({len(closes)})"
        return _neutral

    # ── Compute indicators ───────────────────────────────────────────────────
    vwap  = compute_vwap(candles)
    ema20 = compute_ema(closes, 20)
    rsi   = compute_rsi(closes, 14)
    last_close = closes[-1]

    candle_count = len(closes)

    # ── Signal classification ────────────────────────────────────────────────
    above_vwap  = vwap  > 0 and last_close > vwap
    below_vwap  = vwap  > 0 and last_close < vwap
    above_ema20 = ema20 > 0 and last_close > ema20
    below_ema20 = ema20 > 0 and last_close < ema20

    long_count  = sum([above_vwap, above_ema20, rsi > 60])
    short_count = sum([below_vwap, below_ema20, rsi < 40])

    if above_vwap and above_ema20 and rsi > 60:
        signal = "MOMENTUM_LONG"
        confidence = "HIGH" if rsi > 65 else "MEDIUM"
        reason = (
            f"TRIPLE CONFIRMATION — LONG: "
            f"close={last_close:.0f} > VWAP={vwap:.0f}, "
            f"> EMA20={ema20:.0f}, RSI(14)={rsi:.1f}"
        )
    elif below_vwap and below_ema20 and rsi < 40:
        signal = "MOMENTUM_SHORT"
        confidence = "HIGH" if rsi < 35 else "MEDIUM"
        reason = (
            f"TRIPLE CONFIRMATION — SHORT: "
            f"close={last_close:.0f} < VWAP={vwap:.0f}, "
            f"< EMA20={ema20:.0f}, RSI(14)={rsi:.1f}"
        )
    else:
        signal = "NEUTRAL"
        confidence = "LOW"
        parts = []
        if long_count == 2:
            parts.append(f"Partial LONG ({long_count}/3 conditions)")
        elif short_count == 2:
            parts.append(f"Partial SHORT ({short_count}/3 conditions)")
        else:
            parts.append("No momentum alignment")
        parts.append(
            f"close={last_close:.0f}, VWAP={vwap:.0f}, "
            f"EMA20={ema20:.0f}, RSI={rsi:.1f}"
        )
        reason = ". ".join(parts)

    return {
        "signal":       signal,
        "rsi":          round(rsi,  1),
        "ema20":        round(ema20, 1),
        "vwap":         round(vwap,  1),
        "candle_count": candle_count,
        "confidence":   confidence,
        "reason":       reason,
    }
