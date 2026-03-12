"""
Regime Guard
============
Determines whether market conditions are safe for Triple Calendar Spread trading.

Triple Calendar Spread exploits mean-reversion of the NIFTY futures cost-of-carry curve.
It REQUIRES:
  • A stable, normally-shaped futures curve
  • Adequate liquidity in all three contract months
  • Predictable spread relationships driven by carry, not panic

It BREAKS DOWN when:
  • India VIX spikes (curve distorts, liquidity disappears)
  • Large gap openings (institutional shock, margin calls)
  • Known high-impact events (elections, budget, RBI policy, geopolitical shocks)
  • Extreme curve distortion already present (spreads > historical norms)

Professional prop desks rule: spread trading only when VIX < 18. Full halt when VIX > 25.

Regime levels
─────────────
  SAFE    : All conditions normal. Trade at full size.
  CAUTION : One or more elevated-risk conditions. Trade at 50% size, monitor closely.
  HALT    : Hard stop. Do NOT open new spread positions. Explain reason clearly.
"""

from datetime import date
from typing import Optional

# ─── Configurable thresholds ───────────────────────────────────────────────

VIX_SAFE          = 18.0    # Below this: ideal conditions for spread trading
VIX_CAUTION       = 25.0    # Above this: spreads become unstable → HALT
GAP_HALT_PCT      = 0.025   # 2.5%+ gap from prev close → hard halt
GAP_CAUTION_PCT   = 0.015   # 1.5%+ gap → caution
CURVE_EXTREME_PTS = 100.0   # curve_diff beyond ±100 pts = already distorted
DTE_OPTIMAL_MIN   = 3       # Best window: 3–7 DTE before near expiry
DTE_OPTIMAL_MAX   = 7


class Regime:
    SAFE    = "SAFE"
    CAUTION = "CAUTION"
    HALT    = "HALT"


# ─── Explanations shown in dashboard / Telegram ────────────────────────────

_WHY_VIX_HALT = (
    "India VIX ≥ {vix:.1f} (threshold: {thresh}). "
    "Extreme volatility means: "
    "(1) Futures curve distorts away from cost-of-carry; "
    "(2) Spread relationships become driven by panic/euphoria, not carry; "
    "(3) Liquidity in middle and far months dries up — wide bid-ask destroys edge; "
    "(4) Mean-reversion assumption breaks down — spread can keep expanding indefinitely. "
    "Professional prop desks stop spread trading when VIX > 25."
)

_WHY_VIX_CAUTION = (
    "India VIX = {vix:.1f} (elevated, {lo}–{hi} range). "
    "Spread behavior is less predictable. "
    "Reduce position size by 50%. Monitor curve diff closely."
)

_WHY_GAP_HALT = (
    "Gap opening detected: {gap_pct:.1f}% from previous close "
    "({prev:.0f} → {spot:.0f}). "
    "Large gaps indicate institutional shock, margin call liquidations, or "
    "geopolitical surprise. In gap sessions: "
    "(1) All three futures move abnormally; "
    "(2) Spread relationships lose their normal carry linkage; "
    "(3) Liquidity collapses at the open. Do not enter new spread positions."
)

_WHY_GAP_CAUTION = (
    "Gap opening: {gap_pct:.1f}% from previous close. "
    "Spread liquidity may be impaired at open. "
    "Wait 30 minutes for curve to stabilise before trading."
)

_WHY_EVENT = (
    "Today ({today}) is a blocked high-impact event date. "
    "Events like elections, budget, RBI policy announcements, and geopolitical shocks "
    "break the normal futures curve behaviour: "
    "(1) Market can move ±5–10% on the result; "
    "(2) Futures curve shape depends on market regime, which changes instantly; "
    "(3) Butterfly spread trader may be on the wrong side of a regime shift. "
    "Example: Election result rally can expand spreads from 250 to 400+ pts instantly — "
    "a 3-leg butterfly has no hedge against this directional move."
)

_WHY_CURVE_EXTREME = (
    "Curve diff is {diff:.1f} pts — beyond the normal ±{thresh:.0f} pt range. "
    "The futures curve is already distorted. "
    "Entering a butterfly now means assuming mean-reversion from an extreme: "
    "if distortion is event-driven (not carry-driven), it may not revert. "
    "Wait for curve to normalise before trading."
)

_WHY_DTE_TOO_FAR = (
    "DTE = {dte} days. Optimal spread compression window is {lo}–{hi} DTE. "
    "Further from expiry, spread convergence is slow — holding costs erode edge."
)


# ─── Public API ────────────────────────────────────────────────────────────

def check_trade_safety(
    vix: float,
    spot_price: float = 0.0,
    prev_close: float = 0.0,
    curve_diff: float = 0.0,
    dte: int = 30,
    event_dates: Optional[list] = None,
) -> dict:
    """
    Evaluate all safety conditions and return a composite assessment.

    Returns
    -------
    safe           : bool  – False = do NOT open new positions
    regime         : str   – SAFE | CAUTION | HALT
    reasons        : list  – Blocking reasons (HALT triggers)
    warnings       : list  – Non-blocking advisories (CAUTION)
    vix            : float
    vix_level      : str   – LOW | ELEVATED | EXTREME
    optimal_window : bool  – True if DTE in 3–7 range
    size_factor    : float – 1.0 = full, 0.5 = caution, 0.0 = halt
    """
    if event_dates is None:
        event_dates = []

    reasons  = []   # Hard blocks
    warnings = []   # Soft advisories

    # ── 1. India VIX regime ──────────────────────────────────────────────
    if vix > 0:
        if vix >= VIX_CAUTION:
            reasons.append(_WHY_VIX_HALT.format(
                vix=vix, thresh=int(VIX_CAUTION)
            ))
        elif vix >= VIX_SAFE:
            warnings.append(_WHY_VIX_CAUTION.format(
                vix=vix, lo=int(VIX_SAFE), hi=int(VIX_CAUTION)
            ))

    # ── 2. Gap opening ───────────────────────────────────────────────────
    if spot_price > 0 and prev_close > 0:
        gap_pct = abs(spot_price - prev_close) / prev_close
        if gap_pct >= GAP_HALT_PCT:
            reasons.append(_WHY_GAP_HALT.format(
                gap_pct=gap_pct * 100, prev=prev_close, spot=spot_price
            ))
        elif gap_pct >= GAP_CAUTION_PCT:
            warnings.append(_WHY_GAP_CAUTION.format(gap_pct=gap_pct * 100))

    # ── 3. Event calendar ────────────────────────────────────────────────
    today_str = date.today().isoformat()
    if today_str in event_dates:
        reasons.append(_WHY_EVENT.format(today=today_str))

    # ── 4. Extreme curve distortion ──────────────────────────────────────
    if abs(curve_diff) >= CURVE_EXTREME_PTS:
        reasons.append(_WHY_CURVE_EXTREME.format(
            diff=curve_diff, thresh=CURVE_EXTREME_PTS
        ))

    # ── 5. DTE window advisory ───────────────────────────────────────────
    optimal_window = DTE_OPTIMAL_MIN <= dte <= DTE_OPTIMAL_MAX
    if dte > 15:
        warnings.append(_WHY_DTE_TOO_FAR.format(
            dte=dte, lo=DTE_OPTIMAL_MIN, hi=DTE_OPTIMAL_MAX
        ))

    # ── Determine regime ─────────────────────────────────────────────────
    if reasons:
        regime      = Regime.HALT
        size_factor = 0.0
    elif warnings:
        regime      = Regime.CAUTION
        size_factor = 0.5
    else:
        regime      = Regime.SAFE
        size_factor = 1.0

    return {
        "safe":           regime != Regime.HALT,
        "regime":         regime,
        "reasons":        reasons,
        "warnings":       warnings,
        "vix":            vix,
        "vix_level":      _vix_label(vix),
        "optimal_window": optimal_window,
        "size_factor":    size_factor,
    }


def get_india_vix(broker) -> Optional[float]:
    """
    Fetch live India VIX from broker.
    Returns None if unavailable (never blocks trading on fetch failure).
    """
    try:
        broker_type = type(broker).__name__
        if broker_type == "ZerodhaBroker":
            data = broker.get_ltp(["NSE:INDIA VIX"])
            return float(data.get("NSE:INDIA VIX", {}).get("last_price") or 0) or None
        elif broker_type == "AngelOneBroker":
            # India VIX token on NSE = 99926017
            data = broker.get_ltp(["NSE:99926017"])
            return float(data.get("NSE:India VIX", {}).get("ltp") or 0) or None
    except Exception:
        pass
    return None


def _vix_label(vix: float) -> str:
    if vix <= 0:
        return "UNKNOWN"
    if vix < VIX_SAFE:
        return "LOW"
    if vix < VIX_CAUTION:
        return "ELEVATED"
    return "EXTREME"
