"""
Intraday Strategy Advisor
=========================
Runs once per day at 9:30 AM IST (after the 15-min opening range forms).

Analyses gap%, VIX level/direction, IV–VIX spread, and the opening range
to recommend the single best intraday options strategy.

Strategies covered
------------------
BUY side  (pay premium, profit from movement):
  BUY_STRADDLE   – elevated VIX, no clear direction, flat open
  BUY_STRANGLE   – VIX spike ≥ 25, tail-risk day
  BUY_CE         – clear bullish gap, VIX falling/stable
  BUY_PE         – clear bearish gap, VIX falling/stable

SELL side (collect premium, profit from no movement / time decay):
  SELL_STRADDLE  – low VIX (< 18), IV overpriced (IV–VIX > +4)
  SELL_STRANGLE  – low VIX (< 18), IV mildly overpriced (> +2), tight range

WAIT           – no clear edge, preserve capital
"""
from __future__ import annotations
from datetime import date, datetime, timedelta

import pytz

IST = pytz.timezone("Asia/Kolkata")


# ── Strategy names ───────────────────────────────────────────────────────────
class IntradayStrategy:
    WAIT          = "WAIT"
    BUY_STRADDLE  = "BUY_STRADDLE"
    BUY_STRANGLE  = "BUY_STRANGLE"
    BUY_CE        = "BUY_CE"
    BUY_PE        = "BUY_PE"
    SELL_STRADDLE = "SELL_STRADDLE"
    SELL_STRANGLE = "SELL_STRANGLE"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _atm_strike(spot: float, step: int = 50) -> int:
    return round(spot / step) * step


def _nearest_thursday_label() -> str:
    """Return 'Mon DD' label for the next Thursday (NIFTY weekly expiry)."""
    today      = date.today()
    days_ahead = (3 - today.weekday()) % 7   # 3 = Thursday
    if days_ahead == 0:
        days_ahead = 7                        # already Thursday → next week
    exp = today + timedelta(days=days_ahead)
    return exp.strftime("%b %d")


def _fmt_inr(amount: float) -> str:
    if amount <= 0:
        return "₹0"
    s = f"{int(amount):,}"
    parts = s.split(",")
    if len(parts) <= 2:
        return f"₹{s}"
    last = parts[-1]
    rest = "".join(parts[:-1])
    r = ""
    for i, ch in enumerate(reversed(rest)):
        if i > 0 and i % 2 == 0:
            r = "," + r
        r = ch + r
    return f"₹{r},{last}"


def _leg(action: str, qty: int, label: str, inst_type: str, symbol: str = "") -> dict:
    return {
        "action":    action,
        "qty":       qty,
        "label":     label,
        "symbol":    symbol or label,
        "inst_type": inst_type,
    }


def _fmt_leg(leg: dict) -> str:
    icon = "✅ BUY " if leg["action"] == "BUY" else "🔴 SELL"
    return f"  {icon} {leg['qty']}× {leg['label']} @ MARKET"


# ── Public API ───────────────────────────────────────────────────────────────

def build_intraday_plan(
    spot_price:    float,
    prev_close:    float,
    vix:           float,
    vix_prev:      float,   # VIX from previous scan — used to detect rising/falling
    iv_vix_spread: float,
    intraday_high: float,
    intraday_low:  float,
    call_price:    float,   # ATM call premium (per unit)
    put_price:     float,   # ATM put premium (per unit)
    call_symbol:   str,
    put_symbol:    str,
    lot_size:      int   = 75,
    safety_regime: str   = "SAFE",   # SAFE | CAUTION | HALT
    # ── Momentum confirmation (from momentum_engine) ──────────────────
    rsi:           float = 50.0,
    vwap:          float = 0.0,
    ema20:         float = 0.0,
) -> dict:
    """
    Build intraday trading plan from market-open data.
    Returns a dict ready for DynamoDB storage and dashboard display.
    """
    atm      = _atm_strike(spot_price)
    gap_pct  = ((spot_price - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
    or_range = max(intraday_high - intraday_low, 0.0)   # Opening Range width in pts

    # VIX direction: True=rising, False=falling, None=unknown
    vix_rising = (vix > vix_prev) if vix_prev > 0 else None

    # Straddle cost
    straddle_premium  = call_price + put_price          # per unit
    straddle_cost_inr = straddle_premium * lot_size

    # Buy-strategy SL / targets (% of straddle cost)
    buy_sl_inr  = round(straddle_cost_inr * 0.25, 0)
    buy_tgt_inr = round(straddle_cost_inr * 0.40, 0)

    # OTM strikes for strangle (200 pts each side)
    otm_call = atm + 200
    otm_put  = atm - 200

    weekly_exp   = _nearest_thursday_label()
    entry_window = "9:30–10:00 AM"
    hard_exit    = "1:30 PM"

    strategy    = IntradayStrategy.WAIT
    confidence  = "LOW"
    emoji       = "⏸️"
    reason      = ""
    risk_note   = ""
    sl_note     = ""
    target_note = ""
    legs: list  = []

    # ── 0. HALT → always WAIT ────────────────────────────────────────────────
    if safety_regime == "HALT":
        strategy   = IntradayStrategy.WAIT
        emoji      = "🛑"
        confidence = "NONE"
        reason     = (
            "Regime Guard has HALTED trading. Market conditions are adverse — stay flat."
        )
        risk_note  = "No intraday positions today. Review again after conditions normalise."

    # ── 1. VIX ≥ 25 → BUY_STRANGLE (tail-risk day) ──────────────────────────
    elif vix >= 25.0:
        strategy   = IntradayStrategy.BUY_STRANGLE
        emoji      = "🌪️"
        confidence = "HIGH" if vix >= 27 else "MEDIUM"
        reason     = (
            f"VIX = {vix:.1f} (spike ≥ 25). Market is in tail-risk territory. "
            f"A large directional move is expected in either direction. "
            f"Buy OTM strangle to capture it with limited downside."
        )
        sl_note     = f"Exit if combined premium drops 25%"
        target_note = f"Exit if combined premium rises 50% or at {hard_exit}"
        risk_note   = (
            f"Max loss = premium paid. "
            f"Profit if NIFTY moves 250+ pts from {atm}. "
            f"Strikes: {otm_put} PE + {otm_call} CE ({weekly_exp} weekly)."
        )
        legs = [
            _leg("BUY", 1, f"NIFTY {otm_call} CE  [{weekly_exp}]", "CE", call_symbol),
            _leg("BUY", 1, f"NIFTY {otm_put}  PE  [{weekly_exp}]", "PE", put_symbol),
        ]

    # ── 2. Low VIX + IV overpriced → SELL_STRADDLE ──────────────────────────
    elif vix < 18.0 and iv_vix_spread > 4.0:
        strategy   = IntradayStrategy.SELL_STRADDLE
        emoji      = "📉"
        confidence = "HIGH" if iv_vix_spread > 6 else "MEDIUM"
        reason     = (
            f"VIX = {vix:.1f} (calm market, < 18). "
            f"IV–VIX spread = +{iv_vix_spread:.1f} — options are OVERPRICED vs expected move. "
            f"Sell ATM straddle and collect premium as IV reverts toward VIX."
        )
        sl_note     = "Exit if combined premium INCREASES 50% from entry (hard SL)"
        target_note = f"Exit if premium DECAYS 35% or at {hard_exit}"
        risk_note   = (
            f"Max profit ≈ {_fmt_inr(straddle_cost_inr)}. "
            f"Risk is large if NIFTY gaps — always use a hard stop. "
            f"Strike: ATM {atm} ({weekly_exp} weekly)."
        )
        legs = [
            _leg("SELL", 1, f"NIFTY {atm} CE  [{weekly_exp}]", "CE", call_symbol),
            _leg("SELL", 1, f"NIFTY {atm} PE  [{weekly_exp}]", "PE", put_symbol),
        ]

    # ── 3. Low VIX + mildly overpriced + tight range → SELL_STRANGLE ────────
    elif vix < 18.0 and iv_vix_spread > 2.0 and or_range < 50:
        strategy   = IntradayStrategy.SELL_STRANGLE
        emoji      = "📊"
        confidence = "MEDIUM"
        reason     = (
            f"VIX = {vix:.1f} (low). Opening range = {or_range:.0f} pts (tight). "
            f"IV–VIX spread = +{iv_vix_spread:.1f} (mildly overpriced). "
            f"Market likely to stay range-bound. Sell OTM strangle for time decay "
            f"with more cushion than a straddle."
        )
        sl_note     = f"Exit if NIFTY breaks below {otm_put} or above {otm_call}"
        target_note = f"Exit if combined premium decays 30% or at {hard_exit}"
        risk_note   = (
            f"Strikes 200 pts OTM on each side — cushion zone: {otm_put}–{otm_call}. "
            f"Max loss is large beyond strikes; use level-based SL above."
        )
        legs = [
            _leg("SELL", 1, f"NIFTY {otm_call} CE  [{weekly_exp}]", "CE", call_symbol),
            _leg("SELL", 1, f"NIFTY {otm_put}  PE  [{weekly_exp}]", "PE", put_symbol),
        ]

    # ── 4. Clear bullish gap + VIX falling/stable → BUY_CE ──────────────────
    elif gap_pct > 0.5 and vix_rising is not True and vix < 22:
        strategy   = IntradayStrategy.BUY_CE
        emoji      = "📈"
        vix_dir    = "falling" if vix_rising is False else "stable"
        # Upgrade confidence when VWAP+RSI momentum confirms direction
        _mom_confirms = vwap > 0 and spot_price > vwap and rsi > 55
        if gap_pct > 1.0 and _mom_confirms:
            confidence = "HIGH"
        elif gap_pct > 1.0 or _mom_confirms:
            confidence = "HIGH" if gap_pct > 1.0 else "MEDIUM"
        else:
            confidence = "MEDIUM"
        _mom_note = (
            f" VWAP confirmation: price > VWAP ({vwap:.0f}), RSI={rsi:.0f}."
            if _mom_confirms else ""
        )
        reason     = (
            f"Gap UP {gap_pct:+.2f}% from prev close. VIX = {vix:.1f} ({vix_dir}). "
            f"Classic trend-day setup: bullish momentum + calm VIX. "
            f"Buy ATM call to ride the move.{_mom_note}"
        )
        target_level = round(spot_price + 150)
        sl_level     = round(spot_price - 80)
        sl_note      = f"SL: NIFTY closes below {sl_level} on 15-min candle"
        target_note  = f"Target: NIFTY moves to {target_level}+ (exit by {hard_exit})"
        risk_note    = (
            f"Max loss = call premium paid. "
            f"Exit immediately if gap fills and momentum reverses before 11 AM."
        )
        legs = [
            _leg("BUY", 1, f"NIFTY {atm} CE  [{weekly_exp}]", "CE", call_symbol),
        ]

    # ── 5. Clear bearish gap + VIX falling/stable → BUY_PE ──────────────────
    elif gap_pct < -0.5 and vix_rising is not True and vix < 22:
        strategy   = IntradayStrategy.BUY_PE
        emoji      = "📉"
        vix_dir    = "falling" if vix_rising is False else "stable"
        # Upgrade confidence when VWAP+RSI momentum confirms direction
        _mom_confirms = vwap > 0 and spot_price < vwap and rsi < 45
        if gap_pct < -1.0 and _mom_confirms:
            confidence = "HIGH"
        elif gap_pct < -1.0 or _mom_confirms:
            confidence = "HIGH" if gap_pct < -1.0 else "MEDIUM"
        else:
            confidence = "MEDIUM"
        _mom_note = (
            f" VWAP confirmation: price < VWAP ({vwap:.0f}), RSI={rsi:.0f}."
            if _mom_confirms else ""
        )
        reason     = (
            f"Gap DOWN {gap_pct:+.2f}% from prev close. VIX = {vix:.1f} ({vix_dir}). "
            f"Classic trend-day setup: bearish momentum + calm VIX. "
            f"Buy ATM put to ride the downside.{_mom_note}"
        )
        target_level = round(spot_price - 150)
        sl_level     = round(spot_price + 80)
        sl_note      = f"SL: NIFTY recovers above {sl_level} on 15-min candle"
        target_note  = f"Target: NIFTY falls to {target_level}– (exit by {hard_exit})"
        risk_note    = (
            f"Max loss = put premium paid. "
            f"Exit immediately if bounce starts and momentum reverses before 11 AM."
        )
        legs = [
            _leg("BUY", 1, f"NIFTY {atm} PE  [{weekly_exp}]", "PE", put_symbol),
        ]

    # ── 6. Elevated VIX + flat/choppy open → BUY_STRADDLE ───────────────────
    elif 18.0 <= vix < 25.0 and or_range < 80:
        strategy   = IntradayStrategy.BUY_STRADDLE
        emoji      = "💥"
        confidence = "MEDIUM" if vix > 20 else "LOW"
        reason     = (
            f"VIX = {vix:.1f} (elevated, 18–25). Opening range = {or_range:.0f} pts (no clear direction). "
            f"Market expects a big move but direction is unknown. "
            f"Buy ATM straddle — profit whichever way NIFTY breaks."
        )
        sl_note     = f"Exit if combined premium drops 25% (≈ {_fmt_inr(buy_sl_inr)})"
        target_note = f"Exit if combined premium rises 40% (≈ {_fmt_inr(buy_tgt_inr)}) or at {hard_exit}"
        risk_note   = (
            f"Max loss = {_fmt_inr(straddle_cost_inr)} (premium × {lot_size} units). "
            f"Profit if NIFTY moves 150+ pts from ATM {atm} by {hard_exit}."
        )
        legs = [
            _leg("BUY", 1, f"NIFTY {atm} CE  [{weekly_exp}]", "CE", call_symbol),
            _leg("BUY", 1, f"NIFTY {atm} PE  [{weekly_exp}]", "PE", put_symbol),
        ]

    # ── 7. No edge → WAIT ────────────────────────────────────────────────────
    else:
        strategy   = IntradayStrategy.WAIT
        emoji      = "⏸️"
        confidence = "LOW"
        reason     = (
            f"VIX = {vix:.1f}, Gap = {gap_pct:+.2f}%, Opening Range = {or_range:.0f} pts. "
            f"Conditions don't clearly favour any strategy right now — stay flat."
        )
        risk_note = (
            "Preserve capital. Re-check after 10:30 AM if conditions improve. "
            "Do not force a trade."
        )

    # ── Confidence label for display ─────────────────────────────────────────
    confidence_label = {
        "HIGH":   "🟢 HIGH — Strong signal, all conditions aligned",
        "MEDIUM": "🟡 MEDIUM — Reasonable edge, manage size",
        "LOW":    "🔴 LOW — Weak edge, consider staying flat",
        "NONE":   "⛔ NONE — Do not trade",
    }.get(confidence, confidence)

    return {
        "strategy":          strategy,
        "emoji":             emoji,
        "confidence":        confidence,
        "confidence_label":  confidence_label,
        "reason":            reason,
        "risk_note":         risk_note,
        "sl_note":           sl_note,
        "target_note":       target_note,
        "entry_window":      entry_window,
        "hard_exit":         hard_exit,
        "legs":              legs,
        "legs_text":         "\n".join(_fmt_leg(l) for l in legs) if legs else "No action — stay flat.",
        # Raw metrics shown on dashboard
        "atm_strike":        atm,
        "gap_pct":           round(gap_pct, 2),
        "opening_range":     round(or_range, 1),
        "vix":               round(vix, 2),
        "iv_vix_spread":     round(iv_vix_spread, 2),
        "straddle_cost_inr": round(straddle_cost_inr, 0),
        "generated_at":      datetime.now(IST).isoformat(),
        "date":              datetime.now(IST).strftime("%Y-%m-%d"),
    }
