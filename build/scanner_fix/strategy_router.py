"""
Strategy Router — Daily Plan Builder
=====================================
Takes all engine outputs and produces one clear daily trading plan.

Selection priority (highest wins):
  1. safety_regime == HALT           → WAIT  (never trade)
  2. VOLATILITY + VIX ≥ 25          → BUY_STRANGLE
  3. VOLATILITY + IV–VIX > +4       → SELL_STRADDLE
  4. VOLATILITY + IV–VIX < -3       → BUY_STRADDLE
  5. arb_signal != NONE              → ARB_SYNTHETIC
  6. SPREAD + spread_signal + strength ≥ 2.5 → TRIPLE_CALENDAR
  7. Otherwise                       → WAIT

The output dict is designed to be displayed directly in the dashboard
"Daily Plan" tab — plain-language action cards with specific symbols,
quantities, and estimated capital.
"""

from __future__ import annotations
from typing import Optional

# ─── Strategy names ────────────────────────────────────────────────────────
class Strategy:
    WAIT            = "WAIT"
    TRIPLE_CALENDAR = "TRIPLE_CALENDAR"
    ARB_SYNTHETIC   = "ARB_SYNTHETIC"
    BUY_STRADDLE    = "BUY_STRADDLE"
    SELL_STRADDLE   = "SELL_STRADDLE"
    BUY_STRANGLE    = "BUY_STRANGLE"


# ─── Public API ─────────────────────────────────────────────────────────────

def build_daily_plan(
    safety:        dict,   # from regime_guard.check_trade_safety()
    regime:        dict,   # from regime_detector.detect_regime()
    vol:           dict,   # from volatility_engine.compute_volatility_signal()
    arb:           dict,   # from arbitrage_engine.check_parity()
    spread_signal: str,    # "BUY_BUTTERFLY" | "SELL_BUTTERFLY" | "NONE"
    strength:      float,  # signal_strength score (0-5)
    futures:       dict,   # {"near": sym, "next": sym, "far": sym, ...}
    qty:           int,    # regime-adjusted recommended lot qty
    lot_size:      int = 75,
    min_strength:  float = 2.5,
) -> dict:
    """
    Build the daily trading plan from all engine outputs.

    Returns a dict ready for DynamoDB storage and dashboard display.
    """
    strategy     = Strategy.WAIT
    reason       = "No actionable signal at this time."
    legs         = []
    capital_est  = 0.0
    risk_note    = ""
    emoji        = "⏸️"

    arb_sig      = arb.get("arbitrage_signal", "NONE")
    vol_sig      = vol.get("vol_signal", "NONE")
    trading_rg   = regime.get("regime", "SPREAD")
    vix_val      = regime.get("vix", 0.0)
    iv_spread    = vol.get("iv_vix_spread", 0.0)
    spot         = vol.get("breakeven_upper", 0)   # proxy — not used directly
    straddle_prem = vol.get("straddle_premium", 0.0)

    # ── 1. Regime HALT → absolute no-trade ──────────────────────────────
    if not safety.get("safe", True):
        strategy    = Strategy.WAIT
        emoji       = "🛑"
        block_reasons = safety.get("reasons", [])
        reason = (
            "Trading HALTED by regime guard. "
            + (block_reasons[0] if block_reasons else "Adverse market conditions.")
        )
        risk_note = "No positions should be opened until conditions normalise."
        legs = []
        capital_est = 0.0

    # ── 2. Volatility regime + VIX spike → BUY_STRANGLE ────────────────
    elif trading_rg == "VOLATILITY" and vix_val >= 25.0 and vol_sig == "BUY_STRANGLE":
        strategy    = Strategy.BUY_STRANGLE
        emoji       = "🌪️"
        call_sym    = vol.get("strangle_call_symbol", "OTM+200 CE")
        put_sym     = vol.get("strangle_put_symbol",  "OTM-200 PE")
        strike      = vol.get("strike", 0)
        dte         = vol.get("dte", 0)
        capital_est = float(vol.get("capital_needed", 0))
        reason = (
            f"VIX = {vix_val:.1f} (spike ≥ 25). Market is in tail-risk territory. "
            f"Buy OTM strangle to capture a large directional move in either direction."
        )
        risk_note = (
            f"Max loss = premium paid (≈ ₹{_fmt_inr(capital_est)}). "
            f"Profit if NIFTY moves beyond ±{strike}±200 before expiry ({dte} DTE)."
        )
        legs = [
            _leg("BUY", 1, call_sym, "MARKET", "CE"),
            _leg("BUY", 1, put_sym,  "MARKET", "PE"),
        ]

    # ── 3. Volatility regime + overpriced IV → SELL_STRADDLE ────────────
    elif trading_rg == "VOLATILITY" and vol_sig == "SELL_STRADDLE":
        strategy    = Strategy.SELL_STRADDLE
        emoji       = "📉"
        call_sym    = vol.get("call_symbol", "ATM CE")
        put_sym     = vol.get("put_symbol",  "ATM PE")
        strike      = vol.get("strike", 0)
        dte         = vol.get("dte", 0)
        straddle_inr = float(vol.get("straddle_cost_inr", 0))
        bu           = vol.get("breakeven_upper", 0)
        bl           = vol.get("breakeven_lower", 0)
        capital_est  = float(vol.get("capital_needed", 0))
        reason = (
            f"IV–VIX spread = +{iv_spread:.1f}. Options are OVERPRICED. "
            f"Sell ATM straddle — collect premium as IV reverts toward VIX level."
        )
        risk_note = (
            f"Max profit = ₹{_fmt_inr(straddle_inr)} (straddle premium × {lot_size} lot). "
            f"Breakevens: {bl:.0f} ↔ {bu:.0f} ({dte} DTE). "
            f"Margin required ≈ ₹{_fmt_inr(capital_est)}."
        )
        legs = [
            _leg("SELL", 1, call_sym, "MARKET", "CE"),
            _leg("SELL", 1, put_sym,  "MARKET", "PE"),
        ]

    # ── 4. Volatility regime + cheap IV → BUY_STRADDLE ─────────────────
    elif trading_rg == "VOLATILITY" and vol_sig == "BUY_STRADDLE":
        strategy    = Strategy.BUY_STRADDLE
        emoji       = "💥"
        call_sym    = vol.get("call_symbol", "ATM CE")
        put_sym     = vol.get("put_symbol",  "ATM PE")
        strike      = vol.get("strike", 0)
        dte         = vol.get("dte", 0)
        straddle_inr = float(vol.get("straddle_cost_inr", 0))
        bu           = vol.get("breakeven_upper", 0)
        bl           = vol.get("breakeven_lower", 0)
        capital_est  = straddle_inr
        reason = (
            f"IV–VIX spread = {iv_spread:.1f}. Options are CHEAP vs expected move. "
            f"Buy ATM straddle — profit if NIFTY makes a big move in either direction."
        )
        risk_note = (
            f"Max loss = ₹{_fmt_inr(straddle_inr)} (premium paid). "
            f"Profit if NIFTY closes outside {bl:.0f}–{bu:.0f} at expiry ({dte} DTE)."
        )
        legs = [
            _leg("BUY", 1, call_sym, "MARKET", "CE"),
            _leg("BUY", 1, put_sym,  "MARKET", "PE"),
        ]

    # ── 5. Synthetic arbitrage ───────────────────────────────────────────
    elif arb_sig in ("BUY_FUT_SELL_CALL_BUY_PUT", "SELL_FUT_BUY_CALL_SELL_PUT"):
        strategy    = Strategy.ARB_SYNTHETIC
        emoji       = "⚡"
        mispr       = float(arb.get("arb_mispricing", 0))
        arb_sl      = arb.get("arb_stoploss",  0)
        arb_tgt     = arb.get("arb_target",    0)
        call_sym    = arb.get("call_symbol", "ATM CE")
        put_sym     = arb.get("put_symbol",  "ATM PE")
        near_sym    = futures.get("near", "NEAR FUT")
        capital_est = float(arb.get("capital_needed", 0))
        reason = (
            f"Put-call parity violated by {mispr:.1f} pts. "
            f"Risk-free arbitrage available via synthetic futures."
        )
        risk_note = (
            f"SL = {arb_sl} pts | Target = {arb_tgt} pts. "
            f"Execute all legs simultaneously for true arbitrage."
        )
        # Signal names from arbitrage_engine: BUY_FUT_SELL_CALL_BUY_PUT / SELL_FUT_BUY_CALL_SELL_PUT
        if arb_sig == "BUY_FUT_SELL_CALL_BUY_PUT":
            legs = [
                _leg("BUY",  1, near_sym, "MARKET", "FUT"),
                _leg("SELL", 1, call_sym, "MARKET", "CE"),
                _leg("BUY",  1, put_sym,  "MARKET", "PE"),
            ]
        else:  # SELL_FUT_BUY_CALL_SELL_PUT
            legs = [
                _leg("SELL", 1, near_sym, "MARKET", "FUT"),
                _leg("BUY",  1, call_sym, "MARKET", "CE"),
                _leg("SELL", 1, put_sym,  "MARKET", "PE"),
            ]

    # ── 6. Calm SPREAD regime → TRIPLE_CALENDAR ─────────────────────────
    elif (trading_rg == "SPREAD"
          and spread_signal != "NONE"
          and strength >= min_strength
          and qty > 0):
        strategy    = Strategy.TRIPLE_CALENDAR
        emoji       = "🦋"
        near_sym    = futures.get("near", "NEAR FUT")
        next_sym    = futures.get("next", "NEXT FUT")
        far_sym     = futures.get("far",  "FAR FUT")
        near_exp    = futures.get("near_expiry", "")
        next_exp    = futures.get("next_expiry", "")
        far_exp     = futures.get("far_expiry",  "")
        is_sell     = spread_signal == "SELL_BUTTERFLY"
        direction   = "SELL" if is_sell else "BUY"
        opp         = "BUY"  if is_sell else "SELL"
        capital_est = float(qty) * lot_size * 50   # rough margin estimate
        reason = (
            f"Market is calm (SPREAD regime). "
            f"Triple Calendar signal: {spread_signal} | Strength {strength:.1f}/5."
        )
        risk_note = (
            f"Calendar spread profits from time-decay convergence. "
            f"Qty = {qty} lot-sets × {lot_size} = {qty * lot_size} units per leg. "
            f"Exit when curve_diff reverts to mean or at SL."
        )
        legs = [
            _leg(direction, qty, near_sym, "MARKET", "FUT", near_exp),
            _leg(opp,   qty * 2, next_sym, "MARKET", "FUT", next_exp),
            _leg(direction, qty, far_sym,  "MARKET", "FUT", far_exp),
        ]

    # ── 7. No edge → WAIT ────────────────────────────────────────────────
    else:
        strategy    = Strategy.WAIT
        emoji       = "⏸️"
        regime_msg  = regime.get("reasons", [""])
        reason = (
            f"Market conditions do not favour any strategy right now. "
            + (regime_msg[0] if regime_msg else "")
        )
        risk_note = "Stay flat. Review again at next scan."
        legs = []
        capital_est = 0.0

    return {
        "strategy":        strategy,
        "strategy_emoji":  emoji,
        "reason":          reason,
        "risk_note":       risk_note,
        "legs":            legs,
        "legs_text":       "\n".join(_fmt_leg(l) for l in legs) if legs else "No action.",
        "capital_est":     round(capital_est, 0),
        "capital_est_fmt": _fmt_inr(capital_est),
        "trading_regime":  trading_rg,
        "vol_signal":      vol_sig,
        "arb_signal":      arb_sig,
        "spread_signal":   spread_signal,
        "strength":        round(strength, 2),
        "qty":             qty,
        "vix":             round(vix_val, 2),
        "iv_vix_spread":   round(iv_spread, 2),
    }


# ─── Helpers ────────────────────────────────────────────────────────────────

def _leg(
    action:  str,         # BUY | SELL
    qty:     int,
    symbol:  str,
    order_type: str = "MARKET",
    inst_type:  str = "",  # CE | PE | FUT
    expiry:     str = "",
) -> dict:
    return {
        "action":     action,
        "qty":        qty,
        "symbol":     symbol,
        "order_type": order_type,
        "inst_type":  inst_type,
        "expiry":     expiry,
    }


def _fmt_leg(leg: dict) -> str:
    """Single human-readable action line shown in dashboard."""
    parts = [
        f"  {'✅ BUY ' if leg['action'] == 'BUY' else '🔴 SELL'} "
        f"{leg['qty']}×",
        leg["symbol"],
        f"@ {leg['order_type']}",
    ]
    if leg.get("expiry"):
        parts.append(f"[exp {leg['expiry']}]")
    return " ".join(parts)


def _fmt_inr(amount: float) -> str:
    """Format as Indian number style: ₹1,23,456."""
    if amount <= 0:
        return "₹0"
    s = f"{int(amount):,}"
    # Convert Western comma grouping to Indian style (last 3 then 2s)
    parts = s.split(",")
    if len(parts) <= 2:
        return f"₹{s}"
    last = parts[-1]
    rest = ",".join(parts[:-1]).replace(",", "")
    # Re-format the rest with Indian grouping
    r = ""
    for i, ch in enumerate(reversed(rest)):
        if i > 0 and i % 2 == 0:
            r = "," + r
        r = ch + r
    return f"₹{r},{last}"
