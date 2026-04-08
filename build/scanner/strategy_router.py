"""
Strategy Router — Daily Plan Builder
=====================================
Takes all engine outputs and produces one clear daily trading plan.

Selection priority (highest wins):
  1. safety_regime == HALT              → WAIT  (never trade)
  2. Thursday (expiry day)              → WAIT  (no new option sells)
  3. VOLATILITY + VIX ≥ 25             → BUY_STRANGLE
  4. Iron Condor (Mon/Tue, VIX 14-22, PCR neutral) → IRON_CONDOR  ← daily income
  5. VOLATILITY + IV–VIX > +1.5        → SELL_STRADDLE
  6. VOLATILITY + IV–VIX < -3          → BUY_STRADDLE
  7. FII_BEARISH + PCR_BEARISH + VIX rising → BEAR_CALL_SPREAD
  8. FII_BULLISH + PCR_BULLISH         → BULL_PUT_SPREAD
  9. MOMENTUM_LONG/SHORT (VWAP+EMA20+RSI triple confirm, VIX<22) ← NEW
  10. arb_signal != NONE               → ARB_SYNTHETIC
  11. TREND regime                     → BULL_CALL_SPREAD / BEAR_PUT_SPREAD
  12. SPREAD + spread_signal + strength ≥ 2.5 → TRIPLE_CALENDAR
  13. Otherwise                        → WAIT

The output dict is designed to be displayed directly in the dashboard
"Daily Plan" tab — plain-language action cards with specific symbols,
quantities, and estimated capital.
"""

from __future__ import annotations
from datetime import date
from typing import Optional

import pytz
from win_scorer import score_win_probability

_IST = pytz.timezone("Asia/Kolkata")


# ─── Strategy names ────────────────────────────────────────────────────────
class Strategy:
    WAIT             = "WAIT"
    TRIPLE_CALENDAR  = "TRIPLE_CALENDAR"
    ARB_SYNTHETIC    = "ARB_SYNTHETIC"
    BUY_STRADDLE     = "BUY_STRADDLE"
    SELL_STRADDLE    = "SELL_STRADDLE"
    BUY_STRANGLE     = "BUY_STRANGLE"
    IRON_CONDOR      = "IRON_CONDOR"
    BULL_PUT_SPREAD  = "BULL_PUT_SPREAD"
    BEAR_CALL_SPREAD = "BEAR_CALL_SPREAD"
    BULL_CALL_SPREAD = "BULL_CALL_SPREAD"
    BEAR_PUT_SPREAD  = "BEAR_PUT_SPREAD"
    MOMENTUM_LONG    = "MOMENTUM_LONG"
    MOMENTUM_SHORT   = "MOMENTUM_SHORT"


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
    lot_size:      int    = 75,
    min_strength:  float  = 2.5,
    # ── New: institutional + option chain context ──────────────────────
    fii_signal:    str    = "FII_NEUTRAL",    # cash-market flow
    fii_fut_signal:str    = "FII_FUT_NEUTRAL",# F&O futures positioning (gold standard)
    pcr:           float  = 1.0,              # from option_chain_engine
    pcr_signal:    str    = "PCR_NEUTRAL",    # from option_chain_engine
    max_pain:      int    = 0,                # from option_chain_engine
    iron_condor_plan: Optional[dict] = None,  # pre-built IC plan from iron_condor.py
    day_of_week:   int    = -1,               # 0=Mon … 6=Sun  (-1 = unknown)
    dte:           int    = 14,               # days to near expiry
    event_dates:   list   = [],
    momentum:      Optional[dict] = None,     # from momentum_engine.compute_momentum_signal()
    vix:           float  = 0.0,              # raw VIX value (for win scorer)
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

    arb_sig       = arb.get("arbitrage_signal", "NONE")
    vol_sig       = vol.get("vol_signal", "NONE")
    trading_rg    = regime.get("regime", "SPREAD")
    vix_val       = regime.get("vix", 0.0)
    iv_spread     = vol.get("iv_vix_spread", 0.0)
    straddle_prem = vol.get("straddle_premium", 0.0)

    # Day labels for reasoning text
    _day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    day_label  = _day_names[day_of_week] if 0 <= day_of_week <= 6 else ""

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

    # ── 2. Thursday (weekly expiry day) → no new option sells ───────────
    elif day_of_week == 3:
        strategy  = Strategy.WAIT
        emoji     = "📅"
        reason    = (
            "Thursday = NIFTY weekly expiry day. "
            "Do NOT open new option positions. "
            "Close any open positions by 12:30 PM."
        )
        risk_note = "Same-day gamma risk makes new option sells extremely dangerous."
        legs      = []

    # ── 3. VIX spike OR FII heavy options buying → BUY_STRANGLE ─────────
    # Fires on: VIX ≥ 25 (tail risk)
    #        OR fii_opt_pcr extreme (< 0.5 or > 2.0) — FII making one-sided big bet
    # fii_opt_pcr from fetch_fii_futures() — FII's put/call OI ratio in index options
    elif (
        (trading_rg == "VOLATILITY" and vix_val >= 25.0 and vol_sig == "BUY_STRANGLE")
        or (
            not (iron_condor_plan and day_of_week in (0, 1))   # IC not already preferred
            and safety.get("safe", False)
            and vix_val >= 18.0
            and (fii_fut_signal == "FII_FUT_SHORT" and pcr_signal == "PCR_BEARISH")   # both agree on big down move
        )
    ):
        strategy    = Strategy.BUY_STRANGLE
        emoji       = "🌪️"
        call_sym    = vol.get("strangle_call_symbol", "OTM+200 CE")
        put_sym     = vol.get("strangle_put_symbol",  "OTM-200 PE")
        strike      = vol.get("strike", 0)
        dte         = vol.get("dte", 0)
        capital_est = float(vol.get("capital_needed", 0))
        if vix_val >= 25.0:
            _strangle_trigger = f"VIX={vix_val:.1f} (spike ≥ 25) — tail-risk territory"
        else:
            _strangle_trigger = (
                f"FII net SHORT futures + PCR bearish — institutions positioning "
                f"for a large downward move (VIX={vix_val:.1f})"
            )
        reason = (
            f"{_strangle_trigger}. "
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
    # Threshold relaxed from +4 to +1.5 so this fires on normal overpriced days
    elif trading_rg == "VOLATILITY" and vol_sig == "SELL_STRADDLE" and iv_spread >= 1.5:
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
        _dte        = vol.get("dte", dte)
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
            f"Profit if NIFTY closes outside {bl:.0f}–{bu:.0f} at expiry ({_dte} DTE)."
        )
        legs = [
            _leg("BUY", 1, call_sym, "MARKET", "CE"),
            _leg("BUY", 1, put_sym,  "MARKET", "PE"),
        ]

    # ── 5. Iron Condor (Mon/Tue, VIX 14–22, neutral PCR) ────────────────
    # Primary daily income strategy — defined risk, defined reward
    elif (
        iron_condor_plan is not None
        and day_of_week in (0, 1)           # Monday or Tuesday only
        and 14.0 <= vix_val <= 22.0
        and 0.75 <= pcr <= 1.35
        and dte >= 3
        and date.today().isoformat() not in event_dates
    ):
        strategy    = Strategy.IRON_CONDOR
        emoji       = "🎯"
        ic          = iron_condor_plan
        capital_est = float(ic.get("capital_needed", 0))
        sc_k        = ic.get("short_call", 0)
        sp_k        = ic.get("short_put",  0)
        lc_k        = ic.get("long_call",  0)
        lp_k        = ic.get("long_put",   0)
        net_prem    = ic.get("net_premium", 0)
        max_p       = ic.get("max_profit_inr", 0)
        max_l       = ic.get("max_loss_inr",   0)
        bu          = ic.get("breakeven_upper", 0)
        bl          = ic.get("breakeven_lower", 0)
        reason = (
            f"{day_label}: Selling Iron Condor — defined-risk income strategy. "
            f"VIX={vix_val:.1f} (ideal range 14–22). PCR={pcr:.2f} (neutral). "
            f"Collect ≈{net_prem:.1f} pts net premium. "
            f"Breakevens: {bl:.0f} ↔ {bu:.0f}. "
            f"Target: close at 50% profit (≈₹{_fmt_inr(float(ic.get('target_inr',0)))}). "
            f"FII flow: {fii_signal}. Max pain: {max_pain or 'N/A'}."
        )
        risk_note = (
            f"Max profit ₹{_fmt_inr(float(max_p))} | Max loss ₹{_fmt_inr(float(max_l))} | "
            f"Margin ≈₹{_fmt_inr(capital_est)}. "
            f"Hard exit 1:30 PM. SL if position doubles against you."
        )
        legs = ic.get("legs", [])

    # ── 6. FII bearish → BEAR_CALL_SPREAD ────────────────────────────────
    # Fires on FII futures short (gold standard) OR cash bearish + PCR bearish
    elif (
        (fii_fut_signal == "FII_FUT_SHORT" or
         (fii_signal == "FII_BEARISH" and pcr_signal == "PCR_BEARISH"))
        and vix_val < 22
        and safety.get("safe", False)
    ):
        strategy    = Strategy.BEAR_CALL_SPREAD
        emoji       = "📉"
        call_sym    = vol.get("call_symbol", "ATM CE")
        capital_est = lot_size * 200
        _fii_note   = (
            f"FII net SHORT index futures ({fii_fut_signal})"
            if fii_fut_signal == "FII_FUT_SHORT"
            else f"FII cash sell + PCR={pcr:.2f} (bearish)"
        )
        reason = (
            f"Bearish institutional signal: {_fii_note}. "
            f"Sell ATM Call + Buy OTM+200 Call — collect premium on expected weakness."
        )
        risk_note = (
            f"Defined-risk spread. Max profit = net premium × {lot_size}. "
            f"Max loss = (200 − premium) × {lot_size}. "
            f"Exit if market rallies above short strike."
        )
        legs = [
            _leg("SELL", 1, call_sym, "MARKET", "CE"),
            _leg("BUY",  1, f"{call_sym}+200", "MARKET", "CE"),
        ]

    # ── 7. FII bullish → BULL_PUT_SPREAD ─────────────────────────────────
    # Fires on FII futures long (gold standard) OR cash bullish + PCR bullish
    elif (
        (fii_fut_signal == "FII_FUT_LONG" or
         (fii_signal == "FII_BULLISH" and pcr_signal == "PCR_BULLISH"))
        and vix_val < 22
        and safety.get("safe", False)
    ):
        strategy    = Strategy.BULL_PUT_SPREAD
        emoji       = "📈"
        put_sym     = vol.get("put_symbol", "ATM PE")
        capital_est = lot_size * 200
        _fii_note   = (
            f"FII net LONG index futures ({fii_fut_signal})"
            if fii_fut_signal == "FII_FUT_LONG"
            else f"FII cash buy + PCR={pcr:.2f} (bullish)"
        )
        reason = (
            f"Bullish institutional signal: {_fii_note}. "
            f"Sell ATM Put + Buy OTM-200 Put — collect premium on expected strength."
        )
        risk_note = (
            f"Defined-risk spread. Max profit = net premium × {lot_size}. "
            f"Max loss = (200 − premium) × {lot_size}. "
            f"Exit if market drops below short strike."
        )
        legs = [
            _leg("SELL", 1, put_sym, "MARKET", "PE"),
            _leg("BUY",  1, f"{put_sym}-200", "MARKET", "PE"),
        ]

    # ── 7.5. Momentum (VWAP + EMA20 + RSI triple confirmation) ──────────
    # Only when VIX < 22 and not HALT and not expiry day.
    # Alert + DynamoDB only — user places the CE/PE manually.
    elif (
        momentum is not None
        and momentum.get("signal") in ("MOMENTUM_LONG", "MOMENTUM_SHORT")
        and momentum.get("confidence") in ("HIGH", "MEDIUM")
        and safety.get("safe", False)
        and day_of_week != 3          # not expiry day
        and vix_val < 22.0
    ):
        strategy, legs, capital_est, reason, risk_note, emoji = \
            _build_momentum_plan(momentum, vol, lot_size)

    # ── 8. Trend regime → BULL_CALL_SPREAD or BEAR_PUT_SPREAD ───────────
    # Use when regime is TREND, conditions are safe, not Thu/Fri, DTE ≥ 2.
    # Direction determined by FII futures signal (gold standard) + spot slope.
    elif (
        trading_rg == "TREND"
        and safety.get("safe", False)
        and day_of_week not in (3, 4)   # not Thu/Fri
        and dte >= 2
    ):
        spot_slope = regime.get("spot_slope_pct", 0)
        call_sym   = vol.get("call_symbol", "ATM CE")
        put_sym    = vol.get("put_symbol",  "ATM PE")
        is_bullish = (
            fii_fut_signal == "FII_FUT_LONG"
            or (spot_slope > 0.3 and fii_signal == "FII_BULLISH")
        )
        if is_bullish:
            strategy    = Strategy.BULL_CALL_SPREAD
            emoji       = "🚀"
            capital_est = lot_size * 200
            reason = (
                f"TREND regime detected (spot slope={spot_slope:+.2f}%). "
                f"Bullish bias: {fii_fut_signal} / {fii_signal}. "
                f"Buy ATM Call + Sell OTM+200 Call — defined-risk long on expected rally."
            )
            risk_note = (
                f"Max profit = (200 − net premium) × {lot_size}. "
                f"Max loss = net premium paid × {lot_size}. "
                f"Exit if trend reverses or DTE ≤ 1."
            )
            legs = [
                _leg("BUY",  1, call_sym,         "MARKET", "CE"),
                _leg("SELL", 1, f"{call_sym}+200", "MARKET", "CE"),
            ]
        else:
            strategy    = Strategy.BEAR_PUT_SPREAD
            emoji       = "📉"
            capital_est = lot_size * 200
            reason = (
                f"TREND regime detected (spot slope={spot_slope:+.2f}%). "
                f"Bearish bias: {fii_fut_signal} / {fii_signal}. "
                f"Buy ATM Put + Sell OTM-200 Put — defined-risk short on expected decline."
            )
            risk_note = (
                f"Max profit = (200 − net premium) × {lot_size}. "
                f"Max loss = net premium paid × {lot_size}. "
                f"Exit if trend reverses or DTE ≤ 1."
            )
            legs = [
                _leg("BUY",  1, put_sym,         "MARKET", "PE"),
                _leg("SELL", 1, f"{put_sym}-200", "MARKET", "PE"),
            ]

    # ── 8. Synthetic arbitrage ───────────────────────────────────────────
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

    # ── Momentum context fields (for display even when not the chosen strategy)
    _mom = momentum or {}

    # Build partial plan dict for win scorer (needs strategy + strength)
    _partial = {"strategy": strategy, "strength": round(strength, 2)}
    win_prob = score_win_probability(
        plan           = _partial,
        regime         = regime,
        vol            = vol,
        momentum       = momentum,
        fii_fut_signal = fii_fut_signal,
        pcr_signal     = pcr_signal,
        vix            = vix if vix > 0 else vix_val,
        day_of_week    = day_of_week,
        safety         = safety,
    )

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
        # ── Win probability ────────────────────────────────────────────
        "win_probability":     win_prob,
        "win_probability_pct": f"{win_prob * 100:.0f}%",
        # ── New context fields ─────────────────────────────────────────
        "fii_signal":      fii_signal,
        "pcr":             round(pcr, 3),
        "pcr_signal":      pcr_signal,
        "max_pain":        max_pain,
        "day_of_week":     day_of_week,
        "day_label":       day_label,
        "dte":             dte,
        "date":            date.today().isoformat(),
        # ── Momentum Engine context ────────────────────────────────────
        "momentum_signal":     _mom.get("signal", "NEUTRAL"),
        "momentum_rsi":        round(float(_mom.get("rsi",   50)), 1),
        "momentum_ema20":      round(float(_mom.get("ema20",  0)), 1),
        "momentum_vwap":       round(float(_mom.get("vwap",   0)), 1),
        "momentum_confidence": _mom.get("confidence", "LOW"),
        "momentum_reason":     _mom.get("reason", ""),
    }


# ─── Helpers ────────────────────────────────────────────────────────────────

def _build_momentum_plan(momentum: dict, vol: dict, lot_size: int):
    """Return (strategy, legs, capital_est, reason, risk_note, emoji) for momentum trade."""
    sig        = momentum["signal"]
    conf       = momentum.get("confidence", "MEDIUM")
    rsi        = momentum.get("rsi", 50)
    ema20      = momentum.get("ema20", 0)
    vwap       = momentum.get("vwap", 0)
    mom_reason = momentum.get("reason", "")

    is_long   = sig == "MOMENTUM_LONG"
    strategy  = Strategy.MOMENTUM_LONG if is_long else Strategy.MOMENTUM_SHORT
    emoji     = "🚀" if is_long else "🔻"
    direction = "CE" if is_long else "PE"
    opt_sym   = vol.get("call_symbol" if is_long else "put_symbol", f"ATM {direction}")

    conf_label = "Strong" if conf == "HIGH" else "Moderate"
    reason = (
        f"{conf_label} momentum signal: {mom_reason}. "
        f"RSI(14)={rsi:.1f}, EMA20={ema20:.0f}, VWAP={vwap:.0f}. "
        f"Buy ATM {direction} to ride the intraday directional move. "
        f"Alert only — place manually. Hard exit by 1:30 PM."
    )
    risk_note = (
        f"Max loss = premium paid. "
        f"SL: 25% of entry premium. Target: 40% of entry premium. "
        f"Use 50% of normal lot size (higher gamma risk). "
        f"Do NOT hold overnight."
    )
    # Capital estimate: ~150 pts × lot_size as rough premium placeholder
    capital_est = float(lot_size) * 150.0
    legs = [_leg("BUY", 1, opt_sym, "MARKET", direction)]
    return strategy, legs, capital_est, reason, risk_note, emoji


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
