"""
Win Probability Scorer
======================
Estimates the historical win probability for the selected strategy based on
the confluence of signals that triggered it.

Base probabilities are heuristic-derived from options strategy theory.
After running the backtester (backtester/run.py), update the BASE_PROB
table with actual measured win rates from your historical data.

Usage:
    from win_scorer import score_win_probability
    prob = score_win_probability(daily_plan, regime, vol, momentum,
                                  fii_fut_signal, pcr_signal, vix,
                                  day_of_week, safety)
    # Returns float 0.0 – 0.95
"""

from __future__ import annotations

# ── Base win probabilities per strategy ──────────────────────────────────────
# Source: options theory + NSE historical patterns (update after backtesting)
_BASE_PROB: dict[str, float] = {
    "IRON_CONDOR":      0.65,   # defined-risk, profits from time decay in calm market
    "SELL_STRADDLE":    0.60,   # profits from IV reversion — wider risk
    "SELL_STRANGLE":    0.65,   # wider breakevens than straddle, safer
    "BULL_PUT_SPREAD":  0.62,   # credit spread, bullish — limited risk
    "BEAR_CALL_SPREAD": 0.62,   # credit spread, bearish — limited risk
    "BULL_CALL_SPREAD": 0.55,   # debit spread, needs directional move
    "BEAR_PUT_SPREAD":  0.55,   # debit spread, needs directional move
    "BUY_STRADDLE":     0.50,   # needs big move in either direction
    "BUY_STRANGLE":     0.45,   # cheaper but needs even bigger move
    "MOMENTUM_LONG":    0.58,   # intraday CE buy — directional, short window
    "MOMENTUM_SHORT":   0.58,   # intraday PE buy — directional, short window
    "TRIPLE_CALENDAR":  0.55,   # calendar spread — time decay convergence
    "ARB_SYNTHETIC":    0.70,   # put-call parity arb — near risk-free when real
    "WAIT":             0.00,   # no trade — not scored
}


def score_win_probability(
    plan:           dict,
    regime:         dict,
    vol:            dict,
    momentum:       dict | None,
    fii_fut_signal: str,
    pcr_signal:     str,
    vix:            float,
    day_of_week:    int,
    safety:         dict,
    fii_composite:  dict | None = None,   # from get_fii_composite_signal()
) -> float:
    """
    Compute estimated win probability for the selected strategy.

    Parameters mirror what build_daily_plan() already has computed.
    Returns a float in [0.0, 0.95]. Returns 0.0 for WAIT.
    """
    strategy   = plan.get("strategy", "WAIT")
    base       = _BASE_PROB.get(strategy, 0.50)

    if strategy == "WAIT":
        return 0.0

    bonus = 0.0

    iv_vix_spread = vol.get("iv_vix_spread", 0.0)
    strength      = float(plan.get("strength", 0.0))
    size_factor   = float(safety.get("size_factor", 1.0))
    trading_rg    = regime.get("regime", "SPREAD")

    # ── 1. FII futures + PCR alignment (gold standard — both agree) ──────
    if strategy in ("BULL_PUT_SPREAD", "BULL_CALL_SPREAD", "MOMENTUM_LONG"):
        if fii_fut_signal == "FII_FUT_LONG" and pcr_signal in ("PCR_BULLISH", "PCR_NEUTRAL"):
            bonus += 0.08
        elif fii_fut_signal == "FII_FUT_LONG" or pcr_signal == "PCR_BULLISH":
            bonus += 0.04
    elif strategy in ("BEAR_CALL_SPREAD", "BEAR_PUT_SPREAD", "MOMENTUM_SHORT"):
        if fii_fut_signal == "FII_FUT_SHORT" and pcr_signal in ("PCR_BEARISH", "PCR_NEUTRAL"):
            bonus += 0.08
        elif fii_fut_signal == "FII_FUT_SHORT" or pcr_signal == "PCR_BEARISH":
            bonus += 0.04
    elif strategy == "IRON_CONDOR":
        # IC wants neutral — PCR neutral + no strong FII directional bet
        if pcr_signal == "PCR_NEUTRAL" and fii_fut_signal == "FII_FUT_NEUTRAL":
            bonus += 0.08
        elif pcr_signal == "PCR_NEUTRAL":
            bonus += 0.04
    elif strategy in ("SELL_STRADDLE", "SELL_STRANGLE"):
        # Sell strategies want low FII directional conviction
        if fii_fut_signal == "FII_FUT_NEUTRAL":
            bonus += 0.05

    # ── 2. VIX regime suitability ─────────────────────────────────────────
    if strategy in ("IRON_CONDOR", "SELL_STRADDLE", "SELL_STRANGLE",
                    "BULL_PUT_SPREAD", "BEAR_CALL_SPREAD"):
        # Sell strategies love low VIX (fast IV decay)
        if vix < 15.0:
            bonus += 0.07
        elif vix < 18.0:
            bonus += 0.05
        elif 18.0 <= vix <= 22.0:
            bonus += 0.02
        # VIX > 22 hurts sell strategies — no bonus (potential penalty implicit in base)
    elif strategy in ("BUY_STRADDLE", "BUY_STRANGLE"):
        # Buy strategies love high VIX (bigger expected moves)
        if vix >= 25.0:
            bonus += 0.07
        elif vix >= 20.0:
            bonus += 0.04
    elif strategy in ("MOMENTUM_LONG", "MOMENTUM_SHORT",
                      "BULL_CALL_SPREAD", "BEAR_PUT_SPREAD"):
        # Trend / momentum strategies like moderate VIX
        if 14.0 <= vix <= 20.0:
            bonus += 0.05

    # ── 3. IV–VIX spread alignment ────────────────────────────────────────
    if strategy in ("SELL_STRADDLE", "SELL_STRANGLE"):
        if iv_vix_spread >= 4.0:
            bonus += 0.06   # options very overpriced — great for sellers
        elif iv_vix_spread >= 2.0:
            bonus += 0.04
    elif strategy in ("BUY_STRADDLE", "BUY_STRANGLE"):
        if iv_vix_spread <= -3.0:
            bonus += 0.06   # options very cheap — great for buyers
        elif iv_vix_spread <= -1.5:
            bonus += 0.03

    # ── 4. Momentum confirmation ──────────────────────────────────────────
    mom = momentum or {}
    mom_sig  = mom.get("signal",     "NEUTRAL")
    mom_conf = mom.get("confidence", "LOW")

    if strategy in ("MOMENTUM_LONG", "BULL_PUT_SPREAD", "BULL_CALL_SPREAD"):
        if mom_sig == "MOMENTUM_LONG":
            bonus += 0.08 if mom_conf == "HIGH" else 0.04
    elif strategy in ("MOMENTUM_SHORT", "BEAR_CALL_SPREAD", "BEAR_PUT_SPREAD"):
        if mom_sig == "MOMENTUM_SHORT":
            bonus += 0.08 if mom_conf == "HIGH" else 0.04

    # ── 5. Day-of-week optimality ─────────────────────────────────────────
    if strategy == "IRON_CONDOR" and day_of_week in (0, 1):
        # Monday or Tuesday — most DTE remaining, best for IC
        bonus += 0.05
    elif strategy in ("SELL_STRADDLE", "SELL_STRANGLE") and day_of_week == 0:
        # Monday sell — maximum time for decay before expiry
        bonus += 0.04
    elif strategy in ("MOMENTUM_LONG", "MOMENTUM_SHORT") and day_of_week in (1, 2):
        # Tue/Wed tend to have cleaner intraday trends
        bonus += 0.03

    # ── 6. Signal strength (for spread / calendar trades) ────────────────
    if strategy == "TRIPLE_CALENDAR" and strength >= 3.5:
        bonus += 0.05
    elif strategy == "TRIPLE_CALENDAR" and strength >= 2.5:
        bonus += 0.02

    # ── 7. Regime alignment ───────────────────────────────────────────────
    if strategy in ("SELL_STRADDLE", "IRON_CONDOR") and trading_rg == "SPREAD":
        bonus += 0.04   # calm regime supports premium collection
    elif strategy in ("BUY_STRADDLE", "BUY_STRANGLE") and trading_rg == "VOLATILITY":
        bonus += 0.04
    elif strategy in ("MOMENTUM_LONG", "MOMENTUM_SHORT",
                      "BULL_CALL_SPREAD", "BEAR_PUT_SPREAD") and trading_rg == "TREND":
        bonus += 0.04

    # ── 8. Full-size signal (no CAUTION / HALT size reduction) ───────────
    if size_factor == 1.0:
        bonus += 0.03

    # ── 9. FII 3-5 day trend confirmation (gold standard boost) ──────────
    # If the composite multi-day FII signal agrees with the selected strategy,
    # apply a meaningful bonus — this is the most reliable external signal.
    fii_comp = fii_composite or {}
    fii_comp_dir  = fii_comp.get("direction", "NEUTRAL")
    fii_comp_conf = fii_comp.get("confidence", "LOW")
    fii_consec    = int(fii_comp.get("consecutive_days", 0))

    if strategy in ("BULL_PUT_SPREAD", "BULL_CALL_SPREAD", "MOMENTUM_LONG"):
        if fii_comp_dir == "BULLISH" and fii_comp_conf == "HIGH":
            bonus += 0.08   # 3+ consecutive bullish days — very strong
        elif fii_comp_dir == "BULLISH" and fii_comp_conf == "MEDIUM":
            bonus += 0.05
        elif fii_comp_dir == "BULLISH":
            bonus += 0.02
    elif strategy in ("BEAR_CALL_SPREAD", "BEAR_PUT_SPREAD", "MOMENTUM_SHORT"):
        if fii_comp_dir == "BEARISH" and fii_comp_conf == "HIGH":
            bonus += 0.08
        elif fii_comp_dir == "BEARISH" and fii_comp_conf == "MEDIUM":
            bonus += 0.05
        elif fii_comp_dir == "BEARISH":
            bonus += 0.02
    elif strategy == "IRON_CONDOR":
        # IC benefits from neutral FII (no directional conviction)
        if fii_comp_dir == "NEUTRAL":
            bonus += 0.05
    # Consecutive streak bonus (3+ days same direction = market in clear trend)
    if fii_consec >= 3 and strategy != "WAIT":
        if (fii_comp_dir == "BULLISH" and strategy in ("BULL_PUT_SPREAD", "BULL_CALL_SPREAD", "MOMENTUM_LONG")) or \
           (fii_comp_dir == "BEARISH" and strategy in ("BEAR_CALL_SPREAD", "BEAR_PUT_SPREAD", "MOMENTUM_SHORT")):
            bonus += 0.04   # extra for long streak of institutional conviction

    # TRUE_EXIT / TRUE_ACCUMULATION divergence bonus
    divergence = fii_comp.get("divergence", "NONE")
    if divergence == "TRUE_ACCUMULATION" and strategy in ("BULL_PUT_SPREAD", "BULL_CALL_SPREAD"):
        bonus += 0.04
    elif divergence == "TRUE_EXIT" and strategy in ("BEAR_CALL_SPREAD", "BEAR_PUT_SPREAD"):
        bonus += 0.04

    # ── Cap and return ────────────────────────────────────────────────────
    return round(min(base + bonus, 0.95), 4)
