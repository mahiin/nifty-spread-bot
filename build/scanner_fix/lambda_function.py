"""
NIFTY Triple Calendar Spread – Scanner Lambda
=============================================
Triggered every 30 s by EventBridge during market hours.

What it does:
  1.  Auto-detects near / next / far NIFTY futures
  2.  Fetches live quotes + OI + volume
  3.  Calculates spreads, curve_diff, Z-score
  4.  Detects OI divergence, volume imbalance, expiry compression
  5.  Checks synthetic futures arbitrage (put-call parity)
  6.  Combines signals → strength score → recommended qty
  7.  Computes stoploss and target levels
  8.  Regime Guard: checks VIX, gap, events, curve distortion
  9.  Regime Detector: classifies market as SPREAD / VOLATILITY / TREND
  10. Volatility Engine: computes IV vs VIX divergence, straddle/strangle signals
  11. Strategy Router: picks today's best strategy & builds clear action plan
  12. Stores result + daily plan in DynamoDB
  13. Sends Telegram alert and (optionally) triggers executor via SNS

Environment variables (set in Lambda console):
  ZERODHA_API_KEY        – Kite API key (if broker=zerodha)
  ZERODHA_ACCESS_TOKEN   – Generated daily session token
  ANGEL_API_KEY          – Angel One API key (if broker=angel)
  ANGEL_CLIENT_ID        – Angel One client ID
  ANGEL_PASSWORD         – Angel One password
  ANGEL_TOTP_SECRET      – Angel One TOTP secret
  BROKER                 – zerodha | angel  (default zerodha)
  UNDERLYING             – NIFTY (default)
  LOT_SIZE               – 75 (default)
  ZSCORE_THRESHOLD       – 2.0 (default)
  ARBITRAGE_THRESHOLD    – 15 (default)
  LOOKBACK_WINDOW        – 50 (default)
  TRADING_CAPITAL        – 500000 (default)
  TELEGRAM_BOT_TOKEN     – Your bot token
  TELEGRAM_CHAT_ID       – Your chat / group id
  SNS_EXECUTE_TOPIC_ARN  – (optional) to enable auto-execute
  MODE                   – PAPER | LIVE  (default PAPER)
"""

import json
import os
from datetime import datetime

import boto3
import pytz

from broker_client       import get_broker
from spread_engine       import (
    get_active_futures, compute_spreads, compute_zscore,
    check_oi_divergence, check_volume_imbalance,
    days_to_expiry, expiry_bias,
    signal_strength, recommended_qty, compute_levels,
    is_market_open, is_nse_holiday, past_entry_cutoff,
)
from arbitrage_engine    import check_parity
from alerter             import (send_telegram, send_regime_alert, publish_to_sns,
                                  send_exit_alert, send_error_alert)
from position_monitor    import check_and_exit_positions
from regime_guard        import check_trade_safety, get_india_vix
from regime_detector     import detect_regime
from volatility_engine   import compute_volatility_signal
from strategy_router     import build_daily_plan

# ─── AWS ───────────────────────────────────────────────────────────────────
REGION   = os.environ.get("AWS_REGION_NAME", "ap-south-1")
TABLE    = os.environ.get("DYNAMODB_SIGNALS_TABLE", "nifty_spread_signals")
CFG_TBL  = os.environ.get("CONFIG_TABLE", "nifty_config")
ddb      = boto3.resource("dynamodb", region_name=REGION)
sig_tbl  = ddb.Table(TABLE)
cfg_tbl  = ddb.Table(CFG_TBL)
IST      = pytz.timezone("Asia/Kolkata")

UNDERLYING = os.environ.get("UNDERLYING", "NIFTY")


def _load_config() -> dict:
    """Load runtime config: DynamoDB overrides merged over env-var defaults."""
    defaults = {
        "ZSCORE_THRESHOLD":    os.environ.get("ZSCORE_THRESHOLD",    "2.0"),
        "ZSCORE_EXIT":         os.environ.get("ZSCORE_EXIT",         "0.5"),
        "LOOKBACK_WINDOW":     os.environ.get("LOOKBACK_WINDOW",     "50"),
        "TRADING_CAPITAL":     os.environ.get("TRADING_CAPITAL",     "500000"),
        "LOT_SIZE":            os.environ.get("LOT_SIZE",            "75"),
        "ARBITRAGE_THRESHOLD": os.environ.get("ARBITRAGE_THRESHOLD", "15"),
        "SNS_EXECUTE_ENABLED": os.environ.get("SNS_EXECUTE_ENABLED", "false"),
        "MODE":                os.environ.get("MODE",                "PAPER"),
        "MIN_SIGNAL_STRENGTH": os.environ.get("MIN_SIGNAL_STRENGTH", "2.5"),
        "EVENT_DATES":         os.environ.get("EVENT_DATES",         ""),
        "NSE_HOLIDAYS":        os.environ.get("NSE_HOLIDAYS",        ""),
    }
    try:
        resp = cfg_tbl.scan(ProjectionExpression="config_key, config_value")
        for item in resp.get("Items", []):
            defaults[item["config_key"]] = item["config_value"]
    except Exception:
        pass
    return defaults


def _fetch_scan_history(lookback: int) -> tuple[list, list]:
    """
    Pull last `lookback` records from DynamoDB.
    Returns (curve_diff_history, spot_price_history).
    """
    try:
        resp = sig_tbl.scan(
            Limit=lookback,
            ProjectionExpression="curve_diff, spot_price",
        )
        items = resp.get("Items", [])
        curve_hist = [float(i["curve_diff"]) for i in items if "curve_diff" in i]
        spot_hist  = [float(i["spot_price"])  for i in items if "spot_price"  in i]
        return curve_hist, spot_hist
    except Exception:
        return [], []


def _store_daily_plan(plan: dict) -> None:
    """Persist latest daily plan as a JSON string in DynamoDB config table."""
    try:
        cfg_tbl.put_item(Item={
            "config_key":   "DAILY_PLAN",
            "config_value": json.dumps(plan),
        })
    except Exception:
        pass


def lambda_handler(event, context):
    # Load config first so we can check holidays before doing anything else
    cfg = _load_config()

    # Parse NSE holiday list from config (comma-separated YYYY-MM-DD)
    nse_holidays_raw = cfg.get("NSE_HOLIDAYS", "")
    nse_holidays = [d.strip() for d in nse_holidays_raw.split(",") if d.strip()] or None

    if is_nse_holiday(nse_holidays):
        today = datetime.now(IST).strftime("%Y-%m-%d (%A)")
        return {"statusCode": 200, "body": f"NSE holiday / weekend — {today}"}

    if not is_market_open(nse_holidays):
        return {"statusCode": 200, "body": "Market closed"}

    if past_entry_cutoff("14:30"):
        return {"statusCode": 200, "body": "Past entry cutoff"}

    # cfg already loaded above for holiday check — use it directly
    ZSCORE_THRESH = float(cfg["ZSCORE_THRESHOLD"])
    LOOKBACK      = int(cfg["LOOKBACK_WINDOW"])
    CAPITAL       = float(cfg["TRADING_CAPITAL"])
    LOT_SIZE      = int(cfg["LOT_SIZE"])
    MIN_STRENGTH  = float(cfg["MIN_SIGNAL_STRENGTH"])

    try:
        broker  = get_broker()
        futures = get_active_futures(broker, UNDERLYING)

        # ── Live quotes (pass token IDs; response keyed by symbol) ─────────
        instruments = [
            f"NFO:{futures['near_token']}",
            f"NFO:{futures['next_token']}",
            f"NFO:{futures['far_token']}",
            "NSE:26000",   # NIFTY 50 spot token
        ]
        quotes = broker.get_quote(instruments)

        near_price  = float(quotes[f"NFO:{futures['near']}"]["ltp"])
        next_price  = float(quotes[f"NFO:{futures['next']}"]["ltp"])
        far_price   = float(quotes[f"NFO:{futures['far']}"]["ltp"])
        spot_ohlc   = quotes.get("NSE:NIFTY", {})
        spot_price  = float((spot_ohlc or {}).get("ltp") or near_price)

        intraday_high = float((spot_ohlc or {}).get("high") or spot_price)
        intraday_low  = float((spot_ohlc or {}).get("low")  or spot_price)
        prev_close    = float((spot_ohlc or {}).get("close") or 0)

        # ── India VIX ─────────────────────────────────────────────────────
        vix = get_india_vix(broker)

        # ── Event calendar ────────────────────────────────────────────────
        event_dates_raw = cfg.get("EVENT_DATES", "")
        event_dates = [d.strip() for d in event_dates_raw.split(",") if d.strip()]

        # ── Spreads & DTE ─────────────────────────────────────────────────
        spreads = compute_spreads(near_price, next_price, far_price)
        s1, s2  = spreads["spread1"], spreads["spread2"]
        cdiff   = spreads["curve_diff"]
        dte     = days_to_expiry(futures["near_expiry"])
        exp_b   = expiry_bias(dte)

        # ── Regime Guard (safety check) ───────────────────────────────────
        safety = check_trade_safety(
            vix         = vix or 0.0,
            spot_price  = spot_price,
            prev_close  = prev_close,
            curve_diff  = cdiff,
            dte         = dte,
            event_dates = event_dates,
        )

        # ── Z-score & OI / volume signals ─────────────────────────────────
        curve_hist, spot_hist = _fetch_scan_history(LOOKBACK)
        curve_hist.append(cdiff)
        zscore   = compute_zscore(cdiff, curve_hist)

        oi_data  = check_oi_divergence(quotes, futures["near"], futures["next"])
        vol_data = check_volume_imbalance(quotes, futures["near"], futures["next"])

        # ── Primary spread signal (suppressed if regime = HALT) ───────────
        spread_signal = "NONE"
        if safety["safe"]:
            if zscore > ZSCORE_THRESH:
                spread_signal = "SELL_BUTTERFLY"
            elif zscore < -ZSCORE_THRESH:
                spread_signal = "BUY_BUTTERFLY"

        # ── Signal strength & sizing ──────────────────────────────────────
        strength = signal_strength(
            zscore,
            oi_data["rollover_signal"],
            vol_data["volume_signal"],
            exp_b,
        )
        base_qty = recommended_qty(strength, CAPITAL, LOT_SIZE) if spread_signal != "NONE" else 0
        qty      = int(base_qty * safety["size_factor"])

        levels = (
            compute_levels(spread_signal, cdiff)
            if spread_signal != "NONE"
            else {"stoploss_diff": 0, "target_diff": 0}
        )

        # ── Synthetic arbitrage (suppressed on HALT) ──────────────────────
        if safety["safe"]:
            arb = check_parity(broker, futures, spot_price, quotes)
        else:
            arb = {
                "arbitrage_signal": "NONE", "arb_mispricing": 0,
                "arb_stoploss": 0, "arb_target": 0,
                "call_symbol": "", "put_symbol": "",
            }

        # ── Position Monitor (exit logic runs every scan) ─────────────────
        # Must run before building the record so exit events can be alerted
        exit_events = check_and_exit_positions(
            broker             = broker if safety["safe"] else None,
            current_curve_diff = cdiff,
            current_dte        = dte,
        )
        if exit_events:
            send_exit_alert(exit_events)

        # ── Market Regime Detector ────────────────────────────────────────
        regime_data = detect_regime(
            vix           = vix or 0.0,
            spot_price    = spot_price,
            intraday_high = intraday_high,
            intraday_low  = intraday_low,
            spot_history  = spot_hist,
        )

        # ── Volatility Engine (IV vs VIX) ─────────────────────────────────
        # ATM call/put prices come from the arbitrage engine's already-fetched quotes
        atm_strike    = arb.get("atm_strike", round(spot_price / 50) * 50)
        atm_call_price = float(arb.get("call_price", 0))
        atm_put_price  = float(arb.get("put_price",  0))
        call_sym_atm   = arb.get("call_symbol", "")
        put_sym_atm    = arb.get("put_symbol",  "")

        vol_data_iv = compute_volatility_signal(
            spot_price  = spot_price,
            call_price  = atm_call_price,
            put_price   = atm_put_price,
            strike      = int(atm_strike),
            dte         = dte,
            vix         = vix or 0.0,
            call_symbol = call_sym_atm,
            put_symbol  = put_sym_atm,
            lot_size    = LOT_SIZE,
        )

        # ── Strategy Router – Daily Plan ──────────────────────────────────
        daily_plan = build_daily_plan(
            safety        = safety,
            regime        = regime_data,
            vol           = vol_data_iv,
            arb           = arb,
            spread_signal = spread_signal,
            strength      = strength,
            futures       = futures,
            qty           = qty,
            lot_size      = LOT_SIZE,
            min_strength  = MIN_STRENGTH,
        )
        _store_daily_plan(daily_plan)

        # ── Build DynamoDB record ─────────────────────────────────────────
        ts = datetime.now(IST).isoformat()
        record = {
            "timestamp":        ts,
            "pk":               "SIGNAL",     # GSI partition key for range queries
            "near_symbol":      futures["near"],
            "next_symbol":      futures["next"],
            "far_symbol":       futures["far"],
            "near_price":       str(near_price),
            "next_price":       str(next_price),
            "far_price":        str(far_price),
            "spot_price":       str(spot_price),
            "spread1":          str(round(s1,    2)),
            "spread2":          str(round(s2,    2)),
            "curve_diff":       str(round(cdiff, 2)),
            "zscore":           str(round(zscore, 4)),
            "spread_signal":    spread_signal,
            "signal_strength":  str(strength),
            "recommended_qty":  str(qty),
            "stoploss_diff":    str(levels["stoploss_diff"]),
            "target_diff":      str(levels["target_diff"]),
            "days_to_expiry":   str(dte),
            "expiry_bias":      exp_b,
            "near_oi":          str(oi_data["near_oi"]),
            "next_oi":          str(oi_data["next_oi"]),
            "oi_diff":          str(oi_data["oi_diff"]),
            "rollover_signal":  str(oi_data["rollover_signal"]),
            "vol_ratio":        str(vol_data["vol_ratio"]),
            "volume_signal":    str(vol_data["volume_signal"]),
            # ── Arbitrage ─────────────────────────────────────────────────
            "arbitrage_signal": arb.get("arbitrage_signal", "NONE"),
            "arb_mispricing":   str(arb.get("arb_mispricing", 0)),
            "arb_stoploss":     str(arb.get("arb_stoploss", 0)),
            "arb_target":       str(arb.get("arb_target", 0)),
            "call_symbol":      arb.get("call_symbol", ""),
            "put_symbol":       arb.get("put_symbol", ""),
            # ── Regime Guard ──────────────────────────────────────────────
            "india_vix":        str(round(vix, 2)) if vix else "0",
            "vix_level":        safety["vix_level"],
            "regime":           safety["regime"],
            "optimal_window":   str(safety["optimal_window"]),
            "block_reasons":    " | ".join(safety["reasons"]),
            "regime_warnings":  " | ".join(safety["warnings"]),
            # ── Regime Detector ───────────────────────────────────────────
            "trading_regime":   regime_data["regime"],
            "intraday_range_pct": str(regime_data["intraday_range_pct"]),
            "spot_slope_pct":   str(regime_data["spot_slope_pct"]),
            # ── Volatility Engine ─────────────────────────────────────────
            "call_iv_pct":      str(vol_data_iv["call_iv_pct"]),
            "put_iv_pct":       str(vol_data_iv["put_iv_pct"]),
            "avg_iv_pct":       str(vol_data_iv["avg_iv_pct"]),
            "iv_vix_spread":    str(vol_data_iv["iv_vix_spread"]),
            "vol_signal":       vol_data_iv["vol_signal"],
            "straddle_premium": str(vol_data_iv["straddle_premium"]),
            "breakeven_upper":  str(vol_data_iv["breakeven_upper"]),
            "breakeven_lower":  str(vol_data_iv["breakeven_lower"]),
            # ── Daily Plan (summary) ──────────────────────────────────────
            "daily_strategy":   daily_plan["strategy"],
            "daily_plan_legs":  daily_plan["legs_text"],
        }

        # ── Persist signal ────────────────────────────────────────────────
        sig_tbl.put_item(Item=record)

        # ── Alert & auto-execute ──────────────────────────────────────────
        if not safety["safe"]:
            send_regime_alert(record, safety)
            return {"statusCode": 200, "body": json.dumps(record)}

        has_signal = (
            spread_signal != "NONE" and strength >= MIN_STRENGTH
        ) or arb.get("arbitrage_signal", "NONE") not in ("NONE", "")

        if has_signal:
            send_telegram(record)
            if cfg.get("SNS_EXECUTE_ENABLED", "false").lower() == "true":
                publish_to_sns(record)

        return {"statusCode": 200, "body": json.dumps(record)}

    except Exception as exc:
        import traceback
        err = traceback.format_exc()
        print(f"ERROR: {exc}\n{err}")
        send_error_alert(f"{exc}\n\n{err[:600]}", context="Scanner")
        return {"statusCode": 500, "body": str(exc)}
