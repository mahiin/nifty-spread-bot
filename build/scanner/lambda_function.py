"""
NIFTY Triple Calendar Spread – Scanner Lambda
=============================================
Triggered every 15 min by EventBridge (9:45–11:00 AM IST strategy window, then monitors only).

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
                                  send_exit_alert, send_error_alert,
                                  send_intraday_alert, send_intraday_execution_alert,
                                  send_options_exit_alert, send_premarket_report,
                                  send_margin_alert, send_delta_alert)
from position_monitor    import check_and_exit_positions, check_and_exit_options_positions
from regime_guard        import check_trade_safety, get_india_vix
from regime_detector     import detect_regime
from volatility_engine   import compute_volatility_signal
from strategy_router     import build_daily_plan
from intraday_advisor    import build_intraday_plan
from fii_dii_engine      import (fetch_fii_dii, get_fii_dii_signal,
                                  fetch_fii_futures, get_fii_futures_signal)
from option_chain_engine import fetch_option_chain, get_iron_condor_strikes
from iron_condor         import build_iron_condor_plan, ic_entry_allowed
from greeks_engine       import compute_position_greeks

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
        "SNS_EXECUTE_ENABLED":    os.environ.get("SNS_EXECUTE_ENABLED",    "false"),
        "MODE":                   os.environ.get("MODE",                   "PAPER"),
        "MIN_SIGNAL_STRENGTH":    os.environ.get("MIN_SIGNAL_STRENGTH",    "2.5"),
        "EVENT_DATES":            os.environ.get("EVENT_DATES",            ""),
        "NSE_HOLIDAYS":           os.environ.get("NSE_HOLIDAYS",           ""),
        "ALERT_COOLDOWN_MINUTES": os.environ.get("ALERT_COOLDOWN_MINUTES", "15"),
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
        plan["generated_at"] = datetime.now(IST).strftime("%Y-%m-%d %H:%M IST")
        cfg_tbl.put_item(Item={
            "config_key":   "DAILY_PLAN",
            "config_value": json.dumps(plan),
        })
    except Exception:
        pass


def _store_intraday_plan(plan: dict) -> None:
    """Persist intraday plan (generated once per day at 9:30 AM) in DynamoDB."""
    try:
        cfg_tbl.put_item(Item={
            "config_key":   "INTRADAY_PLAN",
            "config_value": json.dumps(plan),
        })
    except Exception:
        pass


def _execute_intraday_paper(plan: dict, call_price: float, put_price: float) -> bool:
    """
    Record a paper intraday position in DynamoDB.
    Called every scan when INTRADAY_AUTO_EXECUTE=true and strategy != WAIT.
    Dedup guard ensures only one open INTRADAY position at a time.
    Returns True if a new position was created, False if skipped.
    """
    trade_type = plan["strategy"]
    # Skip if already an open intraday position
    try:
        from boto3.dynamodb.conditions import Attr
        resp = ddb.Table(os.environ.get("POSITIONS_TABLE", "nifty_positions")).scan(
            FilterExpression=(
                Attr("status").eq("OPEN") & Attr("strategy_type").eq("INTRADAY")
            ),
            Select="COUNT",
        )
        if resp.get("Count", 0) > 0:
            print("[intraday] Position already open — skipping paper execute")
            return False
    except Exception:
        pass

    # Entry premium: sum of leg premiums at the time the plan was generated
    if trade_type in ("BUY_STRADDLE", "SELL_STRADDLE", "BUY_STRANGLE", "SELL_STRANGLE"):
        entry_premium = round(call_price + put_price, 2)
        leg_count     = 2
    elif trade_type == "BUY_CE":
        entry_premium = round(call_price, 2)
        leg_count     = 1
    elif trade_type == "BUY_PE":
        entry_premium = round(put_price, 2)
        leg_count     = 1
    else:
        return   # WAIT — nothing to execute

    lot_size    = int(os.environ.get("LOT_SIZE", "75"))
    ts          = datetime.now(IST).isoformat()
    pos_id      = f"INTRADAY_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}"
    sl_level    = round(entry_premium * (1 - float(plan.get("sl_pct", 0.25))), 2)
    target_lvl  = round(entry_premium * (1 + float(plan.get("target_pct", 0.40))), 2)

    # Build order ID log for each paper leg
    order_ids = []
    for leg in plan.get("legs", []):
        sym = leg.get("symbol", leg.get("label", ""))
        order_ids.append(f"PAPER_{sym}_{leg['action']}")
        print(f"[PAPER INTRADAY] {leg['action']} {leg['qty']}× {sym}")

    ddb.Table(os.environ.get("POSITIONS_TABLE", "nifty_positions")).put_item(Item={
        "position_id":    pos_id,
        "timestamp":      ts,
        "strategy_type":  "INTRADAY",
        "trade_type":     trade_type,
        "status":         "OPEN",
        "mode":           "PAPER",
        "entry_premium":  str(entry_premium),
        "entry_call_price": str(call_price),
        "entry_put_price":  str(put_price),
        "sl_pct":         "0.25",
        "target_pct":     "0.40",
        "sl_level":       str(sl_level),
        "target_level":   str(target_lvl),
        "hard_exit_time": "13:30",
        "leg_count":      str(leg_count),
        "qty":            str(lot_size),
        "lot_size":       str(lot_size),
        "call_symbol":    plan.get("legs", [{}])[0].get("symbol", "") if "CE" in trade_type or "STRADDLE" in trade_type or "STRANGLE" in trade_type else "",
        "put_symbol":     plan.get("legs", [{}])[-1].get("symbol", "") if "PE" in trade_type or "STRADDLE" in trade_type or "STRANGLE" in trade_type else "",
        "atm_strike":     str(plan.get("atm_strike", "")),
        "order_ids":      json.dumps(order_ids),
        "confidence":     plan.get("confidence", ""),
        "reason":         plan.get("reason", "")[:200],
    })
    print(f"[intraday] Paper position recorded: {pos_id} | entry_premium={entry_premium} | SL={sl_level} | Target={target_lvl}")
    return True


def _within_market_hours() -> bool:
    """Return True during 9:15 AM – 14:30 PM IST (entry window for intraday)."""
    now  = datetime.now(IST)
    mins = now.hour * 60 + now.minute
    return 9 * 60 + 15 <= mins <= 14 * 60 + 30


def _is_before_decision_window() -> bool:
    """Return True before 9:45 AM IST — opening 30 min is too noisy."""
    now = datetime.now(IST)
    return now.hour * 60 + now.minute < 9 * 60 + 45


def _is_past_decision_deadline() -> bool:
    """Return True at/after 11:00 AM IST — strategy window has closed."""
    now = datetime.now(IST)
    return now.hour * 60 + now.minute >= 11 * 60


def _is_strategy_decided_today() -> bool:
    """Return True if a strategy decision (or WAIT) was already made today."""
    try:
        resp = cfg_tbl.get_item(Key={"config_key": "STRATEGY_DECIDED_DATE"})
        last = resp.get("Item", {}).get("config_value", "")
        return last == datetime.now(IST).strftime("%Y-%m-%d")
    except Exception:
        return False


def _mark_strategy_decided() -> None:
    """Persist today's date to prevent re-evaluation after a decision is made."""
    try:
        cfg_tbl.put_item(Item={
            "config_key":   "STRATEGY_DECIDED_DATE",
            "config_value": datetime.now(IST).strftime("%Y-%m-%d"),
        })
    except Exception:
        pass


def _get_stored_intraday_strategy() -> str:
    """Return the last stored intraday plan's strategy (for change-detection alerting)."""
    try:
        resp = cfg_tbl.get_item(Key={"config_key": "INTRADAY_PLAN"})
        raw  = resp.get("Item", {}).get("config_value", "{}")
        plan = json.loads(raw) if raw else {}
        # Only treat as "same day" if the stored plan is from today
        today = datetime.now(IST).strftime("%Y-%m-%d")
        if plan.get("date") == today:
            return plan.get("strategy", "")
        return ""   # new day — treat as changed
    except Exception:
        return ""


def _signal_key(record: dict) -> str:
    """Stable dedup key: spread_signal|arb_signal."""
    return f"{record.get('spread_signal','NONE')}|{record.get('arbitrage_signal','NONE')}"


def _should_alert(record: dict, cooldown_minutes: int) -> bool:
    """
    Return True if a Telegram alert should be sent for this signal.
    Suppressed when the same signal type was sent within cooldown_minutes.
    Always alerts if the signal type changed.
    """
    sig_key = _signal_key(record)
    try:
        last_ts  = cfg_tbl.get_item(Key={"config_key": "LAST_ALERT_TS" }).get("Item", {}).get("config_value", "")
        last_sig = cfg_tbl.get_item(Key={"config_key": "LAST_ALERT_SIG"}).get("Item", {}).get("config_value", "")
        if last_sig != sig_key:
            return True   # signal type changed — always alert immediately
        if last_ts:
            last_dt = datetime.fromisoformat(last_ts)
            if last_dt.tzinfo is None:
                last_dt = IST.localize(last_dt)
            elapsed_min = (datetime.now(IST) - last_dt).total_seconds() / 60
            if elapsed_min < cooldown_minutes:
                return False
    except Exception:
        pass
    return True


def _record_alert(record: dict) -> None:
    """Persist current signal key + timestamp for cooldown tracking."""
    try:
        cfg_tbl.put_item(Item={"config_key": "LAST_ALERT_TS",  "config_value": datetime.now(IST).isoformat()})
        cfg_tbl.put_item(Item={"config_key": "LAST_ALERT_SIG", "config_value": _signal_key(record)})
    except Exception:
        pass


def _get_prev_vix() -> float:
    """Fetch VIX from the most recent DynamoDB signal record (prior scan)."""
    try:
        resp  = sig_tbl.scan(
            Limit=5,
            ProjectionExpression="india_vix",
        )
        items = [i for i in resp.get("Items", []) if i.get("india_vix")]
        if items:
            return float(items[0]["india_vix"])
    except Exception:
        pass
    return 0.0


def _get_day_of_week() -> int:
    """Return current IST day of week: 0=Mon … 6=Sun."""
    return datetime.now(IST).weekday()


def _premarket_report_needed() -> bool:
    """
    Return True only during 8:45–9:05 AM IST window AND
    today's pre-market report hasn't been sent yet.
    """
    now = datetime.now(IST)
    if not (8 * 60 + 45 <= now.hour * 60 + now.minute <= 9 * 60 + 5):
        return False
    try:
        resp = cfg_tbl.get_item(Key={"config_key": "PREMARKET_SENT_DATE"})
        last = resp.get("Item", {}).get("config_value", "")
        return last != now.strftime("%Y-%m-%d")
    except Exception:
        return True


def _mark_premarket_sent() -> None:
    try:
        cfg_tbl.put_item(Item={
            "config_key":   "PREMARKET_SENT_DATE",
            "config_value": datetime.now(IST).strftime("%Y-%m-%d"),
        })
    except Exception:
        pass


def _paper_trade_arb(record: dict) -> None:
    """
    Record a paper arb position in DynamoDB when MODE=PAPER and arb signal is strong.
    Skips if an open ARB position already exists today.
    """
    try:
        from boto3.dynamodb.conditions import Attr
        pos_tbl = ddb.Table(os.environ.get("POSITIONS_TABLE", "nifty_positions"))
        existing = pos_tbl.scan(
            FilterExpression=(
                Attr("status").eq("OPEN") & Attr("strategy_type").eq("ARB")
            ),
            Select="COUNT",
        )
        if existing.get("Count", 0) > 0:
            print("[arb] Paper arb position already open — skipping")
            return

        ts     = datetime.now(IST).isoformat()
        pos_id = f"ARB_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}"
        mispricing = float(record.get("arb_mispricing", 0))
        lot_size   = int(os.environ.get("LOT_SIZE", "75"))
        signal     = record.get("arbitrage_signal", "")

        # SL: mispricing widens by 12pts; Target: 80% reversion
        sl_level     = round(abs(mispricing) + 12, 2)
        target_level = round(abs(mispricing) * 0.2, 2)

        order_ids = [
            f"PAPER_FUT_{record.get('near_symbol','')}",
            f"PAPER_CALL_{record.get('call_symbol','')}",
            f"PAPER_PUT_{record.get('put_symbol','')}",
        ]
        for oid in order_ids:
            print(f"[PAPER ARB] {signal} — {oid}")

        pos_tbl.put_item(Item={
            "position_id":      pos_id,
            "timestamp":        ts,
            "strategy_type":    "ARB",
            "trade_type":       signal,
            "status":           "OPEN",
            "mode":             "PAPER",
            "entry_mispricing": str(mispricing),
            "sl_level":         str(sl_level),
            "target_level":     str(target_level),
            "near_symbol":      record.get("near_symbol", ""),
            "call_symbol":      record.get("call_symbol", ""),
            "put_symbol":       record.get("put_symbol", ""),
            "arb_strike":       record.get("arb_strike", ""),
            "arb_call_price":   record.get("arb_call_price", ""),
            "arb_put_price":    record.get("arb_put_price", ""),
            "arb_synthetic_fut":record.get("arb_synthetic_fut", ""),
            "arb_actual_fut":   record.get("arb_actual_fut", ""),
            "qty":              str(lot_size),
            "hard_exit_time":   "15:25",   # force close before market end
            "order_ids":        json.dumps(order_ids),
            "zscore":           record.get("zscore", ""),
            "india_vix":        record.get("india_vix", ""),
        })
        print(f"[arb] Paper position created: {pos_id}")
    except Exception as exc:
        print(f"[arb] Paper trade record failed: {exc}")


def _run_quick_position_monitors() -> None:
    """
    Lightweight position monitor — runs on every Lambda invocation even when the
    full strategy pipeline is skipped (e.g. before 10 AM, or after strategy decided).
    Checks and exits spread/arb/options positions based on current market prices.
    Non-fatal: any exception is caught and logged.
    """
    try:
        _broker = get_broker()
        _cdiff, _dte = 0.0, 30
        _cprice, _pprice = 0.0, 0.0
        try:
            _fut = get_active_futures(_broker, UNDERLYING)
            _q   = _broker.get_quote([
                f"NFO:{_fut['near_token']}",
                f"NFO:{_fut['next_token']}",
                f"NFO:{_fut['far_token']}",
            ])
            _n  = float(_q[f"NFO:{_fut['near']}"]["ltp"])
            _nx = float(_q[f"NFO:{_fut['next']}"]["ltp"])
            _f  = float(_q[f"NFO:{_fut['far']}"]["ltp"])
            _cdiff = compute_spreads(_n, _nx, _f)["curve_diff"]
            _dte   = days_to_expiry(_fut["near_expiry"])
            # Get ATM option prices for options monitor
            _arb_m = __import__("arbitrage_engine")
            _spot  = _n  # use near futures as spot approximation
            _arb   = _arb_m.check_parity(_broker, _fut, _spot, _q)
            _cprice = float(_arb.get("call_price", 0))
            _pprice = float(_arb.get("put_price",  0))
        except Exception as _qe:
            print(f"[monitors] Quote fetch skipped: {_qe}")
        # Spread / arb exits
        _exits = check_and_exit_positions(_broker, _cdiff, _dte)
        if _exits:
            send_exit_alert(_exits)
        # Options / intraday exits
        _opt_exits = check_and_exit_options_positions(_broker, _cprice + _pprice)
        if _opt_exits:
            _mode = _load_config().get("MODE", "PAPER")
            send_options_exit_alert(_opt_exits, mode=_mode)
    except Exception as _me:
        print(f"[monitors] Quick monitor error (non-fatal): {_me}")


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
        # Entry window closed — but still run position monitors so hard-exit
        # at 1:30 PM is guaranteed to fire even if earlier scans had errors.
        # Runs a minimal broker+exit loop until 15:30 market close.
        if not past_entry_cutoff("15:30"):
            try:
                _broker = get_broker()
                # Spread / arb positions (target / SL / DTE)
                _cdiff  = 0.0
                _dte    = 30
                try:
                    _fut = get_active_futures(_broker, UNDERLYING)
                    _q   = _broker.get_quote([
                        f"NFO:{_fut['near_token']}",
                        f"NFO:{_fut['next_token']}",
                        f"NFO:{_fut['far_token']}",
                    ])
                    _n   = float(_q[f"NFO:{_fut['near']}"]["ltp"])
                    _nx  = float(_q[f"NFO:{_fut['next']}"]["ltp"])
                    _f   = float(_q[f"NFO:{_fut['far']}"]["ltp"])
                    _cdiff = compute_spreads(_n, _nx, _f)["curve_diff"]
                    _dte   = days_to_expiry(_fut["near_expiry"])
                except Exception:
                    pass
                _sprd_exits = check_and_exit_positions(_broker, _cdiff, _dte)
                if _sprd_exits:
                    send_exit_alert(_sprd_exits)

                # Intraday options positions (hard exit 1:30 PM)
                try:
                    _arb_mod = __import__("arbitrage_engine")
                    _spot    = float(list(_q.values())[0].get("ltp", 0)) if '_q' in dir() else 0
                    _arb     = _arb_mod.check_parity(_broker, _fut, _spot, _q) if _spot else {}
                    _cprice  = float(_arb.get("call_price", 0))
                    _pprice  = float(_arb.get("put_price",  0))
                except Exception:
                    _cprice, _pprice = 0.0, 0.0
                _opt_exits = check_and_exit_options_positions(_broker, _cprice + _pprice)
                if _opt_exits:
                    send_options_exit_alert(_opt_exits, mode=cfg.get("MODE", "PAPER"))
            except Exception as _e:
                print(f"[post-cutoff monitor] Error: {_e}")
        return {"statusCode": 200, "body": "Past entry cutoff — monitors ran"}

    # ── 9:45 AM noise gate ───────────────────────────────────────────────────
    # 9:15–9:45 AM is high-noise opening range. Skip strategy evaluation,
    # but still run position monitors so any open positions are watched.
    if _is_before_decision_window():
        _run_quick_position_monitors()
        now_str = datetime.now(IST).strftime("%H:%M")
        print(f"[scanner] Before 9:45 AM ({now_str} IST) — monitors ran, strategy skipped")
        return {"statusCode": 200, "body": f"Before decision window ({now_str} IST)"}

    # ── One-shot guard ────────────────────────────────────────────────────────
    # Once a strategy decision is made (or WAIT declared at 11 AM), skip the
    # expensive full pipeline. Only run monitors to watch existing positions.
    if _is_strategy_decided_today():
        _run_quick_position_monitors()
        print("[scanner] Strategy already decided today — monitors ran, pipeline skipped")
        return {"statusCode": 200, "body": "Strategy decided for today — monitors ran"}

    # cfg already loaded above for holiday check — use it directly
    ZSCORE_THRESH = float(cfg["ZSCORE_THRESHOLD"])
    LOOKBACK      = int(cfg["LOOKBACK_WINDOW"])
    CAPITAL       = float(cfg["TRADING_CAPITAL"])
    LOT_SIZE      = int(cfg["LOT_SIZE"])
    MIN_STRENGTH  = float(cfg["MIN_SIGNAL_STRENGTH"])

    try:
        broker  = get_broker()

        # ── Margin utilization alert (fires once/hour if >60% used) ──────
        try:
            funds = broker.get_funds()
            available_margin = float(funds.get("availablecash", funds.get("net", 0)))
            CAPITAL_CHK = float(cfg.get("TRADING_CAPITAL", os.environ.get("TRADING_CAPITAL", "500000")))
            used_margin  = CAPITAL_CHK - available_margin
            margin_pct   = (used_margin / CAPITAL_CHK * 100) if CAPITAL_CHK > 0 else 0
            if margin_pct > 60:
                send_margin_alert(margin_pct, available_margin, CAPITAL_CHK)
        except Exception:
            pass

        futures = get_active_futures(broker, UNDERLYING)

        # ── Live quotes (NFO futures only — keep NSE spot separate to avoid
        #    mixed-exchange batch failures on Angel One's quote API) ─────────
        instruments = [
            f"NFO:{futures['near_token']}",
            f"NFO:{futures['next_token']}",
            f"NFO:{futures['far_token']}",
        ]
        quotes = broker.get_quote(instruments)

        near_price  = float(quotes[f"NFO:{futures['near']}"]["ltp"])
        next_price  = float(quotes[f"NFO:{futures['next']}"]["ltp"])
        far_price   = float(quotes[f"NFO:{futures['far']}"]["ltp"])

        # Fetch Nifty 50 spot (OHLC) separately — non-critical, falls back to near_price
        spot_ohlc: dict = {}
        try:
            _spot_data = broker.get_quote(["NSE:26000"])
            # Angel One returns tradingSymbol like "Nifty 50" — grab first result
            spot_ohlc = next(iter(_spot_data.values()), {}) if _spot_data else {}
        except Exception as _e:
            print(f"[scanner] Spot OHLC fetch failed (non-fatal): {_e}")

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

        # ── Synthetic arbitrage (suppressed on HALT) ──────────────────────
        if safety["safe"]:
            arb = check_parity(broker, futures, spot_price, quotes)
        else:
            arb = {
                "arbitrage_signal": "NONE", "arb_mispricing": 0,
                "arb_stoploss": 0, "arb_target": 0,
                "call_symbol": "", "put_symbol": "",
            }

        # ── Signal strength & sizing ──────────────────────────────────────
        strength = signal_strength(
            zscore,
            oi_data["rollover_signal"],
            vol_data["volume_signal"],
            exp_b,
        )
        # Spread qty — sized by signal strength
        base_qty   = recommended_qty(strength, CAPITAL, LOT_SIZE) if spread_signal != "NONE" else 0
        spread_qty = int(base_qty * safety["size_factor"])

        # Arb qty — fixed 1 lot (delta-neutral), skip if HALT
        arb_signal_val = arb.get("arbitrage_signal", "NONE") if arb else "NONE"
        arb_qty = LOT_SIZE if (arb_signal_val not in ("NONE", "") and safety["safe"]) else 0

        # Use spread_qty if active, else arb_qty
        qty = spread_qty if spread_signal != "NONE" else arb_qty

        levels = (
            compute_levels(spread_signal, cdiff)
            if spread_signal != "NONE"
            else {"stoploss_diff": 0, "target_diff": 0}
        )

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

        # ── Momentum Engine (VWAP + EMA20 + RSI) ─────────────────────────
        momentum_data = {"signal": "NEUTRAL", "rsi": 50.0, "ema20": 0.0, "vwap": 0.0,
                         "candle_count": 0, "confidence": "LOW", "reason": ""}
        try:
            from momentum_engine import compute_momentum_signal
            momentum_data = compute_momentum_signal(
                broker        = broker,
                near_token    = futures["near_token"],
                spot_price    = spot_price,
                vix           = vix or 0.0,
                iv_vix_spread = vol_data_iv.get("iv_vix_spread", 0.0),
                lot_size      = LOT_SIZE,
            )
            print(f"[scanner] momentum_engine: signal={momentum_data['signal']} "
                  f"rsi={momentum_data['rsi']} ema20={momentum_data['ema20']} "
                  f"vwap={momentum_data['vwap']} conf={momentum_data['confidence']}")
        except Exception as _me:
            print(f"[scanner] momentum_engine failed (non-fatal): {_me}")

        # ── Day-of-week + Institutional Flow (FII/DII) ───────────────────
        day_of_week  = _get_day_of_week()
        fii_data     = fetch_fii_dii()
        fii_signal   = get_fii_dii_signal(fii_data)
        # FII F&O: index futures net long/short — the gold standard signal
        fii_fut_data = fetch_fii_futures()
        fii_fut_sig  = get_fii_futures_signal(fii_fut_data)

        # ── Option Chain: PCR, Max Pain, OI Walls (15-min cache) ─────────
        chain_data  = fetch_option_chain()
        pcr         = float(chain_data.get("pcr", 1.0))
        pcr_signal  = chain_data.get("pcr_signal", "PCR_NEUTRAL")
        max_pain    = int(chain_data.get("max_pain", 0))
        call_wall   = int(chain_data.get("call_wall", 0))
        put_wall    = int(chain_data.get("put_wall", 0))
        iv_skew     = float(chain_data.get("iv_skew", 0.0))
        iv_skew_sig = chain_data.get("iv_skew_signal", "SKEW_NEUTRAL")

        # ── Iron Condor Plan (Mon/Tue when conditions met) ────────────────
        iron_condor_plan = None
        ic_ok, ic_reason = ic_entry_allowed(vix or 0.0, pcr, dte, day_of_week, event_dates)
        if ic_ok and safety["safe"]:
            try:
                iron_condor_plan = build_iron_condor_plan(
                    broker, spot_price, vix or 0.0, dte,
                    futures["near_expiry"], pcr, max_pain, LOT_SIZE,
                )
                print(f"[IC] Plan built: net_premium={iron_condor_plan.get('net_premium')} "
                      f"SL=₹{iron_condor_plan.get('sl_inr')} Target=₹{iron_condor_plan.get('target_inr')}")
            except Exception as ic_err:
                print(f"[IC] Plan build failed: {ic_err}")
                iron_condor_plan = None
        else:
            print(f"[IC] Entry blocked: {ic_reason}")

        # ── Pre-market Report (fires once at 8:45–9:05 AM IST) ───────────
        if _premarket_report_needed():
            DAY_LABELS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            # Determine recommended strategy for the day (plain text)
            if not safety["safe"]:
                _rec_strat = "WAIT — regime HALT"
                _strat_note = safety.get("reasons", ["Adverse conditions"])[0]
            elif ic_ok and iron_condor_plan:
                _rec_strat = "IRON CONDOR"
                _strat_note = (
                    f"Sell {iron_condor_plan.get('short_call')}CE + "
                    f"Buy {iron_condor_plan.get('long_call')}CE / "
                    f"Sell {iron_condor_plan.get('short_put')}PE + "
                    f"Buy {iron_condor_plan.get('long_put')}PE  "
                    f"| Net premium ≈ ₹{iron_condor_plan.get('net_premium', 0):.0f}/share"
                )
            elif day_of_week == 3:
                _rec_strat = "WAIT — expiry day, close positions by 12:30 PM"
                _strat_note = ""
            else:
                _rec_strat = ic_reason  # explain why IC not viable
                _strat_note = ""

            premarket_report = {
                "date":               datetime.now(IST).strftime("%Y-%m-%d"),
                "day_label":          DAY_LABELS[day_of_week],
                "day_of_week":        day_of_week,
                "spot":               spot_price,
                "vix":                vix or 0.0,
                # FII/DII cash market
                "fii_signal":         fii_signal,
                "fii_net_cr":         fii_data.get("fii_net_cr", 0),
                "dii_net_cr":         fii_data.get("dii_net_cr", 0),
                "fii_data_date":      fii_data.get("data_date", ""),
                # FII F&O (index futures positioning — gold standard)
                "fii_fut_signal":     fii_fut_sig,
                "fii_fut_net":        int(fii_fut_data.get("fii_fut_net", 0)),
                "fii_opt_pcr":        fii_fut_data.get("fii_opt_pcr", 1.0),
                # Option chain
                "pcr":                pcr,
                "pcr_signal":         pcr_signal,
                "max_pain":           max_pain,
                "call_wall":          call_wall,
                "put_wall":           put_wall,
                "atm_iv":             float(chain_data.get("atm_iv", 0.0)),
                "iv_skew":            iv_skew,
                "iv_skew_signal":     iv_skew_sig,
                # Strategy
                "recommended_strategy": _rec_strat,
                "strategy_note":      _strat_note,
                # Events — convert list to comma-separated string
                "events_today":       ", ".join(event_dates) if event_dates else "",
                # Context
                "regime":             safety["regime"],
                "iron_condor":        iron_condor_plan,
            }
            try:
                send_premarket_report(premarket_report)
                _mark_premarket_sent()
                print("[premarket] Report sent")
            except Exception as pm_err:
                print(f"[premarket] Report failed: {pm_err}")

        # ── Strategy Router – Daily Plan ──────────────────────────────────
        daily_plan = build_daily_plan(
            safety           = safety,
            regime           = regime_data,
            vol              = vol_data_iv,
            arb              = arb,
            spread_signal    = spread_signal,
            strength         = strength,
            futures          = futures,
            qty              = qty,
            lot_size         = LOT_SIZE,
            min_strength     = MIN_STRENGTH,
            fii_signal       = fii_signal,
            fii_fut_signal   = fii_fut_sig,
            pcr              = pcr,
            pcr_signal       = pcr_signal,
            max_pain         = max_pain,
            iron_condor_plan = iron_condor_plan,
            day_of_week      = day_of_week,
            dte              = dte,
            event_dates      = event_dates,
            momentum         = momentum_data,
            vix              = vix or 0.0,
        )
        _store_daily_plan(daily_plan)

        # ── Win Probability Gate (10 AM–11 AM one-shot decision) ──────────────
        win_prob = daily_plan.get("win_probability", 0.0)
        strategy = daily_plan.get("strategy", "WAIT")
        print(f"[scanner] Strategy={strategy} win_prob={win_prob:.1%}")

        if win_prob >= 0.80 and strategy != "WAIT":
            # High-conviction signal — alert, execute, mark done for the day
            print(f"[scanner] HIGH-CONVICTION signal ({win_prob:.1%}) — executing {strategy}")
            _mark_strategy_decided()
            # Alert via Telegram with win probability prominently shown
            try:
                send_telegram(
                    f"✅ HIGH-CONVICTION STRATEGY ({win_prob:.0%} win probability)\n"
                    f"Strategy: {daily_plan.get('strategy_emoji','')} {strategy}\n"
                    f"{daily_plan.get('reason','')}\n"
                    f"Risk: {daily_plan.get('risk_note','')}\n"
                    f"Legs:\n{daily_plan.get('legs_text','')}"
                )
            except Exception as _ae:
                print(f"[scanner] Telegram alert failed: {_ae}")
        elif _is_past_decision_deadline():
            # 11 AM passed with no high-conviction signal — WAIT for the day
            print(f"[scanner] Past 11 AM deadline, best was {strategy} at {win_prob:.1%} — WAIT today")
            _mark_strategy_decided()
            try:
                send_telegram(
                    f"⏸️ NO TRADE TODAY\n"
                    f"Best signal: {strategy} at {win_prob:.0%} (threshold: 80%)\n"
                    f"Decision window (10–11 AM) closed. Staying flat for the day."
                )
            except Exception as _ae:
                print(f"[scanner] WAIT telegram alert failed: {_ae}")
        else:
            # Still in window but below threshold — keep scanning, no alert yet
            print(f"[scanner] Below 80% threshold ({win_prob:.1%}) — will retry before 11 AM")

        # ── Intraday Advisor (fires once at 9:30 AM IST) ──────────────────
        INTRADAY_AUTO_EXECUTE = cfg.get(
            "INTRADAY_AUTO_EXECUTE",
            os.environ.get("INTRADAY_AUTO_EXECUTE", "true"),
        ).lower() == "true"

        # ── Intraday Advisor — re-evaluates every scan during market hours ──
        # Entry is allowed any time 9:15 AM–2:30 PM (not locked to 9:30 window).
        # Telegram alert fires only when strategy changes; execution alert fires
        # only when a new position is actually created (dedup guard prevents dups).
        if _within_market_hours():
            prev_strategy  = _get_stored_intraday_strategy()
            vix_prev       = _get_prev_vix()
            intraday_plan  = build_intraday_plan(
                spot_price    = spot_price,
                prev_close    = prev_close,
                vix           = vix or 0.0,
                vix_prev      = vix_prev,
                iv_vix_spread = vol_data_iv.get("iv_vix_spread", 0.0),
                intraday_high = intraday_high,
                intraday_low  = intraday_low,
                call_price    = atm_call_price,
                put_price     = atm_put_price,
                call_symbol   = call_sym_atm,
                put_symbol    = put_sym_atm,
                lot_size      = LOT_SIZE,
                safety_regime = safety.get("regime", "SAFE"),
                rsi           = momentum_data.get("rsi",   50.0),
                vwap          = momentum_data.get("vwap",   0.0),
                ema20         = momentum_data.get("ema20",  0.0),
            )
            _store_intraday_plan(intraday_plan)

            # Alert only when strategy changes (avoid Telegram spam every 30s)
            if intraday_plan["strategy"] != prev_strategy:
                send_intraday_alert(intraday_plan)
                print(f"[intraday] Strategy changed: {prev_strategy!r} → {intraday_plan['strategy']!r} "
                      f"(confidence={intraday_plan['confidence']})")
            else:
                print(f"[intraday] Plan unchanged: {intraday_plan['strategy']} "
                      f"(confidence={intraday_plan['confidence']})")

            # Paper-execute whenever strategy is actionable and no position is open
            if INTRADAY_AUTO_EXECUTE and intraday_plan["strategy"] != "WAIT":
                entered = _execute_intraday_paper(intraday_plan, atm_call_price, atm_put_price)
                if entered:
                    order_ids = [leg.get("symbol", "") for leg in intraday_plan.get("legs", [])]
                    send_intraday_execution_alert(intraday_plan, order_ids, mode="PAPER")

        # ── Options position monitor (runs every scan) ────────────────────
        options_exits = check_and_exit_options_positions(
            broker               = broker if safety["safe"] else None,
            current_atm_premium  = atm_call_price + atm_put_price,
        )
        if options_exits:
            send_options_exit_alert(options_exits, mode=cfg.get("MODE", "PAPER"))

        # ── Portfolio Greeks (aggregate all open positions) ───────────────
        try:
            from position_monitor import _get_open_intraday_positions
            open_positions   = _get_open_intraday_positions()
            portfolio_greeks = compute_position_greeks(open_positions, spot_price, LOT_SIZE)
            # Delta alert: fire if |per_lot_delta| > 0.3
            if abs(portfolio_greeks.get("per_lot_delta", 0)) > 0.3:
                send_delta_alert(portfolio_greeks["per_lot_delta"], open_positions)
            # Persist to DynamoDB config for dashboard /risk endpoint
            cfg_tbl.put_item(Item={
                "config_key":   "PORTFOLIO_GREEKS",
                "config_value": json.dumps(portfolio_greeks),
            })
        except Exception as greeks_err:
            print(f"[greeks] Error computing portfolio Greeks: {greeks_err}")
            portfolio_greeks = {}

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
            "stoploss_diff":    str(levels["stoploss_diff"]) if spread_signal != "NONE" else str(arb.get("arb_stoploss", 0)),
            "target_diff":      str(levels["target_diff"])   if spread_signal != "NONE" else str(arb.get("arb_target",   0)),
            "days_to_expiry":   str(dte),
            "expiry_bias":      exp_b,
            "near_oi":          str(oi_data["near_oi"]),
            "next_oi":          str(oi_data["next_oi"]),
            "oi_diff":          str(oi_data["oi_diff"]),
            "rollover_signal":  str(oi_data["rollover_signal"]),
            "rollover_pct":     str(oi_data["rollover_pct"]),
            "rollover_strong":  str(oi_data["rollover_strong"]),
            "vol_ratio":        str(vol_data["vol_ratio"]),
            "volume_signal":    str(vol_data["volume_signal"]),
            # ── Arbitrage ─────────────────────────────────────────────────
            "arbitrage_signal":  arb.get("arbitrage_signal", "NONE"),
            "arb_mispricing":    str(arb.get("arb_mispricing", 0)),
            "arb_stoploss":      str(arb.get("arb_stoploss", 0)),
            "arb_target":        str(arb.get("arb_target", 0)),
            "call_symbol":       arb.get("call_symbol", ""),
            "put_symbol":        arb.get("put_symbol", ""),
            "arb_strike":        str(arb.get("strike", 0)),
            "arb_call_price":    str(arb.get("call_price", 0)),
            "arb_put_price":     str(arb.get("put_price", 0)),
            "arb_synthetic_fut": str(arb.get("synthetic_future", 0)),
            "arb_actual_fut":    str(arb.get("actual_future", 0)),
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
            # ── FII/DII cash market ───────────────────────────────────────
            "fii_signal":       fii_signal,
            "fii_net_cr":       str(fii_data.get("fii_net_cr", 0)),
            "dii_net_cr":       str(fii_data.get("dii_net_cr", 0)),
            # ── FII F&O futures positioning ───────────────────────────────
            "fii_fut_signal":   fii_fut_sig,
            "fii_fut_net":      str(fii_fut_data.get("fii_fut_net", 0)),
            "fii_fut_long":     str(fii_fut_data.get("fii_fut_long", 0)),
            "fii_fut_short":    str(fii_fut_data.get("fii_fut_short", 0)),
            "fii_opt_pcr":      str(fii_fut_data.get("fii_opt_pcr", 1.0)),
            "pcr":              str(round(pcr, 3)),
            "pcr_signal":       pcr_signal,
            "max_pain":         str(max_pain),
            "call_wall":        str(call_wall),
            "put_wall":         str(put_wall),
            "iv_skew":          str(iv_skew),
            "iv_skew_signal":   iv_skew_sig,
            # ── Momentum Engine ───────────────────────────────────────────
            "momentum_signal":  momentum_data.get("signal", "NEUTRAL"),
            "rsi":              str(round(momentum_data.get("rsi",   50.0), 1)),
            "ema20":            str(round(momentum_data.get("ema20",  0.0), 1)),
            "vwap":             str(round(momentum_data.get("vwap",   0.0), 1)),
            # ── Daily Plan (summary) ──────────────────────────────────────
            "daily_strategy":   daily_plan["strategy"],
            "daily_plan_legs":  daily_plan["legs_text"],
            # ── Portfolio Greeks ──────────────────────────────────────────
            "total_delta":      str(portfolio_greeks.get("total_delta", 0)),
            "total_theta_inr":  str(portfolio_greeks.get("total_theta_inr", 0)),
            "total_vega_inr":   str(portfolio_greeks.get("total_vega_inr", 0)),
        }

        # ── Persist signal ────────────────────────────────────────────────
        sig_tbl.put_item(Item=record)

        # ── Alert & auto-execute ──────────────────────────────────────────
        COOLDOWN = int(cfg.get("ALERT_COOLDOWN_MINUTES", "15"))

        if not safety["safe"]:
            if _should_alert(record, COOLDOWN):
                send_regime_alert(record, safety)
                _record_alert(record)
            return {"statusCode": 200, "body": json.dumps(record)}

        # Arb alert only fires when mispricing is meaningfully above threshold (1.5×)
        # to avoid spamming on borderline signals (e.g., 15.x pts barely over 15pt threshold)
        _arb_threshold = float(cfg.get("ARBITRAGE_THRESHOLD", os.environ.get("ARBITRAGE_THRESHOLD", "15")))
        MIN_ARB_ALERT = float(cfg.get("MIN_ARB_ALERT_PTS", str(_arb_threshold * 1.5)))
        arb_mispricing_val = abs(float(arb.get("arb_mispricing", 0)))
        arb_tradeable = (
            arb.get("arbitrage_signal", "NONE") not in ("NONE", "")
            and arb_mispricing_val >= MIN_ARB_ALERT
            and safety["safe"]   # no arb alerts on HALT
        )
        has_signal = (
            spread_signal != "NONE" and strength >= MIN_STRENGTH
        ) or arb_tradeable

        if has_signal and _should_alert(record, COOLDOWN):
            send_telegram(record)
            _record_alert(record)
            # Paper-execute arb signal when in PAPER mode and auto-execute is on
            MODE = cfg.get("MODE", os.environ.get("MODE", "PAPER"))
            if arb_tradeable and INTRADAY_AUTO_EXECUTE and MODE == "PAPER":
                _paper_trade_arb(record)
            if cfg.get("SNS_EXECUTE_ENABLED", "false").lower() == "true":
                publish_to_sns(record)

        return {"statusCode": 200, "body": json.dumps(record)}

    except Exception as exc:
        import traceback
        err = traceback.format_exc()
        print(f"ERROR: {exc}\n{err}")
        send_error_alert(f"{exc}\n\n{err[:600]}", context="Scanner")
        return {"statusCode": 500, "body": str(exc)}
