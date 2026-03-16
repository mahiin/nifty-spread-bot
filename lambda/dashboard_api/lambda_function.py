"""
Dashboard API Lambda – Enhanced
=================================
Sits behind API Gateway.

Endpoints:
  GET  /signals?limit=60      – last N spread scan records
  GET  /positions             – open positions
  GET  /pnl                   – today's P&L summary
  GET  /pnl/history?days=30   – P&L history
  GET  /orders?limit=50       – order history
  POST /orders                – place order (butterfly or single leg)
  POST /positions/close       – close position (reverse all legs)
  GET  /config                – runtime config from DynamoDB
  POST /config                – update runtime config in DynamoDB
  GET  /auth/status           – test broker connectivity
  GET  /daily-plan            – latest strategy recommendation from DynamoDB
  GET  /volatility?limit=60   – IV vs VIX history for chart
  GET  /health                – ping

CORS headers included for browser fetch.
"""

import json
import os
import sys
from datetime import date, datetime, timedelta

import boto3
import pytz
from boto3.dynamodb.conditions import Attr, Key

# ── Broker client (packaged alongside this file at deploy time) ─────────────
sys.path.insert(0, os.path.dirname(__file__))
try:
    from broker_client import get_broker
    BROKER_AVAILABLE = True
except ImportError:
    BROKER_AVAILABLE = False

REGION           = os.environ.get("AWS_REGION_NAME",        "ap-south-1")
SIG_TABLE        = os.environ.get("DYNAMODB_SIGNALS_TABLE", "nifty_spread_signals")
POS_TABLE        = os.environ.get("POSITIONS_TABLE",        "nifty_positions")
PNL_TABLE        = os.environ.get("PNL_TABLE",              "nifty_pnl")
ORD_TABLE        = os.environ.get("ORDERS_TABLE",           "nifty_orders")
CFG_TABLE        = os.environ.get("CONFIG_TABLE",           "nifty_config")
MODE             = os.environ.get("MODE",                   "PAPER")
IST              = pytz.timezone("Asia/Kolkata")
DASHBOARD_SECRET = os.environ.get("DASHBOARD_SECRET",       "")

ddb = boto3.resource("dynamodb", region_name=REGION)

CORS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Headers": "Content-Type,x-dashboard-token",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
}


def _verify_token(event) -> bool:
    """Check x-dashboard-token header. If DASHBOARD_SECRET is empty, auth is open."""
    if not DASHBOARD_SECRET:
        return True  # auth not configured — backward compat
    token = (event.get("headers") or {}).get("x-dashboard-token", "")
    return token == DASHBOARD_SECRET


def _resp(body, code=200):
    return {"statusCode": code, "headers": CORS, "body": json.dumps(body, default=str)}


def _body(event) -> dict:
    raw = event.get("body") or "{}"
    if isinstance(raw, str):
        return json.loads(raw)
    return raw or {}


# ─────────────────────────────────────────────────────────────────────────────
#  READ helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_signals(limit: int = 60) -> list:
    # Use GSI (signal-by-time-index) for efficient descending timestamp query.
    # Falls back to full scan if GSI doesn't exist yet (e.g. first deploy).
    try:
        resp = ddb.Table(SIG_TABLE).query(
            IndexName="signal-by-time-index",
            KeyConditionExpression=Key("pk").eq("SIGNAL"),
            ScanIndexForward=False,   # descending timestamp
            Limit=limit,
        )
        return resp.get("Items", [])
    except Exception:
        # GSI not ready — fall back to scan
        resp  = ddb.Table(SIG_TABLE).scan(Limit=max(limit, 200))
        items = resp.get("Items", [])
        items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        return items[:limit]


def get_positions() -> list:
    resp = ddb.Table(POS_TABLE).scan(
        FilterExpression=Attr("status").eq("OPEN")
    )
    return resp.get("Items", [])


def get_pnl_today() -> dict:
    today = date.today().isoformat()
    resp  = ddb.Table(PNL_TABLE).get_item(Key={"date": today})
    return resp.get("Item", {"date": today, "realised_pnl": 0, "trades": 0})


def get_pnl_history(days: int = 30) -> list:
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    resp   = ddb.Table(PNL_TABLE).scan(
        FilterExpression=Attr("date").gte(cutoff)
    )
    items = resp.get("Items", [])
    items.sort(key=lambda x: x.get("date", ""))
    return items


def get_orders(limit: int = 50) -> list:
    resp  = ddb.Table(ORD_TABLE).scan(Limit=max(limit, 200))
    items = resp.get("Items", [])
    items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return items[:limit]


def get_config() -> dict:
    resp  = ddb.Table(CFG_TABLE).scan()
    items = resp.get("Items", [])
    # Merge env-var defaults with DynamoDB overrides
    defaults = {
        "MODE":                os.environ.get("MODE",                   "PAPER"),
        "BROKER":              os.environ.get("BROKER",                 "angel"),
        "MAX_DAILY_LOSS":      os.environ.get("MAX_DAILY_LOSS",         "5000"),
        "MAX_POSITIONS":       os.environ.get("MAX_POSITIONS",          "3"),
        "MIN_SIGNAL_STRENGTH": os.environ.get("MIN_SIGNAL_STRENGTH",    "2.5"),
        "ZSCORE_THRESHOLD":    os.environ.get("ZSCORE_THRESHOLD",       "2.0"),
        "ZSCORE_EXIT":         os.environ.get("ZSCORE_EXIT",            "0.5"),
        "LOOKBACK_WINDOW":     os.environ.get("LOOKBACK_WINDOW",        "50"),
        "ARBITRAGE_THRESHOLD": os.environ.get("ARBITRAGE_THRESHOLD",    "15"),
        "SNS_EXECUTE_ENABLED": os.environ.get("SNS_EXECUTE_ENABLED",    "false"),
    }
    for item in items:
        defaults[item["config_key"]] = item["config_value"]
    return defaults


# ─────────────────────────────────────────────────────────────────────────────
#  WRITE helpers
# ─────────────────────────────────────────────────────────────────────────────

def update_config(payload: dict):
    table = ddb.Table(CFG_TABLE)
    for key, value in payload.items():
        table.put_item(Item={"config_key": key, "config_value": str(value)})


def _record_order(order_id: str, params: dict, status: str = "PLACED"):
    ts = datetime.now(IST).isoformat()
    ddb.Table(ORD_TABLE).put_item(Item={
        "order_id":        order_id,
        "timestamp":       ts,
        "tradingsymbol":   params.get("tradingsymbol", ""),
        "exchange":        params.get("exchange", "NFO"),
        "transaction_type": params.get("transaction_type", ""),
        "quantity":        str(params.get("quantity", 0)),
        "order_type":      params.get("order_type", "MARKET"),
        "price":           str(params.get("price", 0)),
        "product":         params.get("product", "CARRYFORWARD"),
        "tag":             params.get("tag", ""),
        "status":          status,
        "mode":            _get_effective_mode(),
    })


def _get_effective_mode() -> str:
    """Check DynamoDB config override for MODE, fall back to env var."""
    try:
        resp = ddb.Table(CFG_TABLE).get_item(Key={"config_key": "MODE"})
        return resp.get("Item", {}).get("config_value", MODE)
    except Exception:
        return MODE


def _check_daily_loss_limit() -> tuple[bool, str]:
    """Returns (ok, reason). ok=False means daily loss limit hit."""
    try:
        cfg_resp = ddb.Table(CFG_TABLE).get_item(Key={"config_key": "MAX_DAILY_LOSS"})
        max_loss = float(cfg_resp.get("Item", {}).get("config_value", 5000))
        today    = date.today().isoformat()
        pnl_resp = ddb.Table(PNL_TABLE).get_item(Key={"date": today})
        pnl      = float(pnl_resp.get("Item", {}).get("realised_pnl", 0) or 0)
        if pnl <= -max_loss:
            return False, f"Daily loss limit ₹{max_loss:,.0f} reached (current: ₹{pnl:,.0f})"
    except Exception:
        pass
    return True, ""


# ─────────────────────────────────────────────────────────────────────────────
#  ORDER placement
# ─────────────────────────────────────────────────────────────────────────────

def _safe_place(broker, params: dict) -> str:
    """Place one order and record it. Returns order_id."""
    mode = _get_effective_mode()
    if mode != "LIVE":
        fake_id = f"PAPER_{params['tradingsymbol']}_{params['transaction_type']}"
        _record_order(fake_id, params, status="PAPER")
        return fake_id

    order_id = broker.place_order(
        tradingsymbol    = params["tradingsymbol"],
        exchange         = params.get("exchange", "NFO"),
        transaction_type = params["transaction_type"],
        quantity         = int(params["quantity"]),
        order_type       = params.get("order_type", "MARKET"),
        product          = params.get("product", "CARRYFORWARD"),
        price            = float(params.get("price", 0)),
        tag              = params.get("tag", ""),
    )
    _record_order(order_id, params)
    return order_id


def place_butterfly_order(body: dict) -> dict:
    """
    body: {type, direction, near_symbol, next_symbol, far_symbol, qty_per_leg, product}
    direction: SELL_BUTTERFLY | BUY_BUTTERFLY
    """
    ok, reason = _check_daily_loss_limit()
    if not ok:
        raise ValueError(reason)

    direction  = body["direction"]
    near       = body["near_symbol"]
    nxt        = body["next_symbol"]
    far        = body["far_symbol"]
    qty        = int(body.get("qty_per_leg", 75))
    product    = body.get("product", "CARRYFORWARD")

    if direction == "SELL_BUTTERFLY":
        legs = [(near, "BUY", qty), (nxt, "SELL", qty * 2), (far, "BUY", qty)]
    else:  # BUY_BUTTERFLY
        legs = [(near, "SELL", qty), (nxt, "BUY", qty * 2), (far, "SELL", qty)]

    broker = get_broker() if BROKER_AVAILABLE else None
    order_ids = []
    for sym, side, q in legs:
        params = {
            "tradingsymbol":    sym,
            "exchange":         "NFO",
            "transaction_type": side,
            "quantity":         q,
            "order_type":       "MARKET",
            "product":          product,
            "price":            0,
            "tag":              "BUTTERFLY",
        }
        oid = _safe_place(broker, params)
        order_ids.append(oid)

    return {"order_ids": order_ids, "order_count": len(order_ids), "direction": direction}


def place_single_order(body: dict) -> dict:
    """body: {tradingsymbol, exchange, transaction_type, quantity, order_type, price, product, tag}"""
    ok, reason = _check_daily_loss_limit()
    if not ok:
        raise ValueError(reason)

    broker = get_broker() if BROKER_AVAILABLE else None
    oid = _safe_place(broker, body)
    return {"order_id": oid}


def _get_latest_curve_diff() -> float:
    """Fetch curve_diff from the most recent signal record."""
    try:
        resp = ddb.Table(SIG_TABLE).query(
            IndexName="signal-by-time-index",
            KeyConditionExpression=Key("pk").eq("SIGNAL"),
            ScanIndexForward=False,
            Limit=1,
            ProjectionExpression="curve_diff",
        )
        items = resp.get("Items", [])
        if items:
            return float(items[0].get("curve_diff", 0))
    except Exception:
        pass
    return 0.0


def _write_pnl_api(pnl: float) -> None:
    """Add pnl to today's running total in nifty_pnl table."""
    today = date.today().isoformat()
    try:
        ddb.Table(PNL_TABLE).update_item(
            Key={"date": today},
            UpdateExpression=(
                "SET realised_pnl = if_not_exists(realised_pnl, :zero) + :p, "
                "trades = if_not_exists(trades, :zero) + :one, "
                "#m = :mode"
            ),
            ExpressionAttributeNames={"#m": "mode"},
            ExpressionAttributeValues={
                ":p":    round(pnl, 2),
                ":zero": 0,
                ":one":  1,
                ":mode": _get_effective_mode(),
            },
        )
    except Exception as e:
        print(f"[dashboard_api] Could not write P&L: {e}")


def _build_close_legs(trade_type: str, pos: dict, qty: int) -> list:
    """Return [(symbol, side, qty), ...] for closing a position."""
    near = pos.get("near_symbol", "")
    nxt  = pos.get("next_symbol", "")
    far  = pos.get("far_symbol",  "")
    call = pos.get("call_symbol", "")
    put  = pos.get("put_symbol",  "")

    if "SELL_BUTTERFLY" in trade_type:
        return [(near, "SELL", qty), (nxt, "BUY", qty * 2), (far, "SELL", qty)]
    if "BUY_BUTTERFLY" in trade_type:
        return [(near, "BUY", qty), (nxt, "SELL", qty * 2), (far, "BUY", qty)]
    if "BUY_FUT_SELL_CALL_BUY_PUT" in trade_type:
        return [(near, "SELL", qty), (call, "BUY", qty), (put, "SELL", qty)]
    if "SELL_FUT_BUY_CALL_SELL_PUT" in trade_type:
        return [(near, "BUY", qty), (call, "SELL", qty), (put, "BUY", qty)]
    # Intraday options (SELL_STRADDLE, BUY_STRADDLE, SELL_STRANGLE, BUY_STRANGLE, BUY_CE, BUY_PE)
    is_buy = trade_type in ("BUY_STRADDLE", "BUY_STRANGLE", "BUY_CE", "BUY_PE")
    close_action = "SELL" if is_buy else "BUY"
    legs = []
    if trade_type in ("BUY_STRADDLE", "SELL_STRADDLE", "BUY_STRANGLE", "SELL_STRANGLE"):
        if call: legs.append((call, close_action, qty))
        if put:  legs.append((put,  close_action, qty))
    elif trade_type == "BUY_CE" and call:
        legs.append((call, close_action, qty))
    elif trade_type == "BUY_PE" and put:
        legs.append((put,  close_action, qty))
    return legs


def _calc_close_pnl(trade_type: str, pos: dict, exit_curve_diff: float) -> float:
    """Estimate P&L for manual close. Returns 0 for intraday (no reliable exit premium)."""
    if "BUTTERFLY" in trade_type:
        entry_diff = float(pos.get("entry_curve_diff", 0))
        qty        = int(pos.get("qty", 75))
        diff_change = entry_diff - exit_curve_diff
        if "SELL_BUTTERFLY" in trade_type:
            return round(diff_change * qty, 2)
        if "BUY_BUTTERFLY" in trade_type:
            return round(-diff_change * qty, 2)
    # For arb / intraday — use exit_pnl provided by caller or 0
    return float(pos.get("_exit_pnl_override", 0))


def close_position(body: dict) -> dict:
    """Reverse all legs of an open position and record P&L."""
    position_id = body.get("position_id")
    if not position_id:
        raise ValueError("position_id required")

    resp = ddb.Table(POS_TABLE).get_item(Key={"position_id": position_id})
    pos  = resp.get("Item")
    if not pos:
        raise ValueError(f"Position {position_id} not found")

    trade_type = pos.get("trade_type", "")
    qty        = int(pos.get("qty", 75))

    legs = _build_close_legs(trade_type, pos, qty)
    if not legs:
        raise ValueError(f"Cannot auto-close trade_type: {trade_type}")

    broker    = get_broker() if BROKER_AVAILABLE else None
    order_ids = []
    for sym, side, q in legs:
        if not sym:
            continue
        params = {
            "tradingsymbol":    sym,
            "exchange":         "NFO",
            "transaction_type": side,
            "quantity":         q,
            "order_type":       "MARKET",
            "product":          pos.get("product", "CARRYFORWARD"),
            "price":            0,
            "tag":              "CLOSE",
        }
        oid = _safe_place(broker, params)
        order_ids.append(oid)

    # ── P&L calculation ───────────────────────────────────────────────────────
    # Caller may pass exit_curve_diff or exit_pnl directly in the request body
    exit_curve_diff = float(body.get("exit_curve_diff") or _get_latest_curve_diff())
    if "exit_pnl" in body:
        pnl = float(body["exit_pnl"])
    else:
        pos["_exit_pnl_override"] = 0
        pnl = _calc_close_pnl(trade_type, pos, exit_curve_diff)

    # ── Mark position closed with exit metadata ───────────────────────────────
    now_ts = datetime.now(IST).isoformat()
    ddb.Table(POS_TABLE).update_item(
        Key={"position_id": position_id},
        UpdateExpression=(
            "SET #s = :s, exit_pnl = :p, exit_reason = :r, exit_timestamp = :t"
        ),
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":s": "CLOSED",
            ":p": str(round(pnl, 2)),
            ":r": "MANUAL_CLOSE",
            ":t": now_ts,
        },
    )

    # ── Write P&L to daily summary ────────────────────────────────────────────
    _write_pnl_api(pnl)

    return {"order_ids": order_ids, "order_count": len(order_ids), "pnl": pnl}


def get_daily_plan() -> dict:
    """Fetch the latest daily plan stored by the scanner."""
    try:
        resp = ddb.Table(CFG_TABLE).get_item(Key={"config_key": "DAILY_PLAN"})
        raw  = resp.get("Item", {}).get("config_value", "{}")
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def get_intraday_plan() -> dict:
    """Fetch today's intraday plan stored by the scanner at 9:30 AM."""
    try:
        resp = ddb.Table(CFG_TABLE).get_item(Key={"config_key": "INTRADAY_PLAN"})
        raw  = resp.get("Item", {}).get("config_value", "{}")
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def get_volatility_history(limit: int = 60) -> list:
    """Return recent records with IV/VIX fields for the volatility chart."""
    try:
        resp = ddb.Table(SIG_TABLE).query(
            IndexName="signal-by-time-index",
            KeyConditionExpression=Key("pk").eq("SIGNAL"),
            ScanIndexForward=False,
            Limit=limit,
            ProjectionExpression=(
                "pk, #ts, call_iv_pct, put_iv_pct, avg_iv_pct, "
                "iv_vix_spread, india_vix, vol_signal, trading_regime"
            ),
            ExpressionAttributeNames={"#ts": "timestamp"},
        )
        return resp.get("Items", [])
    except Exception:
        # GSI fallback
        try:
            resp  = ddb.Table(SIG_TABLE).scan(
                Limit=max(limit, 200),
                ProjectionExpression=(
                    "#ts, call_iv_pct, put_iv_pct, avg_iv_pct, "
                    "iv_vix_spread, india_vix, vol_signal, trading_regime"
                ),
                ExpressionAttributeNames={"#ts": "timestamp"},
            )
            items = resp.get("Items", [])
            items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            return items[:limit]
        except Exception:
            return []


def get_backtest(days: int = 30) -> dict:
    """
    Simulate historical performance from stored signals.

    For each signal record:
    - Identify the strategy that would have been selected
    - Apply standard target (+40%) / SL (-25%) / hard-exit (1:30 PM) rules
    - Group by strategy, compute per-strategy win rate, avg P&L, totals

    Returns per-strategy stats + daily simulated P&L list.
    """
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    try:
        resp  = ddb.Table(SIG_TABLE).scan(
            FilterExpression=Attr("timestamp").gte(cutoff),
            ProjectionExpression=(
                "#ts, daily_strategy, straddle_premium, iv_vix_spread, "
                "india_vix, vol_signal, spread_signal, signal_strength, "
                "arbitrage_signal, trading_regime"
            ),
            ExpressionAttributeNames={"#ts": "timestamp"},
        )
        items = resp.get("Items", [])
    except Exception:
        return {"error": "Failed to fetch signals", "strategies": {}, "daily_pnl": []}

    # Group signals by day (first signal of each day per strategy)
    from collections import defaultdict
    by_day = defaultdict(list)
    for item in items:
        ts = item.get("timestamp", "")[:10]
        by_day[ts].append(item)

    strategy_stats = defaultdict(lambda: {"trades": 0, "wins": 0, "total_pnl": 0.0, "pnls": []})
    daily_pnl_list = []

    TARGET_PCT = 0.40   # 40% premium growth = profit
    SL_PCT     = 0.25   # 25% premium loss = stop

    for day_ts in sorted(by_day.keys()):
        day_items = by_day[day_ts]
        # Use the first signal of the day for simulation
        sig = day_items[0] if day_items else {}
        strat = sig.get("daily_strategy", "WAIT")
        if strat == "WAIT":
            continue

        premium = float(sig.get("straddle_premium") or 0)
        if premium <= 0:
            premium = 100.0  # default estimate

        is_sell = strat in ("SELL_STRADDLE", "IRON_CONDOR", "BEAR_CALL_SPREAD", "BULL_PUT_SPREAD")

        # Simulate: scan through day's signals to see if target/SL hit
        outcome = "TIME_EXIT"
        final_pnl = 0.0

        # Check if any signal that day shows 40% move or 25% adverse move
        for s in day_items[1:]:
            ts_item = s.get("timestamp", "")
            if ts_item[11:16] >= "13:30":
                break  # hard exit
            iv_spread = float(s.get("iv_vix_spread") or 0)
            if is_sell:
                if iv_spread <= -(premium * TARGET_PCT / premium * 10):
                    outcome = "TARGET"
                    final_pnl = premium * TARGET_PCT
                    break
                if iv_spread >= (premium * SL_PCT / premium * 10):
                    outcome = "STOP_LOSS"
                    final_pnl = -premium * SL_PCT
                    break
            else:
                if iv_spread >= 2.0:
                    outcome = "TARGET"
                    final_pnl = premium * TARGET_PCT
                    break

        if outcome == "TIME_EXIT":
            final_pnl = premium * 0.15 if is_sell else -premium * 0.05

        strategy_stats[strat]["trades"]    += 1
        strategy_stats[strat]["total_pnl"] += final_pnl
        strategy_stats[strat]["pnls"].append(final_pnl)
        if final_pnl > 0:
            strategy_stats[strat]["wins"] += 1

        daily_pnl_list.append({"date": day_ts, "pnl": round(final_pnl, 2), "strategy": strat})

    # Build summary per strategy
    results = {}
    for strat, st in strategy_stats.items():
        n = st["trades"]
        pnls = st["pnls"]
        avg_pnl = st["total_pnl"] / n if n > 0 else 0
        win_rate = (st["wins"] / n * 100) if n > 0 else 0
        # Max drawdown: largest peak-to-trough of cumulative P&L
        cum = 0.0
        peak = 0.0
        max_dd = 0.0
        for p in pnls:
            cum += p
            peak = max(peak, cum)
            max_dd = max(max_dd, peak - cum)
        results[strat] = {
            "trades":       n,
            "wins":         st["wins"],
            "win_rate_pct": round(win_rate, 1),
            "avg_pnl":      round(avg_pnl, 2),
            "total_pnl":    round(st["total_pnl"], 2),
            "max_drawdown": round(max_dd, 2),
        }

    return {
        "days":       days,
        "strategies": results,
        "daily_pnl":  daily_pnl_list,
    }


def get_portfolio_risk() -> dict:
    """
    Aggregate portfolio risk metrics:
    - Greeks (from PORTFOLIO_GREEKS config key written by scanner)
    - Margin utilization
    - Daily P&L and open position count
    """
    # Greeks from scanner
    greeks = {}
    try:
        resp = ddb.Table(CFG_TABLE).get_item(Key={"config_key": "PORTFOLIO_GREEKS"})
        raw  = resp.get("Item", {}).get("config_value", "{}")
        greeks = json.loads(raw) if raw else {}
    except Exception:
        pass

    # Open positions
    positions   = get_positions()
    open_count  = len(positions)

    # Daily P&L
    pnl_today = get_pnl_today()
    daily_pnl = float(pnl_today.get("realised_pnl", 0))

    # Margin utilization (from config — written by scanner)
    margin_pct = 0.0
    available  = 0.0
    try:
        cfg_resp  = ddb.Table(CFG_TABLE).get_item(Key={"config_key": "TRADING_CAPITAL"})
        capital   = float(cfg_resp.get("Item", {}).get("config_value", 500000))
        # Use last PORTFOLIO_GREEKS timestamp as proxy — margin computed in scanner
        # Fall back to estimating from positions
        margin_pct = min(100.0, open_count * 20.0)   # rough: 20% per position
        available  = capital * (1 - margin_pct / 100)
    except Exception:
        pass

    return {
        "total_delta":     greeks.get("total_delta",     0.0),
        "per_lot_delta":   greeks.get("per_lot_delta",   0.0),
        "total_gamma":     greeks.get("total_gamma",     0.0),
        "total_theta_inr": greeks.get("total_theta_inr", 0.0),
        "total_vega_inr":  greeks.get("total_vega_inr",  0.0),
        "open_positions":  open_count,
        "daily_pnl":       daily_pnl,
        "margin_used_pct": round(margin_pct, 1),
        "margin_available": round(available, 0),
        "timestamp":       datetime.now(IST).isoformat(),
    }


def auth_status() -> dict:
    """Test broker connectivity. Returns {connected, broker, timestamp}."""
    ts = datetime.now(IST).isoformat()
    if not BROKER_AVAILABLE:
        return {"connected": False, "error": "broker_client not packaged", "timestamp": ts}
    try:
        broker = get_broker()
        broker_name = os.environ.get("BROKER", "unknown")
        feed_token  = getattr(broker, "feed_token", None)
        return {"connected": True, "broker": broker_name, "timestamp": ts,
                "feed_token": bool(feed_token)}
    except Exception as exc:
        return {"connected": False, "error": str(exc), "timestamp": ts}


# ─────────────────────────────────────────────────────────────────────────────
#  Lambda handler
# ─────────────────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    path   = (event.get("path") or event.get("rawPath") or "/signals").rstrip("/")
    method = (event.get("httpMethod") or
              event.get("requestContext", {}).get("http", {}).get("method", "GET")).upper()

    if method == "OPTIONS":
        return _resp({})

    # Auth check — /health is exempt so AWS monitoring works
    if not path.endswith("/health") and not _verify_token(event):
        return {"statusCode": 401, "headers": CORS,
                "body": json.dumps({"error": "Unauthorized"})}

    try:
        # ── Read endpoints ────────────────────────────────────────────────────
        if method == "GET":
            qs = event.get("queryStringParameters") or {}

            if path.endswith("/signals"):
                return _resp(get_signals(int(qs.get("limit", 60))))

            if path.endswith("/positions"):
                return _resp(get_positions())

            if path.endswith("/pnl/history"):
                return _resp(get_pnl_history(int(qs.get("days", 30))))

            if path.endswith("/pnl"):
                return _resp(get_pnl_today())

            if path.endswith("/orders"):
                return _resp(get_orders(int(qs.get("limit", 50))))

            if path.endswith("/config"):
                return _resp(get_config())

            if path.endswith("/auth/status"):
                return _resp(auth_status())

            if path.endswith("/daily-plan"):
                return _resp(get_daily_plan())

            if path.endswith("/intraday-plan"):
                return _resp(get_intraday_plan())

            if path.endswith("/volatility"):
                return _resp(get_volatility_history(int(qs.get("limit", 60))))

            if path.endswith("/risk"):
                return _resp(get_portfolio_risk())

            if path.endswith("/backtest"):
                return _resp(get_backtest(int(qs.get("days", 30))))

            if path.endswith("/health"):
                return _resp({"status": "ok", "mode": _get_effective_mode()})

        # ── Write endpoints ───────────────────────────────────────────────────
        if method == "POST":
            body = _body(event)

            if path.endswith("/orders"):
                order_type = body.get("type", "SINGLE")
                if order_type == "BUTTERFLY":
                    result = place_butterfly_order(body)
                else:
                    result = place_single_order(body)
                return _resp(result)

            if path.endswith("/positions/close"):
                result = close_position(body)
                return _resp(result)

            if path.endswith("/config"):
                update_config(body)
                return _resp({"updated": list(body.keys())})

    except Exception as exc:
        import traceback
        print(traceback.format_exc())
        return _resp({"error": str(exc)}, 500)

    return _resp({"error": "unknown path"}, 404)
