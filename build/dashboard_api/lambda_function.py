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

REGION    = os.environ.get("AWS_REGION_NAME",        "ap-south-1")
SIG_TABLE = os.environ.get("DYNAMODB_SIGNALS_TABLE", "nifty_spread_signals")
POS_TABLE = os.environ.get("POSITIONS_TABLE",        "nifty_positions")
PNL_TABLE = os.environ.get("PNL_TABLE",              "nifty_pnl")
ORD_TABLE = os.environ.get("ORDERS_TABLE",           "nifty_orders")
CFG_TABLE = os.environ.get("CONFIG_TABLE",           "nifty_config")
MODE      = os.environ.get("MODE",                   "PAPER")
IST       = pytz.timezone("Asia/Kolkata")

ddb = boto3.resource("dynamodb", region_name=REGION)

CORS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
}


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
    broker = get_broker() if BROKER_AVAILABLE else None
    oid = _safe_place(broker, body)
    return {"order_id": oid}


def close_position(body: dict) -> dict:
    """Reverse all legs of an open position."""
    position_id = body.get("position_id")
    if not position_id:
        raise ValueError("position_id required")

    resp = ddb.Table(POS_TABLE).get_item(Key={"position_id": position_id})
    pos  = resp.get("Item")
    if not pos:
        raise ValueError(f"Position {position_id} not found")

    trade_type = pos.get("trade_type", "")
    near       = pos.get("near_symbol", "")
    nxt        = pos.get("next_symbol", "")
    far        = pos.get("far_symbol",  "")
    qty        = int(pos.get("qty", 75))

    # Determine reverse legs
    if "SELL_BUTTERFLY" in trade_type:
        legs = [(near, "SELL", qty), (nxt, "BUY", qty * 2), (far, "SELL", qty)]
    elif "BUY_BUTTERFLY" in trade_type:
        legs = [(near, "BUY", qty), (nxt, "SELL", qty * 2), (far, "BUY", qty)]
    else:
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

    # Mark position closed
    ddb.Table(POS_TABLE).update_item(
        Key={"position_id": position_id},
        UpdateExpression="SET #s = :s",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": "CLOSED"},
    )

    return {"order_ids": order_ids, "order_count": len(order_ids)}


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
