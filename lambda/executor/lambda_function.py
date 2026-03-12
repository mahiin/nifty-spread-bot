"""
NIFTY Spread Bot – Executor Lambda
====================================
Triggered by SNS message published from scanner Lambda.
Only fires when MODE=LIVE and SNS_EXECUTE_ENABLED=true.

Flow:
  1. Parse signal from SNS payload
  2. Deduplication: skip if same trade_type already has an OPEN position
  3. Risk guards: daily loss limit, max positions, signal strength, entry cutoff
  4. Account balance check: verify sufficient margin before placing orders
  5. Place butterfly or arb orders via broker API
  6. Record open position in DynamoDB
  7. Alert via Telegram (uses shared alerter module)

Fixes applied:
  - Arbitrage signal names aligned with arbitrage_engine output
  - Duplicate position guard prevents opening same trade twice
  - Account balance check before live orders
  - Telegram alerts via shared alerter module
"""

import json
import os
from datetime import datetime, date

import boto3
import pytz
from boto3.dynamodb.conditions import Attr

REGION         = os.environ.get("AWS_REGION_NAME",          "ap-south-1")
IST            = pytz.timezone("Asia/Kolkata")
MODE           = os.environ.get("MODE",                     "PAPER")
LOT_SIZE       = int(os.environ.get("LOT_SIZE",             "75"))
MAX_DAILY_LOSS = float(os.environ.get("MAX_DAILY_LOSS",     "5000"))
MAX_POSITIONS  = int(os.environ.get("MAX_POSITIONS",        "3"))
MIN_STRENGTH   = float(os.environ.get("MIN_SIGNAL_STRENGTH","2.5"))
# Minimum available cash required before placing any order (₹)
MIN_MARGIN_BUFFER = float(os.environ.get("MIN_MARGIN_BUFFER", "50000"))

ddb       = boto3.resource("dynamodb", region_name=REGION)
pos_table = ddb.Table(os.environ.get("POSITIONS_TABLE",         "nifty_positions"))
pnl_table = ddb.Table(os.environ.get("PNL_TABLE",               "nifty_pnl"))


# ─── Risk helpers ─────────────────────────────────────────────────────────

def get_daily_pnl() -> float:
    today = date.today().isoformat()
    try:
        resp = pnl_table.get_item(Key={"date": today})
        val  = resp.get("Item", {}).get("realised_pnl", "0")
        return float(val or 0)
    except Exception:
        return 0.0


def count_open_positions() -> int:
    try:
        resp = pos_table.scan(
            FilterExpression=Attr("status").eq("OPEN"),
            Select="COUNT",
        )
        return resp.get("Count", 0)
    except Exception:
        return 0


def has_open_position_of_type(trade_type: str) -> bool:
    """
    Deduplication guard: returns True if there is already an OPEN position
    with this trade_type. Prevents opening the same trade 10 times in a row
    when a signal persists across multiple scanner invocations.
    """
    try:
        resp = pos_table.scan(
            FilterExpression=(
                Attr("status").eq("OPEN") & Attr("trade_type").eq(trade_type)
            ),
            Select="COUNT",
        )
        return resp.get("Count", 0) > 0
    except Exception:
        return False


def past_time(hhmm: str) -> bool:
    now = datetime.now(IST)
    h, m = map(int, hhmm.split(":"))
    return now >= now.replace(hour=h, minute=m, second=0, microsecond=0)


def check_margin(broker, required_est: float) -> tuple[bool, float]:
    """
    Returns (ok, available_cash).
    ok=True means we have enough margin to place the trade.
    In PAPER mode always returns (True, 999999).
    """
    if MODE != "LIVE":
        return True, 999_999.0
    if broker is None:
        return False, 0.0
    try:
        funds = broker.get_funds()
        available = float(funds.get("available_cash", 0))
        ok = available >= max(required_est, MIN_MARGIN_BUFFER)
        return ok, available
    except Exception as e:
        print(f"[executor] get_funds failed: {e}")
        return False, 0.0


# ─── Order helpers ────────────────────────────────────────────────────────

def place_butterfly(broker, near_sym: str, next_sym: str, far_sym: str,
                    qty_per_leg: int, direction: str) -> list[str]:
    """
    SELL_BUTTERFLY: BUY near, SELL next×2, BUY far
    BUY_BUTTERFLY:  SELL near, BUY next×2, SELL far
    """
    if direction == "SELL_BUTTERFLY":
        legs = [
            (near_sym, "BUY",  qty_per_leg),
            (next_sym, "SELL", qty_per_leg * 2),
            (far_sym,  "BUY",  qty_per_leg),
        ]
    else:
        legs = [
            (near_sym, "SELL", qty_per_leg),
            (next_sym, "BUY",  qty_per_leg * 2),
            (far_sym,  "SELL", qty_per_leg),
        ]

    order_ids = []
    for sym, side, qty in legs:
        if MODE == "LIVE" and broker is not None:
            oid = broker.place_order(
                tradingsymbol=sym, exchange="NFO",
                transaction_type=side, quantity=qty, tag="BUTTERFLY",
            )
            order_ids.append(oid)
        else:
            print(f"[PAPER] {side} {qty}× {sym}")
            order_ids.append(f"PAPER_{sym}_{side}")
    return order_ids


def place_arb(broker, future_sym: str, call_sym: str, put_sym: str,
              qty: int, direction: str) -> list[str]:
    """
    BUY_FUT_SELL_CALL_BUY_PUT:  Buy future, sell call, buy put
    SELL_FUT_BUY_CALL_SELL_PUT: Sell future, buy call, sell put
    """
    if direction == "BUY_FUT_SELL_CALL_BUY_PUT":
        legs = [
            (future_sym, "NFO", "BUY",  qty),
            (call_sym,   "NFO", "SELL", qty),
            (put_sym,    "NFO", "BUY",  qty),
        ]
    else:  # SELL_FUT_BUY_CALL_SELL_PUT
        legs = [
            (future_sym, "NFO", "SELL", qty),
            (call_sym,   "NFO", "BUY",  qty),
            (put_sym,    "NFO", "SELL", qty),
        ]

    order_ids = []
    for sym, exch, side, q in legs:
        if MODE == "LIVE" and broker is not None:
            oid = broker.place_order(
                tradingsymbol=sym, exchange=exch,
                transaction_type=side, quantity=q, tag="ARB",
            )
            order_ids.append(oid)
        else:
            print(f"[PAPER] {side} {q}× {sym}")
            order_ids.append(f"PAPER_{sym}_{side}")
    return order_ids


# ─── Position recording ───────────────────────────────────────────────────

def record_position(signal: dict, order_ids: list, trade_type: str, qty: int):
    ts = datetime.now(IST).isoformat()
    pos_table.put_item(Item={
        "position_id":       ts,
        "timestamp":         ts,
        "trade_type":        trade_type,
        "near_symbol":       signal.get("near_symbol",  ""),
        "next_symbol":       signal.get("next_symbol",  ""),
        "far_symbol":        signal.get("far_symbol",   ""),
        "call_symbol":       signal.get("call_symbol",  ""),
        "put_symbol":        signal.get("put_symbol",   ""),
        "entry_curve_diff":  signal.get("curve_diff",   "0"),
        "stoploss_diff":     signal.get("stoploss_diff","0"),
        "target_diff":       signal.get("target_diff",  "0"),
        "qty":               str(qty),
        "order_ids":         json.dumps(order_ids),
        "status":            "OPEN",
        "mode":              MODE,
        "product":           "CARRYFORWARD",
    })


# ─── Main handler ─────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """Triggered by SNS → executor runs only when a strong signal fires."""

    # ── Parse SNS message ────────────────────────────────────────────────
    try:
        raw    = event["Records"][0]["Sns"]["Message"]
        signal = json.loads(raw)
    except Exception as exc:
        print(f"[executor] Bad SNS payload: {exc}")
        return {"statusCode": 400, "body": "Bad payload"}

    spread_signal = signal.get("spread_signal",    "NONE")
    arb_signal    = signal.get("arbitrage_signal", "NONE")

    # ── Pre-flight checks ─────────────────────────────────────────────────
    if past_time("14:30"):
        print("[executor] Past entry cutoff")
        return {"statusCode": 200, "body": "Cutoff"}

    daily_pnl = get_daily_pnl()
    if daily_pnl <= -MAX_DAILY_LOSS:
        print(f"[executor] Daily loss limit ₹{abs(daily_pnl):.0f} reached")
        return {"statusCode": 200, "body": "Daily loss limit"}

    open_pos = count_open_positions()
    if open_pos >= MAX_POSITIONS:
        print(f"[executor] Max positions ({MAX_POSITIONS}) reached")
        return {"statusCode": 200, "body": "Max positions"}

    strength = float(signal.get("signal_strength", 0))
    if strength < MIN_STRENGTH:
        print(f"[executor] Signal strength {strength:.1f} < {MIN_STRENGTH}")
        return {"statusCode": 200, "body": "Low strength"}

    # ── Determine trade type and check deduplication ─────────────────────
    will_trade_spread = spread_signal in ("SELL_BUTTERFLY", "BUY_BUTTERFLY")
    will_trade_arb    = arb_signal    in ("BUY_FUT_SELL_CALL_BUY_PUT",
                                          "SELL_FUT_BUY_CALL_SELL_PUT")

    if not will_trade_spread and not will_trade_arb:
        return {"statusCode": 200, "body": "No actionable signal"}

    if will_trade_spread and has_open_position_of_type(spread_signal):
        print(f"[executor] Duplicate guard: {spread_signal} already open — skipping")
        return {"statusCode": 200, "body": "Duplicate position"}

    if will_trade_arb and has_open_position_of_type(arb_signal):
        print(f"[executor] Duplicate guard: {arb_signal} already open — skipping")
        return {"statusCode": 200, "body": "Duplicate position"}

    # ── Broker + margin check ──────────────────────────────────────────────
    broker = None
    if MODE == "LIVE":
        try:
            import sys, os as _os
            sys.path.insert(0, _os.path.dirname(__file__))
            from broker_client import get_broker
            broker = get_broker()
        except Exception as exc:
            print(f"[executor] Broker init failed: {exc}")
            return {"statusCode": 500, "body": f"Broker init failed: {exc}"}

    qty_per_leg   = int(signal.get("recommended_qty", LOT_SIZE))
    required_est  = qty_per_leg * 50 * 4   # rough SPAN margin estimate
    margin_ok, available = check_margin(broker, required_est)
    if not margin_ok:
        msg = f"Insufficient margin: ₹{available:,.0f} available, ₹{required_est:,.0f} needed"
        print(f"[executor] {msg}")
        _tg_warn(msg)
        return {"statusCode": 200, "body": msg}

    # ── Execute orders ────────────────────────────────────────────────────
    order_ids  = []
    trade_type = "NONE"

    if will_trade_spread:
        order_ids = place_butterfly(
            broker,
            signal["near_symbol"], signal["next_symbol"], signal["far_symbol"],
            qty_per_leg, spread_signal,
        )
        trade_type = spread_signal

    if will_trade_arb:
        arb_ids = place_arb(
            broker,
            signal["near_symbol"],
            signal.get("call_symbol", ""),
            signal.get("put_symbol",  ""),
            qty_per_leg, arb_signal,
        )
        order_ids.extend(arb_ids)
        trade_type = f"{trade_type}+{arb_signal}" if trade_type != "NONE" else arb_signal

    if trade_type == "NONE" or not order_ids:
        return {"statusCode": 200, "body": "No orders placed"}

    # ── Record position & alert ────────────────────────────────────────────
    record_position(signal, order_ids, trade_type, qty_per_leg)

    try:
        from alerter import send_execution_alert
        send_execution_alert(signal, order_ids, trade_type, MODE)
    except Exception:
        pass

    return {"statusCode": 200, "body": json.dumps({
        "orders": order_ids, "type": trade_type
    })}


def _tg_warn(msg: str):
    """Send a low-priority Telegram warning (no crash if it fails)."""
    try:
        import requests as _req
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID",   "")
        if token and chat_id:
            _req.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id,
                      "text": f"⚠️ <b>Executor Warning</b>\n{msg}",
                      "parse_mode": "HTML"},
                timeout=5,
            )
    except Exception:
        pass
