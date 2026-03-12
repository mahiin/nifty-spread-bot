"""
Position Monitor
================
Called by the scanner Lambda every invocation.
Checks all OPEN positions against exit conditions and closes them when met.

Exit conditions (checked in this order):
  1. DTE ≤ 3              → EXPIRY_RISK  (force close, can't hold through expiry)
  2. curve_diff ≤ target  → TARGET_HIT   (SELL_BUTTERFLY profit)
     curve_diff ≥ target  → TARGET_HIT   (BUY_BUTTERFLY profit)
  3. Adverse move ≥ SL    → STOP_LOSS    (emergency exit)

On exit:
  - Places reverse orders via broker (or paper-logs in PAPER mode)
  - Marks position CLOSED in DynamoDB
  - Calculates and writes realised P&L to nifty_pnl table
  - Returns list of exit events for Telegram alerts
"""

from __future__ import annotations
import json
import os
from datetime import date, datetime
from typing import Optional

import boto3
import pytz

IST          = pytz.timezone("Asia/Kolkata")
REGION       = os.environ.get("AWS_REGION_NAME",         "ap-south-1")
POS_TABLE    = os.environ.get("POSITIONS_TABLE",         "nifty_positions")
PNL_TABLE    = os.environ.get("PNL_TABLE",               "nifty_pnl")
MODE         = os.environ.get("MODE",                    "PAPER")
LOT_SIZE     = int(os.environ.get("LOT_SIZE",            "75"))

_ddb = boto3.resource("dynamodb", region_name=REGION)


# ─── Public API ─────────────────────────────────────────────────────────────

INTRADAY_TYPES = {
    "BUY_STRADDLE", "SELL_STRADDLE",
    "BUY_STRANGLE", "SELL_STRANGLE",
    "BUY_CE", "BUY_PE",
}


def check_and_exit_options_positions(
    broker,
    current_atm_premium: float,   # live call_ltp + put_ltp from volatility engine
) -> list[dict]:
    """
    Called every scan by the scanner Lambda.
    Monitors INTRADAY options positions (BUY/SELL STRADDLE/STRANGLE/CE/PE).

    Exit conditions (checked in order):
      1. current IST time ≥ hard_exit_time  → HARD_EXIT (1:30 PM cutoff)
      2. BUY strategies:  premium dropped ≥ sl_pct   → STOP_LOSS
                          premium rose    ≥ target_pct → TARGET_HIT
      3. SELL strategies: premium rose    ≥ sl_pct   → STOP_LOSS
                          premium dropped ≥ target_pct → TARGET_HIT
    """
    positions = _get_open_intraday_positions()
    if not positions:
        return []

    now_ist = datetime.now(IST)
    events  = []

    for pos in positions:
        entry_premium = float(pos.get("entry_premium", 0))
        sl_pct        = float(pos.get("sl_pct",     0.25))
        target_pct    = float(pos.get("target_pct", 0.40))
        hard_exit     = pos.get("hard_exit_time", "13:30")
        trade_type    = pos.get("trade_type", "")
        qty           = int(pos.get("qty", LOT_SIZE))

        # Use tracked current premium (stored per-leg count)
        leg_count     = int(pos.get("leg_count", 2))
        current_prem  = current_atm_premium if leg_count >= 2 else current_atm_premium / 2.0

        reason = _options_exit_reason(
            trade_type, entry_premium, current_prem,
            sl_pct, target_pct, hard_exit, now_ist,
        )
        if not reason:
            continue

        order_ids = _execute_options_close(broker, pos)
        pnl       = _calc_options_pnl(trade_type, entry_premium, current_prem, qty)
        _mark_closed(pos["position_id"], pnl, reason)
        _write_pnl(pnl)

        events.append({
            "position_id":    pos["position_id"],
            "trade_type":     trade_type,
            "exit_reason":    reason,
            "realised_pnl":   pnl,
            "order_ids":      order_ids,
            "entry_premium":  entry_premium,
            "exit_premium":   round(current_prem, 2),
            "qty":            qty,
            "call_symbol":    pos.get("call_symbol", ""),
            "put_symbol":     pos.get("put_symbol",  ""),
            "atm_strike":     pos.get("atm_strike",  ""),
            "is_intraday":    True,
        })

    return events


def check_and_exit_positions(
    broker,
    current_curve_diff: float,
    current_dte: int,
) -> list[dict]:
    """
    Main entry point called by scanner Lambda on every invocation.

    Returns a list of exit event dicts (one per closed position).
    Empty list means nothing was closed this scan.

    Each event dict:
        position_id, trade_type, exit_reason, realised_pnl,
        order_ids, near_symbol, next_symbol, far_symbol
    """
    positions = _get_open_positions()
    if not positions:
        return []

    events = []
    for pos in positions:
        reason = _exit_reason(pos, current_curve_diff, current_dte)
        if reason:
            order_ids = _execute_close(broker, pos)
            pnl       = _calc_pnl(pos, current_curve_diff)
            _mark_closed(pos["position_id"], pnl, reason)
            _write_pnl(pnl)
            events.append({
                "position_id":  pos["position_id"],
                "trade_type":   pos.get("trade_type", ""),
                "exit_reason":  reason,
                "realised_pnl": pnl,
                "order_ids":    order_ids,
                "near_symbol":  pos.get("near_symbol", ""),
                "next_symbol":  pos.get("next_symbol", ""),
                "far_symbol":   pos.get("far_symbol",  ""),
                "entry_diff":   float(pos.get("entry_curve_diff", 0)),
                "exit_diff":    current_curve_diff,
                "qty":          int(pos.get("qty", LOT_SIZE)),
            })

    return events


# ─── Exit condition logic ────────────────────────────────────────────────────

def _exit_reason(pos: dict, current_diff: float, dte: int) -> Optional[str]:
    trade_type  = pos.get("trade_type", "")
    target_diff = float(pos.get("target_diff",   0))
    sl_diff     = float(pos.get("stoploss_diff", 0))

    # 1. Expiry risk — always exit regardless of P&L
    if dte <= 3:
        return f"EXPIRY_RISK (DTE={dte}, force close)"

    if "SELL_BUTTERFLY" in trade_type:
        # We SOLD when curve_diff was high. Profit when it falls.
        # target_diff = entry - X  (lower is better)
        # sl_diff     = entry + Y  (higher triggers SL)
        if target_diff > 0 and current_diff <= target_diff:
            return f"TARGET_HIT (diff={current_diff:.1f} ≤ {target_diff:.1f})"
        if sl_diff > 0 and current_diff >= sl_diff:
            return f"STOP_LOSS (diff={current_diff:.1f} ≥ SL={sl_diff:.1f})"

    elif "BUY_BUTTERFLY" in trade_type:
        # We BOUGHT when curve_diff was low. Profit when it rises.
        if target_diff > 0 and current_diff >= target_diff:
            return f"TARGET_HIT (diff={current_diff:.1f} ≥ {target_diff:.1f})"
        if sl_diff > 0 and current_diff <= sl_diff:
            return f"STOP_LOSS (diff={current_diff:.1f} ≤ SL={sl_diff:.1f})"

    return None


# ─── Order execution ─────────────────────────────────────────────────────────

def _execute_close(broker, pos: dict) -> list[str]:
    """
    Place reverse orders to close all legs of a position.
    In PAPER mode, just logs and returns fake IDs.
    """
    trade_type = pos.get("trade_type", "")
    near_sym   = pos.get("near_symbol", "")
    next_sym   = pos.get("next_symbol", "")
    far_sym    = pos.get("far_symbol",  "")
    qty        = int(pos.get("qty", LOT_SIZE))
    product    = pos.get("product", "CARRYFORWARD")

    # Determine closing direction (reverse of entry)
    if "SELL_BUTTERFLY" in trade_type:
        # Entry was: BUY near, SELL next×2, BUY far
        # Close is: SELL near, BUY next×2, SELL far
        legs = [
            (near_sym, "SELL", qty),
            (next_sym, "BUY",  qty * 2),
            (far_sym,  "SELL", qty),
        ]
    elif "BUY_BUTTERFLY" in trade_type:
        legs = [
            (near_sym, "BUY",  qty),
            (next_sym, "SELL", qty * 2),
            (far_sym,  "BUY",  qty),
        ]
    elif "BUY_FUT_SELL_CALL_BUY_PUT" in trade_type:
        near_sym = pos.get("near_symbol", "")
        call_sym = pos.get("call_symbol", "")
        put_sym  = pos.get("put_symbol",  "")
        legs = [
            (near_sym, "SELL", qty),
            (call_sym, "BUY",  qty),
            (put_sym,  "SELL", qty),
        ]
    elif "SELL_FUT_BUY_CALL_SELL_PUT" in trade_type:
        near_sym = pos.get("near_symbol", "")
        call_sym = pos.get("call_symbol", "")
        put_sym  = pos.get("put_symbol",  "")
        legs = [
            (near_sym, "BUY",  qty),
            (call_sym, "SELL", qty),
            (put_sym,  "BUY",  qty),
        ]
    else:
        print(f"[position_monitor] Unknown trade_type for close: {trade_type}")
        return []

    order_ids = []
    for sym, side, q in legs:
        if not sym:
            continue
        if MODE == "LIVE" and broker is not None:
            try:
                oid = broker.place_order(
                    tradingsymbol    = sym,
                    exchange         = "NFO",
                    transaction_type = side,
                    quantity         = q,
                    order_type       = "MARKET",
                    product          = product,
                    tag              = "EXIT",
                )
                order_ids.append(oid)
            except Exception as e:
                print(f"[position_monitor] Close order failed {side} {sym}: {e}")
                order_ids.append(f"FAILED_{sym}_{side}")
        else:
            fake_id = f"PAPER_CLOSE_{sym}_{side}"
            print(f"[PAPER EXIT] {side} {q}× {sym}")
            order_ids.append(fake_id)

    return order_ids


# ─── P&L calculation ─────────────────────────────────────────────────────────

def _calc_pnl(pos: dict, exit_diff: float) -> float:
    """
    Approximate P&L for a calendar spread position.

    Calendar spread P&L ≈ (entry_curve_diff − exit_curve_diff) × lot_size
    for SELL_BUTTERFLY (we benefit from convergence).
    Flip sign for BUY_BUTTERFLY.

    This is an approximation — actual P&L depends on exact fill prices.
    """
    entry_diff = float(pos.get("entry_curve_diff", 0))
    qty        = int(pos.get("qty", LOT_SIZE))
    trade_type = pos.get("trade_type", "")

    diff_change = entry_diff - exit_diff  # positive = convergence

    if "SELL_BUTTERFLY" in trade_type:
        raw_pnl = diff_change * qty
    elif "BUY_BUTTERFLY" in trade_type:
        raw_pnl = -diff_change * qty
    else:
        raw_pnl = 0.0

    return round(raw_pnl, 2)


# ─── DynamoDB writes ─────────────────────────────────────────────────────────

def _get_open_intraday_positions() -> list[dict]:
    """Fetch only OPEN intraday options positions."""
    try:
        from boto3.dynamodb.conditions import Attr
        resp = _ddb.Table(POS_TABLE).scan(
            FilterExpression=(
                Attr("status").eq("OPEN") & Attr("strategy_type").eq("INTRADAY")
            )
        )
        return resp.get("Items", [])
    except Exception as e:
        print(f"[position_monitor] Could not fetch intraday positions: {e}")
        return []


def _options_exit_reason(
    trade_type:    str,
    entry_premium: float,
    current_prem:  float,
    sl_pct:        float,
    target_pct:    float,
    hard_exit:     str,
    now_ist,
) -> Optional[str]:
    """Return exit reason string or None if no exit triggered."""
    # 1. Hard time exit (1:30 PM)
    try:
        h, m = map(int, hard_exit.split(":"))
        cutoff = now_ist.replace(hour=h, minute=m, second=0, microsecond=0)
        if now_ist >= cutoff:
            return f"HARD_EXIT (past {hard_exit} IST cutoff)"
    except Exception:
        pass

    if entry_premium <= 0:
        return None

    change_pct = (current_prem - entry_premium) / entry_premium

    is_buy = trade_type in ("BUY_STRADDLE", "BUY_STRANGLE", "BUY_CE", "BUY_PE")

    if is_buy:
        # Profit when premium rises, loss when it falls
        if change_pct >= target_pct:
            return f"TARGET_HIT (premium +{change_pct*100:.1f}% ≥ {target_pct*100:.0f}%)"
        if change_pct <= -sl_pct:
            return f"STOP_LOSS (premium {change_pct*100:.1f}% ≤ -{sl_pct*100:.0f}%)"
    else:
        # SELL: profit when premium falls, loss when it rises
        if change_pct <= -target_pct:
            return f"TARGET_HIT (premium {change_pct*100:.1f}% ≤ -{target_pct*100:.0f}%)"
        if change_pct >= sl_pct:
            return f"STOP_LOSS (premium +{change_pct*100:.1f}% ≥ {sl_pct*100:.0f}%)"

    return None


def _calc_options_pnl(
    trade_type:    str,
    entry_premium: float,
    exit_premium:  float,
    qty:           int,
) -> float:
    """P&L for intraday options positions (approx, per-unit premium × qty)."""
    diff = exit_premium - entry_premium
    is_buy = trade_type in ("BUY_STRADDLE", "BUY_STRANGLE", "BUY_CE", "BUY_PE")
    pnl = diff * qty if is_buy else -diff * qty
    return round(pnl, 2)


def _execute_options_close(broker, pos: dict) -> list[str]:
    """Close all legs of an intraday options position (paper logs in PAPER mode)."""
    trade_type = pos.get("trade_type", "")
    call_sym   = pos.get("call_symbol", "")
    put_sym    = pos.get("put_symbol",  "")
    qty        = int(pos.get("qty", LOT_SIZE))

    # Build closing legs (reverse of entry)
    is_buy = trade_type in ("BUY_STRADDLE", "BUY_STRANGLE", "BUY_CE", "BUY_PE")
    close_action = "SELL" if is_buy else "BUY"

    legs = []
    if trade_type in ("BUY_STRADDLE", "SELL_STRADDLE", "BUY_STRANGLE", "SELL_STRANGLE"):
        if call_sym:
            legs.append((call_sym, close_action, qty))
        if put_sym:
            legs.append((put_sym, close_action, qty))
    elif trade_type == "BUY_CE" and call_sym:
        legs.append((call_sym, close_action, qty))
    elif trade_type == "BUY_PE" and put_sym:
        legs.append((put_sym, close_action, qty))

    order_ids = []
    for sym, side, q in legs:
        if not sym:
            continue
        if MODE == "LIVE" and broker is not None:
            try:
                oid = broker.place_order(
                    tradingsymbol=sym, exchange="NFO",
                    transaction_type=side, quantity=q,
                    order_type="MARKET", product="INTRADAY", tag="INTRADAY_EXIT",
                )
                order_ids.append(oid)
            except Exception as e:
                print(f"[position_monitor] Options close failed {side} {sym}: {e}")
                order_ids.append(f"FAILED_{sym}_{side}")
        else:
            fake_id = f"PAPER_CLOSE_{sym}_{side}"
            print(f"[PAPER INTRADAY EXIT] {side} {q}× {sym}")
            order_ids.append(fake_id)

    return order_ids


def _get_open_positions() -> list[dict]:
    try:
        from boto3.dynamodb.conditions import Attr
        resp = _ddb.Table(POS_TABLE).scan(
            FilterExpression=Attr("status").eq("OPEN")
        )
        return resp.get("Items", [])
    except Exception as e:
        print(f"[position_monitor] Could not fetch positions: {e}")
        return []


def _mark_closed(position_id: str, pnl: float, reason: str) -> None:
    try:
        _ddb.Table(POS_TABLE).update_item(
            Key={"position_id": position_id},
            UpdateExpression=(
                "SET #s = :s, exit_pnl = :p, exit_reason = :r, "
                "exit_timestamp = :t"
            ),
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": "CLOSED",
                ":p": str(round(pnl, 2)),
                ":r": reason,
                ":t": datetime.now(IST).isoformat(),
            },
        )
    except Exception as e:
        print(f"[position_monitor] Could not mark closed {position_id}: {e}")


def _write_pnl(pnl: float) -> None:
    """Add pnl to today's running total in nifty_pnl table."""
    today = date.today().isoformat()
    try:
        pnl_tbl = _ddb.Table(PNL_TABLE)
        # Atomic add — safe for concurrent writes
        pnl_tbl.update_item(
            Key={"date": today},
            UpdateExpression=(
                "SET realised_pnl = if_not_exists(realised_pnl, :zero) + :p, "
                "trades = if_not_exists(trades, :zero) + :one, "
                "#m = :mode"
            ),
            ExpressionAttributeNames={"#m": "mode"},
            ExpressionAttributeValues={
                ":p":    str(round(pnl, 2)),
                ":zero": "0",
                ":one":  "1",
                ":mode": MODE,
            },
        )
    except Exception as e:
        print(f"[position_monitor] Could not write P&L: {e}")
