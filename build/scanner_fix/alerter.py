"""
Alerter module.
Sends Telegram messages and optionally publishes to SNS for auto-execution.
"""
import os
import json
import boto3
import requests

# Regime emoji map
_REGIME_EMOJI = {"SAFE": "✅", "CAUTION": "⚠️", "HALT": "🛑"}


def send_telegram(record: dict):
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return

    spread_sig  = record.get("spread_signal", "NONE")
    arb_sig     = record.get("arbitrage_signal", "NONE")
    strength    = record.get("signal_strength", "0")
    zscore      = record.get("zscore", "0")
    curve_diff  = record.get("curve_diff", "0")
    sl          = record.get("stoploss_diff", "-")
    tgt         = record.get("target_diff", "-")
    qty         = record.get("recommended_qty", "0")

    # Emoji by signal type
    icon = "📊"
    if spread_sig != "NONE":
        icon = "🦋"
    if arb_sig not in ("NONE", ""):
        icon = "⚡"

    regime      = record.get("regime", "SAFE")
    vix_val     = record.get("india_vix", "0")
    vix_level   = record.get("vix_level", "")
    regime_warn = record.get("regime_warnings", "")
    opt_window  = record.get("optimal_window", "False") == "True"

    msg = (
        f"{icon} <b>NIFTY SPREAD SIGNAL</b>\n"
        f"<code>{record.get('timestamp','')[:19]}</code>\n"
        f"\n"
        f"<b>Near:</b> {record.get('near_symbol','')} @ {record.get('near_price','')}\n"
        f"<b>Next:</b> {record.get('next_symbol','')} @ {record.get('next_price','')}\n"
        f"<b>Far :</b> {record.get('far_symbol','')}  @ {record.get('far_price','')}\n"
        f"\n"
        f"Spread1 (Next−Near): {record.get('spread1','')}\n"
        f"Spread2 (Far−Next) : {record.get('spread2','')}\n"
        f"Curve Diff         : {curve_diff}\n"
        f"Z-Score            : {zscore}\n"
        f"\n"
        f"📌 <b>Spread Signal :</b> {spread_sig}\n"
        f"⚡ <b>Arb Signal    :</b> {arb_sig} ({record.get('arb_mispricing','')} pts)\n"
        f"\n"
        f"Strength   : {strength}/5\n"
        f"Qty        : {qty} units\n"
        f"StopLoss   : {sl} (curve_diff)\n"
        f"Target     : {tgt} (curve_diff)\n"
        f"\n"
        f"DTE : {record.get('days_to_expiry','')} days | {record.get('expiry_bias','')}"
        f"{'  ✅ Optimal window' if opt_window else ''}\n"
        f"OI Rollover: {record.get('rollover_signal','')}  "
        f"Vol Imbal: {record.get('volume_signal','')}\n"
        f"\n"
        f"{_REGIME_EMOJI.get(regime,'?')} Regime: <b>{regime}</b>  "
        f"| VIX: {vix_val} ({vix_level})\n"
        + (f"⚠️ {regime_warn}\n" if regime_warn else "")
    )

    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
        timeout=5,
    )


def send_regime_alert(record: dict, safety: dict):
    """
    Send a Telegram alert when trading is HALTED due to adverse market conditions.
    Called instead of the normal signal alert – never publishes to SNS.
    """
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return

    regime   = safety.get("regime", "HALT")
    vix      = record.get("india_vix", "0")
    vix_lvl  = record.get("vix_level", "")
    reasons  = safety.get("reasons", [])
    warnings = safety.get("warnings", [])

    reasons_text  = "\n".join(f"  • {r[:300]}" for r in reasons)  if reasons  else ""
    warnings_text = "\n".join(f"  ⚠️ {w[:200]}" for w in warnings) if warnings else ""

    msg = (
        f"🛑 <b>SPREAD TRADING HALTED</b>\n"
        f"<code>{record.get('timestamp','')[:19]}</code>\n"
        f"\n"
        f"Regime: <b>{regime}</b>  |  VIX: {vix} ({vix_lvl})\n"
        f"NIFTY Spot: {record.get('spot_price','')}  "
        f"| DTE: {record.get('days_to_expiry','')} days\n"
        f"Curve Diff: {record.get('curve_diff','')}  "
        f"| Z-Score: {record.get('zscore','')}\n"
        f"\n"
        f"<b>Why trading is blocked:</b>\n"
        f"{reasons_text}\n"
        + (f"\n<b>Advisory warnings:</b>\n{warnings_text}\n" if warnings_text else "")
        + f"\n<i>No new positions opened. Existing positions unaffected.</i>"
    )

    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
        timeout=5,
    )


def send_exit_alert(events: list):
    """
    Send a Telegram alert for each position that was closed by the monitor.
    Called by the scanner Lambda after position_monitor runs.
    """
    if not events:
        return
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID",   "")
    if not token or not chat_id:
        return

    for ev in events:
        pnl     = float(ev.get("realised_pnl", 0))
        pnl_str = f"₹{abs(pnl):,.0f}"
        pnl_icon = "✅" if pnl >= 0 else "🔴"
        reason  = ev.get("exit_reason", "")
        is_sl   = "STOP_LOSS" in reason
        is_exp  = "EXPIRY" in reason

        icon = "🏁" if "TARGET" in reason else ("🛑" if is_sl else ("⚠️" if is_exp else "📤"))

        msg = (
            f"{icon} <b>POSITION CLOSED [{os.environ.get('MODE','PAPER')}]</b>\n"
            f"Type: <b>{ev.get('trade_type','')}</b>\n"
            f"Reason: {reason}\n"
            f"\n"
            f"Near : {ev.get('near_symbol','')} | Next: {ev.get('next_symbol','')} | Far: {ev.get('far_symbol','')}\n"
            f"Entry Diff : {ev.get('entry_diff',0):.2f}  →  Exit Diff: {ev.get('exit_diff',0):.2f}\n"
            f"\n"
            f"{pnl_icon} <b>Realised P&amp;L (approx): {'+' if pnl>=0 else ''}{pnl:,.0f}</b>\n"
            f"Orders: {len(ev.get('order_ids',[]))} legs closed\n"
        )
        if is_sl:
            msg += "\n⚠️ <i>Stop loss triggered. Review strategy parameters.</i>"
        elif is_exp:
            msg += "\n⚠️ <i>Force-closed due to expiry risk (DTE ≤ 3).</i>"

        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )


def send_error_alert(error_msg: str, context: str = "Scanner"):
    """
    Send a Telegram alert when a Lambda function throws an unhandled error.
    Called from the except block in lambda_handler.
    """
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID",   "")
    if not token or not chat_id:
        return
    msg = (
        f"🚨 <b>LAMBDA ERROR — {context}</b>\n"
        f"<code>{error_msg[:800]}</code>\n"
        f"\n<i>Check CloudWatch logs for full traceback.</i>"
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception:
        pass  # Never let alerter crash the caller


def send_execution_alert(signal: dict, order_ids: list, trade_type: str, mode: str = "PAPER"):
    """Telegram alert when a new position is opened by the executor."""
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID",   "")
    if not token or not chat_id:
        return
    msg = (
        f"✅ <b>ORDER PLACED [{mode}]</b>\n"
        f"Type: <b>{trade_type}</b>\n"
        f"Signal: {signal.get('spread_signal') or signal.get('arbitrage_signal')}\n"
        f"Qty/leg: {signal.get('recommended_qty')} units\n"
        f"Entry CurveDiff: {signal.get('curve_diff')}\n"
        f"StopLoss : {signal.get('stoploss_diff')}\n"
        f"Target   : {signal.get('target_diff')}\n"
        f"Legs placed: {len(order_ids)}\n"
        f"\n📋 <i>Open dashboard to monitor position.</i>"
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception:
        pass


def publish_to_sns(record: dict):
    """Publish signal to SNS → triggers executor Lambda if auto-execute is on."""
    topic_arn = os.environ.get("SNS_EXECUTE_TOPIC_ARN", "")
    if not topic_arn:
        return
    sns = boto3.client("sns", region_name=os.environ.get("AWS_REGION_NAME", "ap-south-1"))
    sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps(record),
        Subject="NIFTY_SPREAD_SIGNAL",
    )
