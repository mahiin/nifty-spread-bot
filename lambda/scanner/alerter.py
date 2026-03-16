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

    # Use arb SL/target fields for arb-only signals, spread fields otherwise
    is_arb_only = arb_sig not in ("NONE", "") and spread_sig == "NONE"
    sl          = record.get("arb_stoploss", "-") if is_arb_only else record.get("stoploss_diff", "-")
    tgt         = record.get("arb_target",   "-") if is_arb_only else record.get("target_diff",   "-")
    sl_label    = "(arb mispricing)" if is_arb_only else "(curve_diff)"
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

    # Build arb legs block when it's an arb signal
    arb_legs_block = ""
    if is_arb_only:
        strike      = record.get("arb_strike", "")
        call_sym    = record.get("call_symbol", "")
        put_sym     = record.get("put_symbol", "")
        call_px     = record.get("arb_call_price", "")
        put_px      = record.get("arb_put_price", "")
        synthetic   = record.get("arb_synthetic_fut", "")
        actual_fut  = record.get("arb_actual_fut", "")
        action      = "BUY" if "BUY_FUT" in arb_sig else "SELL"
        call_action = "SELL" if "BUY_FUT" in arb_sig else "BUY"
        put_action  = "BUY"  if "BUY_FUT" in arb_sig else "SELL"
        arb_legs_block = (
            f"\n<b>Legs to trade:</b>\n"
            f"  {action} Future  : {record.get('near_symbol','')} @ {actual_fut}\n"
            f"  {call_action} Call ({strike} CE): {call_sym} @ {call_px}\n"
            f"  {put_action} Put  ({strike} PE): {put_sym} @ {put_px}\n"
            f"  Synthetic Fut  : {synthetic}  |  Actual: {actual_fut}\n"
            f"  Mispricing     : {record.get('arb_mispricing','')} pts\n"
        )

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
        + arb_legs_block
        + f"\n"
        f"Strength   : {strength}/5\n"
        f"Qty        : {qty} units\n"
        f"StopLoss   : {sl} {sl_label}\n"
        f"Target     : {tgt} {sl_label}\n"
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

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception as e:
        print(f"[alerter] send_telegram failed: {e}")


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

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception as e:
        print(f"[alerter] send_regime_alert failed: {e}")


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

        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
                timeout=5,
            )
        except Exception as e:
            print(f"[alerter] send_exit_alert failed: {e}")


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


def send_intraday_execution_alert(plan: dict, order_ids: list, mode: str = "PAPER"):
    """Telegram alert when a paper/live intraday position is opened."""
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID",   "")
    if not token or not chat_id:
        return

    strat    = plan.get("strategy", "–")
    emoji    = plan.get("emoji", "📊")
    atm      = plan.get("atm_strike", "–")
    cost     = plan.get("straddle_cost_inr", 0)
    sl_note  = plan.get("sl_note", "–")
    tgt_note = plan.get("target_note", "–")
    hard_ex  = plan.get("hard_exit", "1:30 PM")

    legs_lines = ""
    for leg in plan.get("legs", []):
        icon = "✅" if leg["action"] == "BUY" else "🔴"
        legs_lines += f"  {icon} <b>{leg['action']}</b> {leg['qty']}× {leg['label']}\n"

    msg = (
        f"{emoji} <b>INTRADAY POSITION OPENED [{mode}]</b>\n"
        f"<code>{plan.get('generated_at', '')[:19]}</code>\n"
        f"\n"
        f"<b>Strategy:</b> {strat}\n"
        f"<b>ATM Strike:</b> {atm}\n"
        + (f"<b>Entry Premium:</b> ≈ ₹{cost:,.0f}\n" if cost else "")
        + (f"\n<b>Legs placed ({len(order_ids)}):</b>\n{legs_lines}" if legs_lines else "")
        + f"\n"
        f"<b>SL:</b> {sl_note}\n"
        f"<b>Target:</b> {tgt_note}\n"
        f"<b>Hard exit:</b> {hard_ex}\n"
        f"\n"
        f"📋 <i>Monitor in dashboard → Positions tab</i>"
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception:
        pass


def send_options_exit_alert(events: list, mode: str = "PAPER"):
    """Telegram alert when an intraday options position is auto-closed."""
    if not events:
        return
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID",   "")
    if not token or not chat_id:
        return

    for ev in events:
        pnl      = float(ev.get("realised_pnl", 0))
        reason   = ev.get("exit_reason", "")
        strat    = ev.get("trade_type", "")
        entry_p  = ev.get("entry_premium", 0)
        exit_p   = ev.get("exit_premium",  0)
        atm      = ev.get("atm_strike",    "")

        pnl_icon = "✅" if pnl >= 0 else "🔴"
        if "TARGET" in reason:
            icon = "🏁"
        elif "STOP_LOSS" in reason:
            icon = "🛑"
        else:
            icon = "⏰"   # hard time exit

        msg = (
            f"{icon} <b>INTRADAY POSITION CLOSED [{mode}]</b>\n"
            f"Strategy: <b>{strat}</b>  |  ATM: {atm}\n"
            f"\n"
            f"Entry Premium : ₹{float(entry_p):,.2f}\n"
            f"Exit Premium  : ₹{float(exit_p):,.2f}\n"
            f"Reason        : {reason}\n"
            f"\n"
            f"{pnl_icon} <b>Paper P&amp;L: {'+' if pnl >= 0 else ''}₹{pnl:,.0f}</b>\n"
            f"\n"
            f"📋 <i>Check P&amp;L tab for today's running total.</i>"
        )
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
                timeout=5,
            )
        except Exception:
            pass


def send_premarket_report(report: dict):
    """
    Send the 8:45 AM pre-market intelligence brief to Telegram.
    Covers: FII/DII flow, PCR, max pain, day-of-week strategy, events.
    """
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID",   "")
    if not token or not chat_id:
        return

    # Day-of-week strategy guidance
    dow        = report.get("day_of_week", -1)
    day_label  = report.get("day_label", "")
    _dow_guide = {
        0: "✅ Monday — Best day to SELL premium (4 DTE, max theta capture)",
        1: "✅ Tuesday — Good for premium selling (3 DTE)",
        2: "⚠️ Wednesday — Pre-expiry. Reduce size 50%. Consider rolling/closing.",
        3: "🚫 Thursday — Expiry day. Close existing by 12:30 PM. No new sells.",
        4: "📅 Friday — Monthly only. Small size directional trades only.",
    }
    dow_note = _dow_guide.get(dow, "")

    # FII/DII
    fii_net     = report.get("fii_net_cr", 0)
    dii_net     = report.get("dii_net_cr", 0)
    fii_sig     = report.get("fii_signal", "FII_NEUTRAL")
    fii_icon    = "📈" if fii_sig == "FII_BULLISH" else ("📉" if fii_sig == "FII_BEARISH" else "➡️")
    fii_date    = report.get("fii_data_date", "")
    # FII F&O futures (gold standard)
    fii_fut_sig = report.get("fii_fut_signal", "FII_FUT_NEUTRAL")
    fii_fut_net = report.get("fii_fut_net", 0)
    fut_icon    = "🟢" if fii_fut_sig == "FII_FUT_LONG" else ("🔴" if fii_fut_sig == "FII_FUT_SHORT" else "⚪")

    # Option chain
    pcr        = report.get("pcr", 1.0)
    pcr_sig    = report.get("pcr_signal", "PCR_NEUTRAL")
    max_pain   = report.get("max_pain", 0)
    call_wall  = report.get("call_wall", 0)
    put_wall   = report.get("put_wall",  0)
    atm_iv     = report.get("atm_iv", 0.0)
    vix        = report.get("vix", 0.0)

    # Strategy
    strategy   = report.get("recommended_strategy", "WAIT")
    strat_note = report.get("strategy_note", "")

    # Events
    events     = report.get("events_today", "")
    event_line = f"\n⚠️ <b>Events today:</b> {events}" if events else ""

    msg = (
        f"🌅 <b>PRE-MARKET BRIEF — {day_label}</b>\n"
        f"<code>{report.get('date', '')}</code>\n"
        f"\n"
        f"━━━ INSTITUTIONAL FLOW (prev day) ━━━\n"
        f"{fut_icon} FII Futures Net: <b>{fii_fut_net:+,d} contracts</b>  ({fii_fut_sig})\n"
        f"{fii_icon} FII Cash Net:    <b>₹{fii_net:+,.0f} Cr</b>  ({fii_sig})\n"
        f"🏦 DII Cash Net:  <b>₹{dii_net:+,.0f} Cr</b>\n"
        f"<i>Data: {fii_date}</i>\n"
        f"\n"
        f"━━━ OPTION CHAIN ━━━\n"
        f"PCR: <b>{pcr:.2f}</b>  ({pcr_sig})\n"
        f"Max Pain: <b>{max_pain}</b>  (NIFTY tends to gravitate here on expiry)\n"
        f"Call Wall (resistance): <b>{call_wall}</b>\n"
        f"Put Wall  (support)   : <b>{put_wall}</b>\n"
        f"ATM IV: {atm_iv:.1f}%  |  VIX: {vix:.1f}\n"
        f"IV Skew (put−call): {report.get('iv_skew', 0.0):+.1f}  ({report.get('iv_skew_signal', '')})\n"
        f"\n"
        f"━━━ TODAY'S SETUP ━━━\n"
        f"{dow_note}\n"
        f"\n"
        f"Recommended: <b>{strategy}</b>\n"
        + (f"<i>{strat_note}</i>\n" if strat_note else "")
        + event_line
        + f"\n"
        f"\n<b>Entry window: 9:30–10:00 AM only</b>\n"
        f"Hard exit: 1:30 PM (no exceptions)\n"
        f"\n📋 <i>Open dashboard for live signals</i>"
    )

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception:
        pass


def send_intraday_alert(plan: dict):
    """
    Send the daily intraday strategy recommendation to Telegram at 9:30 AM.
    Called once per day by the scanner Lambda.
    """
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID",   "")
    if not token or not chat_id:
        return

    strat      = plan.get("strategy", "WAIT")
    emoji      = plan.get("emoji", "⏸️")
    confidence = plan.get("confidence_label", plan.get("confidence", "–"))
    reason     = plan.get("reason", "")
    risk_note  = plan.get("risk_note", "")
    sl_note    = plan.get("sl_note", "")
    tgt_note   = plan.get("target_note", "")
    entry_win  = plan.get("entry_window", "9:30–10:00 AM")
    hard_exit  = plan.get("hard_exit", "1:30 PM")
    legs_text  = plan.get("legs_text", "No action.")

    # Metrics
    vix        = plan.get("vix", 0)
    gap_pct    = plan.get("gap_pct", 0)
    or_range   = plan.get("opening_range", 0)
    iv_spread  = plan.get("iv_vix_spread", 0)
    atm        = plan.get("atm_strike", "–")

    legs_lines = ""
    for leg in plan.get("legs", []):
        icon = "✅" if leg["action"] == "BUY" else "🔴"
        legs_lines += f"  {icon} <b>{leg['action']}</b> {leg['qty']}× {leg['label']}\n"

    msg = (
        f"{emoji} <b>INTRADAY PLAN — 09:30 AM</b>\n"
        f"<code>{plan.get('generated_at', '')[:19]}</code>\n"
        f"\n"
        f"<b>Strategy:</b> {strat}\n"
        f"<b>Confidence:</b> {confidence}\n"
        f"\n"
        f"<b>Reason:</b>\n{reason}\n"
        f"\n"
        + (f"<b>Legs:</b>\n{legs_lines}\n" if legs_lines else "")
        + (f"<b>SL:</b> {sl_note}\n"       if sl_note  else "")
        + (f"<b>Target:</b> {tgt_note}\n"  if tgt_note else "")
        + f"\n"
        f"<b>Entry window:</b> {entry_win} only\n"
        f"<b>Hard exit:</b> {hard_exit} (no exceptions)\n"
        f"\n"
        f"📊 <i>Market snapshot:</i>\n"
        f"  VIX: {vix:.1f}  |  Gap: {gap_pct:+.2f}%  |  OR: {or_range:.0f} pts\n"
        f"  IV–VIX spread: {iv_spread:+.1f}  |  ATM: {atm}\n"
        + (f"\n⚠️ <i>{risk_note}</i>" if risk_note else "")
    )

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception:
        pass


def send_margin_alert(margin_pct: float, available: float, capital: float):
    """
    Alert when margin utilization exceeds 60%.
    Cooldown: once per hour via MARGIN_ALERT_TS in DynamoDB config.
    """
    import boto3
    from datetime import datetime
    import pytz
    IST = pytz.timezone("Asia/Kolkata")

    # Hourly cooldown check
    try:
        region  = os.environ.get("AWS_REGION_NAME", "ap-south-1")
        cfg_tbl = boto3.resource("dynamodb", region_name=region).Table(
            os.environ.get("CONFIG_TABLE", "nifty_config")
        )
        last_ts_item = cfg_tbl.get_item(Key={"config_key": "MARGIN_ALERT_TS"}).get("Item", {})
        last_ts = last_ts_item.get("config_value", "")
        if last_ts:
            last_dt = datetime.fromisoformat(last_ts)
            if last_dt.tzinfo is None:
                last_dt = IST.localize(last_dt)
            if (datetime.now(IST) - last_dt).total_seconds() < 3600:
                return  # cooldown not expired
        cfg_tbl.put_item(Item={"config_key": "MARGIN_ALERT_TS", "config_value": datetime.now(IST).isoformat()})
    except Exception:
        pass

    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID",   "")
    if not token or not chat_id:
        return

    msg = (
        f"⚠️ <b>MARGIN ALERT</b>\n"
        f"Margin used: <b>{margin_pct:.1f}%</b>\n"
        f"Available  : ₹{available:,.0f}\n"
        f"Capital    : ₹{capital:,.0f}\n"
        f"\n<i>Consider reducing positions to free up margin.</i>"
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception:
        pass


def send_delta_alert(delta: float, positions: list):
    """Alert when portfolio net delta is too high (directional risk)."""
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID",   "")
    if not token or not chat_id:
        return

    pos_lines = ""
    for p in positions[:5]:   # show up to 5 positions
        pos_lines += f"  • {p.get('trade_type','')} | Δ={p.get('delta', '–')}\n"

    msg = (
        f"⚠️ <b>PORTFOLIO DELTA ALERT</b>\n"
        f"Net delta: <b>{delta:+.3f} per lot</b>\n"
        f"Threshold: ±0.30 per lot\n"
        f"\n<b>Open positions:</b>\n{pos_lines}"
        f"\n<i>Hedge required — consider buying/selling a futures leg.</i>"
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
