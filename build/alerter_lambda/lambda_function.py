"""
Alerter Lambda
==============
Receives CloudWatch Alarm notifications via SNS and forwards them
to Telegram as formatted alerts.

Subscribed to the 'nifty-spread-alerts' SNS topic.
CloudWatch Alarms → SNS → this Lambda → Telegram.

Handles:
  - ALARM state → 🚨 critical alert with details
  - OK state    → ✅ recovery notification
  - Malformed   → logs and ignores

Env vars:
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
"""

import json
import os

import requests


def _tg(msg: str) -> None:
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID",   "")
    if not token or not chat_id:
        print("[alerter_lambda] No Telegram credentials configured")
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
        if resp.status_code != 200:
            print(f"[alerter_lambda] Telegram error {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        print(f"[alerter_lambda] Telegram send failed: {e}")


def lambda_handler(event, context):
    for record in event.get("Records", []):
        try:
            raw     = record.get("Sns", {}).get("Message", "{}")
            payload = json.loads(raw)
        except Exception as e:
            print(f"[alerter_lambda] Could not parse SNS message: {e}")
            continue

        alarm_name  = payload.get("AlarmName",       "Unknown Alarm")
        state       = payload.get("NewStateValue",   "UNKNOWN")
        old_state   = payload.get("OldStateValue",   "")
        reason      = payload.get("NewStateReason",  "")
        acct_id     = payload.get("AWSAccountId",    "")
        region      = payload.get("Region",          "")
        timestamp   = payload.get("StateChangeTime", "")[:19].replace("T", " ")

        if state == "ALARM":
            icon = "🚨"
            title = "ALARM TRIGGERED"
        elif state == "OK":
            icon = "✅"
            title = "ALARM RESOLVED"
        elif state == "INSUFFICIENT_DATA":
            icon = "⚠️"
            title = "INSUFFICIENT DATA"
        else:
            icon = "📋"
            title = f"ALARM STATE: {state}"

        msg = (
            f"{icon} <b>{title}</b>\n"
            f"<b>{alarm_name}</b>\n"
            f"<code>{timestamp} UTC</code>\n"
            f"\n"
            f"State: {old_state} → <b>{state}</b>\n"
            f"Region: {region}\n"
            f"\n"
            f"<i>{reason[:400]}</i>\n"
            + (f"\n<i>Account: {acct_id}</i>" if acct_id else "")
        )

        _tg(msg)
        print(f"[alerter_lambda] Forwarded alarm '{alarm_name}' state={state} to Telegram")

    return {"statusCode": 200, "body": "OK"}
