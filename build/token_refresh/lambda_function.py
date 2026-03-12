"""
Token Refresh Lambda
====================
Runs daily at 8:30 AM IST (3:00 UTC) Mon–Fri.
Pre-warms the Angel One JWT session so the scanner Lambda never
needs to perform a TOTP login during live market hours.

Flow:
  1. Force a fresh TOTP login (ignoring any cached token)
  2. Save the new JWT to DynamoDB nifty_config[ANGEL_JWT_TOKEN]
  3. Send a Telegram confirmation or error alert

Env vars required (same as scanner):
  BROKER              – angel | zerodha
  ANGEL_API_KEY
  ANGEL_CLIENT_ID
  ANGEL_PASSWORD
  ANGEL_TOTP_SECRET
  TELEGRAM_BOT_TOKEN  (optional – for status alerts)
  TELEGRAM_CHAT_ID
  SECRETS_MANAGER_NAME (optional – to pull secrets from AWS Secrets Manager)
"""

import json
import os
from datetime import datetime, timedelta

import boto3
import pytz
import requests

REGION  = os.environ.get("AWS_REGION_NAME", "ap-south-1")
IST     = pytz.timezone("Asia/Kolkata")
BROKER  = os.environ.get("BROKER", "angel").lower()

_DDB_TOKEN_KEY = "ANGEL_JWT_TOKEN"


# ─── Secrets helper ──────────────────────────────────────────────────────────

_secrets_cache: dict = {}


def _get_secret(secret_key: str, env_fallback: str = "") -> str:
    global _secrets_cache
    if secret_key in _secrets_cache:
        return _secrets_cache[secret_key]
    secrets_name = os.environ.get("SECRETS_MANAGER_NAME", "")
    if secrets_name and not _secrets_cache:
        try:
            sm = boto3.client("secretsmanager", region_name=REGION)
            resp = sm.get_secret_value(SecretId=secrets_name)
            _secrets_cache = json.loads(resp.get("SecretString", "{}"))
            if secret_key in _secrets_cache:
                return _secrets_cache[secret_key]
        except Exception as e:
            print(f"[token_refresh] Secrets Manager unavailable: {e}")
    return os.environ.get(env_fallback or secret_key, "")


# ─── Telegram alert ──────────────────────────────────────────────────────────

def _tg(msg: str) -> None:
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID",   "")
    if not token or not chat_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception:
        pass


# ─── DynamoDB helpers ─────────────────────────────────────────────────────────

def _config_table():
    ddb = boto3.resource("dynamodb", region_name=REGION)
    return ddb.Table(os.environ.get("CONFIG_TABLE", "nifty_config"))


def _save_session(session: dict) -> None:
    now_ist  = datetime.now(IST)
    midnight = now_ist.replace(hour=23, minute=55, second=0, microsecond=0)
    if now_ist >= midnight:
        midnight = midnight + timedelta(days=1)
    session["expires_at"] = midnight.isoformat()
    _config_table().put_item(Item={
        "config_key":   _DDB_TOKEN_KEY,
        "config_value": json.dumps(session),
    })
    print(f"[token_refresh] Token saved. Expires at {session['expires_at']}")


# ─── Angel One login ──────────────────────────────────────────────────────────

def _angel_fresh_login() -> dict:
    """Do a fresh TOTP login and return the session dict."""
    import pyotp

    api_key    = _get_secret("ANGEL_API_KEY",     "ANGEL_API_KEY")
    client_id  = _get_secret("ANGEL_CLIENT_ID",   "ANGEL_CLIENT_ID")
    password   = _get_secret("ANGEL_PASSWORD",    "ANGEL_PASSWORD")
    totp_secret = _get_secret("ANGEL_TOTP_SECRET", "ANGEL_TOTP_SECRET")

    if not all([api_key, client_id, password, totp_secret]):
        raise ValueError("Missing Angel One credentials")

    totp = pyotp.TOTP(totp_secret).now()

    resp = requests.post(
        "https://apiconnect.angelbroking.com/rest/auth/angelbroking/user/v1/loginByPassword",
        headers={
            "Content-Type":  "application/json",
            "Accept":        "application/json",
            "X-UserType":    "USER",
            "X-SourceID":    "WEB",
            "X-ClientLocalIP": "127.0.0.1",
            "X-ClientPublicIP": "127.0.0.1",
            "X-MACAddress":  "AA:BB:CC:DD:EE:FF",
            "X-PrivateKey":  api_key,
        },
        json={
            "clientcode": client_id,
            "password":   password,
            "totp":       totp,
        },
        timeout=10,
    )
    data = resp.json()
    if not data.get("status") or not data.get("data", {}).get("jwtToken"):
        raise RuntimeError(f"Angel One login failed: {data.get('message', data)}")

    return {
        "jwtToken":         data["data"]["jwtToken"],
        "refreshToken":     data["data"].get("refreshToken", ""),
        "feedToken":        data["data"].get("feedToken",    ""),
        "api_key":          api_key,
        "client_id":        client_id,
    }


# ─── Main handler ─────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M IST")
    print(f"[token_refresh] Starting token refresh — {now_ist}")

    if BROKER != "angel":
        print(f"[token_refresh] Broker is '{BROKER}' — no token refresh needed. Exiting.")
        return {"statusCode": 200, "body": f"Broker={BROKER}, nothing to do"}

    try:
        session = _angel_fresh_login()
        _save_session(session)

        msg = (
            f"✅ <b>Token Refreshed</b>\n"
            f"<code>{now_ist}</code>\n"
            f"Angel One JWT saved to DynamoDB.\n"
            f"Scanner is ready for today's market session."
        )
        _tg(msg)
        print("[token_refresh] Success")
        return {"statusCode": 200, "body": "Token refreshed successfully"}

    except Exception as exc:
        err_msg = str(exc)
        print(f"[token_refresh] ERROR: {err_msg}")
        _tg(
            f"🚨 <b>Token Refresh FAILED</b>\n"
            f"<code>{now_ist}</code>\n"
            f"<code>{err_msg[:400]}</code>\n"
            f"⚠️ Scanner may fail to authenticate today!"
        )
        return {"statusCode": 500, "body": err_msg}
