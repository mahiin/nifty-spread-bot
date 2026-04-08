"""
Broker API abstraction layer.
Supports Zerodha Kite Connect and Angel One SmartAPI.

Angel One token caching:
  JWT tokens expire at midnight IST. Instead of a full TOTP login on every
  Lambda invocation (which would hit rate limits at 60 req/hour), we cache
  the token in DynamoDB nifty_config[ANGEL_JWT_TOKEN]. A separate
  token_refresh Lambda refreshes it once per day at 08:30 IST.

Secrets Manager:
  ANGEL_PASSWORD and ANGEL_TOTP_SECRET are read from AWS Secrets Manager
  when SECRETS_MANAGER_NAME env var is set, falling back to plain env vars.
"""
import io
import json
import os
from datetime import datetime, timedelta

import boto3
import pyotp
import pytz
import requests

IST = pytz.timezone("Asia/Kolkata")

# ─── Secrets Manager helper ────────────────────────────────────────────────

_secrets_cache: dict = {}
_secrets_fetched_at: float = 0.0
_SECRETS_TTL: float = 300.0  # re-fetch from Secrets Manager every 5 minutes

def _get_secret(secret_key: str, env_fallback: str) -> str:
    """
    Read a secret from AWS Secrets Manager if SECRETS_MANAGER_NAME is set,
    otherwise fall back to the environment variable.
    Cache is refreshed every 5 minutes so credential updates take effect quickly.
    """
    import time
    global _secrets_fetched_at
    cache_expired = (time.time() - _secrets_fetched_at) > _SECRETS_TTL
    if secret_key in _secrets_cache and not cache_expired:
        return _secrets_cache[secret_key]

    secret_name = os.environ.get("SECRETS_MANAGER_NAME", "")
    if secret_name:
        try:
            region = os.environ.get("AWS_REGION_NAME", "ap-south-1")
            sm   = boto3.client("secretsmanager", region_name=region)
            resp = sm.get_secret_value(SecretId=secret_name)
            data = json.loads(resp["SecretString"])
            # Cache all secrets from this response
            _secrets_cache.update(data)
            _secrets_fetched_at = time.time()
            if secret_key in _secrets_cache:
                return _secrets_cache[secret_key]
        except Exception as e:
            print(f"[broker_client] Secrets Manager fetch failed ({e}), using env var")

    value = os.environ.get(env_fallback, "")
    _secrets_cache[secret_key] = value
    return value


# ─── Angel One token caching in DynamoDB ──────────────────────────────────

_DDB_TOKEN_KEY = "ANGEL_JWT_TOKEN"


def _get_token_table():
    region = os.environ.get("AWS_REGION_NAME", "ap-south-1")
    cfg_tbl = os.environ.get("CONFIG_TABLE", "nifty_config")
    return boto3.resource("dynamodb", region_name=region).Table(cfg_tbl)


def _load_cached_angel_session() -> dict | None:
    """
    Load JWT token cached in DynamoDB. Returns None if missing or expired.
    Token is considered expired 5 minutes before midnight IST.
    """
    try:
        resp = _get_token_table().get_item(Key={"config_key": _DDB_TOKEN_KEY})
        item = resp.get("Item")
        if not item:
            return None
        data = json.loads(item.get("config_value", "{}"))
        expires_at_str = data.get("expires_at", "")
        if not expires_at_str:
            return None
        expires_at = datetime.fromisoformat(expires_at_str)
        # If we have more than 5 minutes before expiry, use the cached token
        if datetime.now(IST) < expires_at - timedelta(minutes=5):
            return data
        return None
    except Exception as e:
        print(f"[broker_client] Could not load cached token ({e})")
        return None


def save_angel_session(session: dict) -> None:
    """
    Save Angel One JWT session to DynamoDB.
    Called by token_refresh Lambda and on first login.
    Token expires at 23:55 IST (5 min buffer before midnight).
    """
    try:
        now_ist  = datetime.now(IST)
        midnight = now_ist.replace(hour=23, minute=55, second=0, microsecond=0)
        if now_ist >= midnight:
            # After 23:55, set expiry to midnight + 1 min (next day)
            midnight = midnight + timedelta(days=1)
        session["expires_at"] = midnight.isoformat()
        _get_token_table().put_item(Item={
            "config_key":   _DDB_TOKEN_KEY,
            "config_value": json.dumps(session),
        })
    except Exception as e:
        print(f"[broker_client] Could not save token ({e})")


# ─── Zerodha ───────────────────────────────────────────────────────────────

class ZerodhaBroker:
    """Zerodha Kite Connect API client."""

    BASE_URL = "https://api.kite.trade"

    def __init__(self):
        self.api_key      = os.environ["ZERODHA_API_KEY"]
        self.access_token = os.environ["ZERODHA_ACCESS_TOKEN"]
        self.headers = {
            "X-Kite-Version": "3",
            "Authorization":  f"token {self.api_key}:{self.access_token}",
        }

    def _get(self, path, params=None):
        r = requests.get(f"{self.BASE_URL}{path}", headers=self.headers,
                         params=params, timeout=5)
        r.raise_for_status()
        return r.json().get("data", {})

    def get_instruments(self, exchange="NFO"):
        import pandas as pd
        r = requests.get(f"{self.BASE_URL}/instruments/{exchange}",
                         headers=self.headers, timeout=10)
        r.raise_for_status()
        return pd.read_csv(io.StringIO(r.text))

    def get_quote(self, instruments: list) -> dict:
        return self._get("/quote", {"i": instruments})

    def get_ohlc(self, instruments: list) -> dict:
        return self._get("/quote/ohlc", {"i": instruments})

    def get_ltp(self, instruments: list) -> dict:
        return self._get("/quote/ltp", {"i": instruments})

    def get_funds(self) -> dict:
        """Return available cash and margin."""
        try:
            data = self._get("/user/margins")
            equity = data.get("equity", {})
            return {
                "available_cash":   float(equity.get("available", {}).get("cash", 0) or 0),
                "used_margin":      float(equity.get("utilised", {}).get("debits", 0) or 0),
            }
        except Exception:
            return {"available_cash": 0.0, "used_margin": 0.0}

    def place_order(self, tradingsymbol: str, exchange: str,
                    transaction_type: str, quantity: int,
                    order_type: str = "MARKET", product: str = "NRML",
                    price: float = 0.0, tag: str = "") -> str:
        payload = {
            "tradingsymbol":    tradingsymbol,
            "exchange":         exchange,
            "transaction_type": transaction_type,
            "quantity":         quantity,
            "order_type":       order_type,
            "product":          product,
            "price":            price,
            "tag":              tag[:20] if tag else "",
            "validity":         "DAY",
        }
        r = requests.post(f"{self.BASE_URL}/orders/regular",
                          headers=self.headers, data=payload, timeout=5)
        r.raise_for_status()
        return r.json()["data"]["order_id"]

    def get_candles(self, token: str, exchange: str = "NFO",
                    interval: str = "FIVE_MINUTE",
                    from_dt: str = None, to_dt: str = None) -> list:
        """Stub — Zerodha candle fetch not needed; momentum engine uses Angel One only."""
        return []

    def get_positions(self) -> dict:
        return self._get("/portfolio/positions")

    def get_orders(self) -> list:
        return self._get("/orders")


# ─── Angel One ────────────────────────────────────────────────────────────

class AngelOneBroker:
    """
    Angel One SmartAPI client with JWT token caching.

    On __init__:
      1. Tries DynamoDB cache for a valid JWT.
      2. If missing/expired, does a full TOTP login and saves to cache.

    This means at most ONE login per day per account, not one per invocation.
    """

    BASE_URL = "https://apiconnect.angelbroking.com"

    def __init__(self):
        env_api_key = _get_secret("ANGEL_API_KEY", "ANGEL_API_KEY")
        # Set api_key early so _base_headers() works inside _generate_session()
        self.api_key = env_api_key

        # Try cached token first (avoids TOTP login rate limits)
        session = _load_cached_angel_session()
        if not session:
            client_id   = _get_secret("ANGEL_CLIENT_ID",   "ANGEL_CLIENT_ID")
            password    = _get_secret("ANGEL_PASSWORD",    "ANGEL_PASSWORD")
            totp_secret = _get_secret("ANGEL_TOTP_SECRET", "ANGEL_TOTP_SECRET")
            totp        = pyotp.TOTP(totp_secret).now()
            session     = self._generate_session(client_id, password, totp)
            save_angel_session(session)
            print("[broker_client] Angel One: fresh login, token cached")
        else:
            print("[broker_client] Angel One: using cached JWT token")

        # Prefer env/secrets api_key; fall back to the one stored in the cached session
        self.api_key       = env_api_key or session.get("api_key", "")
        self.jwt_token     = session["jwtToken"]
        self.refresh_token = session["refreshToken"]
        self.feed_token    = session.get("feedToken", "")

    def _generate_session(self, client_id: str, password: str, totp: str) -> dict:
        r = requests.post(
            f"{self.BASE_URL}/rest/auth/angelbroking/user/v1/loginByPassword",
            headers=self._base_headers(),
            json={"clientcode": client_id, "password": password, "totp": totp},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        if not data.get("status"):
            raise RuntimeError(f"Angel One login failed: {data.get('message')}")
        return data["data"]

    def _base_headers(self) -> dict:
        return {
            "Content-Type":     "application/json",
            "Accept":           "application/json",
            "X-UserType":       "USER",
            "X-SourceID":       "WEB",
            "X-ClientLocalIP":  "127.0.0.1",
            "X-ClientPublicIP": "127.0.0.1",
            "X-MACAddress":     "00:00:00:00:00:00",
            "X-PrivateKey":     self.api_key,
        }

    # Angel One error codes that mean the JWT has been invalidated
    _AUTH_ERROR_CODES = {"AB1010", "AB1004", "AG8001", "AG8002"}

    def _auth_headers(self) -> dict:
        return {**self._base_headers(), "Authorization": f"Bearer {self.jwt_token}"}

    def _is_auth_error(self, resp: dict) -> bool:
        """Return True if the API response indicates an auth/session failure."""
        errorcode = resp.get("errorcode") or ""
        message   = resp.get("message")  or ""
        # Explicit known auth error codes
        if str(errorcode) in self._AUTH_ERROR_CODES:
            return True
        # Angel One often returns status=false, message=None on expired JWT
        if not resp.get("status") and not message:
            return True
        return False

    def _refresh_auth(self) -> None:
        """Force a fresh TOTP login, update self.jwt_token, and save to DynamoDB."""
        print("[broker_client] Auth failure detected — forcing fresh TOTP login")
        # Invalidate DynamoDB cache so the next scanner invocation also re-logs in
        try:
            _get_token_table().delete_item(Key={"config_key": _DDB_TOKEN_KEY})
        except Exception as e:
            print(f"[broker_client] Could not clear cached token ({e})")

        client_id   = _get_secret("ANGEL_CLIENT_ID",   "ANGEL_CLIENT_ID")
        password    = _get_secret("ANGEL_PASSWORD",    "ANGEL_PASSWORD")
        totp_secret = _get_secret("ANGEL_TOTP_SECRET", "ANGEL_TOTP_SECRET")
        totp        = pyotp.TOTP(totp_secret).now()
        session     = self._generate_session(client_id, password, totp)
        save_angel_session(session)
        self.jwt_token     = session["jwtToken"]
        self.refresh_token = session["refreshToken"]
        self.feed_token    = session.get("feedToken", "")
        print("[broker_client] Fresh login successful, token updated")

    def _get(self, path: str, params: dict = None, _retry: bool = True) -> dict:
        r = requests.get(f"{self.BASE_URL}{path}", headers=self._auth_headers(),
                         params=params, timeout=5)
        r.raise_for_status()
        resp = r.json()
        if not resp.get("status"):
            if _retry and self._is_auth_error(resp):
                self._refresh_auth()
                return self._get(path, params=params, _retry=False)
            raise RuntimeError(
                f"Angel One API error: message={resp.get('message')!r} "
                f"errorcode={resp.get('errorcode')!r} path={path}"
            )
        return resp.get("data", {})

    def _post(self, path: str, payload: dict, _retry: bool = True) -> dict:
        r = requests.post(f"{self.BASE_URL}{path}", headers=self._auth_headers(),
                          json=payload, timeout=5)
        r.raise_for_status()
        resp = r.json()
        if not resp.get("status"):
            if _retry and self._is_auth_error(resp):
                self._refresh_auth()
                return self._post(path, payload=payload, _retry=False)
            raise RuntimeError(
                f"Angel One API error: message={resp.get('message')!r} "
                f"errorcode={resp.get('errorcode')!r} path={path}"
            )
        return resp.get("data", {})

    def get_instruments(self, exchange: str = "NFO"):
        import pandas as pd
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        return df[df["exch_seg"] == exchange].reset_index(drop=True)

    def get_ltp(self, instruments: list) -> dict:
        payload = {"mode": "LTP", "exchangeTokens": self._group_by_exchange(instruments)}
        data    = self._post("/rest/secure/angelbroking/market/v1/quote/", payload)
        result  = {}
        for item in data.get("fetched", []):
            key = f"{item['exchange']}:{item['tradingSymbol']}"
            result[key] = {"ltp": item["ltp"]}
        return result

    def get_quote(self, instruments: list) -> dict:
        payload = {"mode": "FULL", "exchangeTokens": self._group_by_exchange(instruments)}
        data    = self._post("/rest/secure/angelbroking/market/v1/quote/", payload)
        result  = {}
        for item in data.get("fetched", []):
            key = f"{item['exchange']}:{item['tradingSymbol']}"
            result[key] = item
        return result

    def _group_by_exchange(self, instruments: list) -> dict:
        grouped: dict = {}
        for inst in instruments:
            exch, token = inst.split(":", 1)
            grouped.setdefault(exch, []).append(token)
        return grouped

    def place_order(self, tradingsymbol: str, exchange: str,
                    transaction_type: str, quantity: int,
                    order_type: str = "MARKET", product: str = "CARRYFORWARD",
                    price: float = 0.0, tag: str = "") -> str:
        payload = {
            "variety":         "NORMAL",
            "tradingsymbol":   tradingsymbol,
            "symboltoken":     self._resolve_token(tradingsymbol, exchange),
            "transactiontype": transaction_type,
            "exchange":        exchange,
            "ordertype":       order_type,
            "producttype":     product,
            "duration":        "DAY",
            "price":           str(price),
            "squareoff":       "0",
            "stoploss":        "0",
            "quantity":        str(quantity),
        }
        data = self._post("/rest/secure/angelbroking/order/v1/placeOrder", payload)
        return data["orderid"]

    def _resolve_token(self, tradingsymbol: str, exchange: str) -> str:
        if not hasattr(self, "_instrument_cache"):
            self._instrument_cache = self.get_instruments(exchange)
        row = self._instrument_cache[self._instrument_cache["symbol"] == tradingsymbol]
        if row.empty:
            raise ValueError(f"Symbol not found: {tradingsymbol} on {exchange}")
        return str(row.iloc[0]["token"])

    def get_funds(self) -> dict:
        """Return available cash margin."""
        try:
            data = self._get("/rest/secure/angelbroking/user/v1/getRMS")
            return {
                "available_cash": float(data.get("availablecash", 0) or 0),
                "used_margin":    float(data.get("utiliseddebits", 0) or 0),
            }
        except Exception:
            return {"available_cash": 0.0, "used_margin": 0.0}

    def get_candles(self, token: str, exchange: str = "NFO",
                    interval: str = "FIVE_MINUTE",
                    from_dt: str = None, to_dt: str = None) -> list:
        """Fetch OHLCV candles via Angel One getCandleData.
        Returns [[ts, open, high, low, close, volume], ...] or [] on failure.
        """
        from datetime import datetime as _dt
        now = _dt.now(IST)
        if not to_dt:
            to_dt = now.strftime("%Y-%m-%d %H:%M")
        if not from_dt:
            market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
            from_dt = market_open.strftime("%Y-%m-%d %H:%M")
        params = {
            "exchange":    exchange,
            "symboltoken": str(token),
            "interval":    interval,
            "fromdate":    from_dt,
            "todate":      to_dt,
        }
        try:
            resp = self._post(
                "/rest/secure/angelbroking/historical/v1/getCandleData", params
            )
            # Angel One returns data directly (list of candles) on success
            if isinstance(resp, list):
                return resp
            return []
        except Exception as e:
            print(f"[broker_client] get_candles failed: {e}")
            return []

    def get_positions(self) -> dict:
        return self._get("/rest/secure/angelbroking/order/v1/getPosition")

    def get_orders(self) -> list:
        return self._get("/rest/secure/angelbroking/order/v1/getOrderBook") or []


# ─── Factory ───────────────────────────────────────────────────────────────

def get_broker():
    """Returns broker client based on BROKER env var."""
    broker_name = os.environ.get("BROKER", "zerodha").lower()
    if broker_name == "zerodha":
        return ZerodhaBroker()
    if broker_name == "angel":
        return AngelOneBroker()
    raise ValueError(f"Unsupported broker: {broker_name}")
