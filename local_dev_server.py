#!/usr/bin/env python3
"""
Local dev server for NIFTY Spread Bot dashboard.
Serves the HTML at / and provides mock API responses at /api/*

Usage:
    python3 local_dev_server.py
Then open: http://localhost:8080
"""

import json
import random
import math
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import os

PORT = 8080
DASHBOARD = os.path.join(os.path.dirname(__file__), "dashboard", "index.html")

# ─── Mock data generators ──────────────────────────────────────────────────

def _ts(minutes_ago=0):
    t = datetime.now() - timedelta(minutes=minutes_ago)
    return t.strftime("%Y-%m-%dT%H:%M:%S+05:30")

def _signals(limit=60):
    spot = 24350.0
    signals = []
    for i in range(min(limit, 60)):
        t = i * 0.5
        cd = round(12.5 + 8 * math.sin(t / 3) + random.uniform(-2, 2), 2)
        zscore = round(cd / 7.5 + random.uniform(-0.3, 0.3), 4)
        vix = round(17.5 + 2 * math.sin(t / 5) + random.uniform(-0.5, 0.5), 2)
        regime = "HALT" if vix >= 25 else ("CAUTION" if vix >= 18 else "SAFE")
        avg_iv = round(vix + random.uniform(-5, 7), 1)
        iv_spread = round(avg_iv - vix, 2)
        signals.append({
            "timestamp":        _ts(i * 0.5),
            "near_symbol":      "NIFTYJUN25FUT",
            "next_symbol":      "NIFTYJUL25FUT",
            "far_symbol":       "NIFTYAUG25FUT",
            "near_price":       str(round(spot - 5 + random.uniform(-3, 3), 2)),
            "next_price":       str(round(spot + 60 + random.uniform(-3, 3), 2)),
            "far_price":        str(round(spot + 125 + random.uniform(-3, 3), 2)),
            "spot_price":       str(round(spot + random.uniform(-10, 10), 2)),
            "spread1":          str(round(65 + random.uniform(-5, 5), 2)),
            "spread2":          str(round(65 + random.uniform(-5, 5) - cd, 2)),
            "curve_diff":       str(cd),
            "zscore":           str(zscore),
            "spread_signal":    ("SELL_BUTTERFLY" if zscore > 2 else ("BUY_BUTTERFLY" if zscore < -2 else "NONE")),
            "signal_strength":  str(round(min(5, abs(zscore) * 1.5), 1)),
            "recommended_qty":  str(75 if abs(zscore) > 2 else 0),
            "stoploss_diff":    str(round(cd - 15, 2)),
            "target_diff":      str(round(cd - 5, 2)),
            "days_to_expiry":   "18",
            "expiry_bias":      "NEUTRAL",
            "near_oi":          str(random.randint(800000, 1200000)),
            "next_oi":          str(random.randint(600000, 900000)),
            "oi_diff":          str(random.randint(100000, 300000)),
            "rollover_signal":  random.choice(["ROLL_IN", "ROLL_OUT", "NEUTRAL"]),
            "vol_ratio":        str(round(random.uniform(0.8, 1.4), 2)),
            "volume_signal":    random.choice(["HIGH_NEAR", "HIGH_NEXT", "NEUTRAL"]),
            "arbitrage_signal": "NONE",
            "arb_mispricing":   "3.5",
            "arb_stoploss":     "0",
            "arb_target":       "0",
            "call_symbol":      "NIFTY25JUN24400CE",
            "put_symbol":       "NIFTY25JUN24400PE",
            "india_vix":        str(vix),
            "vix_level":        ("HIGH" if vix >= 25 else ("ELEVATED" if vix >= 18 else "NORMAL")),
            "regime":           regime,
            "optimal_window":   "True" if 10 <= 18 <= 25 else "False",
            "block_reasons":    ("VIX at " + str(vix) + " — high volatility" if regime == "HALT" else ""),
            "regime_warnings":  ("Position size reduced 50%" if regime == "CAUTION" else ""),
            "trading_regime":   random.choice(["SPREAD", "SPREAD", "VOLATILITY"]),
            "intraday_range_pct": str(round(random.uniform(0.5, 1.8), 2)),
            "spot_slope_pct":   str(round(random.uniform(-0.5, 0.5), 3)),
            "call_iv_pct":      str(round(avg_iv + random.uniform(-1, 1), 1)),
            "put_iv_pct":       str(round(avg_iv + random.uniform(-1, 1), 1)),
            "avg_iv_pct":       str(avg_iv),
            "iv_vix_spread":    str(iv_spread),
            "vol_signal":       ("SELL_STRADDLE" if iv_spread > 4 else ("BUY_STRADDLE" if iv_spread < -3 else "NONE")),
            "straddle_premium": str(round(random.uniform(200, 320), 2)),
            "straddle_cost_inr": str(round(random.uniform(200, 320) * 75, 0)),
            "breakeven_upper":  str(round(24400 + random.uniform(200, 320), 0)),
            "breakeven_lower":  str(round(24400 - random.uniform(200, 320), 0)),
            "capital_needed":   str(round(random.uniform(200000, 350000), 0)),
            "daily_strategy":   "TRIPLE_CALENDAR",
            "daily_plan_legs":  "  ✅ BUY  1× NIFTYJUN25FUT @ MARKET\n  🔴 SELL 2× NIFTYJUL25FUT @ MARKET\n  ✅ BUY  1× NIFTYAUG25FUT @ MARKET",
        })
    return signals

def _daily_plan():
    return {
        "strategy":        "TRIPLE_CALENDAR",
        "strategy_emoji":  "🦋",
        "reason":          (
            "Market is calm (SPREAD regime). "
            "Triple Calendar signal: SELL_BUTTERFLY | Strength 3.2/5. "
            "Next-month futures are overpriced relative to historical curve."
        ),
        "risk_note":       (
            "Calendar spread profits from time-decay convergence. "
            "Qty = 1 lot-set × 75 = 75 units per leg. "
            "Exit when curve_diff reverts to mean or at SL."
        ),
        "legs": [
            {"action": "BUY",  "qty": 1, "symbol": "NIFTYJUN25FUT", "order_type": "MARKET", "inst_type": "FUT", "expiry": "2025-06-26"},
            {"action": "SELL", "qty": 2, "symbol": "NIFTYJUL25FUT", "order_type": "MARKET", "inst_type": "FUT", "expiry": "2025-07-31"},
            {"action": "BUY",  "qty": 1, "symbol": "NIFTYAUG25FUT", "order_type": "MARKET", "inst_type": "FUT", "expiry": "2025-08-28"},
        ],
        "legs_text":       "  ✅ BUY  1× NIFTYJUN25FUT @ MARKET [exp 2025-06-26]\n  🔴 SELL 2× NIFTYJUL25FUT @ MARKET [exp 2025-07-31]\n  ✅ BUY  1× NIFTYAUG25FUT @ MARKET [exp 2025-08-28]",
        "capital_est":     112500.0,
        "capital_est_fmt": "₹1,12,500",
        "trading_regime":  "SPREAD",
        "vol_signal":      "NONE",
        "arb_signal":      "NONE",
        "spread_signal":   "SELL_BUTTERFLY",
        "strength":        3.2,
        "qty":             1,
        "vix":             17.3,
        "iv_vix_spread":   1.8,
    }

def _positions():
    return [
        {
            "position_id":       "pos_001",
            "trade_type":        "SELL_BUTTERFLY",
            "near_symbol":       "NIFTYJUN25FUT",
            "next_symbol":       "NIFTYJUL25FUT",
            "far_symbol":        "NIFTYAUG25FUT",
            "qty":               "75",
            "entry_curve_diff":  "14.50",
            "stoploss_diff":     "0.00",
            "target_diff":       "10.00",
            "timestamp":         _ts(120),
            "status":            "OPEN",
            "mode":              "PAPER",
            "product":           "CARRYFORWARD",
        }
    ]

def _orders():
    return [
        {"order_id": "PAPER_NIFTYJUN25FUT_BUY",  "timestamp": _ts(120), "tradingsymbol": "NIFTYJUN25FUT", "exchange": "NFO", "transaction_type": "BUY",  "quantity": "75",  "order_type": "MARKET", "price": "0", "product": "CARRYFORWARD", "tag": "BUTTERFLY", "status": "PAPER", "mode": "PAPER"},
        {"order_id": "PAPER_NIFTYJUL25FUT_SELL", "timestamp": _ts(120), "tradingsymbol": "NIFTYJUL25FUT", "exchange": "NFO", "transaction_type": "SELL", "quantity": "150", "order_type": "MARKET", "price": "0", "product": "CARRYFORWARD", "tag": "BUTTERFLY", "status": "PAPER", "mode": "PAPER"},
        {"order_id": "PAPER_NIFTYAUG25FUT_BUY",  "timestamp": _ts(120), "tradingsymbol": "NIFTYAUG25FUT", "exchange": "NFO", "transaction_type": "BUY",  "quantity": "75",  "order_type": "MARKET", "price": "0", "product": "CARRYFORWARD", "tag": "BUTTERFLY", "status": "PAPER", "mode": "PAPER"},
    ]

def _pnl():
    return {"date": datetime.now().strftime("%Y-%m-%d"), "realised_pnl": 3250.0, "trades": 2, "mode": "PAPER"}

def _pnl_history():
    history = []
    for i in range(30):
        d = (datetime.now() - timedelta(days=29 - i)).strftime("%Y-%m-%d")
        history.append({
            "date": d,
            "realised_pnl": round(random.uniform(-4000, 8000), 2),
            "trades": random.randint(0, 4),
            "mode": "PAPER",
        })
    return history

def _config():
    return {
        "MODE":                "PAPER",
        "BROKER":              "angel",
        "MAX_DAILY_LOSS":      "5000",
        "MAX_POSITIONS":       "3",
        "MIN_SIGNAL_STRENGTH": "2.5",
        "ZSCORE_THRESHOLD":    "2.0",
        "ZSCORE_EXIT":         "0.5",
        "LOOKBACK_WINDOW":     "50",
        "ARBITRAGE_THRESHOLD": "15",
        "SNS_EXECUTE_ENABLED": "false",
        "EVENT_DATES":         "2025-07-25,2025-08-06",
    }

def _volatility(limit=60):
    sigs = _signals(limit)
    return [{
        "timestamp":    s["timestamp"],
        "call_iv_pct":  s["call_iv_pct"],
        "put_iv_pct":   s["put_iv_pct"],
        "avg_iv_pct":   s["avg_iv_pct"],
        "iv_vix_spread": s["iv_vix_spread"],
        "india_vix":    s["india_vix"],
        "vol_signal":   s["vol_signal"],
        "trading_regime": s["trading_regime"],
    } for s in sigs]

# ─── HTTP handler ──────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silence default logging

    def _send(self, data, code=200, ct="application/json"):
        body = json.dumps(data).encode() if ct == "application/json" else data
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip("/")
        qs     = parse_qs(parsed.query)

        # Serve dashboard HTML at /
        if path in ("", "/"):
            with open(DASHBOARD, "rb") as f:
                html = f.read()
            # Inject API_BASE so it hits localhost
            html = html.replace(
                b'window.API_GATEWAY_URL || "https://YOUR_API_GATEWAY_URL"',
                b'"http://localhost:' + str(PORT).encode() + b'/api"'
            )
            self._send(html, ct="text/html; charset=utf-8")
            return

        # API routes
        limit = int(qs.get("limit", [60])[0])
        if path == "/api/signals":      self._send(_signals(limit)); return
        if path == "/api/daily-plan":   self._send(_daily_plan());   return
        if path == "/api/positions":    self._send(_positions());     return
        if path == "/api/orders":       self._send(_orders());        return
        if path == "/api/pnl":          self._send(_pnl());           return
        if path == "/api/pnl/history":  self._send(_pnl_history());   return
        if path == "/api/config":       self._send(_config());        return
        if path == "/api/volatility":   self._send(_volatility(limit)); return
        if path == "/api/auth/status":
            self._send({"connected": True, "broker": "angel (mock)", "timestamp": _ts(), "feed_token": True}); return
        if path == "/api/health":
            self._send({"status": "ok", "mode": "PAPER"}); return

        self._send({"error": "not found"}, 404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        _ = self.rfile.read(length)
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip("/")

        if path == "/api/orders":
            self._send({"order_ids": ["PAPER_MOCK_001", "PAPER_MOCK_002", "PAPER_MOCK_003"], "order_count": 3})
        elif path == "/api/positions/close":
            self._send({"order_ids": ["PAPER_CLOSE_001", "PAPER_CLOSE_002"], "order_count": 2})
        elif path == "/api/config":
            self._send({"updated": ["MODE", "BROKER"]})
        else:
            self._send({"error": "not found"}, 404)


if __name__ == "__main__":
    server = HTTPServer(("", PORT), Handler)
    print(f"\n  NIFTY Spread Bot — Local Preview")
    print(f"  ──────────────────────────────────")
    print(f"  Open:  http://localhost:{PORT}")
    print(f"  Press  Ctrl+C  to stop\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")
