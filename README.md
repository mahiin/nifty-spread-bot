# NIFTY Triple Calendar Spread Bot

A complete, serverless trading system for NIFTY 50 futures spread arbitrage.
Runs on AWS Lambda. Cost ≈ ₹300–₹500/month.

---

## What It Does

Continuously monitors the three active NIFTY futures months (Near/Next/Far).
Detects when the middle contract is mis-priced relative to the curve using
Z-score, OI divergence, volume imbalance, and expiry compression signals.
Also checks put-call parity for synthetic futures arbitrage.

Sends Telegram alerts. Can auto-execute orders (set MODE=LIVE).

---

## Strategy Summary

### Triple Calendar / Butterfly Spread

```
spread1   = Next − Near
spread2   = Far  − Next
curve_diff = spread1 − spread2

Z-score = (curve_diff − rolling_mean) / rolling_std

If Z > 2  → Next overpriced → SELL BUTTERFLY
              BUY Near,  SELL 2×Next,  BUY Far

If Z < -2 → Next underpriced → BUY BUTTERFLY
              SELL Near,  BUY 2×Next,  SELL Far
```

### Exit Conditions (Strict Stoploss)
- **Target**: curve_diff reverts 15 points in our favour
- **Stoploss**: curve_diff moves 20 points against us ← capital protection
- **Z-score revert**: Z-score crosses back through ±0.5
- **Time stop**: All positions closed by 15:10 IST

### Synthetic Futures Arbitrage (Put-Call Parity)
```
Synthetic Future = Call − Put + Strike

If Synthetic − Actual > 15 pts:
    BUY Future, SELL Call, BUY Put

If Actual − Synthetic > 15 pts:
    SELL Future, BUY Call, SELL Put
```

---

## Project Structure

```
nifty-spread-bot/
├── lambda/
│   ├── scanner/           # Main spread scanner (runs every 30s)
│   │   ├── lambda_function.py
│   │   ├── broker_client.py
│   │   ├── spread_engine.py
│   │   ├── arbitrage_engine.py
│   │   ├── alerter.py
│   │   └── requirements.txt
│   ├── executor/          # Order placer (triggered by SNS)
│   │   ├── lambda_function.py
│   │   └── requirements.txt
│   └── dashboard_api/     # REST API for dashboard
│       ├── lambda_function.py
│       └── requirements.txt
├── backtest/
│   ├── data_downloader.py # Download NSE historical futures data
│   ├── backtest_spread.py # Full strategy backtest
│   └── requirements.txt
├── ml/
│   ├── train_model.py     # Train RandomForest spread predictor
│   └── requirements.txt
├── dashboard/
│   └── index.html         # Live web dashboard (Chart.js)
├── config/
│   ├── settings.yaml
│   └── risk.yaml
└── deploy/
    ├── setup_aws.sh       # One-time AWS infra setup
    ├── deploy_lambda.sh   # Build + deploy all Lambdas
    └── upload_dashboard.sh
```

---

## AWS Architecture

```
EventBridge (every 30s)
       │
Lambda: scanner
  ├── broker API → get live prices
  ├── DynamoDB   → load history → Z-score
  ├── Compute spread + arb signals
  ├── DynamoDB   → store signal
  ├── Telegram   → alert
  └── SNS        → trigger executor (if LIVE)
       │
Lambda: executor
  ├── Risk checks (daily loss / positions)
  ├── Zerodha API → place orders
  └── DynamoDB   → record position
       │
Lambda: dashboard_api
  └── API Gateway → Web dashboard
```

**AWS Cost estimate:**
| Service | Monthly |
|---------|---------|
| Lambda  | ₹50     |
| DynamoDB| ₹100    |
| API GW  | ₹50     |
| S3      | ₹20     |
| **Total** | **₹220–₹300** |

---

## Setup – Step by Step

### Prerequisites
- AWS CLI configured (`aws configure`)
- Python 3.11+
- Zerodha Kite Connect subscription (₹2000/yr)

### Step 1 – AWS Infrastructure (one-time)

```bash
cd nifty-spread-bot
chmod +x deploy/*.sh
./deploy/setup_aws.sh
```

This creates:
- IAM role with DynamoDB + SNS + S3 permissions
- 3 DynamoDB tables: `nifty_spread_signals`, `nifty_positions`, `nifty_pnl`
- SNS topic for auto-execute
- S3 bucket for dashboard + ML model
- API Gateway (HTTP API)

### Step 2 – Set Secrets

Edit `deploy/.env` (created by setup script):
```bash
ZERODHA_API_KEY=your_api_key
ZERODHA_ACCESS_TOKEN=your_daily_token   # refresh daily
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
TRADING_CAPITAL=500000
MODE=PAPER    # change to LIVE when ready
SNS_EXECUTE_ENABLED=false   # set true for auto-execute
```

> **Never commit `.env` to git.**

### Step 3 – Deploy Lambdas

```bash
./deploy/deploy_lambda.sh
```

This builds zip packages and deploys:
- `nifty-spread-scanner` (runs every 1 min via EventBridge)
- `nifty-spread-executor` (triggered by SNS)
- `nifty-spread-dashboard_api` (behind API Gateway)

### Step 4 – Dashboard

Update the API Gateway URL in `dashboard/index.html`:
```javascript
const API_BASE = "https://YOUR_API_ID.execute-api.ap-south-1.amazonaws.com";
```

Then upload:
```bash
./deploy/upload_dashboard.sh
```

### Step 5 – Zerodha Access Token (Daily)

The access token expires every day. Automate renewal:

```python
# Run this daily at 8:30 AM via a local cron or Lambda
from kiteconnect import KiteConnect
kite = KiteConnect(api_key="YOUR_KEY")
# Generate token via kite.generate_session(request_token, api_secret)
# Then update Lambda env var:
import boto3
boto3.client('lambda').update_function_configuration(
    FunctionName='nifty-spread-scanner',
    Environment={'Variables': {'ZERODHA_ACCESS_TOKEN': new_token, ...}}
)
```

---

## Backtest

### Download Historical Data

```bash
cd backtest
pip install -r requirements.txt
python data_downloader.py --start 2020-01-01 --end 2025-01-01
```

Downloads from NSE Bhavcopy archives (~1000 trading days).

### Run Backtest

```bash
python backtest_spread.py --capital 500000 --zscore_entry 2.0 --sl 20 --tgt 15
```

Example output:
```
Total Trades   : 87
Win Rate       : 62.1%
Total P&L      : ₹4,32,000
Avg Win        : ₹9,800
Avg Loss       : ₹5,200
Max Drawdown   : 8.3%
Sharpe Ratio   : 1.42
```

---

## ML Model (Optional Upgrade)

Train a RandomForest to predict spread reversion:

```bash
cd ml
pip install -r requirements.txt
python train_model.py --lookahead 2
```

The model predicts whether `curve_diff` will narrow in the next 2 bars.
Upload `data/ml/spread_model.pkl` to S3 and enable it in the scanner.

---

## Risk Controls

| Guard | Setting |
|-------|---------|
| Daily loss limit | ₹5,000 (halt all trading) |
| Max concurrent positions | 3 butterfly sets |
| Spread stoploss | 20 points from entry |
| Spread target | 15 points from entry |
| Entry cutoff | 14:30 IST |
| Force exit | 15:10 IST |
| Min signal strength | 2.5 / 5 |

All in `config/risk.yaml` and Lambda env vars.

---

## Telegram Bot Setup

1. Message `@BotFather` on Telegram → `/newbot`
2. Copy the bot token
3. Add bot to a group or get your chat ID via `@userinfobot`
4. Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in deploy/.env

---

## Signal Strength Scoring

| Factor | Score added |
|--------|-------------|
| Z-score magnitude | 0–3 (capped) |
| OI rollover signal | +0.5 |
| Volume imbalance | +0.5 |
| 3–7 days to expiry | +0.5 |
| ≤3 days to expiry | +1.0 |
| **Max possible** | **5.0** |

Only execute if strength ≥ 2.5.

---

## Prompt for Claude / Cursor

Use this prompt to build or extend this system:

```
You are building a production-ready NIFTY 50 futures spread trading bot for AWS Lambda.

STRATEGY:
- Triple Calendar Butterfly Spread: trade relative mispricing between Near / Next / Far month NIFTY futures
- Entry signal: Z-score of curve_diff (= spread1 − spread2) exceeds ±2
- SELL BUTTERFLY when Z > 2 (Next overpriced): BUY Near, SELL 2×Next, BUY Far
- BUY  BUTTERFLY when Z < -2 (Next underpriced): SELL Near, BUY 2×Next, SELL Far
- Exit: target ±15 pts, stoploss ±20 pts on curve_diff, or Z-score revert to ±0.5
- Additional signals: OI divergence, volume imbalance, expiry compression model
- Synthetic arbitrage: check put-call parity. Trade when |Synthetic − Future| > 15 pts

TECH STACK:
- Python 3.11, AWS Lambda, DynamoDB, EventBridge, SNS, API Gateway, S3
- Broker: Zerodha Kite Connect (kiteconnect library)
- Instruments auto-detected from broker instrument dump – never hardcoded

RISK RULES (non-negotiable):
- Max daily loss: ₹5,000 → halt all trading for the day
- Spread stoploss: 20 points (curve_diff must not move further against position)
- Max 3 concurrent butterfly sets
- No new entries after 14:30 IST; force-close all by 15:10 IST
- Mode: PAPER (simulate) or LIVE (real orders). Default PAPER

ARCHITECTURE:
scanner Lambda (EventBridge 30s) → DynamoDB + Telegram + SNS → executor Lambda → orders

Please [describe your specific task here: add a feature / fix a bug / extend the ML model / etc.]

The codebase is at nifty-spread-bot/ with lambda/, backtest/, ml/, dashboard/ subdirectories.
Read existing files before making changes. Keep changes minimal and focused.
```

---

## Important Disclaimers

- This is a tool, not financial advice.
- Spread trading is not risk-free. Liquidity gaps, margin spikes, and execution slippage exist.
- Backtest results do not guarantee future performance.
- Always run in PAPER mode first for at least 2 weeks before switching to LIVE.
- Keep capital you can afford to lose in this strategy.
- Zerodha brokerage: ₹20/order × 4 legs = ₹80 per butterfly set. Factor this in.
