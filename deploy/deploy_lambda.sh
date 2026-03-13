#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
#  deploy_lambda.sh  –  Build and deploy all Lambda functions
#
#  Run:  ./deploy/deploy_lambda.sh
#  Re-run any time code changes are made.
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail
source "$(dirname "$0")/.env"

# ── SET YOUR SECRETS HERE (or export before running) ────────────────────────
# Credentials are read from Secrets Manager in production (SECRETS_NAME set
# in .env after setup_aws.sh). Set env vars here ONLY for PAPER mode testing
# without Secrets Manager.
: "${BROKER:=angel}"
: "${TELEGRAM_BOT_TOKEN:=YOUR_BOT_TOKEN}"
: "${TELEGRAM_CHAT_ID:=YOUR_CHAT_ID}"
: "${TRADING_CAPITAL:=500000}"
: "${MODE:=PAPER}"            # PAPER | LIVE
: "${SNS_EXECUTE_ENABLED:=false}"
: "${INTRADAY_AUTO_EXECUTE:=true}"   # true = paper-execute intraday plan at 9:30 AM
: "${SECRETS_NAME:=}"         # Set by setup_aws.sh. Empty = use env vars.
: "${DASHBOARD_SECRET:=}"    # Dashboard password (empty = no auth, for backward compat)

# Angel One / Zerodha credentials (only needed if NOT using Secrets Manager)
: "${ANGEL_API_KEY:=}"
: "${ANGEL_CLIENT_ID:=}"
: "${ANGEL_PASSWORD:=}"
: "${ANGEL_TOTP_SECRET:=}"
: "${ZERODHA_API_KEY:=}"
: "${ZERODHA_ACCESS_TOKEN:=}"

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BUILD_DIR="$ROOT/build"
mkdir -p "$BUILD_DIR"

echo "========================================================"
echo "  Deploying NIFTY Spread Bot Lambdas"
echo "  Mode: $MODE   Capital: ₹$TRADING_CAPITAL"
echo "  Secrets Manager: ${SECRETS_NAME:-disabled (using env vars)}"
echo "========================================================"

# ─── Helper to zip a Lambda ──────────────────────────────────────────────────
build_zip() {
  local NAME=$1
  local SRC="$ROOT/lambda/$NAME"
  local WORK="$BUILD_DIR/$NAME"
  rm -rf "$WORK" && mkdir -p "$WORK"

  # Install deps into work dir — force Linux wheels (Lambda runs on manylinux)
  pip install -r "$SRC/requirements.txt" -t "$WORK" -q \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 311 \
    --only-binary=:all:
  # Copy source files
  cp "$SRC"/*.py "$WORK"/
  # Copy shared modules from scanner to lambdas that need them
  if [ "$NAME" = "executor" ] || [ "$NAME" = "dashboard_api" ] || [ "$NAME" = "token_refresh" ]; then
    cp "$ROOT/lambda/scanner/broker_client.py" "$WORK"/ 2>/dev/null || true
  fi
  # Executor uses the shared alerter module
  if [ "$NAME" = "executor" ]; then
    cp "$ROOT/lambda/scanner/alerter.py" "$WORK"/ 2>/dev/null || true
  fi
  # Zip
  (cd "$WORK" && zip -r "$BUILD_DIR/$NAME.zip" . -q)
  echo "  Built: $BUILD_DIR/$NAME.zip"
}

# ─── Shared env vars ──────────────────────────────────────────────────────────
# NOTE: Sensitive credentials should be stored in Secrets Manager (SECRETS_MANAGER_NAME).
# Setting them here in env vars is a fallback for PAPER mode / local testing.
COMMON_ENV="Variables={
  BROKER=$BROKER,
  ANGEL_API_KEY=$ANGEL_API_KEY,
  ANGEL_CLIENT_ID=$ANGEL_CLIENT_ID,
  ANGEL_PASSWORD=$ANGEL_PASSWORD,
  ANGEL_TOTP_SECRET=$ANGEL_TOTP_SECRET,
  ZERODHA_API_KEY=$ZERODHA_API_KEY,
  ZERODHA_ACCESS_TOKEN=$ZERODHA_ACCESS_TOKEN,
  TELEGRAM_BOT_TOKEN=$TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID=$TELEGRAM_CHAT_ID,
  TRADING_CAPITAL=$TRADING_CAPITAL,
  MODE=$MODE,
  UNDERLYING=NIFTY,
  LOT_SIZE=75,
  ZSCORE_THRESHOLD=2.0,
  ZSCORE_EXIT=0.5,
  LOOKBACK_WINDOW=50,
  ARBITRAGE_THRESHOLD=15,
  MAX_DAILY_LOSS=5000,
  MAX_POSITIONS=3,
  MIN_SIGNAL_STRENGTH=2.5,
  MIN_MARGIN_BUFFER=50000,
  SNS_EXECUTE_ENABLED=$SNS_EXECUTE_ENABLED,
  INTRADAY_AUTO_EXECUTE=$INTRADAY_AUTO_EXECUTE,
  SNS_EXECUTE_TOPIC_ARN=$SNS_ARN,
  DYNAMODB_SIGNALS_TABLE=nifty_spread_signals,
  POSITIONS_TABLE=nifty_positions,
  PNL_TABLE=nifty_pnl,
  ORDERS_TABLE=nifty_orders,
  CONFIG_TABLE=nifty_config,
  AWS_REGION_NAME=$REGION,
  SECRETS_MANAGER_NAME=$SECRETS_NAME,
  DASHBOARD_SECRET=$DASHBOARD_SECRET
}"

# ─── Deploy or update a Lambda ───────────────────────────────────────────────
deploy_fn() {
  local NAME=$1
  local HANDLER=$2
  local TIMEOUT=$3
  local MEMORY=$4
  local ZIP="$BUILD_DIR/$NAME.zip"
  local FN_NAME="nifty-spread-$NAME"
  local EXTRA_ARGS="${5:-}"
  local S3_KEY="lambda/$NAME.zip"

  # Upload zip to S3 (avoids 70 MB direct-upload limit)
  echo "  Uploading $NAME.zip to s3://$BUCKET/$S3_KEY"
  aws s3 cp "$ZIP" "s3://$BUCKET/$S3_KEY" --region "$REGION" --no-cli-pager

  # Check if function exists
  if aws lambda get-function --function-name "$FN_NAME" --region "$REGION" &>/dev/null; then
    echo "  Updating function: $FN_NAME"
    aws lambda update-function-code \
      --function-name "$FN_NAME" \
      --s3-bucket "$BUCKET" \
      --s3-key "$S3_KEY" \
      --region "$REGION" --no-cli-pager
    aws lambda wait function-updated \
      --function-name "$FN_NAME" \
      --region "$REGION"
    aws lambda update-function-configuration \
      --function-name "$FN_NAME" \
      --environment "$COMMON_ENV" \
      --timeout "$TIMEOUT" \
      --memory-size "$MEMORY" \
      --region "$REGION" --no-cli-pager
  else
    echo "  Creating function: $FN_NAME"
    aws lambda create-function \
      --function-name "$FN_NAME" \
      --runtime python3.11 \
      --role "$ROLE_ARN" \
      --handler "$HANDLER" \
      --code "S3Bucket=$BUCKET,S3Key=$S3_KEY" \
      --timeout "$TIMEOUT" \
      --memory-size "$MEMORY" \
      --environment "$COMMON_ENV" \
      --region "$REGION" --no-cli-pager \
      $EXTRA_ARGS
  fi
  echo "  Done: $FN_NAME"
}

# ─── Build all ───────────────────────────────────────────────────────────────
echo ""
echo "Building Lambda packages..."
build_zip "scanner"
build_zip "executor"
build_zip "dashboard_api"
build_zip "token_refresh"
build_zip "alerter_lambda"

# ─── Deploy all ──────────────────────────────────────────────────────────────
echo ""
echo "Deploying Lambda functions..."
deploy_fn "scanner"        "lambda_function.lambda_handler" 30 512
deploy_fn "executor"       "lambda_function.lambda_handler" 20 256
deploy_fn "dashboard_api"  "lambda_function.lambda_handler" 10 128
deploy_fn "token_refresh"  "lambda_function.lambda_handler" 30 128
deploy_fn "alerter_lambda" "lambda_function.lambda_handler" 10 128

SCANNER_ARN=$(aws lambda get-function --function-name "nifty-spread-scanner" \
  --region "$REGION" --query 'Configuration.FunctionArn' --output text)
EXEC_ARN=$(aws lambda get-function --function-name "nifty-spread-executor" \
  --region "$REGION" --query 'Configuration.FunctionArn' --output text)
TOKEN_ARN=$(aws lambda get-function --function-name "nifty-spread-token_refresh" \
  --region "$REGION" --query 'Configuration.FunctionArn' --output text)
ALERTER_ARN=$(aws lambda get-function --function-name "nifty-spread-alerter_lambda" \
  --region "$REGION" --query 'Configuration.FunctionArn' --output text)

# ─── Attach DLQ to scanner Lambda ─────────────────────────────────────────────
echo ""
echo "Attaching Dead Letter Queue to scanner Lambda..."
aws lambda update-function-configuration \
  --function-name "nifty-spread-scanner" \
  --dead-letter-config "TargetArn=$DLQ_ARN" \
  --region "$REGION" --no-cli-pager && echo "  DLQ attached to scanner" || echo "  Warning: DLQ attach failed"

# ─── EventBridge schedule (scanner every 1 min, Mon–Fri, IST market hours) ──
# Cron runs in UTC. IST = UTC+5:30
#   09:00 IST = 03:30 UTC  |  15:35 IST = 10:05 UTC
# cron(minute  hour  day  month  weekday  year)
# We use 3-10 UTC to cover 08:30–16:29 IST — Lambda's own is_market_open()
# enforces the precise 09:15–15:30 boundary and skips NSE holidays.
echo ""
echo "Creating EventBridge schedule (scanner: every 1 min, Mon-Fri, 03:30-10:05 UTC)..."
RULE_ARN=$(aws events put-rule \
  --name "nifty-spread-scan-1m" \
  --schedule-expression "cron(0/1 3-10 ? * MON-FRI *)" \
  --state ENABLED \
  --region "$REGION" \
  --query RuleArn --output text)

aws lambda add-permission \
  --function-name "nifty-spread-scanner" \
  --statement-id "nifty-spread-eventbridge" \
  --action "lambda:InvokeFunction" \
  --principal "events.amazonaws.com" \
  --source-arn "$RULE_ARN" \
  --region "$REGION" 2>/dev/null || true

aws events put-targets \
  --rule "nifty-spread-scan-1m" \
  --targets "Id=1,Arn=$SCANNER_ARN" \
  --region "$REGION" --no-cli-pager

# ─── EventBridge schedule (token refresh: daily 8:00 AM IST = 2:30 UTC Mon-Fri) ─
echo ""
echo "Creating EventBridge schedule (token-refresh: 8:00 AM IST Mon-Fri)..."
# 2:30 UTC = 8:00 AM IST. Must run BEFORE scanner starts at 3:00 UTC (8:30 IST)
# to avoid race condition where scanner finds no valid cached token and hits TOTP
# rate limits with concurrent fresh-login attempts.
TOKEN_RULE_ARN=$(aws events put-rule \
  --name "nifty-spread-token-refresh" \
  --schedule-expression "cron(30 2 ? * MON-FRI *)" \
  --state ENABLED \
  --region "$REGION" \
  --query RuleArn --output text)

aws lambda add-permission \
  --function-name "nifty-spread-token_refresh" \
  --statement-id "nifty-token-eventbridge" \
  --action "lambda:InvokeFunction" \
  --principal "events.amazonaws.com" \
  --source-arn "$TOKEN_RULE_ARN" \
  --region "$REGION" 2>/dev/null || true

aws events put-targets \
  --rule "nifty-spread-token-refresh" \
  --targets "Id=1,Arn=$TOKEN_ARN" \
  --region "$REGION" --no-cli-pager

# ─── SNS → Executor subscription ─────────────────────────────────────────────
echo ""
echo "Subscribing executor to SNS execute topic..."
aws sns subscribe \
  --topic-arn "$SNS_ARN" \
  --protocol lambda \
  --notification-endpoint "$EXEC_ARN" \
  --region "$REGION" --no-cli-pager 2>/dev/null || true

aws lambda add-permission \
  --function-name "nifty-spread-executor" \
  --statement-id "nifty-spread-sns" \
  --action "lambda:InvokeFunction" \
  --principal "sns.amazonaws.com" \
  --source-arn "$SNS_ARN" \
  --region "$REGION" 2>/dev/null || true

# ─── SNS → Alerter Lambda subscription ───────────────────────────────────────
echo ""
echo "Subscribing alerter_lambda to SNS alerts topic..."
aws sns subscribe \
  --topic-arn "$ALERTS_SNS_ARN" \
  --protocol lambda \
  --notification-endpoint "$ALERTER_ARN" \
  --region "$REGION" --no-cli-pager 2>/dev/null || true

aws lambda add-permission \
  --function-name "nifty-spread-alerter_lambda" \
  --statement-id "nifty-spread-alerts-sns" \
  --action "lambda:InvokeFunction" \
  --principal "sns.amazonaws.com" \
  --source-arn "$ALERTS_SNS_ARN" \
  --region "$REGION" 2>/dev/null || true

echo "  CloudWatch Alarms → $ALERTS_SNS_ARN → alerter_lambda → Telegram ✓"

# ─── API Gateway integration ─────────────────────────────────────────────────
echo ""
echo "Wiring API Gateway..."
API_ARN=$(aws lambda get-function --function-name "nifty-spread-dashboard_api" \
  --region "$REGION" --query 'Configuration.FunctionArn' --output text)

aws lambda add-permission \
  --function-name "nifty-spread-dashboard_api" \
  --statement-id "nifty-spread-apigw" \
  --action "lambda:InvokeFunction" \
  --principal "apigateway.amazonaws.com" \
  --region "$REGION" 2>/dev/null || true

INTEG_ID=$(aws apigatewayv2 create-integration \
  --api-id "$API_ID" \
  --integration-type AWS_PROXY \
  --integration-uri "$API_ARN" \
  --payload-format-version "2.0" \
  --region "$REGION" \
  --query IntegrationId --output text 2>/dev/null || echo "exists")

if [ "$INTEG_ID" != "exists" ]; then
  for METHOD in "GET" "POST"; do
    aws apigatewayv2 create-route \
      --api-id "$API_ID" \
      --route-key "$METHOD /{proxy+}" \
      --target "integrations/$INTEG_ID" \
      --region "$REGION" --no-cli-pager 2>/dev/null || true
  done

  aws apigatewayv2 create-stage \
    --api-id "$API_ID" \
    --stage-name "\$default" \
    --auto-deploy \
    --region "$REGION" --no-cli-pager 2>/dev/null || true
fi

# ─── Summary ─────────────────────────────────────────────────────────────────
API_URL="https://$API_ID.execute-api.$REGION.amazonaws.com"
echo ""
echo "========================================================"
echo "  Deployment complete!"
echo ""
echo "  Lambdas deployed:"
echo "    nifty-spread-scanner       (every 1 min, Mon-Fri market hours)"
echo "    nifty-spread-executor      (SNS-triggered on strong signals)"
echo "    nifty-spread-dashboard_api (API Gateway)"
echo "    nifty-spread-token-refresh (daily 8:30 AM IST, Mon-Fri)"
echo "    nifty-spread-alerter-lambda (CloudWatch alarms → Telegram)"
echo ""
echo "  Alert pipeline:"
echo "    CloudWatch → nifty-spread-alerts SNS → alerter_lambda → Telegram"
echo ""
echo "  API Gateway URL:  $API_URL"
echo "  Endpoints:"
echo "    $API_URL/signals"
echo "    $API_URL/positions"
echo "    $API_URL/pnl"
echo "    $API_URL/daily-plan"
echo "    $API_URL/volatility"
echo ""
echo "  Next: update dashboard/index.html API_BASE with:"
echo "    $API_URL"
echo "  Then run: ./deploy/upload_dashboard.sh"
echo "========================================================"
