#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
#  setup_aws.sh  –  One-time AWS infrastructure setup
#  Run this ONCE before deploying Lambda functions.
#
#  Prerequisites:
#    aws cli configured  (aws configure)
#    Python 3.11+
#    pip
#
#  Usage:
#    chmod +x deploy/setup_aws.sh
#    ./deploy/setup_aws.sh
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

REGION="ap-south-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ROLE_NAME="nifty-spread-bot-lambda-role"
POLICY_NAME="nifty-spread-bot-policy"

# Secrets Manager secret name (stores sensitive broker credentials)
SECRETS_NAME="nifty-spread-bot/credentials"

echo "========================================================"
echo "  NIFTY Spread Bot – AWS Setup"
echo "  Region  : $REGION"
echo "  Account : $ACCOUNT_ID"
echo "========================================================"

# ── IAM Role ─────────────────────────────────────────────────────────────────
echo ""
echo "1/12  Creating IAM Role: $ROLE_NAME"
aws iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document '{
    "Version":"2012-10-17",
    "Statement":[{
      "Effect":"Allow",
      "Principal":{"Service":"lambda.amazonaws.com"},
      "Action":"sts:AssumeRole"
    }]
  }' 2>/dev/null || echo "     Role already exists – skipping"

aws iam attach-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"

aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "$POLICY_NAME" \
  --policy-document '{
    "Version":"2012-10-17",
    "Statement":[
      {
        "Effect":"Allow",
        "Action":["dynamodb:PutItem","dynamodb:GetItem","dynamodb:Scan",
                  "dynamodb:UpdateItem","dynamodb:Query","dynamodb:DeleteItem"],
        "Resource":[
          "arn:aws:dynamodb:'"$REGION"':'"$ACCOUNT_ID"':table/nifty_*",
          "arn:aws:dynamodb:'"$REGION"':'"$ACCOUNT_ID"':table/nifty_*/index/*"
        ]
      },
      {
        "Effect":"Allow",
        "Action":["sns:Publish"],
        "Resource":[
          "arn:aws:sns:'"$REGION"':'"$ACCOUNT_ID"':nifty-spread-execute",
          "arn:aws:sns:'"$REGION"':'"$ACCOUNT_ID"':nifty-spread-alerts"
        ]
      },
      {
        "Effect":"Allow",
        "Action":["sqs:SendMessage","sqs:ReceiveMessage","sqs:DeleteMessage",
                  "sqs:GetQueueAttributes"],
        "Resource":"arn:aws:sqs:'"$REGION"':'"$ACCOUNT_ID"':nifty-spread-dlq"
      },
      {
        "Effect":"Allow",
        "Action":["secretsmanager:GetSecretValue"],
        "Resource":"arn:aws:secretsmanager:'"$REGION"':'"$ACCOUNT_ID"':secret:nifty-spread-bot/*"
      },
      {
        "Effect":"Allow",
        "Action":["s3:GetObject"],
        "Resource":"arn:aws:s3:::nifty-spread-bot-*/*"
      }
    ]
  }'
echo "     Done."

ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$ROLE_NAME"

# ── DynamoDB Tables ───────────────────────────────────────────────────────────
echo ""
echo "2/12  Creating DynamoDB tables"

create_table () {
  local NAME=$1
  local KEY=$2
  aws dynamodb create-table \
    --table-name "$NAME" \
    --attribute-definitions AttributeName="$KEY",AttributeType=S \
    --key-schema AttributeName="$KEY",KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region "$REGION" 2>/dev/null && echo "     Created: $NAME" || echo "     Exists : $NAME"
}

create_table "nifty_spread_signals" "timestamp"
create_table "nifty_positions"      "position_id"
create_table "nifty_pnl"            "date"
create_table "nifty_orders"         "order_id"
create_table "nifty_config"         "config_key"

# ── DynamoDB GSI on nifty_spread_signals ─────────────────────────────────────
# Adds a GSI with partition key 'pk' (fixed='SIGNAL') + sort key 'timestamp'
# so the dashboard API can do efficient range queries instead of full scans.
echo ""
echo "3/12  Adding GSI to nifty_spread_signals (signal-by-time-index)"

# Check if GSI already exists
GSI_EXISTS=$(aws dynamodb describe-table --table-name nifty_spread_signals \
  --region "$REGION" --query "Table.GlobalSecondaryIndexes[?IndexName=='signal-by-time-index'].IndexName" \
  --output text 2>/dev/null || echo "")

if [ -z "$GSI_EXISTS" ]; then
  aws dynamodb update-table \
    --table-name nifty_spread_signals \
    --attribute-definitions \
      AttributeName=pk,AttributeType=S \
      AttributeName=timestamp,AttributeType=S \
    --global-secondary-index-updates '[{
      "Create": {
        "IndexName": "signal-by-time-index",
        "KeySchema": [
          {"AttributeName":"pk","KeyType":"HASH"},
          {"AttributeName":"timestamp","KeyType":"RANGE"}
        ],
        "Projection": {"ProjectionType":"ALL"},
        "BillingMode": "PAY_PER_REQUEST"
      }
    }]' \
    --region "$REGION" 2>/dev/null && echo "     GSI created (building in background)" \
    || echo "     Warning: GSI creation failed — scan will be used as fallback"
else
  echo "     GSI already exists – skipping"
fi

# ── SNS Topic (for auto-execute) ──────────────────────────────────────────────
echo ""
echo "4/12  Creating SNS topic: nifty-spread-execute"
SNS_ARN=$(aws sns create-topic --name "nifty-spread-execute" \
           --region "$REGION" --query TopicArn --output text)
echo "     SNS ARN: $SNS_ARN"

# ── SNS Topic (for CloudWatch alarms → Telegram) ─────────────────────────────
echo ""
echo "5/12  Creating SNS topic: nifty-spread-alerts"
ALERTS_SNS_ARN=$(aws sns create-topic --name "nifty-spread-alerts" \
                  --region "$REGION" --query TopicArn --output text)
echo "     Alerts SNS ARN: $ALERTS_SNS_ARN"

# ── SQS Dead Letter Queue ─────────────────────────────────────────────────────
echo ""
echo "6/12  Creating SQS Dead Letter Queue: nifty-spread-dlq"
DLQ_URL=$(aws sqs create-queue \
  --queue-name "nifty-spread-dlq" \
  --attributes '{"MessageRetentionPeriod":"1209600"}' \
  --region "$REGION" \
  --query QueueUrl --output text 2>/dev/null || \
  aws sqs get-queue-url --queue-name "nifty-spread-dlq" \
    --region "$REGION" --query QueueUrl --output text)
DLQ_ARN=$(aws sqs get-queue-attributes \
  --queue-url "$DLQ_URL" \
  --attribute-names QueueArn \
  --region "$REGION" \
  --query Attributes.QueueArn --output text)
echo "     DLQ ARN: $DLQ_ARN"

# ── S3 Bucket (for ML model + dashboard) ─────────────────────────────────────
BUCKET="nifty-spread-bot-$ACCOUNT_ID"
echo ""
echo "7/12  Creating S3 bucket: $BUCKET"
aws s3 mb "s3://$BUCKET" --region "$REGION" 2>/dev/null || echo "     Bucket exists"
# Enable static website hosting for dashboard
aws s3 website "s3://$BUCKET" --index-document index.html 2>/dev/null || true

# ── API Gateway ───────────────────────────────────────────────────────────────
echo ""
echo "8/12  Creating API Gateway (HTTP API)"
API_ID=$(aws apigatewayv2 create-api \
  --name "nifty-spread-api" \
  --protocol-type HTTP \
  --cors-configuration "AllowOrigins=*,AllowMethods=GET POST OPTIONS,AllowHeaders=Content-Type" \
  --region "$REGION" \
  --query ApiId --output text 2>/dev/null || \
  aws apigatewayv2 get-apis --region "$REGION" \
    --query "Items[?Name=='nifty-spread-api'].ApiId | [0]" --output text)
echo "     API ID: $API_ID"
echo "     API URL: https://$API_ID.execute-api.$REGION.amazonaws.com"

# ── Secrets Manager ───────────────────────────────────────────────────────────
echo ""
echo "9/12  Creating Secrets Manager secret: $SECRETS_NAME"
echo ""
echo "     ⚠️  You need to fill in your broker credentials."
echo "     After setup, run:"
echo "       aws secretsmanager update-secret \\"
echo "         --secret-id \"$SECRETS_NAME\" \\"
echo "         --secret-string '{\"ANGEL_API_KEY\":\"...\",\"ANGEL_CLIENT_ID\":\"...\",\"ANGEL_PASSWORD\":\"...\",\"ANGEL_TOTP_SECRET\":\"...\"}' \\"
echo "         --region $REGION"
echo ""

# Create placeholder secret (will be filled in by user)
aws secretsmanager create-secret \
  --name "$SECRETS_NAME" \
  --description "NIFTY Spread Bot broker credentials" \
  --secret-string '{
    "ANGEL_API_KEY":"FILL_IN",
    "ANGEL_CLIENT_ID":"FILL_IN",
    "ANGEL_PASSWORD":"FILL_IN",
    "ANGEL_TOTP_SECRET":"FILL_IN",
    "ZERODHA_API_KEY":"FILL_IN",
    "ZERODHA_ACCESS_TOKEN":"FILL_IN"
  }' \
  --region "$REGION" 2>/dev/null && echo "     Secret created (fill in values before deploying)" \
  || echo "     Secret already exists – skipping"

# ── Pre-load NSE Holidays into DynamoDB config ────────────────────────────────
echo ""
echo "10/12  Pre-loading NSE holidays into nifty_config table"

# Official NSE trading holidays for 2025 and 2026.
# Update yearly from: https://www.nseindia.com (Market Timings & Holidays)
NSE_HOLIDAYS="2025-02-26,2025-03-14,2025-03-31,2025-04-14,2025-04-18,2025-05-01,\
2025-08-15,2025-08-27,2025-10-02,2025-10-24,2025-11-05,2025-12-25,\
2026-01-26,2026-03-03,2026-03-20,2026-04-03,2026-04-14,2026-05-01,\
2026-08-15,2026-09-14,2026-10-02,2026-11-12,2026-11-23,2026-12-25"

aws dynamodb put-item \
  --table-name "nifty_config" \
  --item "{\"config_key\":{\"S\":\"NSE_HOLIDAYS\"},\"config_value\":{\"S\":\"$NSE_HOLIDAYS\"}}" \
  --region "$REGION" && echo "     NSE holidays saved to DynamoDB" || echo "     Warning: could not save holidays"

# ── CloudWatch Alarms → Alerts SNS ───────────────────────────────────────────
echo ""
echo "11/12  Creating CloudWatch alarms (Lambda errors → Telegram)"

# Scanner Lambda: >2 errors in 5 min → alarm
aws cloudwatch put-metric-alarm \
  --alarm-name "nifty-scanner-errors" \
  --alarm-description "Scanner Lambda error rate too high" \
  --metric-name Errors \
  --namespace AWS/Lambda \
  --dimensions Name=FunctionName,Value=nifty-spread-scanner \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 2 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions "$ALERTS_SNS_ARN" \
  --ok-actions "$ALERTS_SNS_ARN" \
  --region "$REGION" && echo "     ✓ Scanner error alarm" || echo "     Warning: scanner alarm failed"

# Executor Lambda: any error → immediate alarm
aws cloudwatch put-metric-alarm \
  --alarm-name "nifty-executor-errors" \
  --alarm-description "Executor Lambda error — trade may have failed" \
  --metric-name Errors \
  --namespace AWS/Lambda \
  --dimensions Name=FunctionName,Value=nifty-spread-executor \
  --statistic Sum \
  --period 60 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions "$ALERTS_SNS_ARN" \
  --ok-actions "$ALERTS_SNS_ARN" \
  --region "$REGION" && echo "     ✓ Executor error alarm" || echo "     Warning: executor alarm failed"

# Token refresh Lambda: any error → alarm (scanner won't auth)
aws cloudwatch put-metric-alarm \
  --alarm-name "nifty-token-refresh-errors" \
  --alarm-description "Token refresh failed — scanner may not authenticate" \
  --metric-name Errors \
  --namespace AWS/Lambda \
  --dimensions Name=FunctionName,Value=nifty-spread-token-refresh \
  --statistic Sum \
  --period 60 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions "$ALERTS_SNS_ARN" \
  --region "$REGION" && echo "     ✓ Token refresh alarm" || echo "     Warning: token alarm failed"

# Scanner throttle alarm — if Lambda throttles during market hours we miss scans
aws cloudwatch put-metric-alarm \
  --alarm-name "nifty-scanner-throttles" \
  --alarm-description "Scanner Lambda being throttled" \
  --metric-name Throttles \
  --namespace AWS/Lambda \
  --dimensions Name=FunctionName,Value=nifty-spread-scanner \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 3 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions "$ALERTS_SNS_ARN" \
  --region "$REGION" && echo "     ✓ Scanner throttle alarm" || echo "     Warning: throttle alarm failed"

# ── Write env file for deploy_lambda.sh ──────────────────────────────────────
cat > deploy/.env << EOF
REGION=$REGION
ACCOUNT_ID=$ACCOUNT_ID
ROLE_ARN=$ROLE_ARN
SNS_ARN=$SNS_ARN
ALERTS_SNS_ARN=$ALERTS_SNS_ARN
DLQ_ARN=$DLQ_ARN
BUCKET=$BUCKET
API_ID=$API_ID
SECRETS_NAME=$SECRETS_NAME
EOF
echo ""
echo "12/12  Saved deploy/.env"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "========================================================"
echo "  Setup complete. Next steps:"
echo ""
echo "  1. Fill in your broker credentials in Secrets Manager:"
echo "     aws secretsmanager update-secret \\"
echo "       --secret-id \"$SECRETS_NAME\" \\"
echo "       --secret-string '{\"ANGEL_API_KEY\":\"...\", ...}' \\"
echo "       --region $REGION"
echo ""
echo "  2. Set Telegram credentials in deploy/.env:"
echo "     TELEGRAM_BOT_TOKEN=..."
echo "     TELEGRAM_CHAT_ID=..."
echo ""
echo "  3. Run:  ./deploy/deploy_lambda.sh"
echo ""
echo "  4. Upload dashboard:  ./deploy/upload_dashboard.sh"
echo ""
echo "  SNS execute ARN : $SNS_ARN"
echo "  SNS alerts ARN  : $ALERTS_SNS_ARN"
echo "  SQS DLQ ARN     : $DLQ_ARN"
echo "  API Gateway URL : https://$API_ID.execute-api.$REGION.amazonaws.com"
echo "========================================================"
