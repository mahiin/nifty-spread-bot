#!/bin/bash
# Upload static dashboard to S3 and enable public access
set -euo pipefail
source "$(dirname "$0")/.env"

ROOT=$(cd "$(dirname "$0")/.." && pwd)

echo "Uploading dashboard to s3://$BUCKET/ ..."

# Disable block public access so bucket policy can allow public reads
aws s3api put-public-access-block \
  --bucket "$BUCKET" \
  --public-access-block-configuration "BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false"

# Apply bucket policy for public read (no ACLs needed)
aws s3api put-bucket-policy --bucket "$BUCKET" --policy "{
  \"Version\": \"2012-10-17\",
  \"Statement\": [{
    \"Sid\": \"PublicReadGetObject\",
    \"Effect\": \"Allow\",
    \"Principal\": \"*\",
    \"Action\": \"s3:GetObject\",
    \"Resource\": \"arn:aws:s3:::$BUCKET/*\"
  }]
}"

# Upload without ACL flag
aws s3 cp "$ROOT/dashboard/index.html" "s3://$BUCKET/index.html" \
  --content-type "text/html"

DASHBOARD_URL="http://$BUCKET.s3-website.$REGION.amazonaws.com"
echo ""
echo "Dashboard live at: $DASHBOARD_URL"
echo ""
echo "Open the URL above in your browser."
echo "Remember to set window.API_GATEWAY_URL in dashboard/index.html"
echo "or replace 'YOUR_API_GATEWAY_URL' with your actual API Gateway URL."
