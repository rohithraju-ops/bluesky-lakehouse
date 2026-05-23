# scripts/setup_polaris.sh
#!/bin/bash
# One-time Polaris setup: get OAuth token, create catalog pointing at MinIO,
# create the bsky namespace inside it. Idempotent — safe to re-run.

set -euo pipefail

POLARIS_URL="${POLARIS_URL:-http://localhost:8181}"
CLIENT_ID="${CLIENT_ID:-root}"
CLIENT_SECRET="${CLIENT_SECRET:-s3cr3t}"
CATALOG_NAME="lakehouse_catalog"
NAMESPACE_NAME="bsky"
BUCKET="lakehouse"

echo "==> step 1/3: obtaining OAuth2 access token from Polaris..."
TOKEN=$(curl -s -X POST "${POLARIS_URL}/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "scope=PRINCIPAL_ROLE:ALL" \
  -d "client_id=${CLIENT_ID}" \
  -d "client_secret=${CLIENT_SECRET}" \
  | jq -r '.access_token')

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "ERROR: failed to obtain OAuth token. Is polaris running and bootstrapped?"
  exit 1
fi
echo "    token obtained: ${TOKEN:0:24}..."

echo "==> step 2/3: creating catalog '${CATALOG_NAME}' (points at s3://${BUCKET}/warehouse)..."
HTTP_CODE=$(curl -s -o /tmp/polaris-catalog.out -w "%{http_code}" \
  -X POST "${POLARIS_URL}/api/management/v1/catalogs" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d @- <<JSON
{
  "catalog": {
    "name": "${CATALOG_NAME}",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "s3://${BUCKET}/warehouse"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "endpoint": "http://localhost:9000",
      "endpointInternal": "http://minio:9000",
      "pathStyleAccess": true,
      "allowedLocations": ["s3://${BUCKET}"]
    }
  }
}
JSON
)
if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
  echo "    catalog created"
elif [ "$HTTP_CODE" = "409" ]; then
  echo "    catalog already exists, continuing"
else
  echo "ERROR creating catalog (HTTP $HTTP_CODE):"
  cat /tmp/polaris-catalog.out
  exit 1
fi

echo "==> step 3/3: creating namespace '${NAMESPACE_NAME}' in catalog..."
HTTP_CODE=$(curl -s -o /tmp/polaris-ns.out -w "%{http_code}" \
  -X POST "${POLARIS_URL}/api/catalog/v1/${CATALOG_NAME}/namespaces" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"namespace\":[\"${NAMESPACE_NAME}\"]}")
if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
  echo "    namespace created"
elif [ "$HTTP_CODE" = "409" ]; then
  echo "    namespace already exists, continuing"
else
  echo "ERROR creating namespace (HTTP $HTTP_CODE):"
  cat /tmp/polaris-ns.out
  exit 1
fi

echo ""
echo "✓ Polaris setup complete"
echo "    catalog:   ${CATALOG_NAME}"
echo "    namespace: ${NAMESPACE_NAME}"
echo "    bucket:    s3://${BUCKET}/warehouse (Iceberg table data will land here)"