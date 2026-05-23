set -euo pipefail

POLARIS_URL="${POLARIS_URL:-http://localhost:8181}"
CATALOG_NAME="lakehouse_catalog"
CATALOG_ROLE="lakehouse_writer"
PRINCIPAL_ROLE="service_admin"

echo "==> refreshing OAuth token..."
TOKEN=$(curl -s -X POST "${POLARIS_URL}/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL&client_id=root&client_secret=s3cr3t" \
  | jq -r '.access_token')
echo "    token: ${TOKEN:0:20}..."

echo "==> step 1/3: creating catalog role '${CATALOG_ROLE}'..."
curl -s -X POST "${POLARIS_URL}/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"catalogRole\":{\"name\":\"${CATALOG_ROLE}\"}}" | jq .

echo "==> step 2/3: granting CATALOG_MANAGE_CONTENT to '${CATALOG_ROLE}'..."
curl -s -X PUT "${POLARIS_URL}/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/${CATALOG_ROLE}/grants" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}' | jq .

echo "==> step 3/3: assigning catalog role to principal role '${PRINCIPAL_ROLE}'..."
curl -s -X PUT "${POLARIS_URL}/api/management/v1/principal-roles/${PRINCIPAL_ROLE}/catalog-roles/${CATALOG_NAME}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"catalogRole\":{\"name\":\"${CATALOG_ROLE}\"}}" | jq .

echo ""
echo "✓ done — root can now create and write tables in ${CATALOG_NAME}"