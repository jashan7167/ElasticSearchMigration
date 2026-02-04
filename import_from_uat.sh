#!/bin/bash
#
# import_from_uat.sh - Export indices from UAT ES 6.4.2 and import to local
#
# Usage: ./import_from_uat.sh <UAT_ES_URL> <INDEX_NAME>
# Example: ./import_from_uat.sh http://uat-es-server:9200 my-index
#

set -e

UAT_ES="${1}"
INDEX_NAME="${2}"
LOCAL_ES="http://localhost:9201"
TEMP_DIR="./uat_export/${INDEX_NAME}"

if [ -z "$UAT_ES" ] || [ -z "$INDEX_NAME" ]; then
    echo "Usage: $0 <UAT_ES_URL> <INDEX_NAME>"
    echo "Example: $0 http://uat-server:9200 my-index"
    exit 1
fi

echo "============================================"
echo "  Importing from UAT to Local ES 6.4.2"
echo "============================================"
echo "UAT ES:     $UAT_ES"
echo "Index:      $INDEX_NAME"
echo "Local ES:   $LOCAL_ES"
echo "Temp dir:   $TEMP_DIR"
echo ""

# Create temp directory
mkdir -p "$TEMP_DIR"

# ── Step 1: Export mapping ──────────────────────────────────────
echo "[1/5] Exporting mapping from UAT..."
curl -sf "${UAT_ES}/${INDEX_NAME}/_mapping" | \
    jq ".\"${INDEX_NAME}\".mappings" > "${TEMP_DIR}/mapping.json"

if [ $? -eq 0 ]; then
    echo "✓ Mapping exported to ${TEMP_DIR}/mapping.json"
else
    echo "✗ Failed to export mapping"
    exit 1
fi

# ── Step 2: Export settings ──────────────────────────────────────
echo "[2/5] Exporting settings from UAT..."
curl -sf "${UAT_ES}/${INDEX_NAME}/_settings" | \
    jq ".\"${INDEX_NAME}\".settings" > "${TEMP_DIR}/settings.json"

if [ $? -eq 0 ]; then
    echo "✓ Settings exported to ${TEMP_DIR}/settings.json"
else
    echo "✗ Failed to export settings"
    exit 1
fi

# ── Step 3: Export all documents using scroll ──────────────────────
echo "[3/5] Exporting documents from UAT..."

# Initialize scroll
SCROLL_RESPONSE=$(curl -sf "${UAT_ES}/${INDEX_NAME}/_search?scroll=5m&size=1000" -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} }
}')

SCROLL_ID=$(echo "$SCROLL_RESPONSE" | jq -r '._scroll_id')
TOTAL_HITS=$(echo "$SCROLL_RESPONSE" | jq -r '.hits.total')

echo "  Total documents: $TOTAL_HITS"

# Save first batch
echo "$SCROLL_RESPONSE" | jq -c '.hits.hits[]' > "${TEMP_DIR}/docs.jsonl"

DOC_COUNT=$(echo "$SCROLL_RESPONSE" | jq '.hits.hits | length')
FETCHED=$DOC_COUNT

# Continue scrolling
while [ "$DOC_COUNT" -gt 0 ]; do
    SCROLL_RESPONSE=$(curl -sf "${UAT_ES}/_search/scroll" -H 'Content-Type: application/json' -d "{
      \"scroll\": \"5m\",
      \"scroll_id\": \"${SCROLL_ID}\"
    }")
    
    DOC_COUNT=$(echo "$SCROLL_RESPONSE" | jq '.hits.hits | length')
    
    if [ "$DOC_COUNT" -gt 0 ]; then
        echo "$SCROLL_RESPONSE" | jq -c '.hits.hits[]' >> "${TEMP_DIR}/docs.jsonl"
        FETCHED=$((FETCHED + DOC_COUNT))
        echo "  Fetched: $FETCHED / $TOTAL_HITS"
    fi
    
    SCROLL_ID=$(echo "$SCROLL_RESPONSE" | jq -r '._scroll_id')
done

# Clear scroll
curl -sf -XDELETE "${UAT_ES}/_search/scroll" -H 'Content-Type: application/json' -d "{
  \"scroll_id\": \"${SCROLL_ID}\"
}" > /dev/null

echo "✓ Exported $FETCHED documents to ${TEMP_DIR}/docs.jsonl"

# ── Step 4: Create index on local ES ──────────────────────────────
echo "[4/5] Creating index on local ES..."

# Check if index already exists
if curl -sf "${LOCAL_ES}/${INDEX_NAME}" > /dev/null 2>&1; then
    echo "⚠ Index already exists on local ES. Delete it first? (y/N)"
    read -r response
    if [ "$response" = "y" ] || [ "$response" = "Y" ]; then
        curl -sf -XDELETE "${LOCAL_ES}/${INDEX_NAME}"
        echo "  Deleted existing index"
    else
        echo "✗ Aborted"
        exit 1
    fi
fi

# Create index with mapping and settings
MAPPING=$(cat "${TEMP_DIR}/mapping.json")
SETTINGS=$(cat "${TEMP_DIR}/settings.json")

CREATE_BODY=$(jq -n \
    --argjson mappings "$MAPPING" \
    --argjson settings "$SETTINGS" \
    '{mappings: $mappings, settings: $settings}')

curl -sf -XPUT "${LOCAL_ES}/${INDEX_NAME}" \
    -H 'Content-Type: application/json' \
    -d "$CREATE_BODY" > /dev/null

if [ $? -eq 0 ]; then
    echo "✓ Index created on local ES"
else
    echo "✗ Failed to create index"
    exit 1
fi

# ── Step 5: Bulk import documents ──────────────────────────────────
echo "[5/5] Bulk importing documents to local ES..."

# Convert to bulk format
BULK_FILE="${TEMP_DIR}/bulk.jsonl"
> "$BULK_FILE"  # Clear file

while IFS= read -r doc; do
    INDEX_ID=$(echo "$doc" | jq -r '._id')
    TYPE=$(echo "$doc" | jq -r '._type // "doc"')
    SOURCE=$(echo "$doc" | jq -c '._source')
    
    # Bulk action line
    echo "{\"index\":{\"_index\":\"${INDEX_NAME}\",\"_type\":\"${TYPE}\",\"_id\":\"${INDEX_ID}\"}}" >> "$BULK_FILE"
    # Document source line
    echo "$SOURCE" >> "$BULK_FILE"
done < "${TEMP_DIR}/docs.jsonl"

# Send bulk request
curl -sf -XPOST "${LOCAL_ES}/_bulk" \
    -H 'Content-Type: application/x-ndjson' \
    --data-binary "@${BULK_FILE}" > "${TEMP_DIR}/bulk_response.json"

ERRORS=$(jq '.errors' "${TEMP_DIR}/bulk_response.json")
IMPORTED=$(jq '.items | length' "${TEMP_DIR}/bulk_response.json")

if [ "$ERRORS" = "false" ]; then
    echo "✓ Successfully imported $IMPORTED documents"
else
    echo "⚠ Bulk import completed with some errors"
    jq '.items[] | select(.index.error) | .index.error' "${TEMP_DIR}/bulk_response.json"
fi

# ── Verification ──────────────────────────────────────────────────
echo ""
echo "Verifying import..."
LOCAL_COUNT=$(curl -sf "${LOCAL_ES}/${INDEX_NAME}/_count" | jq '.count')
echo "  UAT docs:   $TOTAL_HITS"
echo "  Local docs: $LOCAL_COUNT"

if [ "$LOCAL_COUNT" -eq "$TOTAL_HITS" ]; then
    echo "✓ Document counts match!"
else
    echo "⚠ Document counts differ"
fi

echo ""
echo "============================================"
echo "  Import complete!"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Verify data: curl 'http://localhost:9201/${INDEX_NAME}/_search?pretty'"
echo "  2. Run migration: source venv/bin/activate && python3 main.py"
echo ""
