#!/bin/bash
ES="http://10.44.237.233:30920"

# All indices under 4gb
INDICES=(
  "property-services-enriched"
  "property-assessments-temp"
  "property-assessments-enriched"
  "property-services"
  "property-services-temp"
  "egov-dss-ingest-enriched"
)

TOTAL=${#INDICES[@]}
COUNT=0

for INDEX in "${INDICES[@]}"; do
  COUNT=$((COUNT + 1))
  echo ""
  echo "[$COUNT/$TOTAL] Processing: $INDEX"

  # Force merge
  echo "  → Force merging..."
  MERGE=$(curl -s -X POST "$ES/${INDEX}/_forcemerge?max_num_segments=1")
  FAILED=$(echo $MERGE | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['_shards']['failed'])" 2>/dev/null)
  if [ "$FAILED" = "0" ]; then
    echo "  ✓ Force merge done"
  else
    echo "  ⚠ Force merge had failures: $MERGE"
  fi

  # Restore settings
  echo "  → Restoring settings..."
  RESULT=$(curl -s -X PUT "$ES/${INDEX}/_settings" \
    -H 'Content-Type: application/json' \
    -d '{
      "index": {
        "number_of_replicas"              : 1,
        "refresh_interval"                : "1s",
        "translog.durability"             : "request",
        "translog.flush_threshold_size"   : "512mb",
        "translog.sync_interval"          : "5s",
        "merge.scheduler.max_thread_count": 1
      }
    }')

  ACK=$(echo $RESULT | python3 -c "import sys,json; print(json.load(sys.stdin).get('acknowledged','false'))" 2>/dev/null)
  if [ "$ACK" = "True" ] || [ "$ACK" = "true" ]; then
    echo "  ✓ Settings restored"
  else
    echo "  ⚠ Settings failed: $RESULT"
  fi

done

echo ""
echo "════════════════════════════════════════"
echo " All $TOTAL indices processed"
echo "════════════════════════════════════════"

# Final verification
echo ""
echo "Final state:"
curl -s "$ES/_cat/indices?v&h=index,docs.count,docs.deleted,store.size,rep" | \
  grep -E "$(IFS='|'; echo "${INDICES[*]}")"
