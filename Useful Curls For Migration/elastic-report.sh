ES_USER="elastic"
ES_PASS='YOUR_PASSWORD'
ES_HOST="http://10.44.237.231:30920"

echo "============================================================"
echo "              ELASTICSEARCH CLUSTER REPORT"
echo "============================================================"

echo -e "\n==================== VERSION ===================="
curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/?pretty"

echo -e "\n==================== HEALTH ====================="
curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/_cluster/health?pretty"

echo -e "\n==================== NODES ======================"
curl -s -u "$ES_USER:$ES_PASS" \
"$ES_HOST/_cat/nodes?v&h=name,ip,node.role,master,jdk,heap.percent,ram.percent,cpu,load_1m"

echo -e "\n==================== JVM / HEAP / GC ============"
curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/_nodes/stats/jvm?pretty"

echo -e "\n==================== JVM CONFIG ================="
curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/_nodes/jvm?pretty"

echo -e "\n==================== CPU / RAM =================="
curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/_nodes/stats/os?pretty"

echo -e "\n==================== PROCESS ===================="
curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/_nodes/stats/process?pretty"

echo -e "\n==================== DISK ======================="
curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/_cat/allocation?v"

echo -e "\n==================== FILESYSTEM ================="
curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/_nodes/stats/fs?pretty"

echo -e "\n==================== CLUSTER SETTINGS ==========="
curl -s -u "$ES_USER:$ES_PASS" \
"$ES_HOST/_cluster/settings?include_defaults=true&flat_settings=true&pretty"

echo -e "\n==================== NODE STATS ================="
curl -s -u "$ES_USER:$ES_PASS" \
"$ES_HOST/_nodes/stats?pretty"

echo -e "\n============================================================"
echo "                  REPORT COMPLETE"
echo "============================================================"
