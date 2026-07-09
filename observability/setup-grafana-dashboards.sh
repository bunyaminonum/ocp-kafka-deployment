#!/usr/bin/env bash
# Adds Prometheus as a Grafana datasource and imports three dashboards:
#   - kafka-kraft-dashboard.json  — this repo's own hand-built dashboard (recommended)
#   - confluent-platform.json / confluent-operator.json — Confluent's official examples
# Idempotent - safe to re-run (datasource POST fails harmlessly if it already exists;
# dashboard import uses overwrite: true).
#
# All three are imported via /api/dashboards/import (NOT /api/dashboards/db — that endpoint
# silently ignores the 'inputs' datasource mapping, leaving every panel on an unresolved
# ${DS_PROMETHEUS} placeholder = "No data"). The Prometheus datasource UID is looked up live
# and passed as the input value, so panels bind to the real datasource.
#
# The two Confluent dashboards are also patched before import — as published they show "No
# data" everywhere on this stack, for two confirmed reasons (see README - Troubleshooting):
#   1. Their queries filter on the `kubernetes_namespace` label, but current
#      prometheus-community chart relabeling produces `namespace`.
#   2. Their `component_name` template variable reads `kube_pod_labels`, a kube-state-metrics
#      metric — and kube-state-metrics is disabled here for CPU budget. Rewritten to read the
#      `app` label from the Kafka metrics that already exist.
# This repo's own kafka-kraft-dashboard.json needs neither patch (built for this stack).
set -euo pipefail

NS="${1:-confluent}"
LOCAL_PORT="${2:-3000}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'kill $PF_PID 2>/dev/null; rm -rf "$TMP_DIR"' EXIT

GRAFANA_PASS=$(oc get secret grafana -n "$NS" -o jsonpath='{.data.admin-password}' | base64 -d)

oc port-forward -n "$NS" svc/grafana "$LOCAL_PORT":80 >/dev/null 2>&1 &
PF_PID=$!
sleep 3

curl -s -u "admin:$GRAFANA_PASS" -X POST "http://localhost:$LOCAL_PORT/api/datasources" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"Prometheus\",\"type\":\"prometheus\",\"access\":\"proxy\",\"url\":\"http://prometheus-server.$NS.svc.cluster.local\",\"isDefault\":true}" \
  || echo "(datasource likely already exists - fine)"
echo

# Look up the real Prometheus datasource UID so imports bind to it (not to the placeholder).
DS_UID=$(curl -s -u "admin:$GRAFANA_PASS" "http://localhost:$LOCAL_PORT/api/datasources/name/Prometheus" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['uid'])")

# This repo's own dashboard — already built for this stack, only needs the datasource bound.
python3 -c "
import json
d = json.load(open('$SCRIPT_DIR/kafka-kraft-dashboard.json'))
print(json.dumps({'dashboard': d, 'overwrite': True,
    'inputs': [{'name':'DS_PROMETHEUS','type':'datasource','pluginId':'prometheus','value':'$DS_UID'}]}))
" > "$TMP_DIR/import-kafka-kraft.json"
curl -s -u "admin:$GRAFANA_PASS" -X POST "http://localhost:$LOCAL_PORT/api/dashboards/import" \
  -H "Content-Type: application/json" -d "@$TMP_DIR/import-kafka-kraft.json"
echo

for dash in confluent-platform confluent-operator; do
  curl -s -o "$TMP_DIR/$dash.json" \
    "https://raw.githubusercontent.com/confluentinc/confluent-kubernetes-examples/master/monitoring/grafana-dashboard/$dash.json"
  python3 -c "
import json
raw = open('$TMP_DIR/$dash.json').read()
raw = raw.replace('kubernetes_namespace', 'namespace')
d = json.loads(raw)
d.pop('id', None)
for v in d.get('templating', {}).get('list', []):
    q = v.get('query')
    if isinstance(q, dict) and 'kube_pod_labels' in str(q.get('query', '')):
        q['query'] = 'label_values({confluent_platform=\"true\",platform_confluent_io_type=~\"\$controller_type\"}, app)'
        v['definition'] = q['query']
print(json.dumps({'dashboard': d, 'overwrite': True, 'inputs': [{'name':'DS_PROMETHEUS','type':'datasource','pluginId':'prometheus','value':'$DS_UID'}]}))
" > "$TMP_DIR/import-$dash.json"
  curl -s -u "admin:$GRAFANA_PASS" -X POST "http://localhost:$LOCAL_PORT/api/dashboards/import" \
    -H "Content-Type: application/json" -d "@$TMP_DIR/import-$dash.json"
  echo
done

echo "Done. Open Grafana (see README - Accessing Grafana) and check Dashboards ->"
echo "  'Kafka (KRaft) — Cluster Overview' (this repo's), Confluent Platform, Confluent Operator."
