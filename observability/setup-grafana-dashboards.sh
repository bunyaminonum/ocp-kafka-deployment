#!/usr/bin/env bash
# Adds Prometheus as a Grafana datasource and imports Confluent's official dashboards
# (confluent-platform.json, confluent-operator.json from confluentinc/confluent-kubernetes-
# examples). Idempotent - safe to re-run (datasource POST will fail harmlessly if it already
# exists; dashboard import uses overwrite: true).
#
# The dashboards are patched before import — as published they show "No data" everywhere on
# this stack, for two confirmed reasons (see observability/README.md - Troubleshooting):
#   1. Their queries filter on the `kubernetes_namespace` label, but current
#      prometheus-community chart relabeling produces `namespace`.
#   2. Their `component_name` template variable reads `kube_pod_labels`, a kube-state-metrics
#      metric — and kube-state-metrics is disabled here for CPU budget. Rewritten to read the
#      `app` label from the Kafka metrics that already exist.
set -euo pipefail

NS="${1:-confluent}"
LOCAL_PORT="${2:-3000}"
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
print(json.dumps({'dashboard': d, 'overwrite': True, 'inputs': [{'name':'DS_PROMETHEUS','type':'datasource','pluginId':'prometheus','value':'Prometheus'}]}))
" > "$TMP_DIR/import-$dash.json"
  # Must be /api/dashboards/import — the /api/dashboards/db endpoint silently ignores the
  # 'inputs' mapping, leaving every panel pointing at an unresolved ${DS_PROMETHEUS}
  # placeholder ("No data" on all panels). Confirmed the hard way.
  curl -s -u "admin:$GRAFANA_PASS" -X POST "http://localhost:$LOCAL_PORT/api/dashboards/import" \
    -H "Content-Type: application/json" -d "@$TMP_DIR/import-$dash.json"
  echo
done

echo "Done. Open Grafana (see README - Accessing Grafana) and check Dashboards -> Confluent Platform / Confluent Operator."
