# Observability — JMX Metrics, Prometheus, Grafana

Adds monitoring to the Kafka/KRaftController cluster from the main [README](../README.md):
CFK's built-in JMX Prometheus exporter, scraped by Prometheus, visualized in Grafana with
Confluent's official dashboards. Built and proven on the same practice environment (CRC on
an OCI VM) as the rest of this repo — every command below actually ran, every number quoted
is real.

> **Practice-environment-specific:** the resource trims and manual Route (Step 4) below exist
> because of this node's tight CPU budget and lack of pre-existing ingress. See
> "In enterprise production" callouts throughout for what changes on a real cluster.

## Architecture

```
Kafka / KRaftController pods
  └─ JVM in-process javaagent (jmx_prometheus_javaagent) on :7778
       └─ scraped by Prometheus (auto-discovered via prometheus.io/scrape pod annotation)
            └─ Grafana (Prometheus as datasource) ── Confluent's official dashboards
```

Two separate JMX-related ports, easy to conflate:
- **7203** — real JMX/RMI (`jconsole`, `jmxterm`, remote debugging). Requires the
  authentication configured in Step 1.
- **7778** — the Prometheus exporter. Runs **in-process**, reads local MBeans directly, and
  serves plain HTTP with **no authentication** — confirmed by testing (curled it with zero
  credentials and got real metrics back). The JMX auth in Step 1 does not gate this port.

## Step 1 — JMX authentication secrets

```bash
./scripts/create-jmx-secrets.sh confluent
```

CFK requires JMX auth to be explicitly configured (JMX has been auth-disabled-by-default
since CFK 3.2.1, for security). The secret format is CFK-specific — not a plain
username/password file like the SASL secrets, but a single `jmx` key holding a small YAML
block (`password: |` / `access: |` + indented `<username> <value>` lines). Confirmed against
Confluent's docs, not guessed — see the script for the exact literal format.

## Step 2 — Enable metrics on both CRs

Both `manifests/03-kafka/kafka.yaml` and `manifests/03-kafka/kraft-controller.yaml` already
carry:
```yaml
  metrics:
    jmx:
      authentication:
        secretRef: kafka-jmx-password
      accessControl:
        enabled: true
        secretRef: kafka-jmx-access
```
```bash
oc apply -f manifests/03-kafka/
oc get pods -n confluent -w   # rolling restart, all 6 pods
```

**Free side effect, no extra config needed:** once this is applied, CFK annotates every pod
with `prometheus.io/scrape: "true"` and `prometheus.io/port: "7778"` — confirmed via
`oc get pod kafka-0 -o jsonpath='{.metadata.annotations}'`. This is exactly what
`prometheus-community/prometheus`'s default scrape config looks for, so Step 3's Prometheus
finds all 6 pods with zero manual scrape-config work.

## Step 3 — Prometheus

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install prometheus prometheus-community/prometheus \
  -n confluent -f observability/prometheus-values.yaml
```

See [`prometheus-values.yaml`](prometheus-values.yaml) for the full reasoning behind every
override (SCC UID rejection, the `--set X={}` vs "null every subfield" gotcha, resource
sizing for this specific node's ~89m free CPU at the time).

**Verify targets are actually being scraped:**
```bash
oc port-forward -n confluent svc/prometheus-server 9090:80 &
curl -s localhost:9090/api/v1/targets | python3 -c "
import json,sys
for t in json.load(sys.stdin)['data']['activeTargets']:
    print(t['labels'].get('pod','?'), '->', t['health'])
"
```
**Proven result:** all 6 targets (`kafka-0/1/2`, `kraftcontroller-0/1/2`) showed `up`, zero
`lastError`. A spot-check query
(`kafka_server_brokertopicmetrics_count{name="LogAppendPerSec"}`) returned real non-zero
values (21855 on one controller at test time) — genuine per-metric-type data, not just JVM
housekeeping metrics.

## Step 4 — Grafana

```bash
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
helm upgrade --install grafana grafana/grafana -n confluent -f observability/grafana-values.yaml

oc apply -f observability/grafana-route.yaml
oc get secret grafana -n confluent -o jsonpath='{.data.admin-password}' | base64 -d; echo
```

See [`grafana-values.yaml`](grafana-values.yaml) for the reasoning (same SCC-UID class of
issue as Prometheus, **plus** a root `busybox` init container the chart runs by default to
chown its data directory — disabled, since it would hit the exact same SCC wall and isn't
needed once the fixed UID requirement is removed).

## Step 5 — Datasource + dashboards

```bash
./observability/setup-grafana-dashboards.sh confluent
```
Adds Prometheus as a datasource and imports Confluent's official
[`confluent-platform.json`](https://github.com/confluentinc/confluent-kubernetes-examples/blob/master/monitoring/grafana-dashboard/confluent-platform.json)
and
[`confluent-operator.json`](https://github.com/confluentinc/confluent-kubernetes-examples/blob/master/monitoring/grafana-dashboard/confluent-operator.json)
dashboards via the Grafana HTTP API (no manual UI clicking required, and re-runnable).

## Accessing Grafana

No public ingress on this practice environment (same situation as the console/ArgoCD — see
main README's
["Reaching the web UIs remotely"](../README.md#reaching-the-web-uis-remotely-practice-environment-only)).
Same fix applies: SSH tunnel to local port 443, map the hostname to `127.0.0.1` in your own
hosts file, browse without a port:
```bash
# on your local machine, as Administrator/root:
ssh -i <private-key> -L 443:grafana-confluent.apps-crc.testing:443 <user>@<vm-public-ip>
```
```
127.0.0.1  grafana-confluent.apps-crc.testing
```
`https://grafana-confluent.apps-crc.testing/` — login `admin` / (see Step 4's
`admin-password` command above).

**A mistake worth flagging:** `<service>.<namespace>.svc.cluster.local` (the Kubernetes
in-cluster DNS name, visible in `oc get svc`) is **not** reachable this way, or any way, from
outside the cluster — it only resolves inside the pod network. Grafana needed an actual
OpenShift `Route` (`grafana-route.yaml`) before any external hostname existed for it at all;
CFK/OpenShift GitOps create these automatically for their own components (console, ArgoCD),
but a plain Helm chart like this one does not.

## Troubleshooting (all encountered and confirmed while building this)

| Symptom | Cause / fix |
|---|---|
| `unable to validate against any security context constraint ... runAsUser: 65534` | Chart's default fixed UID rejected by OpenShift SCCs. Null every `securityContext` subfield individually — setting the whole map to `{}` is a no-op against the chart's own defaults (Helm merges values, doesn't replace them) |
| Same SCC error, but from an **init container** | Grafana's `initChownData` (root `busybox`) hits the identical wall. Disable it (`initChownData.enabled: false`) — not needed once the fixed UID requirement is gone |
| `0/1 nodes are available: Insufficient cpu` | Node was already near 100% CPU-requested; lower `resources.requests.cpu` on the component (30m was enough here — check free headroom with `oc describe node \| grep -A6 "Allocated resources"` first) |
| Browser `ERR_CONNECTION_REFUSED` on `<svc>.<ns>.svc.cluster.local` | That's an in-cluster-only DNS name, not a route. Create an actual `Route` and use its `apps-crc.testing` hostname instead |
| `curl: command not found` inside a Kafka/KRaftController pod | `cp-server` is a minimal image (same reason `tar` is also missing — see main README). Use `oc port-forward` and run `curl` from the host instead of inside the pod |
| Dashboards import fine but every panel shows **No data** | Two label mismatches, both confirmed against the live Prometheus API: (1) the published dashboards filter on `kubernetes_namespace`, but current prometheus-community chart relabeling produces `namespace` (`/api/v1/label/kubernetes_namespace/values` returned empty, `namespace` was populated); (2) their `component_name` template variable reads `kube_pod_labels` from kube-state-metrics — disabled here for CPU budget — so the variable resolved empty and every panel filtered on `app=~""`. `setup-grafana-dashboards.sh` now patches both before importing; the same panel queries returned real data (e.g. PartitionCount 433) once the labels matched |

## In enterprise production

- A real deployment uses the **full** `kube-prometheus-stack` (alertmanager,
  node-exporter, kube-state-metrics) or the organization's existing central Prometheus/Grafana
  — this repo's trimmed single-component install exists only because of this practice node's
  CPU budget.
- Prometheus needs a real PVC (`persistentVolume.enabled: true` + a proper `storageClass`) —
  metrics vanish on pod restart here.
- Grafana dashboards/datasources should be provisioned declaratively (the chart's
  `sidecar.dashboards`/`datasources` with ConfigMaps, or a GitOps-managed
  `grafana.ini`/provisioning files) instead of loaded once via API calls that leave no trace
  in git.
- Alerting rules (`prometheus.rules`) and Alertmanager routing to the organization's on-call
  tool (PagerDuty/Opsgenie/Slack) — not configured here at all.
- JMX auth credentials (Step 1) should come from the organization's secret manager, same as
  the SASL credentials elsewhere in this repo.

## File map

```
observability/
├── README.md                       # this file
├── prometheus-values.yaml          # Helm values for prometheus-community/prometheus
├── grafana-values.yaml             # Helm values for grafana/grafana
├── grafana-route.yaml              # OpenShift Route (the chart creates none)
└── setup-grafana-dashboards.sh     # datasource + dashboard import via Grafana's HTTP API
scripts/
└── create-jmx-secrets.sh           # Step 1 — imperative, never commits the password
```
