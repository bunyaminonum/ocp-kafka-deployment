#!/usr/bin/env bash
# Creates the two secrets CFK's spec.metrics.jmx block references (kafka-jmx-password,
# kafka-jmx-access). Same "never commit a real secret" rule as create-sasl-secrets.sh, but a
# DIFFERENT file format: CFK expects a single 'jmx' key whose value is itself a small YAML
# block ('password: |' / 'access: |' followed by indented '<username> <value>' lines) —
# confirmed against the official docs, not guessed.
#
# Note: this secures remote JMX/RMI access (port 7203, e.g. jconsole). It does NOT gate the
# Prometheus exporter endpoint (port 7778) — that one reads local MBeans in-process and
# serves plain HTTP with no auth, by design (see observability/README.md).
set -euo pipefail

NS="${1:-confluent}"
JMX_PASS=$(openssl rand -hex 24)

oc create secret generic kafka-jmx-password -n "$NS" \
  --from-literal=jmx="password: |
  jmx-user $JMX_PASS" \
  --dry-run=client -o yaml | oc apply -f -

oc create secret generic kafka-jmx-access -n "$NS" \
  --from-literal=jmx='access: |
  jmx-user readonly' \
  --dry-run=client -o yaml | oc apply -f -

echo "kafka-jmx-password and kafka-jmx-access created/updated in namespace $NS."
