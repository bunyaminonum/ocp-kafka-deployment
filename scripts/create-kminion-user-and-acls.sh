#!/usr/bin/env bash
# Prepares a dedicated, least-privilege SASL identity for kminion (the consumer-lag exporter):
#   1. Adds a 'kminion' user to the SASL user list (kafka-credential's plain-users.json),
#      WITHOUT widening the app 'client' principal.
#   2. Grants that principal READ-ONLY monitoring ACLs (cluster/topic/group DESCRIBE only) —
#      exactly what kminion needs to enumerate consumer groups and their lag, nothing more.
#   3. Writes the generated password into the 'kminion-sasl' secret consumed by the kminion
#      Deployment (observability/kminion/kminion.yaml).
#
# Why a separate user (not 'client'): with authorization.type=simple enabled, kminion is
# denied unless authorized. Reusing 'client' would force us to give the app principal
# cluster-wide DESCRIBE, breaking least-privilege. A dedicated monitoring principal is the
# enterprise-correct pattern. Confirmed live: kminion authenticated + read consumer-group lag
# only after these ACLs were granted (before, it logged TOPIC_AUTHORIZATION_FAILED).
set -euo pipefail

NS="${1:-confluent}"
POD="kafka-0"
TMP_DIR="$(mktemp -d)"
trap 'oc exec -n "$NS" "$POD" -c kafka -- rm -f /tmp/admin.properties 2>/dev/null; shred -u "$TMP_DIR"/* 2>/dev/null; rmdir "$TMP_DIR"' EXIT

KMINION_PASS=$(openssl rand -hex 24)

# 1. Add kminion to plain-users.json (in place — never `oc delete` a CFK-managed secret).
USERS=$(oc get secret kafka-credential -n "$NS" -o jsonpath='{.data.plain-users\.json}' | base64 -d \
  | python3 -c "import json,sys; d=json.load(sys.stdin); d['kminion']='$KMINION_PASS'; print(json.dumps(d))")
oc get secret kafka-credential -n "$NS" -o jsonpath='{.data.plain-interbroker\.txt}' | base64 -d > "$TMP_DIR/inter.txt"
printf '%s' "$USERS" > "$TMP_DIR/users.json"
oc create secret generic kafka-credential -n "$NS" \
  --from-file=plain-users.json="$TMP_DIR/users.json" \
  --from-file=plain-interbroker.txt="$TMP_DIR/inter.txt" \
  --dry-run=client -o yaml | oc apply -f -

# 2. Grant read-only monitoring ACLs to User:kminion (run as the 'kafka' superuser).
KAFKA_PASS=$(oc get secret kafka-credential -n "$NS" -o jsonpath='{.data.plain-users\.json}' | base64 -d \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['kafka'])")
JKS_PASS=$(oc get secret kafka-generated-jks -n "$NS" -o jsonpath='{.data.jksPassword\.txt}' | base64 -d | cut -d'=' -f2)
printf 'security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka" password="%s";\nssl.truststore.location=/mnt/sslcerts/truststore.jks\nssl.truststore.password=%s\n' \
  "$KAFKA_PASS" "$JKS_PASS" | oc exec -i -n "$NS" "$POD" -c kafka -- sh -c 'cat > /tmp/admin.properties'

acl() { oc exec -n "$NS" "$POD" -c kafka -- kafka-acls --bootstrap-server kafka:9071 \
  --command-config /tmp/admin.properties --add --allow-principal User:kminion "$@"; }
acl --operation Describe --cluster
acl --operation Describe --operation DescribeConfigs --topic '*'
acl --operation Describe --group '*'

# 3. Store kminion's password for the Deployment.
oc create secret generic kminion-sasl -n "$NS" --from-literal=password="$KMINION_PASS" \
  --dry-run=client -o yaml | oc apply -f -

echo "kminion SASL user + read-only monitoring ACLs ready. Now: oc apply -f observability/kminion/kminion.yaml"
