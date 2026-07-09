#!/usr/bin/env bash
# Grants a principal Create/Describe/DescribeConfigs/Delete on a single topic, using the
# 'kafka' superuser identity (granting ACLs is itself an ACL-protected operation, so it must
# be run as a superuser — see manifests/03-kafka/kafka.yaml's authorization block).
#
# DescribeConfigs is easy to forget: `kafka-topics --describe` needs BOTH Describe AND
# DescribeConfigs (it shows partition info AND topic configs) - Describe alone isn't enough.
set -euo pipefail

NS="${1:-confluent}"
PRINCIPAL="${2:?usage: grant-topic-acl.sh <namespace> <principal, e.g. client> <topic>}"
TOPIC="${3:?usage: grant-topic-acl.sh <namespace> <principal> <topic>}"
POD="kafka-0"
TMP_DIR="$(mktemp -d)"
trap 'oc exec -n "$NS" "$POD" -- rm -f /tmp/admin-grant.properties 2>/dev/null; rm -rf "$TMP_DIR"' EXIT

KAFKA_PASS=$(oc get secret kafka-credential -n "$NS" -o jsonpath='{.data.plain-users\.json}' \
  | base64 -d | python3 -c "import json,sys;print(json.load(sys.stdin)['kafka'])")
JKS_PASS=$(oc get secret kafka-generated-jks -n "$NS" -o jsonpath='{.data.jksPassword\.txt}' | base64 -d | cut -d'=' -f2)

cat > "$TMP_DIR/admin.properties" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka" password="$KAFKA_PASS";
ssl.truststore.location=/mnt/sslcerts/truststore.jks
ssl.truststore.password=$JKS_PASS
EOF
oc exec -i -n "$NS" "$POD" -c kafka -- sh -c 'cat > /tmp/admin-grant.properties' < "$TMP_DIR/admin.properties"

oc exec -n "$NS" "$POD" -- kafka-acls --bootstrap-server kafka:9071 --command-config /tmp/admin-grant.properties \
  --add --allow-principal "User:$PRINCIPAL" \
  --operation Create --operation Describe --operation DescribeConfigs --operation Delete \
  --topic "$TOPIC"

echo "Granted Create/Describe/DescribeConfigs/Delete on topic '$TOPIC' to User:$PRINCIPAL."
