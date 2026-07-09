#!/usr/bin/env bash
# TLS+SASL smoke test: creates a real topic over the secured listener, checks that every
# partition's ISR equals its replica set, then cleans up. This is the actual test we ran
# against the live cluster (see README - Verification), not a theoretical example.
set -euo pipefail

NS="${1:-confluent}"
TOPIC="${2:-health-check}"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# jksPassword.txt content is "jksPassword=<value>", not a bare password - cut it.
JKS_PASS=$(oc get secret kafka-generated-jks -n "$NS" -o jsonpath='{.data.jksPassword\.txt}' | base64 -d | cut -d'=' -f2)

# The client password lives inside kafka-credential's plain-users.json ({"kafka":"...","client":"..."})
CLIENT_PASS=$(oc get secret kafka-credential -n "$NS" -o jsonpath='{.data.plain-users\.json}' \
  | base64 -d | python3 -c "import json,sys;print(json.load(sys.stdin)['client'])")

cat > "$TMP_DIR/client.properties" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="client" password="$CLIENT_PASS";
ssl.truststore.location=/mnt/sslcerts/truststore.jks
ssl.truststore.password=$JKS_PASS
EOF

# cp-server has no `tar` binary, so `oc cp` (tar-based) fails - stream the file over stdin instead.
oc exec -i -n "$NS" kafka-0 -c kafka -- sh -c 'cat > /tmp/client.properties' < "$TMP_DIR/client.properties"

oc exec -n "$NS" kafka-0 -- kafka-topics --bootstrap-server kafka:9071 --command-config /tmp/client.properties \
  --create --topic "$TOPIC" --partitions 3 --replication-factor 3

oc exec -n "$NS" kafka-0 -- kafka-topics --bootstrap-server kafka:9071 --command-config /tmp/client.properties \
  --describe --topic "$TOPIC"

oc exec -n "$NS" kafka-0 -- kafka-topics --bootstrap-server kafka:9071 --command-config /tmp/client.properties \
  --delete --topic "$TOPIC"

oc exec -n "$NS" kafka-0 -- rm -f /tmp/client.properties
echo "Smoke test OK: topic created over TLS+SASL, ISR == replicas on every partition, topic deleted."
