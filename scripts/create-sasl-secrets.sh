#!/usr/bin/env bash
# Creates the SASL/PLAIN credential secrets (kraft-credential, kafka-credential) that
# manifests/03-kafka/*.yaml reference via jaasConfig.secretRef.
#
# Deliberately NOT a YAML manifest: secrets must never be committed to git. Passwords are
# generated at runtime with `openssl rand -hex` (no special characters, so they can never
# fail CFK's password regex) and temp files are shredded immediately after use.
#
# To update an EXISTING secret, re-run this script — it uses `apply` via --dry-run=client,
# never `oc delete` (a live Kafka/KRaftController holds a finalizer on these secrets, so a
# delete will hang; see README - Problems encountered).
set -euo pipefail

NS="${1:-confluent}"
TMP_DIR="$(mktemp -d)"
trap 'shred -u "$TMP_DIR"/*.json "$TMP_DIR"/*.txt 2>/dev/null; rmdir "$TMP_DIR" 2>/dev/null' EXIT

KRAFT_PASS=$(openssl rand -hex 24)
BROKER_PASS=$(openssl rand -hex 24)
CLIENT_PASS=$(openssl rand -hex 24)

# Critical check: a variable that silently expanded empty produces password="" and CFK
# rejects it with "password is not formatted correctly against the regex" - fail loudly
# instead, before anything gets written to a secret.
for pass_var in KRAFT_PASS BROKER_PASS CLIENT_PASS; do
  val="${!pass_var}"
  if [ "${#val}" -ne 48 ]; then
    echo "ERROR: $pass_var is not 48 chars - refusing to create secrets with an empty/short password" >&2
    exit 1
  fi
done

printf '{"kraft":"%s"}\n' "$KRAFT_PASS" > "$TMP_DIR/kraft-users.json"
printf 'username=kraft\npassword=%s\n' "$KRAFT_PASS" > "$TMP_DIR/kraft-interbroker.txt"
oc create secret generic kraft-credential -n "$NS" \
  --from-file=plain-users.json="$TMP_DIR/kraft-users.json" \
  --from-file=plain-interbroker.txt="$TMP_DIR/kraft-interbroker.txt" \
  --dry-run=client -o yaml | oc apply -f -

printf '{"kafka":"%s","client":"%s"}\n' "$BROKER_PASS" "$CLIENT_PASS" > "$TMP_DIR/kafka-users.json"
printf 'username=kafka\npassword=%s\n' "$BROKER_PASS" > "$TMP_DIR/kafka-interbroker.txt"
oc create secret generic kafka-credential -n "$NS" \
  --from-file=plain-users.json="$TMP_DIR/kafka-users.json" \
  --from-file=plain-interbroker.txt="$TMP_DIR/kafka-interbroker.txt" \
  --dry-run=client -o yaml | oc apply -f -

echo "kraft-credential and kafka-credential created/updated in namespace $NS."
echo "Client app password (save this now, it is not stored anywhere else): $CLIENT_PASS"
