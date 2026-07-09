# CFK Kafka (KRaft) Deployment at Enterprise Scale — Playbook

## What this document is, and isn't

The main [README](../README.md) explains how this project was built in a **playground**
(a single-node CRC/OpenShift Local set up from scratch on OCI), *why* each decision was
made, and the problems encountered — highly detailed, meant for learning/proof.

This document serves a **different purpose**: a plain, linear **procedure (playbook)** for
someone who will perform this deployment on a real enterprise OpenShift (OCP) environment.
Some of the things we built from scratch in the playground (OpenShift itself, the
self-hosted runner, etc.) are most likely **already present** in an enterprise environment
— this playbook distinguishes what to skip, what to apply as-is, and what needs to be
confirmed with the platform team.

**Whoever follows this playbook may not know the details of the organization's own OCP
environment** — so every step includes "know/ask this before doing it" notes.

> **Update:** The TLS, SASL, and GitOps items listed as "to-do" in Step 6 below have now
> actually been **built and live-tested** in the [`prod-deployment/`](../prod-deployment/)
> folder (Custom SCC, cert-manager TLS, SASL/PLAIN, ArgoCD). The [README](../prod-deployment/README.md)
> in that folder also covers the real problems encountered (RBAC, resource contention,
> secret format errors) — that document should be the primary reference when moving to an
> enterprise environment; this one is more of a general checklist.

---

## Step 0 — Before you start: things to confirm with the platform/DevOps team

Don't proceed without knowing the answers to the questions below. In the playground **we
built all of these from scratch** (because we had no real OCP available); in an enterprise
environment most of these probably already exist — trying to rebuild them is both
unnecessary and risky (you could set up a conflicting configuration on top of something
that already exists).

| Question | Why it matters |
|---|---|
| What's the OCP cluster's API endpoint, do you have access? | The starting point for everything |
| Is there a project/namespace assigned to you, or will you create your own? | Permissions and isolation |
| Is a CFK operator **already installed** on the cluster (cluster-wide, by a central platform team)? | Check with `oc get crd \| grep confluent` — skip Step 2 if so |
| Do you have permission to install CRDs, or is that a platform-admin task? | May need Helm's `--skip-crds` |
| Which SCC policy does the cluster use/enforce? | The playground's "Default SCC" decision may not apply; the organization may have its own Custom SCC |
| What storage classes exist, which one is recommended for production? | The playground's `hostpath-provisioner` is **tied to a single node, not HA, and must never be used in an enterprise environment** |
| Is there a shared self-hosted runner pool for GitHub Actions? | Likely already managed by the platform team — ask before setting up your own |
| Does the runner (wherever it lives) have network access (firewall/VPN/subnet) to the OCP API? | Without it the pipeline won't work at all |
| Does the organization have a secret management standard (Vault, Sealed Secrets, OIDC federation)? | Putting a raw kubeconfig into a GitHub Secret, as we did in the playground, may not be acceptable |
| Does the organization have an internal CA for TLS/SASL? | Deliberately skipped in the playground, required in production |
| Is there a private registry/mirror for Confluent images? | Some enterprise networks block direct Docker Hub access |
| Is a resource (CPU/RAM/disk) quota/limit defined on the namespace? | Size Kafka/KRaftController accordingly |

---

## Step 1 — Playground-specific steps to SKIP here

These only arose from the need "we had no real OCP, so we simulated one from scratch."
Since a real enterprise OCP is already running, they are **skipped entirely**:

- ~~OCI/VM sizing, disk expansion~~
- ~~Installing libvirt/qemu-kvm/NetworkManager~~
- ~~Installing CRC (`crc setup`, `crc start`)~~
- ~~Personal Red Hat pull secret (`console.redhat.com/openshift/create/local`)~~

Start directly from Step 2.

---

## Step 2 — Installing the CFK operator

First, **check whether it's already installed** per the table in Step 0:
```bash
oc get crd | grep confluent
oc get pods -A | grep confluent-operator
```
If already installed, skip this step and go straight to Step 3.

If not installed and you have permission to install it:
```bash
oc new-project <your-project>        # or use the namespace assigned to you

helm repo add confluentinc https://packages.confluent.io/helm
helm repo update

# SCC decision: choose based on the policy confirmed in Step 0
# Option A — Default SCC (what we used in the playground, simpler):
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes \
  --set podSecurity.enabled=false \
  --namespace <your-project>

# Option B — if a Custom SCC is used: apply the SCC object provided by the platform
# team, bind it to the service accounts, install Helm WITHOUT podSecurity.enabled=false.
# Details: https://github.com/confluentinc/confluent-kubernetes-examples/tree/master/security/openshift-security

oc get pods -n <your-project>
oc get crd | grep confluent
```

If you don't have permission to install CRDs:
```bash
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes \
  --skip-crds --namespace <your-project>
```
(The CRDs must already have been installed by the platform team beforehand.)

---

## Step 3 — Deploy the Kafka KRaft cluster

The repo's [`manifests/kraft-controller.yaml`](../manifests/kraft-controller.yaml) and
[`manifests/kafka.yaml`](../manifests/kafka.yaml) can be used **as a starting point**, but
**adjust the values that were shrunk for the playground to the enterprise's actual needs**:

| Field | Playground value | What to do in production |
|---|---|---|
| `dataVolumeCapacity` (KRaftController) | `5Gi` | Scale up based on the real storage class + capacity plan (typical: 20-50Gi) |
| `dataVolumeCapacity` (Kafka) | `10Gi` | Scale up based on real need (typical: 100Gi+) |
| `image.application` / `image.init` | `8.3.0` / `3.3.0` | Use a CP+CFK version pair the organization has approved/tested; it doesn't have to be the very latest |
| `podTemplate.podSecurityContext` | `{}` (Default SCC) | May change depending on the SCC policy confirmed in Step 0/2 |
| TLS/authentication fields | empty (PLAINTEXT) | **Must be filled in for production** — see "What's left for production" in the main README |
| `podTemplate.resources` | undefined | CPU/memory `requests`/`limits` must be added |
| `storageClass` | not specified (default used) | **Explicitly** specify the storage class recommended for your namespace, don't rely on the default |

```bash
oc apply -f manifests/kraft-controller.yaml
oc apply -f manifests/kafka.yaml
oc get pods -n <your-project> -w
```

---

## Step 4 — Verification

The commands in the main README's [Verification](../README.md#verification) section can be
applied exactly as-is — check pod status + create a real test topic and check its ISR. In
short:

```bash
oc get pods -n <your-project>
oc get kraftcontroller,kafka -n <your-project>
oc exec -n <your-project> kafka-0 -- kafka-cluster cluster-id --bootstrap-server kafka:9071
oc exec -n <your-project> kafka-0 -- kafka-topics --bootstrap-server kafka:9071 \
  --create --topic health-check --partitions 3 --replication-factor 3
oc exec -n <your-project> kafka-0 -- kafka-topics --bootstrap-server kafka:9071 \
  --describe --topic health-check
oc exec -n <your-project> kafka-0 -- kafka-topics --bootstrap-server kafka:9071 \
  --delete --topic health-check
```

---

## Step 5 — GitHub Actions pipeline

**Ask first:** does the organization already have a shared self-hosted runner pool? If so,
skip Step 5.2 and write your own workflow directly against that pool's label (`runs-on`).

### 5.1 Repo access
Same as the playground: create an SSH key, add it to the repo as a Deploy Key (or use
whatever the organization's own git access standard is — some organizations may require
SSO/PAT instead).

### 5.2 Self-hosted runner (only if needed)
The steps in [section 8.3](../README.md#automated-deployment-with-github-actions) of the
main README can be applied exactly as-is — the runner having network access to the OCP API
is the **only critical requirement**.

### 5.3 Pipeline identity (ServiceAccount + RBAC)
[`manifests/rbac-github-actions.yaml`](../manifests/rbac-github-actions.yaml) can be applied
as-is after updating the namespace name to your own — the **least privilege** principle
(CRUD only on `kafkas`/`kraftcontrollers`, only in your own namespace) applies in an
enterprise environment too, and is even more critical there.

### 5.4 Secret management — CAUTION
In the playground we base64-encoded the raw kubeconfig and put it in a GitHub Secret.
**Before doing this, ask about the organization's secret management standard** (Step 0).
Alternatives:
- If the organization has Vault or an external secret manager, the kubeconfig/token should
  be pulled from there instead.
- If OIDC federation is supported, short-lived/auto-rotating authentication should be
  preferred over a static token.
- Even if a raw kubeconfig is used, shorten the token's lifetime (`--duration`) according to
  the organization's security policy and define a rotation schedule (1 year was acceptable
  for the playground, but may be far too long in an enterprise environment).

### 5.5 Workflow YAML
[`.github/workflows/deploy-kafka.yaml`](../.github/workflows/deploy-kafka.yaml) can be used
as a template; update the `runs-on` label to match the organization's runner pool.

---

## Step 6 — Production hardening checklist

These were **deliberately skipped** in the playground and must not go live in an enterprise
deployment without review. For detailed reasoning, see the main README's
[Current limitations](../README.md#current-limitations) and
[What's left for production](../README.md#whats-left-for-production) sections. Short list:

- [ ] TLS (broker-broker, broker-controller, client-broker) + SASL/mTLS authentication
- [ ] `podTemplate.resources` (CPU/memory request-limit)
- [ ] Real storage class + production sizing
- [ ] Monitoring (Prometheus/JMX exporter or Control Center)
- [ ] Review of NetworkPolicy, resource quota
- [ ] Backup/disaster recovery strategy
- [ ] Moving secret management to the enterprise standard (see Step 5.4)
- [ ] Confirming the SCC decision against the organization's security policy

---

## Summary flow (at a glance)

```
Step 0: Confirm with the platform team (checklist)
   │
   ▼
Step 1: SKIP the playground-specific steps
   │
   ▼
Step 2: Is the CFK operator already installed? ─── Yes ──▶ Go to Step 3
   │ No
   ▼
Step 2: Install the CFK operator (based on the SCC decision)
   │
   ▼
Step 3: Deploy the Kafka KRaft CRs with production values
   │
   ▼
Step 4: Verify (pods + real topic test)
   │
   ▼
Step 5: GitHub Actions pipeline (check for a runner pool, confirm secret management)
   │
   ▼
Step 6: Complete the production hardening checklist
```
