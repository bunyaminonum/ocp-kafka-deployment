# Kafka (KRaft) Deployment on OpenShift with CFK

This document explains how an Apache Kafka cluster running in KRaft mode (no ZooKeeper)
was deployed on OpenShift using the Confluent for Kubernetes (CFK) operator, why each
decision was made, and how the problems encountered along the way were solved.

This setup was first built and verified in a **practice/development environment** (a
single-node OpenShift simulated on OCI with CRC / OpenShift Local), with the goal of
understanding the mechanics before moving to a real/production OCP cluster.

> **Moving to an enterprise/production environment:** This document walks through every
> step we took from scratch in the playground (including installing CRC itself). A separate,
> simplified document — [`docs/enterprise-playbook.md`](docs/enterprise-playbook.md) — tells
> you which steps to skip in a real enterprise OCP environment and which ones need to be
> confirmed with the platform team.
>
> **Security/GitOps hardening:** The setup in this README is deliberately deployed without
> TLS/SASL, using GitHub Actions (push-based). The [`prod-deployment/`](prod-deployment/)
> folder contains the SAME Kafka KRaft cluster hardened with Custom SCC + TLS + SASL/PLAIN +
> ArgoCD (pull-based GitOps), fully live-tested —
> see [`prod-deployment/README.md`](prod-deployment/README.md).

## Two deployment profiles

This repo contains **two alternative** deployment profiles for the same `confluent`
namespace (not both at once — pick one):

| Profile | Folder | Security | Automation | Use case |
|---|---|---|---|---|
| **Quickstart** | [`manifests/`](manifests/) | PLAINTEXT (none) | GitHub Actions (push) | Learning / quick trial |
| **Production** | [`prod-deployment/`](prod-deployment/) | Custom SCC + TLS + SASL | ArgoCD (pull GitOps) | Hardened, deployment-ready |

## Table of contents

1. [Architecture overview](#architecture-overview)
2. [Versions used](#versions-used)
3. [Practice environment (CRC) — specific to this environment only, skipped in production](#practice-environment-crc)
4. [Installing the CFK operator](#installing-the-cfk-operator)
5. [The OpenShift SCC decision](#the-openshift-scc-decision)
6. [Deploying the Kafka KRaft cluster](#deploying-the-kafka-kraft-cluster)
7. [Verification](#verification)
8. [Automated deployment with GitHub Actions](#automated-deployment-with-github-actions)
9. [Problems encountered and how they were solved](#problems-encountered-and-how-they-were-solved)
10. [Current limitations](#current-limitations)
11. [What's left for production](#whats-left-for-production)
12. [Resources](#resources)

---

## Architecture overview

```
                         ┌─────────────────────────┐
                         │   confluent-operator     │  (CFK operator installed via Helm,
                         │   (Deployment, 1 pod)     │   watches the Kafka/KRaftController CRs)
                         └────────────┬─────────────┘
                                      │ reconcile
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                       ▼
   ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
   │ kraftcontroller-0/1/2 │  │      kafka-0/1/2      │  │  (future: Connect,    │
   │  (StatefulSet, 3 pods)│◄─┤  (StatefulSet, 3 pods) │  │   Schema Registry,    │
   │  metadata/Raft quorum │  │   brokers              │  │   Control Center...)  │
   └─────────────────────┘  └─────────────────────┘  └─────────────────────┘
```

- **KRaft**: Kafka no longer needs ZooKeeper; it manages metadata internally via the Raft
  consensus algorithm. In CFK this is modeled as a separate `KRaftController` CRD.
- **Operator pattern**: Helm installs ONLY the operator itself. Actual components like
  Kafka/KRaftController are requested as separate CR (Custom Resource) YAMLs applied with
  `oc apply`; the operator watches these and creates the underlying StatefulSet/Service/
  Secret/ConfigMap objects, continuously monitoring their health (reconcile loop).

## Versions used

| Component | Version | Note |
|---|---|---|
| CFK (Confluent for Kubernetes) | **3.3.0** | Latest release at the time this document was written (released 2026-06-23) |
| Confluent Platform / Kafka image (`cp-server`) | **8.3.0** | Latest release |
| `confluent-init-container` | **3.3.0** | **Rule**: the init container tag always tracks CFK's own version, not the CP version |
| Kubernetes (OpenShift underlying version) | 1.35.5 | Within the 1.28-1.36 range supported by CFK 3.3.0 |

**Known caveat:** CP 8.3.0 has a known memory leak in the RocksDB layer for applications
using the Kafka Streams client library (KAFKA-20616 / KAFKA-20688). This affects **only
Kafka Streams applications**, not the brokers/controllers we deployed here — so we went
ahead with 8.3.0. Worth knowing if a Kafka Streams-based consumer is deployed later.

## Practice environment (CRC)

> This section is **specific only** to the practice/learning environment on this OCI
> machine. On a real/production OCP cluster, start directly from
> [Installing the CFK operator](#installing-the-cfk-operator).

Setting up a real, multi-node OCP cluster (RHCOS) on a single OCI VM wasn't practical.
Instead, Red Hat's official **CRC (CodeReady Containers / OpenShift Local)** tool was used
to bring up a single-node cluster with a real OpenShift API:

- OCI instance: `VM.Standard.E5.Flex`, resized to 4 OCPU (8 vCPU) / 32GB RAM / 100GB disk
  (OCPU/RAM could be resized live because it's a flexible "Flex" shape; the disk's GPT
  table was fixed with `sgdisk -e` and extended with `growpart` + `resize2fs`).
- Prerequisite packages: `qemu-kvm`, `libvirt-daemon-system`, `libvirt-clients`,
  `network-manager`. **Note:** while installing NetworkManager, the main SSH interface
  (`enp0s5`) was deliberately left "unmanaged" (`/etc/NetworkManager/conf.d/unmanaged.conf`)
  — otherwise NetworkManager could have taken over that interface and disrupted the SSH
  connection.
- `crc setup` + `crc start` brought up a single-node OpenShift 4.22.1 cluster (6 vCPU /
  24GB RAM / 80GB disk allocated to CRC's own VM).
- Pull secret: a personal Red Hat account pull secret obtained from
  `console.redhat.com/openshift/create/local`, saved as `~/.crc/pull-secret.json`.

## Installing the CFK operator

```bash
# 1. Namespace
oc new-project confluent          # in production a project/namespace is likely already assigned

# 2. Confluent's official Helm repo
helm repo add confluentinc https://packages.confluent.io/helm
helm repo update

# 3. Install the CFK operator (with Default SCC — see below)
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes \
  --set podSecurity.enabled=false \
  --namespace confluent

# 4. Verify
oc get pods -n confluent
oc get crd | grep confluent
```

`helm upgrade --install` is idempotent (updates if already installed, installs otherwise) —
this is exactly why the same command will also be used in the GitHub Actions pipeline.

## The OpenShift SCC decision

**This is a completely separate topic from Kafka's SSL/SASL security** — it can't be
skipped even without TLS, because without it the pods won't start at all.

OpenShift controls which Linux UID a pod runs as far more strictly than plain Kubernetes
(SCC = Security Context Constraint). CFK wants to run its containers with a fixed **UID
1001** by default, but OpenShift's `restricted-v2` SCC assigns a random UID range to each
namespace and doesn't allow pods to step outside of it.

Confluent's official solution ([source](https://github.com/confluentinc/confluent-kubernetes-examples/tree/master/security/openshift-security))
offers two paths:

- **Default SCC (what we used, recommended)**: disable CFK's fixed-UID enforcement with
  `--set podSecurity.enabled=false` in the Helm install AND `spec.podTemplate.
  podSecurityContext: {}` in every CR; OpenShift then assigns its own random UID.
- **Custom SCC (advanced)**: define a custom SCC object (`uidRangeMin`/`uidRangeMax`) and
  bind it to the service accounts — more control but more complex.

We went with Default SCC. This decision can be revisited when adding certificates/TLS in
production.

## Deploying the Kafka KRaft cluster

Manifests: [`manifests/kraft-controller.yaml`](manifests/kraft-controller.yaml),
[`manifests/kafka.yaml`](manifests/kafka.yaml)

```bash
oc apply -f manifests/kraft-controller.yaml
oc apply -f manifests/kafka.yaml
oc get pods -n confluent -w
```

**Key fields:**

- `spec.replicas: 3` — a **mandatory rule** for KRaft: the controller count must be odd and
  at least 3 (Raft requires a majority vote; an even number risks split-brain). 3 for the
  Kafka broker as well, the classic HA minimum.
- `spec.dataVolumeCapacity` — a separate PVC request per replica. **In production** this
  should be sized according to the real storage class (see [Limitations](#current-limitations)).
- `spec.dependencies.kRaftController.clusterRef.name` — tells the Kafka CR which
  KRaftController to bind to. `zookeeper` and `kRaftController` cannot both be specified.
- `spec.podTemplate.podSecurityContext: {}` — applying the SCC decision above.
- TLS/authentication fields **deliberately left empty** — when unspecified, CFK opens a
  PLAINTEXT listener by default. Consistent with our decision to defer security.

## Verification

Pod status alone isn't enough (it only shows the process is up); real broker health must be
verified with client commands:

```bash
oc get pods -n confluent                       # should show 7/7 Running, RESTARTS=0
oc get kraftcontroller,kafka -n confluent       # STATUS should be RUNNING

# Real end-to-end test
oc exec -n confluent kafka-0 -- kafka-cluster cluster-id --bootstrap-server kafka:9071
oc exec -n confluent kafka-0 -- kafka-topics --bootstrap-server kafka:9071 \
  --create --topic health-check --partitions 3 --replication-factor 3
oc exec -n confluent kafka-0 -- kafka-topics --bootstrap-server kafka:9071 \
  --describe --topic health-check   # Isr should equal Replicas on every partition
oc exec -n confluent kafka-0 -- kafka-topics --bootstrap-server kafka:9071 \
  --delete --topic health-check
```

Tested in this environment: a 3-partition/RF3 topic was created successfully, all
partitions fully in-sync (no under-replicated partitions), leaders evenly spread across the
3 brokers.

Also, the transient `Node X disconnected` messages seen in the logs right after deployment
are normal — KRaft quorum nodes can go through a brief connect/disconnect cycle while first
establishing connections. What matters is that after these messages, healthy periodic
activity (like `Log roller completed`) continues uninterrupted with no errors/restarts.

## Automated deployment with GitHub Actions

Instead of manually running `oc apply`, a GitHub Actions pipeline was set up that
automatically applies `manifests/kraft-controller.yaml` and `manifests/kafka.yaml` whenever
they're pushed to `main` (or triggered manually).

### 8.1 Three separate authentication mechanisms — don't mix them up

This section involves three completely independent credentials/mechanisms:

1. **SSH key**: for running `git push` (as a human) from this machine — has nothing to do
   with GitHub Actions itself.
2. **Runner registration token**: for registering this machine as a GitHub Actions
   self-hosted runner — a short-lived (~1 hour), one-time token issued by GitHub.
3. **ServiceAccount token**: for the pipeline (the job running on the runner) to log in to
   the OCP cluster — a long-lived (1 year) token we generate on the cluster side.

### 8.2 Git repo and SSH access

```bash
git config --global user.name "<name>"
git config --global user.email "<email>"
ssh-keygen -t ed25519 -C "<machine-label>" -f ~/.ssh/id_ed25519 -N ""
cat ~/.ssh/id_ed25519.pub   # this output gets added to GitHub
```

The public key was added under the repo's **Settings → Deploy keys → Add deploy key**
(with Write access checked) — deliberately chosen as a **Deploy Key** instead of a
"Personal SSH key" on the whole account, because this machine is only used for this one
repo; a Deploy Key's access is also limited to that repo (it can't reach other repos on the
account).

```bash
ssh -T git@github.com   # expect "Hi <user>/<repo>! You've successfully authenticated"
git clone git@github.com:<user>/<repo>.git
```

### 8.3 Setting up the self-hosted runner

**Why self-hosted (not GitHub-hosted)?** GitHub's own hosted runners run over the internet,
on GitHub's cloud servers. Since an enterprise OCP cluster is almost certainly on a private
network with no internet access, a GitHub-hosted runner could never reach the cluster's
API. This means the runner needs to be registered on a machine that has network access to
the cluster (here, this OCI machine; in an enterprise environment, a machine on that
network).

**Security note:** Self-hosted runners are risky on **public repos** — anyone can open a
Pull Request and run code on the runner (i.e. this machine) from that PR's CI steps. Make
sure the repo is **private**.

The runner version and download link were pulled from GitHub's official releases API (no
guessed/static version number was used):
```bash
curl -s https://api.github.com/repos/actions/runner/releases/latest \
  | grep -E '"tag_name"|browser_download_url.*linux-x64.*tar.gz'
```
Version resolved in this environment: **v2.335.1**.

```bash
mkdir -p ~/actions-runner && cd ~/actions-runner
curl -o actions-runner-linux-x64-2.335.1.tar.gz -L \
  https://github.com/actions/runner/releases/download/v2.335.1/actions-runner-linux-x64-2.335.1.tar.gz
tar xzf ./actions-runner-linux-x64-2.335.1.tar.gz

# Token: obtained from repo → Settings → Actions → Runners → New self-hosted runner
# (valid ~1 hour, one-time use, never stored statically)
./config.sh --url https://github.com/<user>/<repo> --token <TOKEN> \
  --unattended --name oci-ocp-runner --labels self-hosted,ocp --work _work

# Install as a systemd service so it stays up even if the terminal is closed
sudo ./svc.sh install
sudo ./svc.sh start
sudo ./svc.sh status
```

`--labels self-hosted,ocp`: matched by `runs-on: [self-hosted, ocp]` in the workflow YAML —
if there were multiple self-hosted runners, labeling would be essential to route jobs to
the right machine.

### 8.4 A restricted-privilege identity for the pipeline (ServiceAccount + RBAC)

Instead of `kubeadmin` (full privileges), the pipeline needs a **least privilege** identity
that can only manage Kafka/KRaftController in the `confluent` namespace — so that even if
the credential leaks or misbehaves, the damage stays confined to the `confluent` namespace.

Manifest: [`manifests/rbac-github-actions.yaml`](manifests/rbac-github-actions.yaml)

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: github-actions-deployer
  namespace: confluent
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: github-actions-deployer
  namespace: confluent
rules:
  - apiGroups: ["platform.confluent.io"]
    resources: ["kafkas", "kraftcontrollers"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: [""]
    resources: ["pods", "pods/log", "events", "persistentvolumeclaims"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["apps"]
    resources: ["statefulsets"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: github-actions-deployer
  namespace: confluent
subjects:
  - kind: ServiceAccount
    name: github-actions-deployer
    namespace: confluent
roleRef:
  kind: Role
  name: github-actions-deployer
  apiGroup: rbac.authorization.k8s.io
```

**Why `Role` (not `ClusterRole`):** we deliberately scope the permission to only the
`confluent` namespace. Full CRUD on `kafkas`/`kraftcontrollers` (the pipeline's actual job),
read-only on `pods`/`events`/`statefulsets` (for post-deploy verification — creating/
deleting pods is the CFK operator's job, not the pipeline's).

```bash
oc apply -f manifests/rbac-github-actions.yaml
oc get sa,role,rolebinding -n confluent | grep github-actions
```

### 8.5 Token, kubeconfig, and GitHub Secret

```bash
TOKEN=$(oc create token github-actions-deployer -n confluent --duration=8760h)
API_SERVER=$(oc whoami --show-server)

oc config set-cluster ci-cluster --server="$API_SERVER" --insecure-skip-tls-verify=true --kubeconfig=./ci-kubeconfig
oc config set-credentials github-actions-deployer --token="$TOKEN" --kubeconfig=./ci-kubeconfig
oc config set-context github-actions-deployer --cluster=ci-cluster --user=github-actions-deployer --namespace=confluent --kubeconfig=./ci-kubeconfig
oc config use-context github-actions-deployer --kubeconfig=./ci-kubeconfig

oc get kafka --kubeconfig=./ci-kubeconfig   # verifies authentication + authorization + namespace all in one shot
```

- `oc create token ... --duration=8760h`: modern Kubernetes (1.24+) no longer issues
  automatic/permanent tokens to ServiceAccounts; a time-bound token is minted on demand via
  the `TokenRequest API` (`--duration` is automatically shortened if it exceeds what the
  cluster allows).
- A separate `./ci-kubeconfig` file: the personal `~/.kube/config` (which carries kubeadmin
  privileges) was never touched — the pipeline's restricted identity lives in a physically
  isolated file.
- `--insecure-skip-tls-verify=true`: added deliberately because CRC uses a self-signed
  certificate. **Should not be used in production** — a real CA certificate
  (`certificate-authority-data`) should be added instead.

This file must never go into git (it holds a live token):
```bash
echo "ci-kubeconfig" >> .gitignore
git add .gitignore && git commit -m "Add .gitignore for local CI kubeconfig" && git push
```

The file was added to GitHub's encrypted Secret store as a single base64 string:
```bash
base64 -w0 ci-kubeconfig
```
Repo → **Settings → Secrets and variables → Actions → New repository secret** →
Name: `KUBECONFIG_B64`, Value: the base64 output above.

### 8.6 The workflow YAML

File: [`.github/workflows/deploy-kafka.yaml`](.github/workflows/deploy-kafka.yaml)

```yaml
name: Deploy Kafka KRaft to OCP

on:
  push:
    branches: [main]
    paths:
      - 'manifests/kraft-controller.yaml'
      - 'manifests/kafka.yaml'
  workflow_dispatch: {}

jobs:
  deploy:
    runs-on: [self-hosted, ocp]
    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Setup kubeconfig
        run: |
          echo "${{ secrets.KUBECONFIG_B64 }}" | base64 -d > $RUNNER_TEMP/kubeconfig
          echo "KUBECONFIG=$RUNNER_TEMP/kubeconfig" >> "$GITHUB_ENV"

      - name: Apply KRaftController
        run: oc apply -f manifests/kraft-controller.yaml

      - name: Apply Kafka
        run: oc apply -f manifests/kafka.yaml

      - name: Wait for rollout and verify
        run: |
          oc get kraftcontroller,kafka -n confluent
          oc get pods -n confluent
```

- `on.push.branches/paths`: triggers only on `main`, only when these two manifests change
  (so a README edit doesn't needlessly touch Kafka).
- `workflow_dispatch: {}`: can also be triggered manually from the GitHub UI with
  "Run workflow".
- `runs-on: [self-hosted, ocp]`: matches the label given when the runner was registered.
- `Setup kubeconfig` step: decodes the secret and writes it to `$RUNNER_TEMP` (the runner's
  own temp folder, auto-cleaned after the job), then writes to `GITHUB_ENV` so all
  subsequent steps inherit the `KUBECONFIG` environment variable.

### 8.7 Pipeline verification tests

**Test 1 — mechanical verification (no-op apply):** the pipeline was triggered manually
while the manifests had no changes. Runner journal log:
```
Running job: deploy
Job deploy completed with result: Succeeded
```
This proved authentication (token valid), authorization (RBAC didn't return `Forbidden`),
and that the runner could pick up jobs — but since Kafka already existed, `apply` didn't
change anything (Kubernetes apply is idempotent).

**Test 2 — deployment from scratch (the real test):** the Kafka/KRaftController in the
cluster was manually deleted entirely:
```bash
oc delete -f manifests/kafka.yaml
oc delete -f manifests/kraft-controller.yaml
oc get kafka,kraftcontroller,pods,pvc -n confluent   # confirmed everything was empty/gone
```
The pipeline was then triggered again via `workflow_dispatch`. Result:
- Runner log: a second `Running job: deploy` → `Job deploy completed with result: Succeeded`.
- `oc get pvc -n confluent`: **entirely new PVC UIDs** (different from before) — proof it was
  genuinely provisioned from scratch, none of the old resources were reused.
- All pods came up `Running` with `0` restarts within ~2-3 minutes.
- A real topic test was run again for verification (`pipeline-from-scratch-test`, 3
  partitions/RF3) — all partitions fully in-sync, leaders evenly distributed.

Together, these two tests proved end-to-end that the pipeline can both "leave things alone
when nothing changed" and "bring up a full cluster from scratch."

## Problems encountered and how they were solved

These are real errors we hit — you may run into the same ones in a production environment:

### 1. `crc status` / `crc oc-env`: "Unable to connect to kvm driver"
Even when the cluster itself is running (verifiable with `virsh -c qemu:///system list`),
some `crc` CLI commands can throw this error on the client side — it has nothing to do with
libvirt/kvm group membership (it recurred even with full membership). This is **purely a
CRC-specific client bug**, and won't be an issue on a real OCP. Fix: connect directly with
`oc` via the merged kubeconfig (`~/.kube/config`, `crc-admin` context) instead of using
`crc status`.

### 2. Kafka/KRaftController pods never get created: SCC Forbidden
On the first attempt, forgetting the `podTemplate.podSecurityContext: {}` field produced:
```
unable to validate against any security context constraint: [...]
provider restricted-v2: .containers[0].runAsUser: Invalid value: 1001: must be in the ranges: [...]
```
See [The OpenShift SCC decision](#the-openshift-scc-decision) for the root cause and fix.

### 3. PVCs get Bound at a larger size than requested
`dataVolumeCapacity: 5Gi`/`10Gi` was requested but the PVCs got Bound at `79Gi`. This isn't
a bug — CRC's default `hostpath-provisioner` doesn't actually enforce the capacity quota,
it reports the entire available disk. **On real production infrastructure (a
network-attached storage class that enforces quotas), the requested size will be applied
exactly as specified.**

## Current limitations

These are deliberately skipped/simplified points — must be addressed before production:

- ❌ **No TLS/SASL** — all listeners are PLAINTEXT. (Deliberate — a user decision; certificates
  will be added in a separate step.)
- ❌ Pod `resources.requests/limits` (CPU/memory) not defined.
- ❌ `dataVolumeCapacity` values were shrunk to fit the practice environment's disk limits
  (5Gi/10Gi) — must be scaled up to the real need in production.
- ❌ Monitoring/JMX metric export not configured.
- ❌ No Network Policy defined.
- ❌ No backup/disaster recovery strategy.
- ⚠️ Default SCC was used (not Custom SCC) — should be revisited if tighter isolation is needed.
- ⚠️ GitHub Secret storing the raw kubeconfig (base64) — should be migrated to a secret
  manager if the organization has one.
- ⚠️ Self-hosted runner was set up on a single machine (this OCI VM) for a single project —
  an enterprise environment likely has a shared runner pool; check with the platform team
  before setting up a new one.

## What's left for production

1. Add TLS (broker-broker, broker-controller, client-broker) + SASL/mTLS authentication.
2. Define CPU/memory request-limits via `podTemplate.resources`.
3. Size `dataVolumeCapacity` to the real storage class and the organization's capacity plan.
4. Add monitoring with Confluent Control Center or a Prometheus/JMX exporter.
5. Review `PodDisruptionBudget` (CFK creates one automatically, verify it), `NetworkPolicy`,
   and resource quotas.
6. Consider moving to a Custom SCC (tighter UID/GID control).
7. Review GitHub Actions secret management — here we stored the raw kubeconfig as a single
   GitHub Secret; if the organization has a secret manager standard (Vault, OIDC
   federation, etc.), migrate to it. Also define a rotation strategy for the token inside
   `KUBECONFIG_B64` (valid for 1 year).
8. For the move to an enterprise environment, go through the clarification questions in
   [`docs/enterprise-playbook.md`](docs/enterprise-playbook.md) with the platform/DevOps team.

## Resources

- [Deploy Confluent for Kubernetes - CFK](https://docs.confluent.io/operator/current/co-deploy-cfk.html)
- [Configure and Manage KRaft Using CFK](https://docs.confluent.io/operator/current/co-configure-kraft.html)
- [Confluent for Kubernetes Quick Start](https://docs.confluent.io/operator/current/co-quickstart.html)
- [Confluent for Kubernetes Release Notes](https://docs.confluent.io/operator/current/release-notes.html)
- [Plan for Confluent Platform Deployment Using CFK](https://docs.confluent.io/operator/current/co-plan.html)
- [confluent-kubernetes-examples: openshift-security](https://github.com/confluentinc/confluent-kubernetes-examples/tree/master/security/openshift-security)
- [Release Notes for Confluent Platform 8.3](https://docs.confluent.io/platform/current/release-notes/index.html)
