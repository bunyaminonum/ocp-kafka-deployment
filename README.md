# Kafka (KRaft) on OpenShift with CFK — Custom SCC + TLS + SASL + GitOps

A single, production-shaped Apache Kafka (KRaft mode, no ZooKeeper) deployment on OpenShift
using the Confluent for Kubernetes (CFK) operator: Custom SCC, TLS (cert-manager), SASL/PLAIN
authentication, and two alternative automation paths (GitHub Actions push, ArgoCD GitOps pull).

This document is a **playbook**: every step says *what* to run, *why* it's needed, and how
it changes in a real enterprise environment (`> In enterprise production:` callouts). It was
built and fully live-tested end to end — see [Verification](#step-6--verification) for the
real evidence.

There is only **one** deployment shape in this repo — no separate "quickstart" vs "production"
tracks. Everything here is TLS+SASL+Custom-SCC by default, so there's no ambiguity about
which folder is the real one.

## Table of contents

1. [Architecture](#architecture)
2. [Versions used](#versions-used)
3. [Before you start (enterprise checklist)](#before-you-start-enterprise-checklist)
4. [The practice environment used to build this](#the-practice-environment-used-to-build-this)
5. [Step 0 — Prerequisites & variables](#step-0--prerequisites--variables)
6. [Step 1 — Namespace + Custom SCC](#step-1--namespace--custom-scc)
7. [Step 2 — CFK operator](#step-2--cfk-operator)
8. [Step 3 — TLS via cert-manager](#step-3--tls-via-cert-manager)
9. [Step 4 — SASL/PLAIN credential secrets](#step-4--saslplain-credential-secrets)
10. [Step 5 — Kafka + KRaftController CRs](#step-5--kafka--kraftcontroller-crs)
11. [Step 6 — Verification](#step-6--verification)
12. [Step 7 — Automation](#step-7--automation)
13. [Quick troubleshooting](#quick-troubleshooting)
14. [Repo file map](#repo-file-map)
15. [Scope: what's genuinely tested vs. config-only](#scope-whats-genuinely-tested-vs-config-only)
16. [What's left for real production](#whats-left-for-real-production)
17. [Resources](#resources)

---

## Architecture

```
Custom SCC ──▶ CFK Operator ──▶ KRaftController (3) ◀──TLS──▶ Kafka (3 brokers)
(UID 1000-1005)   (Helm)         signed by cert-manager's CA (ca-pair-sslcerts)
                                 SASL/PLAIN + TLS listeners
                                        ▲
                    (Option B) ArgoCD/GitOps ── watches git, applies via pull
                    (Option A) GitHub Actions ── applies via push, on a self-hosted runner
```

Layering (each layer depends on the one before it): **Custom SCC → CFK operator → CA +
SASL secrets → Kafka CRs → automation (GitHub Actions or GitOps)**.

- **KRaft**: Kafka no longer needs ZooKeeper; it manages metadata internally via the Raft
  consensus algorithm. In CFK this is modeled as a separate `KRaftController` CRD.
- **Operator pattern**: Helm installs ONLY the operator. Kafka/KRaftController are requested
  as separate CR (Custom Resource) YAMLs applied with `oc apply`; the operator watches them
  and creates the underlying StatefulSet/Service/Secret/ConfigMap objects, continuously
  reconciling their state.

## Versions used

| Component | Version | Note |
|---|---|---|
| CFK (Confluent for Kubernetes) | **3.3.0** | Latest release at the time this was written |
| Confluent Platform / Kafka image (`cp-server`) | **8.3.0** | Latest release |
| `confluent-init-container` | **3.3.0** | **Rule**: the init container tag always tracks CFK's own version, not the CP version |
| cert-manager Operator for Red Hat OpenShift | v1.20.0 | OLM, `redhat-operators` catalog, `stable-v1` channel |
| OpenShift GitOps (ArgoCD) operator | v1.21.1 | OLM, `redhat-operators` catalog, `latest` channel |
| Kubernetes (OpenShift underlying version) | 1.35.5 | Within the 1.28-1.36 range supported by CFK 3.3.0 |

**Known caveat:** CP 8.3.0 has a known memory leak in the RocksDB layer for applications
using the Kafka Streams client library (KAFKA-20616 / KAFKA-20688). This affects **only
Kafka Streams applications**, not the brokers/controllers deployed here — worth knowing if a
Kafka Streams-based consumer is deployed later.

## Before you start (enterprise checklist)

Don't proceed on a real enterprise OpenShift cluster without knowing the answers below. In
the practice environment we built every one of these from scratch (there was no real OCP
available); on a real cluster, most of them already exist — rebuilding them is both
unnecessary and risky (a conflicting config could land on top of something that already
exists).

| Question | Why it matters |
|---|---|
| What's the OCP cluster's API endpoint, do you have access? | The starting point for everything |
| Is there a project/namespace assigned to you, or will you create your own? | Permissions and isolation |
| Is a CFK operator **already installed** cluster-wide by a central platform team? | Check with `oc get crd \| grep confluent` — skip Step 2 if so |
| Do you have permission to install CRDs, or is that a platform-admin task? | May need Helm's `--skip-crds` |
| Does the cluster already have an approved/central Custom SCC for Confluent? | Don't create a new one — just bind it (Step 1) |
| What storage classes exist, which one is recommended for production? | This repo's practice storage class (`hostpath-provisioner`) is **single-node, not HA — never use it in a real environment** |
| Does the organization have an internal CA for TLS? | Step 3 uses a self-signed root CA here — production must chain from the org's PKI |
| Is there a shared self-hosted runner pool for GitHub Actions, or a shared ArgoCD instance? | Likely already managed by the platform team — ask before setting up your own |
| Does the runner/ArgoCD have network access (firewall/VPN/subnet) to the OCP API? | Without it, automation won't work at all |
| Does the organization have a secret management standard (Vault, Sealed Secrets, External Secrets, OIDC federation)? | Putting a raw kubeconfig into a GitHub Secret (Step 7, Option A) may not be acceptable |
| Is there a private registry/mirror for Confluent images? | Some enterprise networks block direct Docker Hub access |
| Is a resource (CPU/RAM/disk) quota/limit defined on the namespace? | Size Kafka/KRaftController accordingly (Step 5) |

## The practice environment used to build this

> Specific to how **this repo** was originally built and validated — skip straight to
> [Step 0](#step-0--prerequisites--variables) if you already have access to a real OCP cluster.

Setting up a real, multi-node OCP cluster (RHCOS) on a single VM wasn't practical, so Red
Hat's official **CRC (CodeReady Containers / OpenShift Local)** was used to bring up a
single-node cluster with a real OpenShift API:

- OCI instance: `VM.Standard.E5.Flex`, resized to 4 OCPU (8 vCPU) / 32GB RAM / 100GB disk.
- Prerequisite packages: `qemu-kvm`, `libvirt-daemon-system`, `libvirt-clients`,
  `network-manager` (the main SSH interface was deliberately left "unmanaged" so
  NetworkManager wouldn't disrupt the SSH session).
- `crc setup` + `crc start` brought up a single-node OpenShift 4.22.1 cluster (6 vCPU / 24GB
  RAM / 80GB disk allocated to CRC's own VM).
- Pull secret: a personal Red Hat account pull secret from
  `console.redhat.com/openshift/create/local`.

Everything from [Step 0](#step-0--prerequisites--variables) onward is standard OpenShift —
none of it is specific to CRC.

---

## Step 0 — Prerequisites & variables

```bash
oc whoami                 # cluster-admin or sufficient privileges
helm version              # v3+
oc get storageclass       # a default StorageClass should exist

export NS=confluent        # target namespace
```

Resources: 3+3 broker/controller ≈ 2.5 vCPU / 7 GB requests. On a single-node/constrained
environment, make room first.

> **In enterprise production:** the namespace and quotas are usually predefined by the
> platform team; you'll only use the namespace assigned to you. StorageClass = HA/block-based
> enterprise storage (ODF/Ceph, Trident, Portworx) — CRC's `hostpath-provisioner` must never
> be used in production.

---

## Step 1 — Namespace + Custom SCC

Files: [`manifests/01-namespace-and-scc/namespace.yaml`](manifests/01-namespace-and-scc/namespace.yaml),
[`manifests/01-namespace-and-scc/scc.yaml`](manifests/01-namespace-and-scc/scc.yaml)

```bash
oc apply -f manifests/01-namespace-and-scc/namespace.yaml
oc apply -f manifests/01-namespace-and-scc/scc.yaml
oc adm policy add-scc-to-user confluent-scc -z confluent-operator -n $NS
oc adm policy add-scc-to-user confluent-scc -z default -n $NS
```

**Why:** OpenShift controls which Linux UID a pod runs as far more strictly than plain
Kubernetes (SCC = Security Context Constraint). CFK's containers run with a fixed **UID
1001**, but OpenShift's default `restricted-v2` SCC assigns a random per-namespace UID range
and won't allow a pod to step outside it — so without an SCC change, pods never start at all
(`runAsUser: Invalid value: 1001: must be in the ranges: [...]`).
[Confluent's official guidance](https://github.com/confluentinc/confluent-kubernetes-examples/tree/master/security/openshift-security)
offers a "Default SCC" path (disable CFK's UID enforcement, let OpenShift assign a random
UID) or a "Custom SCC" path (open a specific UID/GID range and bind it narrowly). This repo
uses **Custom SCC**: a `SecurityContextConstraints` object opening UID 1000-1005 / fsGroup
1001-1005, bound only to Kafka's two ServiceAccounts — a narrower, more auditable grant than
disabling UID enforcement outright.

Bind the SCC to the ServiceAccounts **before** installing the operator, so the very first
pod doesn't crashloop waiting for it.

> **In enterprise production:** there's usually a central, security-team-approved Confluent
> SCC already — don't create a new one, just `add-scc-to-user` against it. The UID range is
> the security team's call, not a per-project decision.

---

## Step 2 — CFK operator

First, **check whether it's already installed** (see the enterprise checklist above):
```bash
oc get crd | grep confluent
oc get pods -n $NS | grep confluent-operator
```
If already installed cluster-wide, skip straight to [Step 3](#step-3--tls-via-cert-manager).

Otherwise:
```bash
oc new-project $NS
helm repo add confluentinc https://packages.confluent.io/helm && helm repo update
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes \
  --namespace $NS \
  --set serviceAccount.create=true --set serviceAccount.name=confluent-operator
```

**Verify:**
```bash
oc get pods -n $NS                                  # confluent-operator Running
oc get crd | grep -q kafkas.platform.confluent.io && echo "CRD OK"
oc get pod -n $NS -l app=confluent-operator \
  -o jsonpath='{.items[0].metadata.annotations.openshift\.io/scc}{"\n"}'   # => confluent-scc
```

**Why no `podSecurity.enabled=false`:** that flag is only needed on the Default-SCC path.
Since the Custom SCC already allows UID 1001, there's nothing to disable — we just pin the
operator's ServiceAccount name so the SCC binding above reliably applies to it.
`helm upgrade --install` is idempotent (updates if already installed, installs otherwise) —
the same command is safe to re-run in CI.

If you don't have permission to install CRDs, add `--skip-crds` (the platform team must have
installed them beforehand).

> **In enterprise production:** CFK is usually installed cluster-wide or by the platform
> team; images are pulled from an internal registry
> (`--set image.registry=registry.internal…` + a pull secret) rather than Docker Hub.

---

## Step 3 — TLS via cert-manager

Files: [`manifests/02-cert-manager/operator.yaml`](manifests/02-cert-manager/operator.yaml),
[`manifests/02-cert-manager/root-ca.yaml`](manifests/02-cert-manager/root-ca.yaml)

Red Hat's official **cert-manager Operator for Red Hat OpenShift** is installed via OLM —
deliberately chosen over the community Helm chart for being OpenShift-native, Red
Hat-supported, and auto-updatable.

```bash
oc apply -f manifests/02-cert-manager/operator.yaml
oc get csv -n cert-manager-operator -w        # wait for Succeeded, then Ctrl+C
oc apply -f manifests/02-cert-manager/root-ca.yaml
oc get certificate -n $NS                      # wait for READY=True
```

**Why:** cert-manager only generates the **root CA** and writes it to a secret named exactly
`ca-pair-sslcerts` — not an arbitrary name, it's the exact name CFK's `autoGeneratedCerts:
true` feature looks for (confirmed from the Confluent documentation). For each
broker/controller pod, CFK itself generates and signs the **leaf** certificate using this CA
— cert-manager never touches leaf certs, it only manages the root authority's
generation/rotation (`renewBefore: 720h`).

**Honesty note:** the CA here is self-signed — trusted only inside this cluster, not by
anyone externally.

> **In enterprise production:** self-signed is **not** used. `issuerRef` should point to a
> `ClusterIssuer` chained to the organization's internal PKI (Vault PKI / AD CS / an
> intermediate CA), so certificates are part of the corporate trust chain. The secret name
> stays `ca-pair-sslcerts` either way.

---

## Step 4 — SASL/PLAIN credential secrets

> CFK does **not natively support SASL/SCRAM** in its CRD API (only a manual
> `configOverrides` hack is possible) — this is a verified Confluent fact, not a preference.
> **SASL/PLAIN** is used instead, cleanly managed via `jaasConfig`. Passwords are generated
> with `openssl rand -hex` (no special characters, so they can never fail CFK's password
> regex). Secrets are created **imperatively — never committed to git or written into a
> manifest.**

```bash
./scripts/create-sasl-secrets.sh $NS
```

This creates two secrets (see the script for the exact commands):
- `kraft-credential` — referenced **identically** by both the KRaftController and Kafka's
  `controllerListener` section (a rule Confluent's documentation states explicitly —
  otherwise Kafka can't connect to the controller).
- `kafka-credential` — for Kafka's client-facing internal listener; contains a separate
  `client` user for applications.

**Critical rules:** `password=` must **never be empty** — an unset shell variable silently
produces an empty password and CFK rejects it with `password is not formatted correctly
against the regex`. To update an **existing** secret, never `oc delete` it (a live
Kafka/KRaftController holds a finalizer on it, so the delete hangs) — the script already
uses `oc apply` via `--dry-run=client -o yaml` for exactly this reason.

> **In enterprise production:** passwords come from Vault/External Secrets Operator and are
> synced/rotated automatically. Where possible, mTLS client-auth or SASL/OAUTHBEARER
> (Keycloak/PingFederate) is preferred over SASL/PLAIN for stronger authentication.

---

## Step 5 — Kafka + KRaftController CRs

Files: [`manifests/03-kafka/kraft-controller.yaml`](manifests/03-kafka/kraft-controller.yaml),
[`manifests/03-kafka/kafka.yaml`](manifests/03-kafka/kafka.yaml)

```bash
oc apply -f manifests/03-kafka/
oc get pods -n $NS -w                 # kraftcontroller-0/1/2, then kafka-0/1/2 => 1/1
oc get kafka,kraftcontroller -n $NS   # STATUS=RUNNING
```

**Critical fields:**
- `spec.replicas: 3` on both — a **mandatory rule** for KRaft: the controller count must be
  odd and at least 3 (Raft requires a majority vote; an even number risks split-brain). 3 for
  the Kafka broker too, the classic HA minimum.
- `tls.autoGeneratedCerts: true` — consumes the CA from Step 3 and signs each pod's leaf cert.
- `listeners.*.authentication.type: plain` + `jaasConfig.secretRef` — wires in the secrets
  from Step 4.
- `dependencies.kRaftController.controllerListener` on the Kafka CR must match the
  KRaftController's `listeners.controller` **exactly** (same TLS + same `secretRef`) or Kafka
  can't reach the controller.
- No `podSecurityContext` — the Custom SCC from Step 1 already handles UID/GID.
- `podTemplate.affinity.podAntiAffinity` uses `preferred` (soft), because the practice
  environment is single-node. On a real multi-node cluster this **same YAML** spreads the 3
  pods across 3 different nodes automatically.

> **In enterprise production:** images are pulled from an internal registry; `storageClass`
> is explicitly specified; `dataVolumeCapacity` is sized for real need (Kafka 100-500Gi+);
> anti-affinity becomes `required` + `oneReplicaPerNode: true` +
> `rackAssignment.nodeLabels: [topology.kubernetes.io/zone]` for real multi-AZ spread;
> `resources` reflect real workload sizing (cpu 4/mem 16Gi+ per broker is typical).

---

## Step 6 — Verification

Pod status alone isn't enough (it only shows the process is up) — real broker health needs
a client-level check:

```bash
oc get pods -n $NS                       # should show 7/7 Running, RESTARTS=0
oc get kraftcontroller,kafka -n $NS       # STATUS should be RUNNING

./scripts/verify-deployment.sh $NS health-check
```

The script performs a real TLS+SASL/PLAIN smoke test (create a 3-partition/RF3 topic over
the encrypted+authenticated listener, confirm ISR == replicas on every partition, delete the
topic). **Tested end to end in this environment**: a real authenticated+encrypted
produce/consume was performed (`Processed a total of 1 messages`), and attempting to connect
**without** authentication failed (the unauthenticated client crashed with an
`OutOfMemoryError` — not a clean "access denied" message, but the outcome is the same:
unauthorized access does not work; the broker itself was unaffected, confirmed via
`RESTARTS: 0`).

The transient `Node X disconnected` messages seen in the logs right after deployment are
normal — KRaft quorum nodes go through a brief connect/disconnect cycle while first
establishing connections. What matters is that healthy periodic activity (like `Log roller
completed`) continues afterward with no errors/restarts.

---

## Step 7 — Automation

> In an enterprise environment, deployment isn't manual `oc apply` — it's automated. Two
> models are documented here; **pick one** (running both against the same manifests is
> harmless but redundant — see the push-vs-pull comparison below).

### Option A — GitHub Actions (push-based)

**Push (GitHub Actions) vs Pull (ArgoCD):**
- **Push**: a git push triggers a GitHub event → a runner picks it up → the runner connects
  to the cluster and runs `oc apply`. A static kubeconfig has to leave the cluster (stored as
  a GitHub Secret).
- **Pull**: ArgoCD is a controller running **inside** the cluster — it continuously watches
  git and reconciles drift itself. No static credential ever leaves the cluster. This is why
  production environments generally prefer pull.

Automation identity: [`manifests/04-automation/github-actions-rbac.yaml`](manifests/04-automation/github-actions-rbac.yaml)
(least-privilege — CRUD only on `kafkas`/`kraftcontrollers`, read-only on
`pods`/`events`/`statefulsets`, scoped only to `$NS`).

**7.A.1 — Three separate credentials, don't mix them up:**
1. **SSH key** — for running `git push` as a human; unrelated to GitHub Actions itself.
2. **Runner registration token** — for registering a machine as a self-hosted runner; a
   short-lived (~1 hour), one-time token issued by GitHub.
3. **ServiceAccount token** — for the pipeline to authenticate to the OCP cluster; a
   long-lived token minted on the cluster side.

**7.A.2 — Repo access:**
```bash
ssh-keygen -t ed25519 -C "<machine-label>" -f ~/.ssh/id_ed25519 -N ""
cat ~/.ssh/id_ed25519.pub   # add under repo Settings -> Deploy keys -> Add deploy key (with Write access)
ssh -T git@github.com       # expect "Hi <user>/<repo>! You've successfully authenticated"
```
A repo-scoped **Deploy Key** is used instead of a personal SSH key on the whole GitHub
account — its access is limited to this one repo.

**7.A.3 — Self-hosted runner:** GitHub-hosted runners run over the internet on GitHub's own
cloud; an enterprise OCP cluster is almost always on a private network with no path from
there — so the runner must live on a machine that **does** have network access to the
cluster API.
```bash
curl -s https://api.github.com/repos/actions/runner/releases/latest \
  | grep -E '"tag_name"|browser_download_url.*linux-x64.*tar.gz'   # resolve the real latest version, don't guess it

mkdir -p ~/actions-runner && cd ~/actions-runner
curl -o actions-runner-linux-x64-<version>.tar.gz -L <url-from-above>
tar xzf ./actions-runner-linux-x64-<version>.tar.gz

# token from repo -> Settings -> Actions -> Runners -> New self-hosted runner (one-time, ~1h validity)
./config.sh --url https://github.com/<user>/<repo> --token <TOKEN> \
  --unattended --name ocp-runner --labels self-hosted,ocp --work _work

sudo ./svc.sh install && sudo ./svc.sh start && sudo ./svc.sh status
```
`--labels self-hosted,ocp` matches `runs-on: [self-hosted, ocp]` in the workflow YAML.

**Security note:** self-hosted runners are risky on **public repos** — anyone can open a Pull
Request and run code on the runner from that PR's CI steps. Keep the repo private.

**7.A.4 — Token, kubeconfig, GitHub Secret:**
```bash
oc apply -f manifests/04-automation/github-actions-rbac.yaml

TOKEN=$(oc create token github-actions-deployer -n $NS --duration=8760h)
API_SERVER=$(oc whoami --show-server)

oc config set-cluster ci-cluster --server="$API_SERVER" --kubeconfig=./ci-kubeconfig
oc config set-credentials github-actions-deployer --token="$TOKEN" --kubeconfig=./ci-kubeconfig
oc config set-context github-actions-deployer --cluster=ci-cluster --user=github-actions-deployer --namespace=$NS --kubeconfig=./ci-kubeconfig
oc config use-context github-actions-deployer --kubeconfig=./ci-kubeconfig

oc get kafka --kubeconfig=./ci-kubeconfig   # verifies auth + authz + namespace in one shot
base64 -w0 ci-kubeconfig                     # paste as GitHub Secret KUBECONFIG_B64
```
`oc create token ... --duration=8760h`: modern Kubernetes (1.24+) no longer issues automatic
permanent ServiceAccount tokens — a time-bound token is minted on demand via the
`TokenRequest` API. `ci-kubeconfig` is a physically separate file from the personal
`~/.kube/config` (which carries cluster-admin), and is already in `.gitignore` — it must
never be committed, since it holds a live bearer token. Repo → **Settings → Secrets and
variables → Actions → New repository secret** → name `KUBECONFIG_B64`.

**7.A.5 — Workflow:** [`.github/workflows/deploy-kafka.yaml`](.github/workflows/deploy-kafka.yaml)
triggers on push to `manifests/03-kafka/**` (only the application layer — not the bootstrap
layers 01/02/04), or manually via `workflow_dispatch`, and runs `oc apply -f
manifests/03-kafka/` on the self-hosted runner.

**Verified with two real tests:** (1) a no-op trigger with no manifest changes — the run
succeeded, proving auth/authz/runner pickup all work, `apply` was simply idempotent; (2) the
whole Kafka/KRaftController was deleted from the cluster and the pipeline re-triggered — new
PVC UIDs (proof of a genuine from-scratch provision), all pods `Running` with 0 restarts
within ~2-3 minutes, and a real topic test passed again.

> **In enterprise production:** check first whether a shared self-hosted runner pool already
> exists — don't set up a new one. Storing a raw kubeconfig in a GitHub Secret should be
> replaced with the organization's actual secret standard (Vault, OIDC federation) if one
> exists, and the token's `--duration` should follow the org's rotation policy (1 year may be
> far too long).

### Option B — ArgoCD / OpenShift GitOps (pull-based)

Files: [`manifests/04-automation/gitops-operator.yaml`](manifests/04-automation/gitops-operator.yaml),
[`manifests/04-automation/gitops-rbac.yaml`](manifests/04-automation/gitops-rbac.yaml),
[`manifests/04-automation/gitops-application.yaml`](manifests/04-automation/gitops-application.yaml)

```bash
oc apply -f manifests/04-automation/gitops-operator.yaml
oc get csv -n openshift-gitops -w        # wait for Succeeded

# repo access (read-only deploy key)
oc create secret generic kafka-repo -n openshift-gitops \
  --from-literal=type=git --from-literal=url=<GIT_REPO> --from-file=sshPrivateKey=$HOME/.ssh/id_ed25519
oc label secret kafka-repo -n openshift-gitops argocd.argoproj.io/secret-type=repository

oc apply -f manifests/04-automation/gitops-rbac.yaml
oc apply -f manifests/04-automation/gitops-application.yaml
oc get application kafka -n openshift-gitops   # SYNC=Synced, HEALTH=Healthy
```

Red Hat's official **OpenShift GitOps** operator only supports the `AllNamespaces` install
mode, so it's installed into `openshift-operators`, which automatically creates the
`openshift-gitops` namespace and a ready ArgoCD instance.

**Scoping decision:** the `Application` deliberately watches only `manifests/03-kafka/`, not
the whole `manifests/` tree. The bootstrap layer (`01-namespace-and-scc/`,
`02-cert-manager/`, `04-automation/` — including its own operator install) is installed once,
manually, by a human/platform team — it isn't touched on every Kafka change. Separating the
application layer (Kafka CRs) into GitOps from the infrastructure layer (manual/admin) is
genuine enterprise practice.

**Why RBAC is applied manually, not synced by the Application:** ArgoCD's own
`application-controller` ServiceAccount has no permissions on `kafkas`/`kraftcontrollers`
until [`gitops-rbac.yaml`](manifests/04-automation/gitops-rbac.yaml) is applied — a
controller granting itself its own permissions would be a privilege-escalation anti-pattern,
so this file belongs to the bootstrap layer.

`syncPolicy.automated`: `prune: true` (a resource removed from git is also removed from the
cluster), `selfHeal: true` (a manual `oc edit` is automatically reverted back to the git
state).

**Verified with a live test:** an annotation was added to the Kafka CR purely via `git commit
+ push`, **without ever running `oc apply`** — the annotation appeared on the real object in
the cluster, proving the git → ArgoCD → cluster chain works with zero human intervention on
the cluster side.

> **In enterprise production:** the GitOps operator and repo access are typically already set
> up by the platform team; the `Application` is usually defined inside a central ArgoCD
> `AppProject` with its own RBAC boundary. On a resource-constrained single node you may need
> to lower ArgoCD's default component resource requests and disable `dex`/SSO
> (`spec.sso: null`) — not needed on real production-sized nodes.

---

## Quick troubleshooting

| Symptom | Cause / fix |
|---|---|
| Pod `Pending`, `Insufficient cpu` | Resources exhausted; free up capacity / review the quota |
| No pods, `runAsUser: Invalid value: 1001` | SCC binding missing (Step 1, `add-scc-to-user`) |
| TLS not coming up | `ca-pair-sslcerts` missing or `READY!=True` (Step 3) |
| `password is not formatted correctly against the regex` | SASL password is EMPTY; verify `${#pass}`, update via `apply` (Step 4) |
| `oc delete secret` hangs | CFK finalizer; `Ctrl+C` → `oc patch ... finalizers:null`; use `apply` from the start next time |
| `Keystore was tampered with` | Parse the JKS password with `cut -d'=' -f2` |
| Kafka can't reach the controller | `controllerListener` doesn't match `listeners.controller` |
| ArgoCD `app path does not exist` | Manifests weren't committed/pushed to git |
| ArgoCD `Forbidden ... kafkas` | ArgoCD's ServiceAccount RBAC missing (Step 7.B) |

---

## Repo file map

```
manifests/
├── 01-namespace-and-scc/
│   ├── namespace.yaml           # confluent namespace
│   └── scc.yaml                  # Custom SecurityContextConstraints (UID 1000-1005)
├── 02-cert-manager/
│   ├── operator.yaml             # Red Hat cert-manager operator (OLM)
│   └── root-ca.yaml               # Self-signed bootstrap Issuer + root CA
├── 03-kafka/
│   ├── kraft-controller.yaml     # KRaftController: TLS + SASL/PLAIN + resources + affinity
│   └── kafka.yaml                 # Kafka: TLS + SASL/PLAIN + resources + affinity
└── 04-automation/
    ├── github-actions-rbac.yaml  # least-privilege identity for Option A
    ├── gitops-operator.yaml       # Red Hat OpenShift GitOps operator (OLM)
    ├── gitops-rbac.yaml            # ArgoCD's permissions in $NS (applied manually)
    └── gitops-application.yaml     # ArgoCD Application (watches only 03-kafka/)
scripts/
├── create-sasl-secrets.sh        # Step 4 — imperative, never commits passwords
└── verify-deployment.sh          # Step 6 — TLS+SASL smoke test
.github/workflows/deploy-kafka.yaml  # Option A workflow (triggers on manifests/03-kafka/**)
```

**Applied once, manually (bootstrap layer):** `01-namespace-and-scc/`, `02-cert-manager/`,
`04-automation/` (including its own operator installs), `scripts/create-sasl-secrets.sh`.
**Continuously automated (application layer):** only `03-kafka/`.

## Scope: what's genuinely tested vs. config-only

On a single-node practice environment, some things can genuinely be built and tested end to
end, while others only mean "the config is correct — the real test needs multi-node
production capacity." Drawn honestly:

| Topic | Status |
|---|---|
| Custom SCC | ✅ Real, live-tested |
| TLS (cert-manager + autoGeneratedCerts) | ✅ Real, proven with a live produce/consume test |
| SASL/PLAIN authentication | ✅ Real, used in the live test; unauthenticated access confirmed to fail |
| GitHub Actions (push) | ✅ Real, proven with a no-op run and a full delete+redeploy-from-scratch run |
| ArgoCD GitOps (pull) | ✅ Real, proven with a live git-to-cluster sync test |
| Resource requests/limits | ✅ Real, but sized for this small practice environment, not real production load |
| Pod anti-affinity | ⚠️ Config is correct but `preferred` (soft) — `required` would strand pods with nowhere to schedule on one node. The same YAML enforces hard isolation automatically on a real multi-node cluster |
| Rack/multi-AZ awareness | ❌ Can't be tested on a single node, not included here |
| SASL/SCRAM | ❌ CFK doesn't natively support it (verified fact, not a choice) — SASL/PLAIN is used instead |
| Enterprise internal PKI | ❌ Root CA here is self-signed — production must chain from the organization's own CA |
| Enterprise storage (ODF/Ceph/Trident) | ❌ Still CRC's `hostpath-provisioner` |

## What's left for real production

1. Replace the self-signed root CA with a `ClusterIssuer` chained to the organization's
   internal PKI.
2. Size `dataVolumeCapacity` and `podTemplate.resources` for real workload/storage-class
   capacity (this repo's values are shrunk for a single-node practice environment).
3. Switch anti-affinity from `preferred` to `required` + `oneReplicaPerNode: true` +
   `rackAssignment.nodeLabels` for real multi-AZ spread.
4. Add monitoring (Confluent Control Center or a Prometheus/JMX exporter).
5. Review `PodDisruptionBudget` (CFK creates one automatically — verify it), `NetworkPolicy`,
   and namespace resource quotas.
6. Move secret management to the organization's actual standard (Vault, External Secrets
   Operator, OIDC federation) instead of a raw kubeconfig in a GitHub Secret / imperative
   `oc create secret`.
7. Consider mTLS client-auth or SASL/OAUTHBEARER instead of SASL/PLAIN for stronger
   authentication.
8. Pull images from an internal registry mirror instead of Docker Hub; consider image
   signing (Cosign).
9. Define a backup/disaster-recovery strategy (Cluster Linking / MirrorMaker2).
10. Walk through the [enterprise checklist](#before-you-start-enterprise-checklist) with the
    platform/DevOps team before deploying to a real cluster.

## Resources

- [Deploy Confluent for Kubernetes - CFK](https://docs.confluent.io/operator/current/co-deploy-cfk.html)
- [Configure and Manage KRaft Using CFK](https://docs.confluent.io/operator/current/co-configure-kraft.html)
- [Confluent for Kubernetes Quick Start](https://docs.confluent.io/operator/current/co-quickstart.html)
- [Confluent for Kubernetes Release Notes](https://docs.confluent.io/operator/current/release-notes.html)
- [Plan for Confluent Platform Deployment Using CFK](https://docs.confluent.io/operator/current/co-plan.html)
- [confluent-kubernetes-examples: openshift-security](https://github.com/confluentinc/confluent-kubernetes-examples/tree/master/security/openshift-security)
- [Release Notes for Confluent Platform 8.3](https://docs.confluent.io/platform/current/release-notes/index.html)
