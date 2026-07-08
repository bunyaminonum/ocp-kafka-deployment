# CFK ile OpenShift Üzerinde Kafka (KRaft) Deployment'ı

Bu doküman, Confluent for Kubernetes (CFK) operatörü kullanılarak OpenShift üzerinde
KRaft modunda (ZooKeeper'sız) bir Apache Kafka cluster'ının nasıl kurulduğunu,
neden bu kararların alındığını ve karşılaşılan sorunların nasıl çözüldüğünü anlatır.

Bu kurulum önce bir **pratik/geliştirme ortamında** (OCI üzerinde CRC / OpenShift Local
ile simüle edilmiş tek node'lu OpenShift) yapılıp doğrulandı, gerçek/production bir OCP
cluster'ına taşınmadan önce mantığın anlaşılması amaçlandı.

> **Kurumsal/production ortamına geçerken:** Bu doküman playground'da (CRC) sıfırdan
> yaptığımız her adımı (CRC kurulumu dahil) anlatır. Gerçek bir kurumsal OCP ortamında
> hangi adımların atlanacağını, hangilerinin platform ekibiyle netleştirilmesi gerektiğini
> ayrı ve sadeleştirilmiş bir doküman olan [`docs/kurumsal-playbook.md`](docs/kurumsal-playbook.md)'da bul.

## İçindekiler

1. [Mimari özeti](#mimari-özeti)
2. [Kullanılan versiyonlar](#kullanılan-versiyonlar)
3. [Pratik ortam (CRC) — sadece bu ortama özgü, production'da atlanacak](#pratik-ortam-crc)
4. [CFK operatörünün kurulumu](#cfk-operatörünün-kurulumu)
5. [OpenShift SCC kararı](#openshift-scc-kararı)
6. [Kafka KRaft cluster'ının kurulumu](#kafka-kraft-clusterının-kurulumu)
7. [Doğrulama](#doğrulama)
8. [GitHub Actions ile otomatik deployment](#github-actions-ile-otomatik-deployment)
9. [Karşılaşılan sorunlar ve çözümleri](#karşılaşılan-sorunlar-ve-çözümleri)
10. [Şu anki durumun sınırlamaları](#şu-anki-durumun-sınırlamaları)
11. [Production'a geçiş için yapılacaklar](#productiona-geçiş-için-yapılacaklar)
12. [Kaynaklar](#kaynaklar)

---

## Mimari özeti

```
                         ┌─────────────────────────┐
                         │   confluent-operator     │  (Helm ile kurulan CFK operatörü,
                         │   (Deployment, 1 pod)     │   Kafka/KRaftController CR'larını izler)
                         └────────────┬─────────────┘
                                      │ reconcile
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                       ▼
   ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
   │ kraftcontroller-0/1/2 │  │      kafka-0/1/2      │  │  (ileride: Connect,   │
   │  (StatefulSet, 3 pod) │◄─┤   (StatefulSet, 3 pod) │  │   Schema Registry,    │
   │  metadata/Raft quorum │  │   broker'lar          │  │   Control Center...)  │
   └─────────────────────┘  └─────────────────────┘  └─────────────────────┘
```

- **KRaft**: Kafka artık ZooKeeper'a ihtiyaç duymuyor; metadata'yı kendi içinde Raft
  konsensüs algoritmasıyla yönetiyor. CFK'da bu, ayrı bir `KRaftController` CRD'si
  olarak modellenmiş.
- **Operator pattern**: Helm ile SADECE operatörün kendisi kurulur. Kafka/KRaftController
  gibi gerçek bileşenler ayrı CR (Custom Resource) YAML'ları `oc apply` ile istenir;
  operatör bunları görüp arkasında StatefulSet/Service/Secret/ConfigMap nesnelerini
  oluşturur ve sürekli sağlığını izler (reconcile loop).

## Kullanılan versiyonlar

| Bileşen | Versiyon | Not |
|---|---|---|
| CFK (Confluent for Kubernetes) | **3.3.0** | Bu doküman yazıldığında en güncel sürüm (2026-06-23 yayınlandı) |
| Confluent Platform / Kafka image (`cp-server`) | **8.3.0** | En güncel sürüm |
| `confluent-init-container` | **3.3.0** | **Kural**: init container tag'i her zaman CFK'nın kendi versiyonunu takip eder, CP versiyonunu değil |
| Kubernetes (OpenShift altyapısı) | 1.35.5 | CFK 3.3.0'ın desteklediği 1.28-1.36 aralığında |

**Bilinen bir uyarı:** CP 8.3.0'da Kafka Streams (client-side) kütüphanesini kullanan
uygulamalarda RocksDB katmanında bilinen bir memory leak var (KAFKA-20616 / KAFKA-20688).
Bu **sadece Kafka Streams uygulamalarını** etkiliyor, bizim kurduğumuz broker/controller'ları
etkilemiyor — bu yüzden 8.3.0 ile devam edildi. İleride Kafka Streams tabanlı bir consumer
uygulaması devreye alınırsa bu bilinmeli.

## Pratik ortam (CRC)

> Bu bölüm **sadece** bu OCI makinesindeki pratik/öğrenme ortamına özgüdür.
> Gerçek/production bir OCP cluster'ında CFK kurulumuna doğrudan
> [CFK operatörünün kurulumu](#cfk-operatörünün-kurulumu) bölümünden başlanacak.

Gerçek bir OCP cluster'ı tek bir OCI VM'de kurmak (multi-node, RHCOS) pratik değildi.
Bunun yerine Red Hat'in resmi **CRC (CodeReady Containers / OpenShift Local)** aracıyla
tek node'lu, gerçek bir OpenShift API'sine sahip bir cluster kuruldu:

- OCI instance: `VM.Standard.E5.Flex`, 4 OCPU (8 vCPU) / 32GB RAM / 100GB disk'e büyütüldü
  (esnek "Flex" shape olduğu için OCPU/RAM canlı büyütülebildi; disk için GPT tablosu
  `sgdisk -e` ile düzeltilip `growpart` + `resize2fs` ile genişletildi).
- Prerequisite paketler: `qemu-kvm`, `libvirt-daemon-system`, `libvirt-clients`, `network-manager`.
  **Dikkat:** NetworkManager kurulurken ana SSH arayüzü (`enp0s5`) bilerek "unmanaged"
  bırakıldı (`/etc/NetworkManager/conf.d/unmanaged.conf`) — aksi halde NetworkManager
  bu arayüzü devralıp SSH bağlantısını kesintiye uğratabilirdi.
- `crc setup` + `crc start` ile OpenShift 4.22.1 tek-node cluster ayağa kaldırıldı
  (CRC'nin kendi VM'ine 6 vCPU / 24GB RAM / 80GB disk ayrıldı).
- Pull secret: `console.redhat.com/openshift/create/local` üzerinden alınan kişisel
  Red Hat hesabı pull secret'ı `~/.crc/pull-secret.json` olarak kaydedildi.

## CFK operatörünün kurulumu

```bash
# 1. Namespace
oc new-project confluent          # production'da muhtemelen zaten bir proje/namespace verilecek

# 2. Confluent'ın resmi Helm reposu
helm repo add confluentinc https://packages.confluent.io/helm
helm repo update

# 3. CFK operatörünü kur (Default SCC ile — aşağıya bakın)
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes \
  --set podSecurity.enabled=false \
  --namespace confluent

# 4. Doğrula
oc get pods -n confluent
oc get crd | grep confluent
```

`helm upgrade --install` idempotent'tir (zaten kuruluysa günceller, değilse kurar) —
bu yüzden GitHub Actions pipeline'ında da bu komut aynen kullanılacak.

## OpenShift SCC kararı

**Bu, Kafka'nın SSL/SASL güvenliğinden tamamen ayrı bir konu** — TLS eklemesek bile
atlanamaz, çünkü olmadan pod'lar hiç başlamaz.

OpenShift, pod'ların hangi Linux UID'siyle çalışacağını normal Kubernetes'ten daha
sıkı kontrol eder (SCC = Security Context Constraint). CFK varsayılan olarak
konteynerleri sabit **UID 1001** ile çalıştırmak ister, ama OpenShift'in
`restricted-v2` SCC'si her namespace'e rastgele bir UID aralığı atar ve pod'ların
bunun dışına çıkmasına izin vermez.

Confluent'ın resmi çözümü ([kaynak](https://github.com/confluentinc/confluent-kubernetes-examples/tree/master/security/openshift-security))
iki yol sunuyor:

- **Default SCC (kullandığımız, önerilen)**: Helm kurulumunda `--set podSecurity.enabled=false`
  VE her CR'da `spec.podTemplate.podSecurityContext: {}` ile CFK'nın sabit UID
  dayatmasını kapatmak; OpenShift kendi rastgele UID'sini atar.
- **Custom SCC (ileri seviye)**: Özel bir SCC objesi tanımlayıp (`uidRangeMin`/`uidRangeMax`)
  service account'lara bağlamak — daha fazla kontrol ama daha karmaşık.

Biz Default SCC ile ilerledik. Production'da sertifika/TLS eklerken bu karar
gözden geçirilebilir.

## Kafka KRaft cluster'ının kurulumu

Manifestler: [`manifests/kraft-controller.yaml`](manifests/kraft-controller.yaml),
[`manifests/kafka.yaml`](manifests/kafka.yaml)

```bash
oc apply -f manifests/kraft-controller.yaml
oc apply -f manifests/kafka.yaml
oc get pods -n confluent -w
```

**Önemli alanlar:**

- `spec.replicas: 3` — KRaft için **zorunlu kural**: kontrolcü sayısı tek ve en az 3
  olmalı (Raft çoğunluk oyu ister, çift sayı split-brain riski taşır). Kafka broker'ı
  için 3, klasik HA minimumu.
- `spec.dataVolumeCapacity` — her replica için ayrı PVC isteği. **Production'da**
  gerçek storage class'a göre büyütülmeli (bkz. [Sınırlamalar](#şu-anki-durumun-sınırlamaları)).
- `spec.dependencies.kRaftController.clusterRef.name` — Kafka CR'ı hangi
  KRaftController'a bağlanacağını burada belirtir. `zookeeper` ve `kRaftController`
  aynı anda belirtilemez.
- `spec.podTemplate.podSecurityContext: {}` — yukarıdaki SCC kararının uygulanması.
- TLS/authentication alanları **bilerek boş** — belirtilmeyince CFK varsayılan olarak
  PLAINTEXT listener açıyor. Bu, güvenliği sonraya bırakma kararımızla uyumlu.

## Doğrulama

Pod durumu yeterli değil (sadece process'in ayakta olduğunu gösterir), gerçek broker
sağlığı client komutlarıyla doğrulanmalı:

```bash
oc get pods -n confluent                       # 7/7 Running, RESTARTS=0 olmalı
oc get kraftcontroller,kafka -n confluent       # STATUS=RUNNING olmalı

# Gerçek uçtan uca test
oc exec -n confluent kafka-0 -- kafka-cluster cluster-id --bootstrap-server kafka:9071
oc exec -n confluent kafka-0 -- kafka-topics --bootstrap-server kafka:9071 \
  --create --topic saglik-testi --partitions 3 --replication-factor 3
oc exec -n confluent kafka-0 -- kafka-topics --bootstrap-server kafka:9071 \
  --describe --topic saglik-testi   # tüm partition'larda Isr = Replicas olmalı
oc exec -n confluent kafka-0 -- kafka-topics --bootstrap-server kafka:9071 \
  --delete --topic saglik-testi
```

Bu ortamda test edildi: 3 partition/RF3 topic başarıyla oluşturuldu, tüm partition'lar
tam ISR'de (under-replicated partition yok), leader'lar 3 broker'a dengeli dağıldı.

Ayrıca deploy sonrası loglarda görülen geçici `Node X disconnected` mesajları normaldir —
KRaft quorum node'ları ilk bağlantı kurulurken kısa süreli bağlan/kopar döngüsü yaşayabilir.
Kritik olan, bu mesajlardan sonra loglarda hata/restart olmadan sağlıklı periyodik
aktivitenin (`Log roller completed` gibi) kesintisiz devam etmesidir.

## GitHub Actions ile otomatik deployment

Manifestleri elle `oc apply` etmek yerine, `main` branch'e push edildiğinde (veya elle
tetiklendiğinde) `manifests/kraft-controller.yaml` ve `manifests/kafka.yaml`'ı otomatik
uygulayan bir GitHub Actions pipeline'ı kuruldu.

### 8.1 Üç ayrı kimlik doğrulama mekanizması — karıştırılmamalı

Bu bölümde birbirinden tamamen bağımsız üç credential/mekanizma var:

1. **SSH key**: Bu makineden (insan olarak) `git push` yapabilmek için — GitHub Actions'ın
   kendisiyle hiç ilgisi yok.
2. **Runner registration token**: Bu makineyi GitHub Actions self-hosted runner olarak
   kaydetmek için — GitHub'ın verdiği, kısa ömürlü (~1 saat), tek seferlik bir token.
3. **ServiceAccount token**: Pipeline'ın (runner üzerinde çalışan job'ın) OCP cluster'ına
   login olması için — cluster tarafında ürettiğimiz, uzun ömürlü (1 yıl) bir token.

### 8.2 Git repo ve SSH erişimi

```bash
git config --global user.name "<isim>"
git config --global user.email "<email>"
ssh-keygen -t ed25519 -C "<makine-etiketi>" -f ~/.ssh/id_ed25519 -N ""
cat ~/.ssh/id_ed25519.pub   # bu çıktıyı GitHub'a eklenecek
```

Public key, repo'nun **Settings → Deploy keys → Add deploy key** (Write access işaretli)
kısmına eklendi — hesabın genelindeki bir "Personal SSH key" yerine bilerek **Deploy Key**
seçildi, çünkü bu makine sadece bu tek repo için kullanılıyor; Deploy Key'in erişimi de
sadece o repo ile sınırlı (hesabın diğer repolarına erişemez).

```bash
ssh -T git@github.com   # "Hi <kullanıcı>/<repo>! You've successfully authenticated" beklenir
git clone git@github.com:<kullanıcı>/<repo>.git
```

### 8.3 Self-hosted runner kurulumu

**Neden self-hosted (GitHub-hosted değil)?** GitHub'ın kendi barındırdığı runner'lar
internet üzerinden, GitHub'ın bulut sunucularında çalışır. Kurumsal bir OCP cluster'ı
neredeyse kesin şekilde özel bir ağda/internete kapalı olacağından, GitHub-hosted bir
runner cluster'ın API'sine hiç ulaşamaz. Bunun için runner'ı, cluster'a ağ erişimi olan
bir makineye (burada bu OCI makinesi, kurumsal ortamda ilgili ağdaki bir makine) kaydetmek
gerekiyor.

**Güvenlik notu:** Self-hosted runner'lar **public repo'larda** risklidir — herkes bir
Pull Request açıp o PR'ın CI adımlarında runner'da (yani bu makinede) kod çalıştırabilir.
Repo'nun **private** olduğundan emin olunmalı.

Runner sürümü ve indirme linki GitHub'ın resmi releases API'sinden alındı (tahmini/statik
bir versiyon numarası kullanılmadı):
```bash
curl -s https://api.github.com/repos/actions/runner/releases/latest \
  | grep -E '"tag_name"|browser_download_url.*linux-x64.*tar.gz'
```
Bu ortamda çözülen versiyon: **v2.335.1**.

```bash
mkdir -p ~/actions-runner && cd ~/actions-runner
curl -o actions-runner-linux-x64-2.335.1.tar.gz -L \
  https://github.com/actions/runner/releases/download/v2.335.1/actions-runner-linux-x64-2.335.1.tar.gz
tar xzf ./actions-runner-linux-x64-2.335.1.tar.gz

# Token: repo → Settings → Actions → Runners → New self-hosted runner sayfasından alınır
# (~1 saat geçerli, tek seferlik, statik olarak burada tutulmaz)
./config.sh --url https://github.com/<kullanıcı>/<repo> --token <TOKEN> \
  --unattended --name oci-ocp-runner --labels self-hosted,ocp --work _work

# Terminal kapansa bile ayakta kalsın diye systemd servisi olarak kur
sudo ./svc.sh install
sudo ./svc.sh start
sudo ./svc.sh status
```

`--labels self-hosted,ocp`: workflow YAML'ında `runs-on: [self-hosted, ocp]` ile bu etiketle
eşleştirilecek — birden fazla self-hosted runner olsaydı, işleri doğru makineye yönlendirmek
için etiketleme önemli olurdu.

### 8.4 Pipeline için kısıtlı yetkili kimlik (ServiceAccount + RBAC)

Pipeline'ın `kubeadmin` (tam yetkili) yerine, sadece `confluent` namespace'inde
Kafka/KRaftController yönetebilen, **least privilege** (en az yetki) bir kimliği olmalı —
credential sızarsa/yanlış çalışırsa bile hasar `confluent` namespace'iyle sınırlı kalsın diye.

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

**Neden `Role` (`ClusterRole` değil):** İzni bilerek sadece `confluent` namespace'iyle
sınırlandırıyoruz. `kafkas`/`kraftcontrollers` üzerinde tam CRUD var (pipeline'ın asıl işi),
`pods`/`events`/`statefulsets` üzerinde sadece okuma var (deploy sonrası doğrulama için —
pod silme/oluşturma yetkisi CFK operatörünün işi, pipeline'ın değil).

```bash
oc apply -f manifests/rbac-github-actions.yaml
oc get sa,role,rolebinding -n confluent | grep github-actions
```

### 8.5 Token, kubeconfig ve GitHub Secret

```bash
TOKEN=$(oc create token github-actions-deployer -n confluent --duration=8760h)
API_SERVER=$(oc whoami --show-server)

oc config set-cluster ci-cluster --server="$API_SERVER" --insecure-skip-tls-verify=true --kubeconfig=./ci-kubeconfig
oc config set-credentials github-actions-deployer --token="$TOKEN" --kubeconfig=./ci-kubeconfig
oc config set-context github-actions-deployer --cluster=ci-cluster --user=github-actions-deployer --namespace=confluent --kubeconfig=./ci-kubeconfig
oc config use-context github-actions-deployer --kubeconfig=./ci-kubeconfig

oc get kafka --kubeconfig=./ci-kubeconfig   # authentication + authorization + namespace testini tek seferde doğrular
```

- `oc create token ... --duration=8760h`: Modern Kubernetes'te (1.24+) ServiceAccount'lara
  artık otomatik/kalıcı token verilmiyor; `TokenRequest API` ile talep üzerine süreli token
  üretiliyor (`--duration` cluster'ın izin verdiği maksimumu aşarsa otomatik kısaltılır).
- Ayrı bir `./ci-kubeconfig` dosyası: kişisel `~/.kube/config`'e (kubeadmin yetkisi içerir)
  hiç dokunulmadı — pipeline'a ait kısıtlı kimlik fiziksel olarak izole bir dosyada.
- `--insecure-skip-tls-verify=true`: CRC kendi kendine imzalı sertifika kullandığı için
  bilerek eklendi. **Production'da kullanılmamalı**, yerine gerçek CA sertifikası
  (`certificate-authority-data`) eklenmeli.

Bu dosya asla git'e girmemeli (canlı bir token içeriyor):
```bash
echo "ci-kubeconfig" >> .gitignore
git add .gitignore && git commit -m "Add .gitignore for local CI kubeconfig" && git push
```

Dosya, GitHub'ın şifreli Secret deposuna tek bir base64 metni olarak eklendi:
```bash
base64 -w0 ci-kubeconfig
```
Repo → **Settings → Secrets and variables → Actions → New repository secret** →
Name: `KUBECONFIG_B64`, Value: yukarıdaki base64 çıktısı.

### 8.6 Workflow YAML

Dosya: [`.github/workflows/deploy-kafka.yaml`](.github/workflows/deploy-kafka.yaml)

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

- `on.push.branches/paths`: Sadece `main`'e, sadece bu iki manifest değiştiğinde tetiklenir
  (README değişikliği gereksiz yere Kafka'ya dokunmasın diye).
- `workflow_dispatch: {}`: GitHub arayüzünden elle "Run workflow" ile de tetiklenebilir.
- `runs-on: [self-hosted, ocp]`: Runner'ı kaydederken verdiğimiz etiketle eşleşir.
- `Setup kubeconfig` adımı: Secret'ı çözüp `$RUNNER_TEMP`'e (runner'ın kendi geçici
  klasörü, iş bitince otomatik temizlenir) yazar, `GITHUB_ENV`'e yazarak sonraki tüm
  adımlara `KUBECONFIG` ortam değişkenini miras bırakır.

### 8.7 Pipeline doğrulama testleri

**Test 1 — mekanik doğrulama (no-op apply):** Manifestlerde değişiklik yokken pipeline
elle tetiklendi. Runner journal log'u:
```
Running job: deploy
Job deploy completed with result: Succeeded
```
Bu, authentication (token geçerli), authorization (RBAC `Forbidden` vermedi) ve runner'ın
job alabildiğini kanıtladı — ama Kafka zaten var olduğu için `apply` bir şey değiştirmedi
(Kubernetes apply idempotent'tir).

**Test 2 — sıfırdan deployment (gerçek test):** Cluster'daki Kafka/KRaftController elle
tamamen silindi:
```bash
oc delete -f manifests/kafka.yaml
oc delete -f manifests/kraft-controller.yaml
oc get kafka,kraftcontroller,pods,pvc -n confluent   # hepsi boş/yok olduğu doğrulandı
```
Ardından pipeline `workflow_dispatch` ile tekrar tetiklendi. Sonuç:
- Runner log'u: ikinci bir `Running job: deploy` → `Job deploy completed with result: Succeeded`.
- `oc get pvc -n confluent`: **tamamen yeni PVC UID'leri** (öncekilerden farklı) — gerçekten
  sıfırdan provision edildiğinin kanıtı, eski hiçbir kaynak yeniden kullanılmadı.
- Tüm pod'lar ~2-3 dakika içinde `Running`, `0` restart ile ayağa kalktı.
- Doğrulama için tekrar gerçek bir topic testi yapıldı (`pipeline-sifirdan-test`, 3 partition/RF3)
  — tüm partition'lar tam ISR'de, leader'lar dengeli dağılmış.

Bu iki test birlikte, pipeline'ın hem "değişiklik yoksa dokunma" hem "sıfırdan tam bir
cluster'ı ayağa kaldırabilme" senaryolarını uçtan uca kanıtladı.

## Karşılaşılan sorunlar ve çözümleri

Bunlar gerçek karşılaşılan hatalar — production ortamında da aynılarıyla karşılaşılabilir:

### 1. `crc status` / `crc oc-env`: "Unable to connect to kvm driver"
Cluster'ın kendisi (`virsh -c qemu:///system list` ile doğrulanabilir) çalışıyor
olsa bile, `crc` CLI'ın bazı komutları client tarafında bu hatayı verebiliyor —
libvirt/kvm grup üyeliğiyle ilgisi yok (tam üyelikle bile tekrar edildi). Bu **sadece
CRC'ye özgü bir client bug**, gerçek OCP'de bu sorun olmayacak. Çözüm: `crc status`
yerine doğrudan `oc` ile merge edilmiş kubeconfig üzerinden (`~/.kube/config`,
`crc-admin` context) bağlanmak.

### 2. Kafka/KRaftController pod'ları hiç oluşmuyor: SCC Forbidden
İlk denemede `podTemplate.podSecurityContext: {}` alanı unutulunca:
```
unable to validate against any security context constraint: [...]
provider restricted-v2: .containers[0].runAsUser: Invalid value: 1001: must be in the ranges: [...]
```
hatası alındı. Kök neden ve çözüm için [OpenShift SCC kararı](#openshift-scc-kararı)
bölümüne bakın.

### 3. PVC'ler istenen boyuttan büyük Bound oluyor
`dataVolumeCapacity: 5Gi`/`10Gi` istendi ama PVC'ler `79Gi` olarak Bound oldu.
Bu bir hata değil — CRC'nin varsayılan `hostpath-provisioner`'ı kapasite kotasını
gerçek anlamda uygulamıyor, mevcut disk alanının tamamını rapor ediyor. **Gerçek
production altyapısında (network-attached, kota uygulayan bir storage class ile)
istenen boyut birebir uygulanacak.**

## Şu anki durumun sınırlamaları

Bunlar bilerek atlanan/basitleştirilen noktalar — production öncesi tamamlanmalı:

- ❌ **TLS/SASL yok** — tüm listener'lar PLAINTEXT. (Bilerek — kullanıcı kararı, sertifikalar ayrı adımda eklenecek.)
- ❌ Pod `resources.requests/limits` (CPU/memory) tanımlanmadı.
- ❌ `dataVolumeCapacity` değerleri pratik ortamın disk sınırına göre küçültüldü (5Gi/10Gi) — production'da gerçek ihtiyaca göre büyütülmeli.
- ❌ Monitoring/JMX metrik export'u yapılandırılmadı.
- ❌ Network Policy tanımlanmadı.
- ❌ Backup/disaster recovery stratejisi yok.
- ⚠️ Default SCC kullanıldı (Custom SCC değil) — daha sıkı izolasyon isteniyorsa gözden geçirilmeli.
- ⚠️ GitHub Secret'ta ham kubeconfig (base64) saklandı — kurumun bir secret manager standardı varsa ona taşınmalı.
- ⚠️ Self-hosted runner tek bir makinede (bu OCI VM'i), tek bir proje için kuruldu — kurumsal ortamda muhtemelen paylaşımlı bir runner havuzu vardır, tekrar kurmadan önce platform ekibine danış.

## Production'a geçiş için yapılacaklar

1. TLS (broker-broker, broker-controller, client-broker) + SASL/mTLS authentication ekle.
2. `podTemplate.resources` ile CPU/memory request-limit tanımla.
3. Gerçek storage class'a göre `dataVolumeCapacity` boyutlarını kurumun kapasite planına göre ayarla.
4. Confluent Control Center veya Prometheus/JMX exporter ile monitoring ekle.
5. `PodDisruptionBudget` (CFK otomatik oluşturuyor, doğrula), `NetworkPolicy`, resource quota gözden geçir.
6. Custom SCC'ye geçmeyi değerlendir (daha sıkı UID/GID kontrolü).
7. GitHub Actions secret yönetimini gözden geçir — burada ham kubeconfig'i tek bir GitHub
   Secret olarak sakladık; kurumun bir secret manager standardı (Vault, OIDC federasyonu vb.)
   varsa ona taşınmalı. Ayrıca `KUBECONFIG_B64` içindeki token'ın (1 yıl geçerli) rotasyon
   stratejisi belirlenmeli.
8. Kurumsal ortama geçiş için [`docs/kurumsal-playbook.md`](docs/kurumsal-playbook.md)'daki
   netleştirme sorularını platform/DevOps ekibiyle konuş.

## Kaynaklar

- [Deploy Confluent for Kubernetes - CFK](https://docs.confluent.io/operator/current/co-deploy-cfk.html)
- [Configure and Manage KRaft Using CFK](https://docs.confluent.io/operator/current/co-configure-kraft.html)
- [Confluent for Kubernetes Quick Start](https://docs.confluent.io/operator/current/co-quickstart.html)
- [Confluent for Kubernetes Release Notes](https://docs.confluent.io/operator/current/release-notes.html)
- [Plan for Confluent Platform Deployment Using CFK](https://docs.confluent.io/operator/current/co-plan.html)
- [confluent-kubernetes-examples: openshift-security](https://github.com/confluentinc/confluent-kubernetes-examples/tree/master/security/openshift-security)
- [Release Notes for Confluent Platform 8.3](https://docs.confluent.io/platform/current/release-notes/index.html)
