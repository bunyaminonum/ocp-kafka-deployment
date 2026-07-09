# Kurumsal Ölçekte CFK ile Kafka (KRaft) Deployment — Playbook

## Bu doküman ne için, ne değil

Ana [README](../README.md), bu projenin bir **playground'da** (OCI üzerinde sıfırdan
kurulmuş, tek node'lu CRC/OpenShift Local) nasıl inşa edildiğini, her kararın *neden*
alındığını ve karşılaşılan sorunları anlatıyor — öğrenme/ispat amaçlı, çok detaylı.

Bu doküman **farklı bir amaca** hizmet ediyor: gerçek bir kurumsal OpenShift (OCP)
ortamında bu deployment'ı yapacak birinin izleyeceği, sade ve doğrusal bir **prosedür
(playbook)**. Playground'da bizim sıfırdan kurduğumuz bazı şeyler (OpenShift'in kendisi,
self-hosted runner, vs.) kurumsal ortamda büyük ihtimalle **zaten mevcut** — bu playbook
neyin atlanacağını, neyin aynen uygulanacağını, neyin platform ekibiyle netleştirilmesi
gerektiğini ayırt ediyor.

**Bu playbook'u uygulayan kişi, kurumun kendi OCP ortamının detaylarını bilmiyor
olabilir** — bu yüzden her adımda "bunu yapmadan önce şunu bil/sor" notları var.

> **Güncelleme:** Aşağıdaki Adım 6'da "yapılacaklar" olarak listelenen TLS, SASL ve
> GitOps artık [`prod-deployment/`](../prod-deployment/) klasöründe **gerçekten kurulup
> canlı test edildi** (Custom SCC, cert-manager TLS, SASL/PLAIN, ArgoCD). O klasördeki
> [README](../prod-deployment/README.md), karşılaşılan gerçek sorunları (RBAC, kaynak
> sıkışıklığı, secret format hataları) da içeriyor — kurumsal ortama geçerken en çok
> o dokümana bakılmalı, burası daha çok genel checklist'tir.

---

## Adım 0 — Başlamadan önce: platform/DevOps ekibiyle netleştirilmesi gerekenler

Aşağıdaki soruların cevabını bilmeden ilerleme. Playground'da bunların **hepsini biz
sıfırdan kurduk** (çünkü elimizde gerçek bir OCP yoktu); kurumsal ortamda bunların çoğu
muhtemelen zaten var — tekrar kurmaya çalışmak hem gereksiz hem riskli (var olan bir
şeyin üzerine çakışan, çelişen bir konfigürasyon kurabilirsin).

| Soru | Neden önemli |
|---|---|
| OCP cluster'ının API endpoint'i nedir, erişimin var mı? | Her şeyin başlangıç noktası |
| Sana ayrılmış bir proje/namespace var mı, yoksa kendin mi açacaksın? | Yetki ve izolasyon |
| Cluster'da **zaten kurulu bir CFK operatörü** var mı (merkezi platform ekibi tarafından, cluster çapında)? | `oc get crd \| grep confluent` ile kontrol et — varsa Adım 2'yi atla |
| CRD kurma yetkin var mı, yoksa bu platform-admin işi mi? | Helm `--skip-crds` gerekebilir |
| Cluster hangi SCC politikasını kullanıyor/zorunlu kılıyor? | Playground'daki "Default SCC" kararı geçerli olmayabilir, kurumun kendi Custom SCC'si olabilir |
| Hangi storage class'lar var, hangisi production için önerilen? | Playground'daki `hostpath-provisioner` **tek node'a bağımlı, HA değil, kurumsal ortamda kesinlikle kullanılmamalı** |
| GitHub Actions için paylaşımlı bir self-hosted runner havuzu var mı? | Muhtemelen platform ekibi tarafından zaten yönetiliyordur — kendi runner'ını kurmadan önce sor |
| Runner'ın (nerede olursa olsun) OCP API'sine ağ erişimi (firewall/VPN/subnet) var mı? | Yoksa pipeline hiç çalışmaz |
| Kurumun bir secret management standardı var mı (Vault, Sealed Secrets, OIDC federasyonu)? | Playground'da yaptığımız gibi ham kubeconfig'i GitHub Secret'a koymak kabul edilmeyebilir |
| TLS/SASL için kurumun bir internal CA'sı var mı? | Playground'da bilerek atlandı, production'da gerekli |
| Confluent image'ları için private registry/mirror var mı? | Bazı kurumsal ağlar doğrudan Docker Hub erişimini engeller |
| Kaynak (CPU/RAM/disk) kotası/limiti namespace'ine tanımlı mı? | Kafka/KRaftController boyutlandırmasını buna göre yap |

---

## Adım 1 — Playground'a özgü, burada ATLANACAK adımlar

Bunlar sadece "elimizde gerçek OCP yoktu, sıfırdan bir tane simüle ettik" ihtiyacından
doğdu. Gerçek bir kurumsal OCP zaten çalışır durumda olduğu için **tamamen atlanır**:

- ~~OCI/VM boyutlandırma, disk büyütme~~
- ~~libvirt/qemu-kvm/NetworkManager kurulumu~~
- ~~CRC kurulumu (`crc setup`, `crc start`)~~
- ~~Kişisel Red Hat pull secret'ı (`console.redhat.com/openshift/create/local`)~~

Doğrudan Adım 2'den başla.

---

## Adım 2 — CFK operatörünün kurulumu

Önce Adım 0'daki tabloya göre **zaten kurulu mu diye kontrol et**:
```bash
oc get crd | grep confluent
oc get pods -A | grep confluent-operator
```
Zaten kuruluysa bu adımı atla, doğrudan Adım 3'e geç.

Kurulu değilse ve senin kurma yetkin varsa:
```bash
oc new-project <senin-projen>        # veya sana verilen namespace'i kullan

helm repo add confluentinc https://packages.confluent.io/helm
helm repo update

# SCC kararı: Adım 0'da netleştirdiğin politikaya göre seç
# Seçenek A — Default SCC (playground'da kullandığımız, daha basit):
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes \
  --set podSecurity.enabled=false \
  --namespace <senin-projen>

# Seçenek B — Custom SCC kullanılıyorsa: platform ekibinin sağladığı SCC objesini
# uygula, service account'lara bağla, Helm'i podSecurity.enabled=false OLMADAN kur.
# Detay: https://github.com/confluentinc/confluent-kubernetes-examples/tree/master/security/openshift-security

oc get pods -n <senin-projen>
oc get crd | grep confluent
```

CRD kurma yetkin yoksa:
```bash
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes \
  --skip-crds --namespace <senin-projen>
```
(CRD'lerin platform ekibi tarafından önceden kurulmuş olması gerekir.)

---

## Adım 3 — Kafka KRaft cluster'ını deploy et

Repo'daki [`manifests/kraft-controller.yaml`](../manifests/kraft-controller.yaml) ve
[`manifests/kafka.yaml`](../manifests/kafka.yaml) **başlangıç noktası olarak** kullanılabilir,
ama playground'a özgü küçültülmüş değerleri **kurumsal ihtiyaca göre değiştir**:

| Alan | Playground değeri | Production'da ne yapılmalı |
|---|---|---|
| `dataVolumeCapacity` (KRaftController) | `5Gi` | Gerçek storage class + kapasite planına göre büyüt (tipik: 20-50Gi) |
| `dataVolumeCapacity` (Kafka) | `10Gi` | Gerçek ihtiyaca göre büyüt (tipik: 100Gi+) |
| `image.application` / `image.init` | `8.3.0` / `3.3.0` | Kurumun onayladığı/test ettiği bir CP+CFK versiyon çiftini kullan, illa en güncel olması şart değil |
| `podTemplate.podSecurityContext` | `{}` (Default SCC) | Adım 0/2'de netleştirdiğin SCC politikasına göre değişebilir |
| TLS/authentication alanları | boş (PLAINTEXT) | **Production'da mutlaka doldurulmalı** — bkz. ana README'deki "Production'a geçiş için yapılacaklar" |
| `podTemplate.resources` | tanımsız | CPU/memory `requests`/`limits` mutlaka eklenmeli |
| `storageClass` | belirtilmedi (default kullanıldı) | Namespace'ine önerilen storage class'ı **açıkça** belirt, default'a güvenme |

```bash
oc apply -f manifests/kraft-controller.yaml
oc apply -f manifests/kafka.yaml
oc get pods -n <senin-projen> -w
```

---

## Adım 4 — Doğrulama

Ana README'deki [Doğrulama](../README.md#doğrulama) bölümündeki komutlar birebir
uygulanabilir — pod durumu + gerçek bir test topic oluşturup ISR'yi kontrol et. Özetle:

```bash
oc get pods -n <senin-projen>
oc get kraftcontroller,kafka -n <senin-projen>
oc exec -n <senin-projen> kafka-0 -- kafka-cluster cluster-id --bootstrap-server kafka:9071
oc exec -n <senin-projen> kafka-0 -- kafka-topics --bootstrap-server kafka:9071 \
  --create --topic saglik-testi --partitions 3 --replication-factor 3
oc exec -n <senin-projen> kafka-0 -- kafka-topics --bootstrap-server kafka:9071 \
  --describe --topic saglik-testi
oc exec -n <senin-projen> kafka-0 -- kafka-topics --bootstrap-server kafka:9071 \
  --delete --topic saglik-testi
```

---

## Adım 5 — GitHub Actions pipeline

**Önce sor:** Kurumda zaten paylaşımlı bir self-hosted runner havuzu var mı? Varsa
Adım 5.2'yi atla, doğrudan kendi workflow'unu o havuzun etiketiyle (`runs-on`) yaz.

### 5.1 Repo erişimi
Playground'daki gibi: SSH key oluştur, repo'ya Deploy Key olarak ekle (ya da kurumun
kendi git erişim standardı neyse onu kullan — bazı kurumlarda SSO/PAT zorunlu olabilir).

### 5.2 Self-hosted runner (sadece gerekliyse)
Ana README'nin [8.3 bölümü](../README.md#github-actions-ile-otomatik-deployment)'ndeki
adımlar birebir uygulanabilir — runner'ın OCP API'sine ağ erişimi olan bir makineye
kurulması **tek kritik gereksinim**.

### 5.3 Pipeline kimliği (ServiceAccount + RBAC)
[`manifests/rbac-github-actions.yaml`](../manifests/rbac-github-actions.yaml) namespace
adını kendi namespace'ine göre güncelleyip aynen uygulanabilir — **least privilege**
prensibi (sadece kendi namespace'in, sadece `kafkas`/`kraftcontrollers` üzerinde CRUD)
kurumsal ortamda da geçerli, hatta daha kritik.

### 5.4 Secret yönetimi — DİKKAT
Playground'da ham kubeconfig'i base64'leyip GitHub Secret'a koyduk. **Bunu yapmadan önce
kurumun secret yönetim standardını sor** (Adım 0). Alternatifler:
- Kurumun Vault/harici secret manager'ı varsa, kubeconfig/token oradan çekilmeli.
- OIDC federasyonu destekleniyorsa, statik token yerine kısa ömürlü/otomatik rotasyonlu
  kimlik doğrulama tercih edilmeli.
- Ham kubeconfig kullanılacaksa bile, token ömrünü (`--duration`) kurumun güvenlik
  politikasına göre kısalt ve bir rotasyon takvimi belirle (1 yıl playground için
  kabul edilebilirdi, kurumsal ortamda çok uzun olabilir).

### 5.5 Workflow YAML
[`.github/workflows/deploy-kafka.yaml`](../.github/workflows/deploy-kafka.yaml) örnek
olarak kullanılabilir; `runs-on` etiketini kurumun runner havuzuna göre güncelle.

---

## Adım 6 — Production hardening kontrol listesi

Bunlar playground'da **bilerek atlandı**, kurumsal deployment'ta gözden geçirilmeden
canlıya alınmamalı. Detaylı gerekçeler için ana README'nin
[Şu anki durumun sınırlamaları](../README.md#şu-anki-durumun-sınırlamaları) ve
[Production'a geçiş için yapılacaklar](../README.md#productiona-geçiş-için-yapılacaklar)
bölümlerine bak. Kısa liste:

- [ ] TLS (broker-broker, broker-controller, client-broker) + SASL/mTLS authentication
- [ ] `podTemplate.resources` (CPU/memory request-limit)
- [ ] Gerçek storage class + production boyutları
- [ ] Monitoring (Prometheus/JMX exporter veya Control Center)
- [ ] NetworkPolicy, resource quota gözden geçirmesi
- [ ] Backup/disaster recovery stratejisi
- [ ] Secret yönetiminin kurumsal standarda taşınması (bkz. Adım 5.4)
- [ ] SCC kararının kurumun güvenlik politikasıyla teyit edilmesi

---

## Özet akış (tek bakışta)

```
Adım 0: Platform ekibiyle netleştir (checklist)
   │
   ▼
Adım 1: Playground-özel adımları ATLA
   │
   ▼
Adım 2: CFK operatörü zaten kurulu mu? ─── Evet ──▶ Adım 3'e geç
   │ Hayır
   ▼
Adım 2: CFK operatörünü kur (SCC kararına göre)
   │
   ▼
Adım 3: Kafka KRaft CR'larını production değerleriyle deploy et
   │
   ▼
Adım 4: Doğrula (pod + gerçek topic testi)
   │
   ▼
Adım 5: GitHub Actions pipeline (runner havuzu var mı kontrol et, secret yönetimini netleştir)
   │
   ▼
Adım 6: Production hardening kontrol listesini tamamla
```
