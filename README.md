# CFK ile OpenShift Üzerinde Kafka (KRaft) Deployment'ı

Bu doküman, Confluent for Kubernetes (CFK) operatörü kullanılarak OpenShift üzerinde
KRaft modunda (ZooKeeper'sız) bir Apache Kafka cluster'ının nasıl kurulduğunu,
neden bu kararların alındığını ve karşılaşılan sorunların nasıl çözüldüğünü anlatır.

Bu kurulum önce bir **pratik/geliştirme ortamında** (OCI üzerinde CRC / OpenShift Local
ile simüle edilmiş tek node'lu OpenShift) yapılıp doğrulandı, gerçek/production bir OCP
cluster'ına taşınmadan önce mantığın anlaşılması amaçlandı.

## İçindekiler

1. [Mimari özeti](#mimari-özeti)
2. [Kullanılan versiyonlar](#kullanılan-versiyonlar)
3. [Pratik ortam (CRC) — sadece bu ortama özgü, production'da atlanacak](#pratik-ortam-crc)
4. [CFK operatörünün kurulumu](#cfk-operatörünün-kurulumu)
5. [OpenShift SCC kararı](#openshift-scc-kararı)
6. [Kafka KRaft cluster'ının kurulumu](#kafka-kraft-clusterının-kurulumu)
7. [Doğrulama](#doğrulama)
8. [Karşılaşılan sorunlar ve çözümleri](#karşılaşılan-sorunlar-ve-çözümleri)
9. [Şu anki durumun sınırlamaları](#şu-anki-durumun-sınırlamaları)
10. [Production'a geçiş için yapılacaklar](#productiona-geçiş-için-yapılacaklar)
11. [Kaynaklar](#kaynaklar)

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

## Production'a geçiş için yapılacaklar

1. TLS (broker-broker, broker-controller, client-broker) + SASL/mTLS authentication ekle.
2. `podTemplate.resources` ile CPU/memory request-limit tanımla.
3. Gerçek storage class'a göre `dataVolumeCapacity` boyutlarını kurumun kapasite planına göre ayarla.
4. Confluent Control Center veya Prometheus/JMX exporter ile monitoring ekle.
5. `PodDisruptionBudget` (CFK otomatik oluşturuyor, doğrula), `NetworkPolicy`, resource quota gözden geçir.
6. Custom SCC'ye geçmeyi değerlendir (daha sıkı UID/GID kontrolü).
7. GitHub Actions pipeline'ına secret/credential yönetimini (kubeconfig, pull secret) güvenli şekilde entegre et (bkz. sıradaki bölüm).

## Kaynaklar

- [Deploy Confluent for Kubernetes - CFK](https://docs.confluent.io/operator/current/co-deploy-cfk.html)
- [Configure and Manage KRaft Using CFK](https://docs.confluent.io/operator/current/co-configure-kraft.html)
- [Confluent for Kubernetes Quick Start](https://docs.confluent.io/operator/current/co-quickstart.html)
- [Confluent for Kubernetes Release Notes](https://docs.confluent.io/operator/current/release-notes.html)
- [Plan for Confluent Platform Deployment Using CFK](https://docs.confluent.io/operator/current/co-plan.html)
- [confluent-kubernetes-examples: openshift-security](https://github.com/confluentinc/confluent-kubernetes-examples/tree/master/security/openshift-security)
- [Release Notes for Confluent Platform 8.3](https://docs.confluent.io/platform/current/release-notes/index.html)
