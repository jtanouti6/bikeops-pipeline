# BikeOps — Runbook GCP (Artifact Registry, GCS, Dataproc Serverless)

> Objectif : rejouer rapidement le **push d’image** et l’**exécution d’un batch Spark serverless** sur GCP.

## 0) Variables d’environnement (à définir 1 fois par session)

```bash
# ✏️ adapte PROJECT_ID si besoin
export PROJECT_ID="fil-rouge-pipeline"
export REGION="europe-west1"

# Artifact Registry
export REPO="bikeops"               # repository de conteneurs
export AR_HOST="${REGION}-docker.pkg.dev"

# Bucket GCS (données + scripts)
export BUCKET="${PROJECT_ID}-bikeops-dev"
```

---

## 1) Installation et configuration gcloud (WSL/Linux)

```bash
# Installer le CLI (méthode APT)
sudo apt-get update && sudo apt-get install -y ca-certificates curl gnupg
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg  | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main"  | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt-get update && sudo apt-get install -y google-cloud-cli

# Initialiser la CLI et sélectionner le projet
gcloud init
gcloud config set project "$PROJECT_ID"

# (facultatif) fixer région/zone par défaut
gcloud config set compute/region "$REGION"
gcloud config set compute/zone "${REGION}-b"
```

**Pourquoi ?**  
Installer le CLI, authentifier l’utilisateur, et définir le projet par défaut évite de répéter des flags.

---

## 2) Artifact Registry — créer le repo & auth Docker

```bash
# Activer l’API
gcloud services enable artifactregistry.googleapis.com

# Créer un repo Docker (idempotent)
gcloud artifacts repositories create "$REPO"   --repository-format=docker   --location="$REGION"   --description="BikeOps container images" || echo "Repo déjà créé"

# Configurer Docker pour pousser vers Artifact Registry
gcloud auth configure-docker "$AR_HOST" --quiet

# Vérifier
gcloud artifacts repositories list --location="$REGION"   --filter="format=docker AND name~'/$REPO$'"
```

**Pourquoi ?**  
Artifact Registry héberge nos images. La configuration Docker permet à `docker push` d’utiliser le token gcloud.

---

## 3) Construire & pousser l’image ETL

### 3.1 Dockerfile (rappel des points clés)
- Base : `python:3.11-slim-bookworm`
- Java 17 (`openjdk-17-jre-headless`)
- Ajout user **spark** + `HOME=/home/spark` (Dataproc exécute non-root)
- Copie `requirements.txt`, `src/`, `contracts/`, `configs/`
- `PYTHONPATH=/app/src`

### 3.2 Build local, tag & push

```bash
# Build local (ex. depuis bikeops/)
docker build -f docker/etl.Dockerfile -t bikeops-etl:dev .

# Pusher avec un tag traçable
export IMAGE_NAME="etl"
export LOCAL_IMAGE="bikeops-etl:dev"
export TAG="develop-$(git rev-parse --short HEAD 2>/dev/null || date +%y%m%d%H%M)"
export AR_IMAGE="${AR_HOST}/${PROJECT_ID}/${REPO}/${IMAGE_NAME}:${TAG}"

docker tag "${LOCAL_IMAGE}" "${AR_IMAGE}"
docker push "${AR_IMAGE}"

# Vérifier la présence de l’image
gcloud artifacts docker images list   "${AR_HOST}/${PROJECT_ID}/${REPO}"   --format="table(IMAGE,VERSION,DIGEST,UPDATE_TIME)" | head -n 20
```

**Pourquoi ?**  
Avoir une image **immutable** (taggée) contenant **Python+Java+deps+code**. Dataproc tirera cette image.

---

## 4) GCS — créer le bucket et y déposer données + scripts

```bash
# Activer Dataproc (on l’utilisera ensuite)
gcloud services enable dataproc.googleapis.com

# Créer un bucket régional (idempotent)
gsutil mb -p "$PROJECT_ID" -l "$REGION" -b on "gs://${BUCKET}" || echo "Bucket OK"

# Déposer un CSV bronze (ex. stations)
gsutil cp ./data/bronze/stations.csv "gs://${BUCKET}/bronze/stations.csv"

# Créer un petit "launcher" qui forcera data_root en GCS et lancera l’ETL
cat > /tmp/stations_gcs_launcher.py <<'PY'
from bikeops.utils import config as C
def _load_profile_override(_="configs/local.yaml"):
    return {"data_root": "gs://__BUCKET__/data", "spark": {"timezone": "UTC"}}
C.load_profile = _load_profile_override
from bikeops.etl.stations_ref import main
if __name__ == "__main__":
    main()
PY
sed -i "s#__BUCKET__#${BUCKET}#g" /tmp/stations_gcs_launcher.py
gsutil cp /tmp/stations_gcs_launcher.py "gs://${BUCKET}/jobs/stations_gcs_launcher.py"

# Vérifier
gsutil ls -r "gs://${BUCKET}"
```

**Pourquoi ?**  
Dataproc exécute un **script stocké dans GCS**. Le launcher évite d’éditer ton image : il redirige `data_root` vers GCS.

---

## 5) IAM — donner les bons rôles au service account du job

```bash
# Compute Engine default SA (utilisé par défaut par Dataproc Serverless)
export PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
export DP_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# Rôle Dataproc Worker (obligatoire pour les batches serverless)
gcloud projects add-iam-policy-binding "$PROJECT_ID"   --member="serviceAccount:${DP_SA}"   --role="roles/dataproc.worker"

# Lire les images dans Artifact Registry
gcloud projects add-iam-policy-binding "$PROJECT_ID"   --member="serviceAccount:${DP_SA}"   --role="roles/artifactregistry.reader"

# Accès GCS (lecture/écriture objets sur le bucket)
gsutil iam ch "serviceAccount:${DP_SA}:roles/storage.objectAdmin" "gs://${BUCKET}"
```

**Pourquoi ?**  
Sans ces rôles, le batch échoue (impossible d’orchestrer, de tirer l’image, ou d’écrire sur GCS).

---

## 6) Dataproc Serverless — soumettre un batch PySpark

```bash
# (optionnel) choisir explicitement une runtime version Dataproc
# --runtime-version=2.2 (ex) — sinon, la "default" du moment est utilisée

gcloud dataproc batches submit pyspark   "gs://${BUCKET}/jobs/stations_gcs_launcher.py"   --project="${PROJECT_ID}"   --region="${REGION}"   --container-image="${AR_IMAGE}"   --batch="bikeops-stations-$(date +%H%M%S)"   --properties="spark.sql.session.timeZone=UTC"
# --runtime-version=2.2   # ← tu peux ajouter ce flag pour figer la version
```

**Pourquoi ?**  
C’est la commande qui **lance** le job : Dataproc télécharge le script depuis GCS, **démarre un runtime Spark**, **tire l’image**, et exécute.

**Vérifier la sortie** :
```bash
gsutil ls -r "gs://${BUCKET}/data/silver/stations_silver/"
```

---

## 7) Dépannage — erreurs fréquentes & correctifs

- **`not found: …/etl:YOUR_TAG`**  
  → Utilise **le vrai tag** poussé. Liste les images pour le retrouver :
  ```bash
  gcloud artifacts docker images list "${AR_HOST}/${PROJECT_ID}/${REPO}"
  ```

- **`PERMISSION_DENIED: dataproc.agents.*`**  
  → Il manque **`roles/dataproc.worker`** au **service account du batch** (`${DP_SA}`).

- **`no basic auth credentials` / 401 sur pull d’image**  
  → Manque **`roles/artifactregistry.reader`** sur `${DP_SA}`.

- **`AccessDeniedException` sur GCS**  
  → Donne **`roles/storage.objectAdmin`** sur le **bucket** à `${DP_SA}`.

- **`mkdir: cannot create directory '/home/spark'`**  
  → Le runtime tourne en **non-root**. Dans l’image, **ajouter un user `spark`** avec un home writable :
  ```dockerfile
  RUN useradd -m -s /bin/bash spark && mkdir -p /app && chown -R spark:spark /app /home/spark
  USER spark
  ENV HOME=/home/spark
  ```

- **Avertissement `No runtime version specified`**  
  → Non bloquant. Ajoute `--runtime-version=MAJOR.MINOR` pour figer (ex.: `2.2`).

---

## 8) Exécuter d’autres étapes (même principe)

- **Availability Silver** : créer `availability_gcs_launcher.py` en important `bikeops.etl.ingestion_availability.main` et rediriger `data_root` vers GCS comme plus haut, puis `batches submit`.
- **Weather Silver** : idem avec `bikeops.etl.ingestion_weather.main`.
- **Gold** : idem avec `bikeops.etl.gold_station_hourly.main` puis `gold_city_daily.main`.

> Un launcher = 10 lignes, même modèle. Un batch = 1 commande `gcloud`.  
> Les sorties s’écrivent sous `gs://$BUCKET/data/{silver|gold}/...`.

---

## 9) Nettoyage (facultatif)

```bash
# Supprimer les batches terminés (UI ou CLI)
gcloud dataproc batches list --region "$REGION"
# (Dataproc Serverless n’a pas de cluster à arrêter)

# Supprimer les objets de test (GCS)
gsutil -m rm -r "gs://${BUCKET}/data/silver/stations_silver/"
# Supprimer l’image/tags si besoin (UI Artifact Registry ou CLI)
```

---

## 10) Résumé ultra-compact (cheat-sheet)

```bash
# AR
gcloud services enable artifactregistry.googleapis.com
gcloud artifacts repositories create "$REPO" --repository-format=docker --location="$REGION"
gcloud auth configure-docker "$AR_HOST" --quiet
docker build -f docker/etl.Dockerfile -t bikeops-etl:dev .
docker tag bikeops-etl:dev "$AR_HOST/$PROJECT_ID/$REPO/etl:TAG"
docker push "$AR_HOST/$PROJECT_ID/$REPO/etl:TAG"

# GCS
gcloud services enable dataproc.googleapis.com
gsutil mb -p "$PROJECT_ID" -l "$REGION" -b on "gs://$BUCKET" || true
gsutil cp ./data/bronze/stations.csv "gs://$BUCKET/bronze/stations.csv"
gsutil cp /tmp/stations_gcs_launcher.py "gs://$BUCKET/jobs/stations_gcs_launcher.py"

# IAM
export PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
export DP_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$DP_SA" --role="roles/dataproc.worker"
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$DP_SA" --role="roles/artifactregistry.reader"
gsutil iam ch "serviceAccount:$DP_SA:roles/storage.objectAdmin" "gs://$BUCKET"

# Batch
gcloud dataproc batches submit pyspark "gs://$BUCKET/jobs/stations_gcs_launcher.py"   --project="$PROJECT_ID" --region="$REGION"   --container-image="$AR_HOST/$PROJECT_ID/$REPO/etl:TAG"   --properties="spark.sql.session.timeZone=UTC"
```


export PROJECT_ID="fil-rouge-pipeline"
export REGION="europe-west9"
export REPO="bikeops"
export AR_HOST="${REGION}-docker.pkg.dev"

gcloud config set project "$PROJECT_ID"
gcloud services enable artifactregistry.googleapis.com

# créer le repo docker en ew9 (idempotent)
gcloud artifacts repositories create "$REPO" \
  --repository-format=docker --location="$REGION" \
  --description="BikeOps images (ew9)" || echo "repo ok"
gcloud auth configure-docker "$AR_HOST" --quiet
# si encore "Unauthenticated":
gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin "https://${AR_HOST}"


gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="user:tanoutijaouad@gmail.com" \
  --role="roles/artifactregistry.writer"

  # remplace par ton tag si différent
export LOCAL_IMAGE="bikeops-etl:dev-sparkuser"
export TAG="uri-fix-08df13e"
export AR_IMAGE="${AR_HOST}/${PROJECT_ID}/${REPO}/etl:${TAG}"

docker tag "${LOCAL_IMAGE}" "${AR_IMAGE}"
docker push "${AR_IMAGE}"

# vérif
gcloud artifacts docker images list \
  "${AR_HOST}/${PROJECT_ID}/${REPO}" \
  --format="table(IMAGE,VERSION,UPDATE_TIME)" | head -n 20

# build + push (plateforme amd64)
./docker-push.sh uri-fix-$(git rev-parse --short HEAD)

# sans rebuild (re-push un tag déjà buildé localement)
BUILD=0 ./docker-push.sh test-repush etl docker/etl.Dockerfile .

# push + latest
PUSH_LATEST=1 ./docker-push.sh main-$(git rev-parse --short HEAD)