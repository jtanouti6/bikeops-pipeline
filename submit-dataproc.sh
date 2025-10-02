#!/usr/bin/env bash
set -euo pipefail

# Usage
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -lt 2 ]]; then
  echo "Usage: $0 <IMAGE_TAG> <JOB_URI> [BATCH_PREFIX]"
  echo "Ex:    $0 fix-sparkuser-v3-caccef5 gs://fil-rouge-pipeline-bikeops-dev/jobs/stations_gcs_launcher.py stations"
  echo
  echo "Env optionnels:"
  echo "  PROJECT_ID   (def.: fil-rouge-pipeline)"
  echo "  REGION       (def.: europe-west1)"
  echo "  REPO         (def.: bikeops)"
  echo "  IMAGE_NAME   (def.: etl)"
  echo "  SA_EMAIL     (service account explicite) - optionnel"
  echo "  WAIT         (1 attendre fin, 0 fire&forget ; def.: 1)"
  echo "  RUNTIME_VER  (def.: 2.2)"
  exit 1
fi

# Args requis
IMAGE_TAG="$1"
JOB_URI="$2"
BATCH_PREFIX="${3:-bikeops-stations}"

# Config
PROJECT_ID="${PROJECT_ID:-fil-rouge-pipeline}"
REGION="${REGION:-europe-west1}"
REPO="${REPO:-bikeops}"
IMAGE_NAME="${IMAGE_NAME:-etl}"
WAIT="${WAIT:-1}"
RUNTIME_VER="${RUNTIME_VER:-2.2}"

AR_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE_NAME}:${IMAGE_TAG}"
BATCH_ID="${BATCH_PREFIX}-$(date +%H%M%S)"

# Pré-checks rapides (sécurisant)
CURRENT_PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
[[ "$CURRENT_PROJECT" == "$PROJECT_ID" ]] || { echo "❌ gcloud project actif='$CURRENT_PROJECT' ≠ '$PROJECT_ID'"; exit 2; }
[[ "$JOB_URI" =~ ^gs:// ]] || { echo "❌ JOB_URI doit être un gs://... : $JOB_URI"; exit 3; }
gsutil -q stat "$JOB_URI" || { echo "❌ JOB_URI introuvable: $JOB_URI"; exit 4; }
gcloud artifacts docker images describe "$AR_IMAGE" --format='value(image)' >/dev/null 2>&1 \
  || { echo "❌ Image introuvable sur Artifact Registry: $AR_IMAGE"; exit 5; }

echo "🚀 Submit Dataproc Serverless"
echo "  Project : ${PROJECT_ID}"
echo "  Region  : ${REGION}"
echo "  Image   : ${AR_IMAGE}"
echo "  Job     : ${JOB_URI}"
echo "  BatchID : ${BATCH_ID}"
echo "  Runtime : ${RUNTIME_VER}"

# Propriétés Spark (une seule ligne, sans espaces parasites)
PROPS="spark.sql.session.timeZone=UTC,\
spark.driverEnv.HOME=/tmp/spark-home,\
spark.executorEnv.HOME=/tmp/spark-home,\
spark.driverEnv.HADOOP_USER_NAME=spark,\
spark.executorEnv.HADOOP_USER_NAME=spark,\
spark.driver.extraJavaOptions=-Duser.name=spark,\
spark.executor.extraJavaOptions=-Duser.name=spark,\
spark.hadoop.hadoop.security.authentication=simple"

# Commande
CMD=( gcloud dataproc batches submit pyspark "${JOB_URI}"
  --project="${PROJECT_ID}"
  --region="${REGION}"
  --batch="${BATCH_ID}"
  --container-image="${AR_IMAGE}"
  --version="${RUNTIME_VER}"
  --properties="${PROPS}"
  --labels="app=bikeops,env=dev,component=etl"
)

# Service Account (optionnel)
if [[ -n "${SA_EMAIL:-}" ]]; then
  CMD+=( --service-account="${SA_EMAIL}" )
fi

# Submit
"${CMD[@]}"

# Attente/diagnostic
if [[ "${WAIT}" == "1" ]]; then
  echo "⏳ Attente de la fin du batch..."
  gcloud dataproc batches wait "${BATCH_ID}" --region "${REGION}" --project "${PROJECT_ID}"
  echo "🔎 Description:"
  gcloud dataproc batches describe "${BATCH_ID}" --region "${REGION}" --project "${PROJECT_ID}" \
    --format="yaml(state,status,stateTime,logsUri)"
fi

echo "✅ Done: ${BATCH_ID}"
