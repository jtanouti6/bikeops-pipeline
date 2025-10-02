#!/usr/bin/env bash
set -euo pipefail

# Usage
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -lt 2 ]]; then
  echo "Usage: $0 <IMAGE_TAG> <JOB_URI> [BATCH_PREFIX]"
  echo "Ex:    $0 fix-sparkuser-v3-caccef5 gs://fil-rouge-pipeline-bikeops-dev/jobs/stations_launcher.py stations"
  echo
  echo "Env optionnels:"
  echo "  PROJECT_ID   (def.: fil-rouge-pipeline)"
  echo "  REGION       (def.: europe-west1)"
  echo "  REPO         (def.: bikeops)"
  echo "  IMAGE_NAME   (def.: etl)"
  echo "  SA_EMAIL     (service account, ex.: compute default SA) - optionnel"
  echo "  WAIT         (1 pour attendre la fin, 0 sinon ; def.: 1)"
  echo
  exit 1
fi

# Args requis
IMAGE_TAG="$1"
JOB_URI="$2"
BATCH_PREFIX="${3:-bikeops-stations}"

# Config (surcharges possibles via env)
PROJECT_ID="${PROJECT_ID:-fil-rouge-pipeline}"
REGION="${REGION:-europe-west1}"
REPO="${REPO:-bikeops}"
IMAGE_NAME="${IMAGE_NAME:-etl}"
WAIT="${WAIT:-1}"

AR_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE_NAME}:${IMAGE_TAG}"
BATCH_ID="${BATCH_PREFIX}-$(date +%H%M%S)"

echo "🚀 Submit Dataproc Serverless"
echo "  Project : ${PROJECT_ID}"
echo "  Region  : ${REGION}"
echo "  Image   : ${AR_IMAGE}"
echo "  Job     : ${JOB_URI}"
echo "  BatchID : ${BATCH_ID}"

# Commande de base
CMD=( gcloud dataproc batches submit pyspark "${JOB_URI}"
  --project="${PROJECT_ID}"
  --region="${REGION}"
  --batch="${BATCH_ID}"
  --container-image="${AR_IMAGE}"
  --properties="spark.driverEnv.HOME=/tmp/spark-home,spark.executorEnv.HOME=/tmp/spark-home,\
  spark.driverEnv.HADOOP_USER_NAME=spark,\
  spark.executorEnv.HADOOP_USER_NAME=spark,\
  spark.driver.extraJavaOptions=-Duser.name=spark,\
  spark.executor.extraJavaOptions=-Duser.name=spark,\
  spark.hadoop.hadoop.security.authentication=simple"

  --labels=app=bikeops,env=dev,component=etl
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
  gcloud dataproc batches describe "${BATCH_ID}" --region "${REGION}" --project "${PROJECT_ID}" --format="yaml(status,state,stateTime)"
fi

echo "✅ Done: ${BATCH_ID}"
