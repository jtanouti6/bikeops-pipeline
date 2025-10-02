#!/usr/bin/env bash
set -euo pipefail

# Usage
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -lt 1 ]]; then
  echo "Usage: $0 <TAG> [IMAGE_NAME] [DOCKERFILE] [CONTEXT]"
  echo "Ex:    $0 fix-sparkuser-v3-caccef5 etl docker/etl.Dockerfile ."
  exit 1
fi

TAG="$1"
IMAGE_NAME="${2:-etl}"
DOCKERFILE="${3:-docker/etl.Dockerfile}"
CONTEXT="${4:-.}"

# Config (surchargables via env)
PROJECT_ID="${PROJECT_ID:-fil-rouge-pipeline}"
REGION="${REGION:-europe-west1}"
REPO="${REPO:-bikeops}"
AR_HOST="${AR_HOST:-${REGION}-docker.pkg.dev}"
BUILD_OPTS="${BUILD_OPTS:---pull --platform linux/amd64}"   # <— tweak 1
PUSH_LATEST="${PUSH_LATEST:-0}"

LOCAL_IMAGE="${IMAGE_NAME}:${TAG}"
AR_IMAGE="${AR_HOST}/${PROJECT_ID}/${REPO}/${IMAGE_NAME}:${TAG}"

# Vérifs
[[ -f "${DOCKERFILE}" ]] || { echo "❌ Dockerfile introuvable: ${DOCKERFILE}"; exit 3; }
[[ -d "${CONTEXT}" ]]    || { echo "❌ Context introuvable: ${CONTEXT}"; exit 2; }

# GCP sanity
CURRENT_PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
[[ "${CURRENT_PROJECT}" == "${PROJECT_ID}" ]] || {
  echo "❌ gcloud project actif='${CURRENT_PROJECT}', attendu='${PROJECT_ID}'"
  echo "   ➜ corrige avec: gcloud config set project '${PROJECT_ID}'"
  exit 4
}
# (optionnel) vérifier l’existence du repo
gcloud artifacts repositories describe "${REPO}" --location "${REGION}" >/dev/null 2>&1 || {
  echo "❌ Repo AR manquant: ${REPO} (region: ${REGION})"
  echo "   ➜ crée-le: gcloud artifacts repositories create '${REPO}' --repository-format=docker --location='${REGION}'"
  exit 5
}

# Build local
if [[ "${BUILD:-1}" != "0" ]]; then
  echo "🔨 Build: ${LOCAL_IMAGE}  (Dockerfile=${DOCKERFILE}, context=${CONTEXT})"
  docker build ${BUILD_OPTS} -f "${DOCKERFILE}" -t "${LOCAL_IMAGE}" "${CONTEXT}"
fi

# Auth & push
echo "🔐 Auth registre: ${AR_HOST}"
gcloud auth configure-docker "${AR_HOST}" --quiet

echo "🏷️  Tag -> ${AR_IMAGE}"
docker tag "${LOCAL_IMAGE}" "${AR_IMAGE}"

echo "🚀 Push -> ${AR_IMAGE}"
docker push "${AR_IMAGE}"

if [[ "${PUSH_LATEST}" == "1" ]]; then
  LATEST="${AR_HOST}/${PROJECT_ID}/${REPO}/${IMAGE_NAME}:latest"
  echo "🏷️  Tag -> ${LATEST}"
  docker tag "${LOCAL_IMAGE}" "${LATEST}"
  echo "🚀 Push -> ${LATEST}"
  docker push "${LATEST}"
fi

echo "🔎 Images récentes:"
gcloud artifacts docker images list \
  "${AR_HOST}/${PROJECT_ID}/${REPO}" \
  --format="table(IMAGE,VERSION,DIGEST,UPDATE_TIME)" | head -n 20

echo "✅ Pushed: ${AR_IMAGE}"
