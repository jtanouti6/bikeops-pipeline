#!/usr/bin/env bash
set -euo pipefail

# Usage
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -lt 1 ]]; then
  echo "Usage: $0 <TAG> [IMAGE_NAME] [DOCKERFILE] [CONTEXT]"
  echo "Ex:    $0 fix-sparkuser-v3-caccef5 etl docker/etl.Dockerfile ."
  exit 1
fi

TAG="$1"                                   # obligatoire
IMAGE_NAME="${2:-etl}"                     # défaut: etl
DOCKERFILE="${3:-docker/etl.Dockerfile}"   # défaut: docker/etl.Dockerfile
CONTEXT="${4:-.}"                          # défaut: racine du repo

# Config (surcharge possible via env)
PROJECT_ID="${PROJECT_ID:-fil-rouge-pipeline}"
REGION="${REGION:-europe-west1}"
REPO="${REPO:-bikeops}"
AR_HOST="${AR_HOST:-${REGION}-docker.pkg.dev}"

LOCAL_IMAGE="${IMAGE_NAME}:${TAG}"
AR_IMAGE="${AR_HOST}/${PROJECT_ID}/${REPO}/${IMAGE_NAME}:${TAG}"

# Vérifs
[[ -f "${DOCKERFILE}" ]] || { echo "❌ Dockerfile introuvable: ${DOCKERFILE}"; exit 3; }
[[ -d "${CONTEXT}" ]]    || { echo "❌ Context introuvable: ${CONTEXT}"; exit 2; }

# Build local (désactiver avec BUILD=0)
if [[ "${BUILD:-1}" != "0" ]]; then
  echo "🔨 Build: ${LOCAL_IMAGE} (Dockerfile=${DOCKERFILE}, context=${CONTEXT})"
  docker build -f "${DOCKERFILE}" -t "${LOCAL_IMAGE}" "${CONTEXT}"
fi

# Auth & push
echo "🔐 Auth registre: ${AR_HOST}"
gcloud auth configure-docker "${AR_HOST}" --quiet

echo "🏷️  Tag -> ${AR_IMAGE}"
docker tag "${LOCAL_IMAGE}" "${AR_IMAGE}"

echo "🚀 Push -> ${AR_IMAGE}"
docker push "${AR_IMAGE}"

echo "✅ Pushed: ${AR_IMAGE}"

