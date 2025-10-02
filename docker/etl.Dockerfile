# ETL: Python 3.11 + Java 17 (Debian 12)
FROM python:3.11-slim-bookworm

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl ca-certificates procps tini \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# --- IMPORTANT: préparer l'utilisateur spark (UID/GID 1099) attendu par Dataproc ---
# Dataproc lance le conteneur en spark:1099 ; il faut une entrée /etc/passwd et /etc/group correspondante.
RUN groupadd -g 1099 spark || true \
 && useradd -u 1099 -g 1099 -m -d /home/spark -s /bin/bash spark || true \
 && mkdir -p /home/spark/.pip \
 && chown -R 1099:1099 /home/spark \
 && chmod -R 0777 /home/spark

# Code & deps
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY contracts ./contracts
COPY configs ./configs

ENV PYTHONPATH=/app/src
ENV SPARK_LOCAL_IP=127.0.0.1
ENV TZ=UTC

# NB: Ne pas définir USER ici (Dataproc l'ignore). On laisse Dataproc exécuter en spark:1099.
# tini est présent pour compatibilité; Dataproc gère le process d’entrée.
CMD ["bash"]
