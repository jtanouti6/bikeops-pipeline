# Contrats de données (BikeOps)

- **Objectif**: décrire les schémas, règles de normalisation, clés et contrôles qualité pour Bronze→Silver→Gold.
- **Usage**:
  1) Les scripts d’ingestion lisent ces YAML pour caster/normaliser.
  2) Les contrôles `quality_checks` sont évalués dans Spark (compte les anomalies).

## Partitions Silver
- `availability`: `dt=YYYY-MM-DD`, `hour=HH` (écritures incrémentales rapides)
- `stations`, `weather`: `dt` optionnel, selon la fréquence d’update

## Déduplication
- `stations`: par `station_id` (garder la version la plus récente si champ `updated_at` ou `ingestion_ts`)
- `availability`: `(station_id, observed_at)` + `prefer: ingestion_ts`

## Qualité (exemples)
- *capacity_non_negative*: `capacity >= 0`
- *lat_long_valid*: latitude/longitude dans les bornes
- *bikes_non_negative*: `bikes_available >= 0`

> Les valeurs `normalize` sont appliquées dans l’ingestion (trim/lower/title).
