# FastFood.ma : pipeline Big Data avec Spark, Kafka et MinIO

Projet Écosystème Big Data 2 (EMSI, 5e année, 2026-2027).
Ce dépôt contient l'environnement Docker et les notebooks des **Parties 1 à 3 (jalon 1)**.
Les Parties 4 à 6 (streaming, GraphX, GraphFrames) seront ajoutées au jalon 2.

## Architecture

```
producer.py -> Kafka (topic orders) -> Spark Structured Streaming -> MinIO (Bronze / Silver / Gold)
```

Services Docker : Kafka (mode KRaft), Spark Master, 2 Workers Spark, MinIO (stockage compatible S3), JupyterLab (PySpark).

## Versions et dépendances

| Composant | Version |
|---|---|
| Image de base Spark | `apache/spark:3.5.1-python3` |
| PySpark (pip) | 3.5.1 |
| Delta Lake | `delta-spark` 3.1.0 (jars `delta-spark_2.12-3.1.0`, `delta-storage-3.1.0`) |
| Accès S3 / MinIO | `hadoop-aws-3.3.4`, `aws-java-sdk-bundle-1.12.262` |
| Connecteur Kafka | `spark-sql-kafka-0-10_2.12-3.5.1`, `kafka-clients-3.4.1`, `commons-pool2-2.11.1` |
| GraphFrames | `graphframes-0.8.3-spark3.5-s_2.12` (jar), `graphframes==0.6` (pip) |
| Kafka | `apache/kafka:3.7.0` (KRaft) |
| MinIO | `cgr.dev/chainguard/minio:latest` |
| Autres paquets Python | jupyterlab, pandas, numpy, matplotlib, networkx, kafka-python, awscli, boto3 |

Tous les jars sont téléchargés pendant le build de l'image (voir `Dockerfile`).
Une seule image sert au Master, aux Workers et à Jupyter, ce qui garantit les mêmes versions de Spark et de Python entre driver et executors.

Note : les images `minio/minio` et `minio/mc` ne sont plus disponibles sur Docker Hub. Nous utilisons l'image `cgr.dev/chainguard/minio`, et l'AWS CLI remplace le client `mc` (`aws s3 ls`, `aws s3 cp`).

## Prérequis

- Windows avec Docker Desktop (moteur WSL 2) démarré
- 8 Go de RAM disponibles recommandés
- Ports libres : 8080, 7077, 8888, 4040, 9000, 9001, 29092

## Lancement (PowerShell, depuis ce dossier)

```powershell
mkdir work
docker compose up -d --build
docker compose ps

# Topic Kafka (3 partitions)
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092

# Bucket MinIO
docker exec -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e AWS_DEFAULT_REGION=us-east-1 jupyter aws --endpoint-url http://minio:9000 s3 mb s3://lake
```

Interfaces :

| Service | URL |
|---|---|
| Spark Master | http://localhost:8080 (2 Workers ALIVE attendus) |
| JupyterLab | http://localhost:8888 |
| Spark UI du driver | http://localhost:4040 |
| MinIO (API) | http://localhost:9000 (identifiants `minioadmin` / `minioadmin`) |

## Génération des données

```powershell
docker exec jupyter python3 /work/producer.py --mode file --n 50000

$e = @("-e","AWS_ACCESS_KEY_ID=minioadmin","-e","AWS_SECRET_ACCESS_KEY=minioadmin","-e","AWS_DEFAULT_REGION=us-east-1")
docker exec @e jupyter aws --endpoint-url http://minio:9000 s3 cp /work/orders_history.jsonl s3://lake/bronze/history/orders_history.jsonl
docker exec @e jupyter aws --endpoint-url http://minio:9000 s3 cp /work/restaurants.json s3://lake/reference/restaurants.json
```

Le générateur simule environ 5 % d'événements en retard (jusqu'à 15 minutes) et environ 2 % de doublons.
Le mode flux (Partie 4) se lance avec `--mode kafka --rate 20`.

## Notebooks (dossier `work/`)

| Notebook | Contenu |
|---|---|
| `partie1_cluster.ipynb` | SparkSession sur le cluster, test de lecture/écriture S3A sur MinIO |
| `partie2_rdd.ipynb` | RDD : CA et panier moyen par ville, top 5 restaurants, lineage |
| `partie3_dataframes.ipynb` | DataFrames avec schéma explicite, SQL, jointure, UDF, Parquet partitionné, `explain(True)` |

Exécuter un seul notebook à la fois, ou fermer la session précédente (`spark.stop()`) : en mode standalone, une application prend par défaut tous les cœurs du cluster. On peut aussi limiter chaque session avec `.config("spark.cores.max", "2")`.

## Organisation du lac (bucket `lake`)

```
lake/
  bronze/history/            historique brut (orders_history.jsonl)
  reference/                 restaurants.json
  silver/orders_history/     Parquet nettoyé, partitionné par city
```

Les dossiers `bronze/orders_raw/`, `silver/orders_clean/`, `gold/` et `checkpoints/` seront créés au jalon 2.

## Choix techniques à signaler

- `spark.sql.shuffle.partitions = 8` : la valeur par défaut (200) produit des centaines de petits fichiers Parquet et ralentit les petits jeux de données.
- Les doublons (même `order_id` et même `status`) sont retirés avant toute agrégation.
- Les fichiers lus par les executors doivent être sur `s3a://` : le dossier `/work` n'existe que dans le conteneur Jupyter.

## Arrêt

```powershell
docker compose down      # conserve les données MinIO (volume minio-data)
docker compose down -v   # supprime aussi le volume
```
