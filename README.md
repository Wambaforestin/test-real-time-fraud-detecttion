# Test Real-Time Fraud Detection Pipeline

Dans le cadre de mon apprentissage en Data Engineering, j'ai voulu créer un pipeline de détection de fraude en temps réel pour comprendre comment fonctionnent différentes technologies de streaming. Ce projet m'a permis de tester Kafka (avec Schema Registry pour la gestion des schémas), HDFS (système de fichiers distribué), MongoDB, et surtout Spark Streaming dans un contexte pratique. Bien que j'aie rencontré de nombreux défis techniques durant le développement, ce fut une excellente opportunité d'apprentissage pour voir comment ces technologies s'intègrent ensemble dans une architecture de traitement en temps réel.

## Architecture

```
Producer (Python) → Kafka → Spark Streaming → 3 sinks:
                                            ├─ HDFS (toutes transactions)
                                            ├─ MongoDB (fraudes uniquement)
                                            └─ Kafka fraud_alerts (fraudes)
```

## Démarrage

### Étape 1 : Lancer l'infrastructure Docker
```powershell
docker-compose up -d
```

Vérifier que les 9 conteneurs sont démarrés :
```powershell
docker ps --filter "name=fraud-"
```

### Étape 2 : Initialiser HDFS
```powershell
docker exec fraud-namenode hdfs dfs -mkdir -p /data/transactions /checkpoints/fraud-detection
```

### Étape 3 : Démarrer le job Spark Streaming
```powershell
docker exec -d fraud-spark-master /opt/spark/bin/spark-submit `
  --master spark://fraud-spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 `
  --conf spark.mongodb.write.connection.uri=mongodb://admin:admin123@fraud-mongodb:27017/fraud_db.fraud_alerts?authSource=admin `
  /opt/spark-jobs/fraud_detection.py
```

### Étape 4 : Lancer le producer de transactions
```powershell
uv run python producers/transaction_producer.py
```

## Vérification du pipeline

### Consulter les transactions dans HDFS
```powershell
docker exec fraud-namenode hdfs dfs -ls -R /data/transactions | Select-Object -First 20
```

### Consulter les alertes de fraude dans MongoDB
```powershell
docker exec fraud-mongodb mongosh -u admin -p admin123 --eval "use fraud_db; db.fraud_alerts.find().limit(5)"
```

### Consulter le topic Kafka fraud_alerts
```powershell
docker exec fraud-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic fraud_alerts --from-beginning --max-messages 5
```

### Suivre les logs Spark
```powershell
docker logs fraud-spark-master -f
```

## Interfaces web disponibles

| Service | URL | Description |
|---------|-----|-------------|
| Spark Master | http://localhost:8080 | Interface de monitoring Spark |
| Schema Registry | http://localhost:8081 | Registre des schémas Avro |
| Mongo Express | http://localhost:8082 | Interface web MongoDB |
| HDFS NameNode | http://localhost:9870 | Interface web HDFS |

## Arrêt du pipeline

Arrêter le producer avec Ctrl+C dans son terminal.

Arrêter le job Spark :
```powershell
docker exec fraud-spark-master pkill -f spark-submit
```

Arrêter l'infrastructure complète :
```powershell
docker-compose down
```

## Structure du projet

```
real-time-fraud-detecttion/
├── docker-compose.yml          # Configuration infrastructure
├── schemas/
│   ├── transaction_v1.avsc     # Schéma initial sans champs fraude
│   └── transaction_v2.avsc     # Schéma évolué avec is_fraud et fraud_reason
├── producers/
│   └── transaction_producer.py # Générateur de transactions synthétiques
├── spark-jobs/
│   └── fraud_detection.py      # Job Spark Streaming de détection
└── README.md
```

## Règles de détection de fraude

Le système implémente 4 règles de détection :

1. Montant élevé : Transactions supérieures à 5000 USD
2. Vélocité : Plus de 5 transactions par minute pour un même client
3. Location suspecte : Détection aléatoire (5% des cas)
4. Anomalie comportementale : Détection aléatoire (5% des cas)

## Dépendances Python

Installation des dépendances :
```bash
uv add confluent-kafka faker
```

## Caractéristiques techniques

- Evolution de schéma : Le producer bascule du schéma V1 au V2 après 20 messages
- Architecture multi-sink : Les données sont écrites simultanément dans 3 destinations
- Partitionnement par date : HDFS stocke les données avec partitionnement year/month/day
- Format optimisé : Utilisation du format Parquet pour la compression et les performances
- Filtrage intelligent : MongoDB ne stocke que les transactions frauduleuses

## Notes d'implémentation

Le producer envoie les transactions au format JSON pour simplifier le POC. Le job Spark parse ce JSON, applique les règles de détection, et écrit dans les trois destinations. MongoDB reçoit uniquement les fraudes détectées via un filtre sur le champ is_fraud_detected.
