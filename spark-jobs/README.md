# Spark Streaming Job - Guide d'Exécution

## Architecture

```
Kafka (transactions) 
    ↓
Spark Streaming
    ├─ Désérialisation Avro
    ├─ Fenêtre glissante (5 min)
    ├─ Détection fraude
    ├─ HDFS: TOUTES les transactions (Parquet)
    └─ MongoDB: SEULEMENT les fraudes
```

## Pré-requis

### 1. Infrastructure démarrée
```bash
docker-compose up -d
docker-compose ps  # Vérifier que tous les services sont UP
```

### 2. Topic Kafka créé
```bash
docker exec fraud-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --partitions 3 \
  --replication-factor 1
```

### 3. Répertoires HDFS initialisés
```bash
# Créer les répertoires
docker exec fraud-namenode hdfs dfs -mkdir -p /data/transactions
docker exec fraud-namenode hdfs dfs -mkdir -p /checkpoints/fraud-detection

# Vérifier
docker exec fraud-namenode hdfs dfs -ls /data
```

## Lancement du Job Spark

### Méthode 1 : Spark Submit (Recommandé)

```bash
docker exec fraud-spark-master spark-submit \
  --master spark://fraud-spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
  --conf spark.mongodb.write.connection.uri=mongodb://admin:admin123@fraud-mongodb:27017/fraud_db.fraud_alerts?authSource=admin \
  /opt/spark-jobs/fraud_detection.py
```

### Méthode 2 : Script PowerShell

```powershell
# Copier le job dans le conteneur (si volumes non montés)
docker cp spark-jobs/fraud_detection.py fraud-spark-master:/opt/spark-jobs/

# Soumettre le job
docker exec fraud-spark-master spark-submit `
  --master spark://fraud-spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-avro_2.12:3.4.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 `
  --conf spark.mongodb.write.connection.uri=mongodb://admin:admin123@fraud-mongodb:27017/fraud_db.fraud_alerts?authSource=admin `
  /opt/spark-jobs/fraud_detection.py
```

## Workflow Complet

### 1. Démarrer l'infrastructure
```bash
docker-compose up -d
```

### 2. Initialiser HDFS
```bash
docker exec fraud-namenode hdfs dfs -mkdir -p /data/transactions
docker exec fraud-namenode hdfs dfs -mkdir -p /checkpoints/fraud-detection
```

### 3. Lancer le producteur (dans un terminal)
```bash
python producers/transaction_producer.py
```

### 4. Lancer le job Spark (dans un autre terminal)
```bash
docker exec fraud-spark-master spark-submit \
  --master spark://fraud-spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
  --conf spark.mongodb.write.connection.uri=mongodb://admin:admin123@fraud-mongodb:27017/fraud_db.fraud_alerts?authSource=admin \
  /opt/spark-jobs/fraud_detection.py
```

## Vérification des Résultats

### HDFS (Data Lake)
```bash
# Lister les fichiers
docker exec fraud-namenode hdfs dfs -ls /data/transactions

# Voir la structure partitionnée
docker exec fraud-namenode hdfs dfs -ls -R /data/transactions

# Exemple de sortie:
# /data/transactions/year=2026/month=2/day=10/part-00000.parquet
```

### MongoDB (Alertes Fraude)
```bash
# Via mongosh
docker exec fraud-mongodb mongosh -u admin -p admin123 --eval "
  use fraud_db;
  db.fraud_alerts.countDocuments();
  db.fraud_alerts.find().limit(5).pretty();
"

# Via Mongo Express (UI web)
# http://localhost:8082
# Login: admin / admin123
# Database: fraud_db
# Collection: fraud_alerts
```

### Spark UI
```
http://localhost:8080  # Spark Master UI
http://localhost:4040  # Application UI (quand le job tourne)
```

## Logique de Détection

### Sources de Fraude

1. **is_fraud original** (du producteur V2)
   - Pré-marqué par le producteur

2. **Fenêtre glissante** (5 minutes)
   - Total montant > 5000€ sur 5 min
   - 5+ transactions en 5 min (velocity)

3. **Agrégations par card_last_4**
   ```python
   window_total_amount     # Somme sur 5 min
   window_transaction_count # Nombre sur 5 min
   window_avg_amount       # Moyenne sur 5 min
   ```

### Exemple de Détection

```python
# Transaction 1: 2000€ à 10:00
# Transaction 2: 2500€ à 10:02
# Transaction 3: 1500€ à 10:04
# Total fenêtre = 6000€ > 5000€ → FRAUDE détectée
# Reason: "window_amount_exceeded"
```

## Monitoring

### Logs Spark
```bash
# Logs du job
docker logs fraud-spark-master -f

# Logs du worker
docker logs fraud-spark-worker -f
```

### Métriques temps réel
```bash
# Nombre de partitions HDFS
docker exec fraud-namenode hdfs dfs -count /data/transactions

# Nombre d'alertes MongoDB
docker exec fraud-mongodb mongosh -u admin -p admin123 --eval "
  use fraud_db;
  db.fraud_alerts.countDocuments()
"
```

## Arrêt

```bash
# Arrêter le producteur: Ctrl+C

# Le job Spark tourne en continu
# Pour l'arrêter: Ctrl+C dans le terminal du spark-submit

# Ou kill le job via Spark Master UI
# http://localhost:8080 → Kill application
```

## Dépannage

### Erreur: Cannot connect to Kafka
```bash
# Vérifier Kafka
docker exec fraud-kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

### Erreur: HDFS namenode not found
```bash
# Vérifier HDFS
docker exec fraud-namenode hdfs dfsadmin -report
```

### Erreur: MongoDB authentication failed
```bash
# Vérifier MongoDB
docker exec fraud-mongodb mongosh -u admin -p admin123 --eval "db.adminCommand('ping')"
```

### Checkpoints corrompus
```bash
# Supprimer les checkpoints
docker exec fraud-namenode hdfs dfs -rm -r /checkpoints/fraud-detection

# Recréer
docker exec fraud-namenode hdfs dfs -mkdir -p /checkpoints/fraud-detection
```

## Configuration Avancée

### Augmenter les ressources Spark
```yaml
# Dans docker-compose.yml
spark-worker:
  environment:
    - SPARK_WORKER_MEMORY=4G  # Au lieu de 2G
    - SPARK_WORKER_CORES=4    # Au lieu de 2
```

### Changer la fenêtre glissante
```python
# Dans fraud_detection.py
WINDOW_DURATION = "10 minutes"  # Au lieu de 5
SLIDE_DURATION = "2 minutes"    # Au lieu de 1
```

### Changer le seuil de fraude
```python
# Dans fraud_detection.py
FRAUD_AMOUNT_THRESHOLD = 10000.0  # Au lieu de 5000.0
```

## Performance

- **Batch Interval**: 30s pour HDFS, 10s pour MongoDB
- **Watermark**: 10 minutes (pour les événements en retard)
- **Débit attendu**: ~100-1000 transactions/sec
- **Latence**: <5 secondes de bout en bout

## Formats de Sortie

### HDFS Parquet
```
/data/transactions/
├── year=2026/
│   ├── month=2/
│   │   ├── day=10/
│   │   │   ├── part-00000-xxx.parquet
│   │   │   ├── part-00001-xxx.parquet
│   │   │   └── ...
```

### MongoDB Document
```json
{
  "_id": ObjectId("..."),
  "transaction_id": "uuid-xxx",
  "transaction_time": ISODate("2026-02-10T10:30:15.123Z"),
  "customer_id": "CUST_0042",
  "card_last_4": "1234",
  "amount": 6543.21,
  "merchant_id": "MERCH_0089",
  "merchant_category": "TRAVEL",
  "country": "FR",
  "city": "Paris",
  "is_fraud": true,
  "fraud_reason": "window_amount_exceeded",
  "risk_score": 0.92,
  "window_total_amount": 7890.45,
  "window_transaction_count": 3,
  "processed_at": ISODate("2026-02-10T10:30:16.000Z")
}
```
