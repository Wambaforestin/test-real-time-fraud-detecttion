# Transaction Producer - Guide d'Utilisation

## Installation des dépendances

```bash
# Avec uv
uv pip install confluent-kafka[avro] avro-python3 faker

# Ou via pyproject.toml
uv pip install -e .
```

## Démarrage

### 1. Lancer l'infrastructure
```bash
docker-compose up -d
```

### 2. Créer le topic Kafka
```bash
docker exec fraud-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic transactions
```

### 3. Lancer le producteur
```bash
python producers/transaction_producer.py
```

## Fonctionnement

### Phase 1 : Messages 1-20 (Schema V1)
```
[001] V1 | ✓ Normal | $  234.56 | CUST_0042 | RETAIL
[002] V1 | ✓ Normal | $  789.12 | CUST_0015 | RESTAURANT
...
[020] V1 | ✓ Normal | $  456.78 | CUST_0033 | GROCERIES
```

### Phase 2 : Messages 21+ (Schema V2)
```
🔄 SWITCHING TO SCHEMA V2 (Adding fraud detection fields)

[021] V2 | ✓ Normal | $  123.45 | CUST_0012 | ONLINE
[022] V2 | 🚨 FRAUD | $6543.21 | CUST_0008 | TRAVEL | Reason: unusual_amount
[023] V2 | ✓ Normal | $  345.67 | CUST_0025 | HEALTH
[024] V2 | 🚨 FRAUD | $  234.56 | CUST_0012 | RETAIL | Reason: velocity_check_failed
```

## Scénarios de Fraude

### 1. Montant > 5000€
```python
if amount > 5000:
    is_fraud = True
    fraud_reason = "unusual_amount"
    risk_score = 0.85-0.99
```

### 2. Velocity Attack (5+ transactions/minute)
```python
if len(recent_transactions_in_last_minute) >= 5:
    is_fraud = True
    fraud_reason = "velocity_check_failed"
    risk_score = 0.80-0.95
```

### 3. Localisation Suspecte (5% aléatoire)
```python
if random.random() < 0.05:
    is_fraud = True
    fraud_reason = "suspicious_location"
    risk_score = 0.70-0.85
```

### 4. Anomalie Générale (5% aléatoire)
```python
if random.random() < 0.05:
    is_fraud = True
    fraud_reason = "anomaly_detected"
    risk_score = 0.65-0.80
```

## Vérification

### Consumer Kafka (temps réel)
```bash
docker exec -it fraud-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --from-beginning
```

### Schema Registry
```bash
# Voir les schémas enregistrés
curl http://localhost:8081/subjects

# Voir les versions
curl http://localhost:8081/subjects/transactions-value/versions
```

## Arrêt

```
Ctrl+C dans le terminal du producteur
```

## Statistiques

Le producteur affiche :
- Nombre total de messages
- Nombre de fraudes détectées
- Taux de fraude (uniquement pour V2)

```
📊 Final Statistics:
  Total messages: 50
  Fraud detected: 7
  Fraud rate: 23.3% (V2 only)
```

## Dépannage

### Erreur: Could not connect to broker
- Vérifier que Kafka est démarré : `docker-compose ps`
- Tester la connexion : `docker exec fraud-kafka kafka-broker-api-versions --bootstrap-server localhost:9092`

### Erreur: Schema Registry unreachable
- Vérifier : `curl http://localhost:8081/subjects`
- Redémarrer : `docker-compose restart fraud-schema-registry`

### Erreur: Topic does not exist
- Créer le topic : `docker exec fraud-kafka kafka-topics --create --bootstrap-server localhost:9092 --topic transactions --partitions 3 --replication-factor 1`
