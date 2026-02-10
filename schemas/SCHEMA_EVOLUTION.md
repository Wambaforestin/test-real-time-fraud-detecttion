# Schema Evolution - Guide de Compatibilité Avro

## Versions des Schémas

### transaction_v1.avsc (Version Initiale)
- Schéma de base sans détection de fraude
- Champs : transaction_id, amount, merchant, customer, location, etc.

### transaction_v2.avsc (Version Évoluée)
- Ajout de 2 nouveaux champs :
  - `is_fraud` (boolean, default: false)
  - `fraud_reason` (string, default: "none")

---

## Comment Schema Registry Gère la Compatibilité

### 1. Enregistrement des Schémas

```bash
# Enregistrer V1 (première fois)
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "...contenu transaction_v1.avsc..."}' \
  http://localhost:8081/subjects/transactions-value/versions

# Réponse : {"id": 1}

# Plus tard, enregistrer V2 (évolution)
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "...contenu transaction_v2.avsc..."}' \
  http://localhost:8081/subjects/transactions-value/versions

# Réponse : {"id": 2}
```

### 2. Types de Compatibilité

Par défaut, Schema Registry utilise **BACKWARD** (rétrocompatible).

```bash
# Vérifier le mode de compatibilité
curl http://localhost:8081/config

# Modes disponibles :
# - BACKWARD : Nouveaux schémas lisent anciennes données
# - FORWARD : Anciens schémas lisent nouvelles données  
# - FULL : Les deux (BACKWARD + FORWARD)
# - NONE : Pas de vérification
```

### 3. Compatibilité BACKWARD (par défaut)

**Règle** : Un consumer avec V2 peut lire des messages écrits en V1.

```
Producer V1 (sans is_fraud) → Kafka → Consumer V2 (avec is_fraud)
                                      ↓
                            is_fraud = false (valeur par défaut)
                            fraud_reason = "none" (valeur par défaut)
```

**Pourquoi ça marche ?**
- Les nouveaux champs `is_fraud` et `fraud_reason` ont des **valeurs par défaut**
- Quand V2 lit un message V1, il utilise ces defaults
- **OBLIGATOIRE** : Tout nouveau champ DOIT avoir un default pour BACKWARD

### 4. Scénarios de Migration

#### Scénario A : Passage progressif Producer V1 → V2
```
Jour 1 :
  Producer V1 → Kafka (messages sans is_fraud)
  Consumer V1 → Lit normalement

Jour 2 : Mise à jour du Consumer
  Producer V1 → Kafka (messages sans is_fraud)
  Consumer V2 → Lit les messages, is_fraud = false (default)
  ✅ Fonctionne grâce à BACKWARD

Jour 3 : Mise à jour du Producer
  Producer V2 → Kafka (messages avec is_fraud)
  Consumer V2 → Lit normalement
  ✅ Fonctionne
```

#### Scénario B : Rollback (si besoin)
```
Producer V2 → Kafka (messages avec is_fraud)
Consumer V1 → Ignore les nouveaux champs is_fraud/fraud_reason
✅ Fonctionne si mode = FORWARD ou FULL
❌ Échoue si mode = BACKWARD only
```

### 5. Migration Sécurisée V1 → V2

**Étape 1 : Enregistrer V2 dans Schema Registry**
```bash
curl -X POST http://localhost:8081/subjects/transactions-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @schemas/transaction_v2.avsc
```

**Étape 2 : Schema Registry valide la compatibilité**
```
Schema Registry vérifie :
  ✅ Nouveaux champs ont des defaults → Compatible
  ✅ Anciens champs non modifiés → Compatible
  ✅ Aucun champ supprimé → Compatible
  
Si OK → Schéma V2 enregistré avec ID=2
Si KO → Erreur 409 (incompatible)
```

**Étape 3 : Déployer les Consumers en V2**
```python
# Consumer utilise automatiquement le dernier schéma
from confluent_kafka.avro import AvroConsumer

consumer = AvroConsumer({
    'bootstrap.servers': 'fraud-kafka:29092',
    'group.id': 'fraud-detector',
    'schema.registry.url': 'http://fraud-schema-registry:8081'
})

# Lit les messages V1 ET V2 sans problème
for msg in consumer:
    data = msg.value()  # Avro désérialisé automatiquement
    print(data['is_fraud'])  # false si message V1, true/false si V2
```

**Étape 4 : Déployer les Producers en V2**
```python
from confluent_kafka.avro import AvroProducer

producer = AvroProducer({
    'bootstrap.servers': 'fraud-kafka:29092',
    'schema.registry.url': 'http://fraud-schema-registry:8081'
})

# Envoie maintenant des messages V2
producer.produce(
    topic='transactions',
    value={
        'transaction_id': '123',
        'amount': 100.0,
        # ... autres champs V1 ...
        'is_fraud': True,           # Nouveau champ
        'fraud_reason': 'velocity'  # Nouveau champ
    }
)
```

### 6. Règles pour Schema Evolution Compatible

✅ **Autorisé (BACKWARD compatible)** :
- Ajouter un champ avec default
- Supprimer un champ avec default
- Élargir une union type (ex: string → [null, string])

❌ **Interdit (casse la compatibilité)** :
- Ajouter un champ SANS default
- Changer le type d'un champ (ex: string → int)
- Renommer un champ
- Supprimer un champ obligatoire

### 7. Commandes Utiles

```bash
# Lister tous les schémas
curl http://localhost:8081/subjects

# Voir toutes les versions d'un schéma
curl http://localhost:8081/subjects/transactions-value/versions

# Voir une version spécifique
curl http://localhost:8081/subjects/transactions-value/versions/1
curl http://localhost:8081/subjects/transactions-value/versions/2

# Tester la compatibilité AVANT d'enregistrer
curl -X POST http://localhost:8081/compatibility/subjects/transactions-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @schemas/transaction_v2.avsc

# Réponse : {"is_compatible": true}
```

---

## Résumé

**V1 → V2 est BACKWARD compatible** parce que :
1. Les 2 nouveaux champs ont des **default values**
2. Aucun champ existant n'a été modifié
3. Schema Registry valide automatiquement la compatibilité

**Migration sans downtime** :
1. Enregistrer V2 dans Schema Registry
2. Déployer Consumers V2 (lisent V1 et V2)
3. Déployer Producers V2 (écrivent V2)
4. Les anciens messages V1 restent lisibles avec defaults

**Avantages Avro + Schema Registry** :
- Migration progressive sans casser les consumers
- Validation automatique de la compatibilité
- Pas besoin de redéployer tout en même temps
- Rollback possible si configuré en mode FULL
