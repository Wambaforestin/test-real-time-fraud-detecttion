#!/usr/bin/env python3
"""
Transaction Producer - Kafka Avro Producer with Schema Evolution
Sends transactions to Kafka with fraud detection logic
First 20 messages use V1 schema, then switches to V2
"""

import json
import time
import uuid
import random
from datetime import datetime, timedelta
from typing import Dict, List
from pathlib import Path

from confluent_kafka import Producer


# Configuration
KAFKA_BROKER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_NAME = "transactions"

# Paths to schemas
SCHEMA_DIR = Path(__file__).parent.parent / "schemas"
SCHEMA_V1_PATH = SCHEMA_DIR / "transaction_v1.avsc"
SCHEMA_V2_PATH = SCHEMA_DIR / "transaction_v2.avsc"


class TransactionGenerator:
    """Generate realistic transaction data with fraud scenarios"""
    
    MERCHANT_CATEGORIES = [
        "RETAIL", "RESTAURANT", "TRAVEL", "ENTERTAINMENT", 
        "GROCERIES", "HEALTH", "UTILITIES", "ONLINE", "OTHER"
    ]
    
    CARD_TYPES = ["CREDIT", "DEBIT", "PREPAID"]
    
    TRANSACTION_TYPES = ["PURCHASE", "WITHDRAWAL", "TRANSFER", "REFUND", "PAYMENT"]
    
    COUNTRIES = ["US", "FR", "UK", "DE", "CA", "ES", "IT", "JP"]
    
    CITIES = {
        "US": ["New York", "Los Angeles", "Chicago", "Houston"],
        "FR": ["Paris", "Lyon", "Marseille", "Toulouse"],
        "UK": ["London", "Manchester", "Birmingham", "Liverpool"],
        "DE": ["Berlin", "Munich", "Hamburg", "Frankfurt"],
        "CA": ["Toronto", "Vancouver", "Montreal", "Calgary"],
        "ES": ["Madrid", "Barcelona", "Valencia", "Seville"],
        "IT": ["Rome", "Milan", "Naples", "Turin"],
        "JP": ["Tokyo", "Osaka", "Kyoto", "Nagoya"]
    }
    
    def __init__(self):
        # Track customer transaction history for velocity checks
        self.customer_transactions: Dict[str, List[float]] = {}
        self.customers = [f"CUST_{i:04d}" for i in range(1, 51)]  # 50 customers
        self.merchants = [f"MERCH_{i:04d}" for i in range(1, 101)]  # 100 merchants
    
    def generate_transaction(self, use_v2: bool = False) -> Dict:
        """Generate a single transaction with potential fraud indicators"""
        
        customer_id = random.choice(self.customers)
        merchant_id = random.choice(self.merchants)
        country = random.choice(self.COUNTRIES)
        
        # Base transaction
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": int(datetime.now().timestamp() * 1000),
            "amount": round(random.uniform(5.0, 2000.0), 2),
            "currency": "USD",
            "merchant_id": merchant_id,
            "merchant_category": random.choice(self.MERCHANT_CATEGORIES),
            "customer_id": customer_id,
            "card_type": random.choice(self.CARD_TYPES),
            "card_last_4": f"{random.randint(1000, 9999)}",
            "location": {
                "country": country,
                "city": random.choice(self.CITIES[country]),
                "latitude": round(random.uniform(-90, 90), 6),
                "longitude": round(random.uniform(-180, 180), 6)
            },
            "transaction_type": random.choice(self.TRANSACTION_TYPES),
            "is_online": random.choice([True, False]),
            "device_id": f"DEV_{random.randint(1000, 9999)}" if random.random() > 0.5 else None,
            "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}" if random.random() > 0.5 else None,
            "risk_score": None,  # Will be calculated
            "customer_profile": {
                "age": random.randint(18, 75) if random.random() > 0.3 else None,
                "account_age_days": random.randint(30, 3650),
                "average_transaction_amount": round(random.uniform(50.0, 500.0), 2),
                "transaction_count_24h": len([t for t in self.customer_transactions.get(customer_id, []) 
                                             if t > time.time() - 86400])
            },
            "metadata": {}
        }
        
        # Apply fraud scenarios
        is_fraud, fraud_reason, risk_score = self._detect_fraud(transaction)
        transaction["risk_score"] = risk_score
        
        # Add V2 fields if needed
        if use_v2:
            transaction["is_fraud"] = is_fraud
            transaction["fraud_reason"] = fraud_reason
        
        # Track transaction for velocity checks
        if customer_id not in self.customer_transactions:
            self.customer_transactions[customer_id] = []
        self.customer_transactions[customer_id].append(time.time())
        
        return transaction
    
    def _detect_fraud(self, transaction: Dict) -> tuple:
        """
        Detect fraud based on rules:
        1. Amount > 5000 → High risk
        2. 5+ transactions in last minute → Velocity attack
        3. Random 5% fraud for variety
        """
        is_fraud = False
        fraud_reason = "none"
        risk_score = round(random.uniform(0.0, 0.3), 2)  # Base risk
        
        customer_id = transaction["customer_id"]
        amount = transaction["amount"]
        
        # Rule 1: High amount
        if amount > 5000:
            is_fraud = True
            fraud_reason = "unusual_amount"
            risk_score = round(random.uniform(0.85, 0.99), 2)
        
        # Rule 2: Velocity check (5+ transactions in last minute)
        recent_transactions = [
            t for t in self.customer_transactions.get(customer_id, [])
            if t > time.time() - 60
        ]
        if len(recent_transactions) >= 5:
            is_fraud = True
            fraud_reason = "velocity_check_failed"
            risk_score = max(risk_score, round(random.uniform(0.80, 0.95), 2))
        
        # Rule 3: Random suspicious location (5% chance)
        if not is_fraud and random.random() < 0.05:
            is_fraud = True
            fraud_reason = "suspicious_location"
            risk_score = round(random.uniform(0.70, 0.85), 2)
        
        # Rule 4: Random anomaly (5% chance)
        if not is_fraud and random.random() < 0.05:
            is_fraud = True
            fraud_reason = "anomaly_detected"
            risk_score = round(random.uniform(0.65, 0.80), 2)
        
        return is_fraud, fraud_reason, risk_score


def delivery_report(err, msg):
    """Callback called once for each message produced to indicate delivery result"""
    if err is not None:
        print(f"\u274c Message delivery failed: {err}")
    # Success is silent for cleaner output


def main():
    """Main producer loop"""
    
    print("=" * 70)
    print("🚀 Transaction Producer - Kafka JSON with Schema Evolution")
    print("=" * 70)
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Topic: {TOPIC_NAME}")
    print("=" * 70)
    
    # Configure producer
    producer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'transaction-producer'
    }
    
    print("\n🔌 Connecting to Kafka...")
    producer = Producer(producer_config)
    print("  ✓ Connected successfully")
    
    # Initialize generator
    generator = TransactionGenerator()
    
    message_count = 0
    fraud_count = 0
    
    try:
        print("\n" + "=" * 70)
        print("📤 Starting message production...")
        print("  Phase 1: Sending 20 messages with Schema V1 (no fraud fields)")
        print("  Phase 2: Switching to Schema V2 (with fraud detection)")
        print("=" * 70 + "\n")
        
        while True:
            message_count += 1
            
            # Switch to V2 after 20 messages
            if message_count == 21:
                print("\n" + "🔄" * 35)
                print("🔄 SWITCHING TO SCHEMA V2 (Adding fraud detection fields)")
                print("🔄" * 35 + "\n")
            
            use_v2 = message_count > 20
            schema_version = "V2" if use_v2 else "V1"
            
            # Generate transaction
            transaction = generator.generate_transaction(use_v2=use_v2)
            
            # Track fraud
            if use_v2 and transaction.get("is_fraud", False):
                fraud_count += 1
            
            # Produce message as JSON
            key = transaction["customer_id"].encode('utf-8')
            value = json.dumps(transaction).encode('utf-8')
            
            try:
                producer.produce(
                    topic=TOPIC_NAME,
                    key=key,
                    value=value,
                    callback=delivery_report
                )
                
                # Print summary
                fraud_indicator = "🚨 FRAUD" if use_v2 and transaction.get("is_fraud") else "✓ Normal"
                print(f"[{message_count:03d}] {schema_version} | {fraud_indicator} | "
                      f"${transaction['amount']:8.2f} | {transaction['customer_id']} | "
                      f"{transaction['merchant_category']:15s}", end="")
                
                if use_v2 and transaction.get("is_fraud"):
                    print(f" | Reason: {transaction['fraud_reason']}")
                else:
                    print()
                
                producer.poll(0)
                
            except Exception as e:
                print(f"\n❌ Error producing message: {e}")
            
            # Wait between messages
            time.sleep(1)
            
            # Flush every 10 messages
            if message_count % 10 == 0:
                print(f"\n📊 Stats: {message_count} messages sent, {fraud_count} frauds detected\n")
                producer.flush()
    
    except KeyboardInterrupt:
        print("\n\n⏹️  Stopping producer...")
    
    finally:
        print(f"\n📊 Final Statistics:")
        print(f"  Total messages: {message_count}")
        print(f"  Fraud detected: {fraud_count}")
        print(f"  Fraud rate: {(fraud_count/max(message_count-20, 1)*100):.1f}% (V2 only)")
        print("\n🔄 Flushing remaining messages...")
        producer.flush()
        print("✅ Producer stopped cleanly")


if __name__ == "__main__":
    main()
