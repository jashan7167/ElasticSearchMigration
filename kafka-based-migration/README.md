# Kafka-Based Elasticsearch Migration

This version uses Kafka as the transport layer for ES 6.4.2 → 8.11 migration.

## Architecture

```
Old ES 6.4.2 → Producer → Kafka Topics → Consumer → New ES 8.11.0
```

## Benefits

- **Decoupled**: Producer and consumer run independently
- **Fault Tolerant**: Messages persist in Kafka if consumer fails
- **Replayable**: Can re-consume from Kafka if needed
- **Scalable**: Can run multiple consumers in parallel
- **Same CDC Path**: Uses same Kafka infrastructure for bulk + real-time

## Quick Start

### 1. Start Kafka

```bash
docker-compose up -d
```

### 2. Run Full Pipeline

```bash
cd kafka-based-migration
source ../venv/bin/activate
python3 main.py
# Choose option 1
```

### 3. Or Run Components Separately

**Producer (Old ES → Kafka):**
```bash
python3 producer.py
```

**Consumer (Kafka → New ES):**
```bash
python3 consumer.py
```

## Files

- `main.py` - Orchestrator with menu options
- `producer.py` - Reads from old ES and publishes to Kafka
- `consumer.py` - Consumes from Kafka and writes to new ES
- `config.py` - Configuration (shared)
- `mapping_transformer.py` - 6.x → 8.x mapping transformation
- `validator.py` - Post-migration validation

## Usage Modes

### Mode 1: Full Pipeline
Runs producer and consumer together. Best for one-time migration.

### Mode 2: Producer Only  
Only publish to Kafka. Useful when you want to prepare data first.

### Mode 3: Consumer Only
Only consume from Kafka. Useful for replaying or when producer already ran.

### Mode 4: Validation Only
Just validate existing data without migration.

## Monitoring

Check Kafka topics:
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
docker exec -it kafka kafka-consumer-groups --describe --group es-migration-consumer --bootstrap-server localhost:9092
```

Check document counts:
```bash
# Old ES
curl 'http://localhost:9201/_cat/indices?v'

# New ES  
curl 'http://localhost:9200/_cat/indices?v'
```
