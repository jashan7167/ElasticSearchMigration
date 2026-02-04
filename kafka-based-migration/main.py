#!/usr/bin/env python3
"""
main.py - Orchestrator for Kafka-Based ES Migration

This version:
1. Produces: Read from old ES → Publish to Kafka
2. Consumes: Read from Kafka → Write to new ES

Benefits:
- Kafka acts as buffer/queue
- Producer and consumer are decoupled
- Can replay if consumer fails
- Better fault tolerance
"""

import sys
import argparse

from config import OLD_ES, NEW_ES, KAFKA
from es_client import ElasticsearchClient
from logger import get_logger
from validator import validate_migration

logger = get_logger("main")


def preflight_checks():
    """Verify connectivity to old ES, new ES, and Kafka."""
    logger.info("── Preflight checks ──")
    
    # Check old ES
    old_es = ElasticsearchClient(OLD_ES.host, OLD_ES.username, OLD_ES.password)
    old_version = old_es.get_version()
    logger.info("OLD  cluster reachable – version %s  (%s)", old_version, OLD_ES.host)
    
    # Check new ES
    new_es = ElasticsearchClient(NEW_ES.host, NEW_ES.username, NEW_ES.password)
    new_version = new_es.get_version()
    logger.info("NEW  cluster reachable – version %s  (%s)", new_version, NEW_ES.host)
    
    # Check Kafka
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({'bootstrap.servers': KAFKA.bootstrap_servers})
        metadata = admin.list_topics(timeout=5)
        logger.info("KAFKA reachable – %d topics (%s)", 
                   len([t for t in metadata.topics.keys() if not t.startswith('_')]),
                   KAFKA.bootstrap_servers)
    except Exception as e:
        logger.error("KAFKA not reachable: %s", e)
        sys.exit(1)
    
    logger.info("── Preflight OK ──\n")


def run_producer():
    """Run the producer process."""
    from producer import BulkToKafkaProducer
    producer = BulkToKafkaProducer()
    producer.run()


def run_consumer():
    """Run the consumer process."""
    from consumer import KafkaToESConsumer
    consumer = KafkaToESConsumer()
    consumer.run()


def run_full_pipeline():
    """Run producer then consumer sequentially."""
    logger.info("\n[PHASE 1] Starting Producer (Old ES → Kafka)...")
    run_producer()
    logger.info("[PHASE 1] Producer complete.\n")
    
    logger.info("[PHASE 2] Starting Consumer (Kafka → New ES)...")
    
    # Run consumer directly (not in thread) - it will auto-stop when idle
    run_consumer()
    
    logger.info("[PHASE 2] Consumer complete.\n")
    
    logger.info("[PHASE 3] Running validation...")
    validate_migration()


def main():
    parser = argparse.ArgumentParser(
        description='ES 6.4.2 → 8.11 Migration (Kafka-Based)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python3 main.py              # Run full pipeline (producer + consumer + validation)
  python3 main.py --producer   # Run producer only
  python3 main.py --consumer   # Run consumer only
  python3 main.py --validate   # Run validation only
        '''
    )
    parser.add_argument('--producer', action='store_true', help='Run producer only (Old ES → Kafka)')
    parser.add_argument('--consumer', action='store_true', help='Run consumer only (Kafka → New ES)')
    parser.add_argument('--validate', action='store_true', help='Run validation only')
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("  ES 6.4.2 → 8.11 Migration (Kafka-Based)")
    logger.info("="*60)
    
    preflight_checks()
    
    if args.producer:
        logger.info("\nRunning Producer Only...")
        run_producer()
        logger.info("\n✓ Producer finished. Messages are in Kafka.")
        logger.info("Run consumer with: python3 main.py --consumer")
    
    elif args.consumer:
        logger.info("\nRunning Consumer Only...")
        run_consumer()
    
    elif args.validate:
        logger.info("\nRunning Validation Only...")
        validate_migration()
    
    else:
        # Default: run full pipeline
        run_full_pipeline()
    
    logger.info("\n" + "="*60)
    logger.info("  Migration Complete")
    logger.info("="*60)


if __name__ == "__main__":
    main()
