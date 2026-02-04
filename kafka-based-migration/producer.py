#!/usr/bin/env python3
"""
producer.py - Read from Old ES and Publish to Kafka

This producer:
1. Discovers indices in old ES cluster
2. Scrolls through all documents
3. Publishes each document to Kafka topic (topic = index name)
"""

import json
import time
from typing import List, Dict
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config import OLD_ES, KAFKA, MIGRATION
from es_client import ElasticsearchClient
from logger import get_logger

logger = get_logger("producer")


class BulkToKafkaProducer:
    def __init__(self):
        self.old_es = ElasticsearchClient(
            OLD_ES.host,
            username=OLD_ES.username,
            password=OLD_ES.password,
            timeout=OLD_ES.timeout
        )
        self.producer = Producer({
            'bootstrap.servers': KAFKA.bootstrap_servers,
            'client.id': 'es-migration-producer',
            'linger.ms': 10,  # Batch messages for better throughput
            'batch.size': 16384,
            'compression.type': 'snappy'
        })
        self.stats = {
            'total_published': 0,
            'failed': 0
        }
    
    def _delivery_report(self, err, msg):
        """Callback for delivery reports."""
        if err is not None:
            logger.error('Message delivery failed: %s', err)
            self.stats['failed'] += 1
        else:
            self.stats['total_published'] += 1
    
    def create_topics(self, indices: List[str]):
        """Create Kafka topics for indices if they don't exist."""
        admin_client = AdminClient({'bootstrap.servers': KAFKA.bootstrap_servers})
        
        metadata = admin_client.list_topics(timeout=10)
        existing_topics = set(metadata.topics.keys())
        
        new_topics = []
        for index in indices:
            if index not in existing_topics:
                new_topics.append(NewTopic(
                    index,
                    num_partitions=3,  # Parallel consumption
                    replication_factor=1
                ))
        
        if new_topics:
            logger.info("Creating %d Kafka topics...", len(new_topics))
            fs = admin_client.create_topics(new_topics)
            
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info("  ✓ Topic '%s' created", topic)
                except Exception as e:
                    logger.warning("  ⚠ Topic '%s': %s", topic, e)
        else:
            logger.info("All topics already exist")
    
    def publish_index(self, index_name: str, batch_size: int = 1000):
        """Read all documents from an index and publish to Kafka."""
        logger.info("[%s] Starting to publish documents...", index_name)
        
        doc_count = 0
        start_time = time.time()
        
        try:
            for scroll_id, hits in self.old_es.scroll_search(
                index_name,
                scroll_size=MIGRATION.scroll_size
            ):
                for hit in hits:
                    # Prepare message in CDC format
                    message = {
                        "_index": index_name,
                        "_id": hit["_id"],
                        "data": hit["_source"]
                    }
                    
                    # Publish to Kafka
                    self.producer.produce(
                        topic=index_name,
                        value=json.dumps(message).encode('utf-8'),
                        callback=self._delivery_report
                    )
                    
                    doc_count += 1
                    
                    # Poll to handle delivery callbacks
                    if doc_count % batch_size == 0:
                        self.producer.poll(0)
                        logger.info("[%s] Published %d documents...", index_name, doc_count)
            
            # Flush remaining messages
            self.producer.flush(timeout=30)
            
            elapsed = time.time() - start_time
            logger.info("[%s] ✓ Published %d documents in %.2f seconds (%.0f docs/sec)",
                       index_name, doc_count, elapsed, doc_count/elapsed if elapsed > 0 else 0)
            
            return doc_count
            
        except Exception as e:
            logger.error("[%s] Failed to publish: %s", index_name, e, exc_info=True)
            return 0
    
    def run(self):
        """Main producer flow."""
        logger.info("="*60)
        logger.info("  ES → Kafka Producer (Bulk Migration)")
        logger.info("="*60)
        logger.info("Old ES: %s", OLD_ES.host)
        logger.info("Kafka:  %s", KAFKA.bootstrap_servers)
        logger.info("")
        
        # Discover indices
        all_indices = self.old_es.get_indices()
        indices = [idx for idx in all_indices if idx not in MIGRATION.skip_indices]
        
        logger.info("Discovered %d indices to migrate", len(indices))
        logger.info("Indices: %s", ', '.join(indices[:10]) + ('...' if len(indices) > 10 else ''))
        
        if not indices:
            logger.warning("No indices to migrate!")
            return
        
        # Create Kafka topics
        self.create_topics(indices)
        
        # Wait for topics to be ready
        time.sleep(2)
        
        # Publish each index
        total_docs = 0
        for index in indices:
            count = self.publish_index(index)
            total_docs += count
        
        logger.info("")
        logger.info("="*60)
        logger.info("  Producer Summary")
        logger.info("="*60)
        logger.info("Total documents published: %d", self.stats['total_published'])
        logger.info("Failed deliveries: %d", self.stats['failed'])
        logger.info("")


def main():
    producer = BulkToKafkaProducer()
    producer.run()


if __name__ == "__main__":
    main()
