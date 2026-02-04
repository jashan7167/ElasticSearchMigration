#!/usr/bin/env python3
"""
consumer.py - Consume from Kafka and Write to New ES

This consumer:
1. Subscribes to all Kafka topics (or specified topics)
2. Consumes messages in batches
3. Transforms mappings from 6.x → 8.x
4. Bulk writes to new ES cluster
"""

import json
import signal
import sys
import time
from typing import List, Dict, Any
from confluent_kafka import Consumer, KafkaError

from config import NEW_ES, KAFKA, MIGRATION
from es_client import ElasticsearchClient
from mapping_transformer import transform_index
from logger import get_logger

logger = get_logger("consumer")


class KafkaToESConsumer:
    def __init__(self):
        self.new_es = ElasticsearchClient(
            NEW_ES.host,
            username=NEW_ES.username,
            password=NEW_ES.password,
            timeout=NEW_ES.timeout
        )
        
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA.bootstrap_servers,
            'group.id': KAFKA.consumer_group,
            'auto.offset.reset': KAFKA.auto_offset_reset,
            'enable.auto.commit': False,  # Manual commit for reliability
            'max.poll.interval.ms': 300000  # 5 minutes
        })
        
        self.buffer: Dict[str, List[Dict]] = {}
        self.stats = {
            'consumed': 0,
            'written': 0,
            'errors': 0
        }
        self.running = True
        self.indices_created = set()
        
        # Setup signal handlers for graceful shutdown (only in main thread)
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except ValueError:
            # Signal only works in main thread - ignore in worker threads
            pass
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info("Shutdown signal received. Flushing buffer...")
        self.running = False
    
    def ensure_index_exists(self, index_name: str, sample_doc: Dict):
        """Create index on new ES if it doesn't exist (with transformed mapping)."""
        if index_name in self.indices_created:
            return
        
        if self.new_es.index_exists(index_name):
            logger.info("[%s] Index already exists on new cluster", index_name)
            self.indices_created.add(index_name)
            return
        
        logger.info("[%s] Creating index on new cluster...", index_name)
        
        try:
            # Get mapping and settings from old ES
            from es_client import ElasticsearchClient
            from config import OLD_ES
            
            old_es = ElasticsearchClient(
                OLD_ES.host,
                username=OLD_ES.username,
                password=OLD_ES.password,
                timeout=OLD_ES.timeout
            )
            
            raw_mapping = old_es.get_mapping(index_name)
            raw_settings = old_es.get_settings(index_name)
            
            # Transform for 8.x
            mapping_8x, settings_8x = transform_index(index_name, raw_mapping, raw_settings)
            
            # Create index (mapping_8x is already just the properties, not wrapped)
            create_body = {"mappings": mapping_8x}
            if settings_8x:
                create_body["settings"] = {"index": settings_8x}
            
            self.new_es.create_index(index_name, create_body)
            logger.info("[%s] ✓ Index created", index_name)
            self.indices_created.add(index_name)
            
        except Exception as e:
            logger.error("[%s] Failed to create index: %s", index_name, e, exc_info=True)
            raise
    
    def parse_message(self, msg) -> Dict[str, Any]:
        """Parse Kafka message to document."""
        try:
            value = json.loads(msg.value().decode('utf-8'))
            
            index = value.get("_index") or value.get("index") or msg.topic()
            doc_id = value.get("_id") or value.get("id")
            source = value.get("data") or value.get("source") or value.get("doc")
            
            if source is None:
                source = {k: v for k, v in value.items() if k not in ("_index", "index", "_id", "id")}
            
            return {"_index": index, "_id": doc_id, "_source": source}
        
        except Exception as e:
            logger.warning("Failed to parse message: %s", e)
            return None
    
    def buffer_document(self, doc: Dict):
        """Add document to buffer."""
        index = doc["_index"]
        if index not in self.buffer:
            self.buffer[index] = []
        self.buffer[index].append(doc)
    
    def flush_buffer(self):
        """Flush all buffered documents to ES."""
        if not self.buffer:
            return
        
        for index, docs in self.buffer.items():
            if not docs:
                continue
            
            try:
                # Build bulk body
                lines = []
                for doc in docs:
                    meta = {"index": {"_index": index}}
                    if doc.get("_id"):
                        meta["index"]["_id"] = doc["_id"]
                    lines.append(json.dumps(meta))
                    lines.append(json.dumps(doc["_source"]))
                
                bulk_body = "\n".join(lines) + "\n"
                
                # Send to ES
                resp = self.new_es.bulk(bulk_body)
                
                if resp.get("errors"):
                    logger.warning("[%s] Bulk write had errors", index)
                    for item in resp.get("items", []):
                        if "error" in item.get("index", {}):
                            logger.error("  Error: %s", item["index"]["error"])
                            self.stats['errors'] += 1
                else:
                    self.stats['written'] += len(docs)
                    logger.info("[%s] ✓ Wrote %d documents", index, len(docs))
            
            except Exception as e:
                logger.error("[%s] Failed to flush buffer: %s", index, e, exc_info=True)
                self.stats['errors'] += len(docs)
        
        self.buffer.clear()
    
    def run(self, topics: List[str] = None, stop_event=None):
        """Main consumer loop.
        
        Args:
            topics: List of topics to consume from (auto-discovers if None)
            stop_event: Optional threading.Event to signal stop from external caller
        """
        logger.info("="*60)
        logger.info("  Kafka → ES Consumer (Bulk Migration)")
        logger.info("="*60)
        logger.info("Kafka:  %s", KAFKA.bootstrap_servers)
        logger.info("New ES: %s", NEW_ES.host)
        logger.info("Batch:  %d documents", KAFKA.batch_size)
        logger.info("")
        
        # Discover topics if not specified
        if not topics:
            metadata = self.consumer.list_topics(timeout=10)
            topics = [t for t in metadata.topics.keys() 
                     if not t.startswith('_') and t not in MIGRATION.skip_indices]
        
        if not topics:
            logger.error("No topics to consume from!")
            return
        
        logger.info("Subscribing to %d topics: %s", len(topics), topics)
        self.consumer.subscribe(topics)
        
        logger.info("Consuming messages... (Ctrl+C to stop)")
        logger.info("")
        
        last_flush = time.time()
        flush_interval = 5  # Flush every 5 seconds
        idle_count = 0
        max_idle = 30  # Stop after 30 consecutive idle polls
        
        try:
            while self.running:
                # Check external stop signal
                if stop_event and stop_event.is_set():
                    logger.info("Stop signal received from orchestrator")
                    break
                
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    idle_count += 1
                    # No message, check if we should flush
                    if time.time() - last_flush > flush_interval and self.buffer:
                        self.flush_buffer()
                        self.consumer.commit()
                        last_flush = time.time()
                    
                    # Auto-stop if idle too long (all messages consumed)
                    if idle_count >= max_idle:
                        logger.info("No more messages to consume (idle for %d seconds)", max_idle)
                        break
                    continue
                
                # Reset idle counter on message received
                idle_count = 0
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka error: %s", msg.error())
                    continue
                
                # Parse message
                doc = self.parse_message(msg)
                if doc:
                    # Ensure index exists
                    self.ensure_index_exists(doc["_index"], doc)
                    
                    # Buffer document
                    self.buffer_document(doc)
                    self.stats['consumed'] += 1
                    
                    # Flush if batch size reached
                    total_buffered = sum(len(docs) for docs in self.buffer.values())
                    if total_buffered >= KAFKA.batch_size:
                        self.flush_buffer()
                        self.consumer.commit()
                        last_flush = time.time()
        
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        
        finally:
            # Final flush
            logger.info("Flushing final buffer...")
            self.flush_buffer()
            self.consumer.commit()
            self.consumer.close()
            
            logger.info("")
            logger.info("="*60)
            logger.info("  Consumer Summary")
            logger.info("="*60)
            logger.info("Messages consumed: %d", self.stats['consumed'])
            logger.info("Documents written:  %d", self.stats['written'])
            logger.info("Errors:            %d", self.stats['errors'])
            logger.info("")


def main():
    consumer = KafkaToESConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
