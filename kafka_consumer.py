"""
kafka_consumer.py
─────────────────
Listens to Kafka topics that feed Elasticsearch and writes
incoming messages into the NEW (8.x) cluster in real time.

This runs in parallel with (or after) the bulk reindex so that
any docs produced while the migration is in flight are captured.

Architecture
────────────
  App  ──►  Kafka Topic  ──►  [THIS CONSUMER]  ──►  New ES 8.x
                          └──►  Old ES 6.x  (existing producer path)

Lifecycle
─────────
1.  pre_migration   – consumer is started BEFORE bulk reindex begins.
                      This guarantees no messages are missed.
2.  during_migration– consumer keeps running; duplicates are harmless
                      because we use _id-based indexing (idempotent).
3.  post_migration  – consumer continues until operator confirms
                      cutover is stable, then it can be stopped.

Topic discovery
───────────────
  • Explicit list  (config.KAFKA.topics)         – highest priority
  • Prefix filter  (config.KAFKA.topic_prefix)   – auto-discover
  • Fallback       – ALL topics (prefix = "")
"""

import json
import time
import signal
import threading
import traceback
from typing import Any, Dict, List, Optional, Set

from logger     import get_logger
from config     import KAFKA, MIGRATION
from es_client  import get_new_client

logger = get_logger("kafka_consumer")

# ── optional: graceful shutdown flag ─────────────────────────────────
_STOP_EVENT = threading.Event()


def _signal_handler(sig, frame):
    logger.info("Signal %s received – setting stop event …", sig)
    _STOP_EVENT.set()

signal.signal(signal.SIGINT,  _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ──────────────────────────────────────────────
# Topic resolution
# ──────────────────────────────────────────────
def discover_topics(consumer) -> List[str]:
    """
    Return the list of topics this consumer should listen to.
    Uses the priority order: explicit list > prefix filter > all.
    """
    if KAFKA.topics:
        logger.info("Using explicit topic list: %s", KAFKA.topics)
        return KAFKA.topics

    try:
        all_topics: Set[str] = set(consumer.list_topics(timeout=10).topics.keys())
    except Exception as exc:
        logger.warning("Kafka topic discovery failed: %s", exc)
        return []
    # remove internal Kafka topics
    all_topics = {t for t in all_topics if not t.startswith("__")}

    if KAFKA.topic_prefix:
        filtered = {t for t in all_topics if t.startswith(KAFKA.topic_prefix)}
        logger.info("Prefix '%s' matched %d topics: %s",
                    KAFKA.topic_prefix, len(filtered), filtered)
        return list(filtered)

    logger.info("No prefix set – subscribing to ALL %d topics: %s",
                len(all_topics), all_topics)
    return list(all_topics)


# ──────────────────────────────────────────────
# Message → ES document
# ──────────────────────────────────────────────
def parse_message(msg) -> Optional[Dict[str, Any]]:
    """
    Extract index name, doc id, and source from a Kafka message.

    Expected message value format (adjust to YOUR schema):
    {
        "_index":  "orders",          # target ES index
        "_id":     "order-12345",     # doc id  (optional – ES auto-generates if missing)
        "data":    { … }              # the actual document body
    }

    If your messages have a different shape, modify this function.
    """
    try:
        value = json.loads(msg.value().decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.warning("Skipping unparseable message (offset %s, topic %s): %s",
                       msg.offset(), msg.topic(), exc)
        return None

    # ── flexible extraction ──────────────────────────────────────────
    index  = value.get("_index") or value.get("index") or msg.topic()
    doc_id = value.get("_id")    or value.get("id")
    # If the whole payload IS the document (no "data" wrapper):
    source = value.get("data")   or value.get("source") or value.get("doc")
    if source is None:
        # Treat the entire payload as the document, minus meta keys
        source = {k: v for k, v in value.items() if k not in ("_index", "index", "_id", "id")}

    return {"_index": index, "_id": doc_id, "_source": source}


# ──────────────────────────────────────────────
# Bulk writer to new ES cluster
# ──────────────────────────────────────────────
def _build_bulk_body(docs: List[Dict]) -> str:
    lines: List[str] = []
    for doc in docs:
        meta: Dict[str, Any] = {"index": {"_index": doc["_index"]}}
        if doc.get("_id"):
            meta["index"]["_id"] = doc["_id"]
        lines.append(json.dumps(meta))
        lines.append(json.dumps(doc["_source"]))
    return "\n".join(lines) + "\n"


def flush_to_es(docs: List[Dict]) -> int:
    """Bulk-insert a batch of docs into new ES.  Returns count written."""
    if not docs:
        return 0
    es = get_new_client()
    body = _build_bulk_body(docs)

    retries = MIGRATION.max_retries
    for attempt in range(1, retries + 1):
        try:
            result = es.bulk(body)
            if result.get("errors"):
                for item in result.get("items", []):
                    op   = next(iter(item))
                    info = item[op]
                    if "error" in info:
                        logger.warning("Bulk error (doc %s): %s", info.get("_id"), info["error"])
            return len(docs)
        except Exception as exc:
            wait = MIGRATION.retry_delay_sec * (2 ** (attempt - 1))
            logger.warning("Flush attempt %d/%d failed: %s – retry in %ds …",
                           attempt, retries, exc, wait)
            time.sleep(wait)

    logger.error("Flush FAILED after %d retries for %d docs", retries, len(docs))
    return 0


# ──────────────────────────────────────────────
# Main consumer loop
# ──────────────────────────────────────────────
def run_consumer(stop_event: threading.Event = None):
    """
    Start the Kafka consumer.  Blocks until stop_event is set or
    SIGINT / SIGTERM is received.

    Requires:  pip install confluent-kafka
    """
    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError:
        raise ImportError(
            "confluent-kafka is required.  Install with:  pip install confluent-kafka"
        )

    if stop_event is None:
        stop_event = _STOP_EVENT

    conf = {
        "bootstrap.servers": KAFKA.bootstrap_servers,
        "group.id":          KAFKA.consumer_group,
        "auto.offset.reset": KAFKA.auto_offset_reset,
        "enable.auto.commit": True,
    }

    consumer = Consumer(conf)

    topics = []
    for attempt in range(1, 4):
        topics = discover_topics(consumer)
        if topics:
            break
        wait = min(5 * attempt, 15)
        logger.warning("Kafka topics unavailable (attempt %d/3); retrying in %ds …",
                       attempt, wait)
        time.sleep(wait)

    if not topics:
        logger.warning("No Kafka topics found / Kafka unreachable – consumer exiting.")
        consumer.close()
        return

    consumer.subscribe(topics)
    logger.info("Kafka consumer started on topics: %s", topics)

    buffer: List[Dict] = []
    total_written      = 0

    try:
        while not stop_event.is_set():
            msg = consumer.poll(KAFKA.poll_timeout_sec)

            if msg is None:
                # timeout – flush whatever we have
                if buffer:
                    written = flush_to_es(buffer)
                    total_written += written
                    logger.debug("Flushed %d docs (total %d)", written, total_written)
                    buffer.clear()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka error: %s", msg.error())
                continue

            # parse & buffer
            doc = parse_message(msg)
            if doc:
                buffer.append(doc)

            # flush when buffer reaches batch size
            if len(buffer) >= KAFKA.batch_size:
                written = flush_to_es(buffer)
                total_written += written
                logger.info("Flushed batch – %d docs written (total %d)",
                            written, total_written)
                buffer.clear()

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt – flushing remaining buffer …")
    finally:
        # final flush
        if buffer:
            written = flush_to_es(buffer)
            total_written += written

        consumer.close()
        logger.info("Kafka consumer closed.  Total docs written to new ES: %d", total_written)

    return total_written