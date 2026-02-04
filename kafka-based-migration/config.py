"""
config.py - Centralized Configuration for ES 6.4.2 → 8.11 Migration
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class OldClusterConfig:
    host: str = os.getenv("OLD_ES_HOST", "http://localhost:9201")
    username: Optional[str] = os.getenv("OLD_ES_USER", None)
    password: Optional[str] = os.getenv("OLD_ES_PASS", None)
    timeout: int = 30


@dataclass
class NewClusterConfig:
    host: str = os.getenv("NEW_ES_HOST", "http://localhost:9200")
    username: Optional[str] = os.getenv("NEW_ES_USER", None)
    password: Optional[str] = os.getenv("NEW_ES_PASS", None)
    timeout: int = 30


@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    consumer_group: str = "es-migration-consumer"
    topic_prefix: str = ""                         # filter topics by prefix (empty = all)
    topics: List[str] = field(default_factory=list)  # explicit topic list (overrides prefix)
    auto_offset_reset: str = "earliest"            # earliest | latest
    poll_timeout_sec: float = 5.0
    batch_size: int = 500


@dataclass
class MigrationConfig:
    # Bulk reindex settings
    bulk_size: int = 1000                   # docs per bulk request
    scroll_size: int = 5000                 # docs per scroll page
    max_retries: int = 3
    retry_delay_sec: int = 5
    parallel_workers: int = 4               # concurrent index migrations

    # Indices to skip (system / internal)
    skip_indices: List[str] = field(default_factory=lambda: [
        ".kibana", ".security", ".monitoring",
        ".reporting", ".async-search"
    ])

    # Log file
    log_file: str = "migration.log"


# ─── Top-level singleton ────────────────────────────────────────────
OLD_CLUSTER   = OldClusterConfig()
NEW_CLUSTER   = NewClusterConfig()
KAFKA         = KafkaConfig()
MIGRATION     = MigrationConfig()

# Aliases for backward compatibility
OLD_ES = OLD_CLUSTER
NEW_ES = NEW_CLUSTER