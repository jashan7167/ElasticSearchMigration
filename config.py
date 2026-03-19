"""
config.py - Optimized for Production: 3-node cluster, 65-core CPU
"""
import os
from dataclasses import dataclass, field
from typing import List, Optional


def _env_flag(name: str, default: str = "true") -> bool:
    value = os.getenv(name, default).strip().lower()
    return value in {"1", "true", "yes", "on"}


@dataclass
class OldClusterConfig:
    host: str = os.getenv("OLD_ES_HOST", "http://10.44.237.231:30920")
    username: Optional[str] = os.getenv("OLD_ES_USER", None)
    password: Optional[str] = os.getenv("OLD_ES_PASS", None)
    timeout: int = 600          # ⬆ 10 min — large scroll/bulk won't cut off


@dataclass
class NewClusterConfig:
    host: str = os.getenv("NEW_ES_HOST", "http://10.44.237.233:30920")
    username: Optional[str] = os.getenv("NEW_ES_USER", None)
    password: Optional[str] = os.getenv("NEW_ES_PASS", None)
    timeout: int = 600          # ⬆ match source timeout


@dataclass
class KafkaConfig:
    enabled: bool = _env_flag("KAFKA_ENABLED", "false")
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    consumer_group: str = "es-migration-consumer"
    topic_prefix: str = ""
    topics: List[str] = field(default_factory=list)
    auto_offset_reset: str = "earliest"
    poll_timeout_sec: float = 5.0
    batch_size: int = 500


@dataclass
class MigrationConfig:
    # ── Throughput ────────────────────────────────────────────────────
    # 65 cores → reserve ~5 for OS/ES overhead → 60 usable
    # 3 nodes → each node handles ~20 parallel shard streams comfortably
    parallel_workers: int = int(os.getenv("MIGRATION_PARALLEL_WORKERS", "8"))
    # Bulk size: 1000–2000 docs is sweet spot for large docs;
    # push to 2000 since you have CPU headroom + fast network assumed.
    bulk_size: int = int(os.getenv("MIGRATION_BULK_SIZE", "1000"))
    # Scroll: 10 000 keeps heap pressure reasonable on 3-node cluster.
    # Go higher only if your ES heap is ≥ 30 GB per node.
    scroll_size: int = int(os.getenv("MIGRATION_SCROLL_SIZE", "5000"))

    # ── Timeout guards ────────────────────────────────────────────────
    # 20 min bulk timeout — covers huge indices with slow flush
    bulk_timeout_sec: int = int(os.getenv("MIGRATION_BULK_TIMEOUT_SEC", "1200"))

    # ── Resilience ───────────────────────────────────────────────────
    min_bulk_chunk_size: int = int(os.getenv("MIGRATION_MIN_BULK_CHUNK_SIZE", "100"))
    max_retries: int = int(os.getenv("MIGRATION_MAX_RETRIES", "7"))
    retry_delay_sec: int = int(os.getenv("MIGRATION_RETRY_DELAY_SEC", "5"))
    resume_enabled: bool = _env_flag("MIGRATION_RESUME_ENABLED", "false")
    checkpoint_file: str = os.getenv(
        "MIGRATION_CHECKPOINT_FILE", ".migration_checkpoints.json"
    )

    # ── Skip lists ───────────────────────────────────────────────────
    skip_indices: List[str] = field(default_factory=lambda: [
        ".kibana", ".security", ".monitoring",
        ".reporting", ".async-search"
    ])
    skip_index_prefixes: List[str] = field(default_factory=lambda: [
        p.strip()
        for p in os.getenv(
            "MIGRATION_SKIP_INDEX_PREFIXES", "jaeger-span-,jaeger-service"
        ).split(",")
        if p.strip()
    ])

    log_file: str = "migration.log"


# ─── Top-level singletons ────────────────────────────────────────────
OLD_CLUSTER = OldClusterConfig()
NEW_CLUSTER = NewClusterConfig()
KAFKA       = KafkaConfig()
MIGRATION   = MigrationConfig()
