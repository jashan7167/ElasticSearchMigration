"""
main.py  ─  ES 6.4.2 → 8.11 Migration Orchestrator
════════════════════════════════════════════════════

Usage
─────
    python main.py                          # full migration (reindex + kafka + validate)
    python main.py --indices orders users   # migrate only specific indices
    python main.py --kafka-only             # only run the Kafka consumer (no reindex)
    python main.py --reindex-only           # only run the bulk reindex (no kafka)
    python main.py --validate-only          # only run validation against existing clusters
    python main.py --dry-run                # discover indices & topics, do nothing

Phase order
───────────
  ① Start Kafka consumer in background  (captures live writes)
  ② Run bulk reindex for all indices    (parallel, configurable workers)
  ③ Stop Kafka consumer                 (flushes remaining buffer)
  ④ Run validation & print report

env vars  (see config.py)
─────────
  OLD_ES_HOST   OLD_ES_USER   OLD_ES_PASS
  NEW_ES_HOST   NEW_ES_USER   NEW_ES_PASS
  KAFKA_BOOTSTRAP
"""

import argparse
import sys
import time
import threading
from typing import List, Optional

# ── make sure the local modules are importable ──────────────────────
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from logger            import get_logger
from config            import OLD_CLUSTER, NEW_CLUSTER, KAFKA, MIGRATION
from es_client         import get_old_client, get_new_client
from reindex_engine    import migrate_all_indices
from kafka_consumer    import run_consumer
from validator         import (
    compare_counts, sample_docs, check_new_cluster_health, print_report
)

logger = get_logger("main")


# ──────────────────────────────────────────────
# Pre-flight checks
# ──────────────────────────────────────────────
def preflight():
    """Verify connectivity to both ES clusters before doing anything."""
    logger.info("── Preflight checks ──")

    # old cluster
    try:
        old  = get_old_client()
        info = old.info()
        ver  = info.get("version", {}).get("number", "?")
        logger.info("OLD  cluster reachable – version %s  (%s)", ver, OLD_CLUSTER.host)
    except Exception as exc:
        logger.error("CANNOT reach OLD cluster at %s : %s", OLD_CLUSTER.host, exc)
        sys.exit(1)

    # new cluster
    try:
        new  = get_new_client()
        info = new.info()
        ver  = info.get("version", {}).get("number", "?")
        logger.info("NEW  cluster reachable – version %s  (%s)", ver, NEW_CLUSTER.host)
    except Exception as exc:
        logger.error("CANNOT reach NEW cluster at %s : %s", NEW_CLUSTER.host, exc)
        sys.exit(1)

    logger.info("── Preflight OK ──\n")


# ──────────────────────────────────────────────
# Dry-run  (discovery only)
# ──────────────────────────────────────────────
def dry_run(indices: Optional[List[str]] = None):
    old = get_old_client()
    discovered = indices or old.get_indices()

    logger.info("\n── DRY RUN ────────────────────────────────────")
    logger.info("Indices to migrate (%d):", len(discovered))
    for idx in discovered:
        count = old.index_count(idx)
        logger.info("    %-40s %d docs", idx, count)

    # Kafka topics
    try:
        from confluent_kafka import Consumer
        consumer = Consumer({
            "bootstrap.servers": KAFKA.bootstrap_servers,
            "group.id":          KAFKA.consumer_group,
        })
        from kafka_consumer import discover_topics
        topics = discover_topics(consumer)
        consumer.close()
        logger.info("\nKafka topics (%d):", len(topics))
        for t in topics:
            logger.info("    %s", t)
    except ImportError:
        logger.warning("\nconfluent-kafka not installed – skipping topic discovery.")
    except Exception as exc:
        logger.warning("\nKafka topic discovery failed: %s", exc)

    logger.info("── END DRY RUN ─────────────────────────────────\n")


# ──────────────────────────────────────────────
# Orchestration
# ──────────────────────────────────────────────
def run_migration(indices: Optional[List[str]] = None,
                  run_kafka: bool = True,
                  run_reindex: bool = True,
                  run_validate: bool = True):
    """
    Main orchestration loop.
    """
    start = time.time()
    kafka_stop   = threading.Event()
    kafka_thread = None
    migration_results = []

    # ── ① Kafka consumer (background thread) ──────────────────────
    if run_kafka:
        logger.info("\n[PHASE 1] Starting Kafka consumer in background …")
        kafka_thread = threading.Thread(
            target=run_consumer,
            kwargs={"stop_event": kafka_stop},
            daemon=True,
        )
        kafka_thread.start()
        logger.info("Kafka consumer thread started (tid=%s)", kafka_thread.ident)
        # small grace period so consumer can subscribe
        time.sleep(3)
    else:
        logger.info("\n[PHASE 1] Kafka consumer SKIPPED (--reindex-only or --validate-only)")

    # ── ② Bulk reindex ─────────────────────────────────────────────
    if run_reindex:
        logger.info("\n[PHASE 2] Starting bulk reindex …")
        migration_results = migrate_all_indices(indices)
        logger.info("[PHASE 2] Bulk reindex complete.")
    else:
        logger.info("\n[PHASE 2] Bulk reindex SKIPPED")

    # ── ③ Stop Kafka consumer ──────────────────────────────────────
    if kafka_thread and kafka_thread.is_alive():
        logger.info("\n[PHASE 3] Stopping Kafka consumer (flushing buffer) …")
        kafka_stop.set()
        kafka_thread.join(timeout=30)
        if kafka_thread.is_alive():
            logger.warning("Kafka thread did not exit cleanly within 30 s.")
        else:
            logger.info("[PHASE 3] Kafka consumer stopped.")
    else:
        logger.info("\n[PHASE 3] Kafka consumer already stopped / was not started.")

    # ── ④ Validate ─────────────────────────────────────────────────
    if run_validate:
        logger.info("\n[PHASE 4] Running validation …")

        old      = get_old_client()
        all_idxs = indices or old.get_indices()

        # give new cluster a moment to refresh counts
        time.sleep(2)

        count_report    = compare_counts(all_idxs)
        cluster_health  = check_new_cluster_health()

        # sample 10 docs from each index
        sample_results = {}
        for idx in all_idxs:
            sample_results[idx] = sample_docs(idx, sample_size=10)

        print_report(
            count_report=count_report,
            sample_results=sample_results,
            cluster_health=cluster_health,
            migration_results=migration_results,
        )
    else:
        logger.info("\n[PHASE 4] Validation SKIPPED")

    elapsed = time.time() - start
    logger.info("\n── Migration finished in %.1f seconds ──", elapsed)


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Elasticsearch 6.4.2 → 8.11 Migration Tool"
    )
    parser.add_argument(
        "--indices", nargs="*", default=None,
        help="Migrate only these indices (default: all discovered indices)",
    )
    parser.add_argument(
        "--kafka-only", action="store_true",
        help="Only run the Kafka consumer – no reindex or validation",
    )
    parser.add_argument(
        "--reindex-only", action="store_true",
        help="Only run the bulk reindex – no Kafka or validation",
    )
    parser.add_argument(
        "--validate-only", action="store_true",
        help="Only run validation – assumes data is already migrated",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Discover indices & Kafka topics without moving any data",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info("=" * 60)
    logger.info("  ES 6.4.2 → 8.11 Migration  |  %s",
                time.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    preflight()

    if args.dry_run:
        dry_run(args.indices)
        return

    # derive flags from mutually-exclusive CLI switches
    run_kafka   = not (args.reindex_only or args.validate_only)
    run_reindex = not (args.kafka_only   or args.validate_only)
    run_validate= not (args.kafka_only   or args.reindex_only)

    if args.kafka_only:
        # blocking: run consumer until SIGINT
        run_consumer()
        return

    run_migration(
        indices=args.indices,
        run_kafka=run_kafka,
        run_reindex=run_reindex,
        run_validate=run_validate,
    )


if __name__ == "__main__":
    main()