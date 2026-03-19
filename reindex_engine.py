"""
reindex_engine.py
─────────────────
Scrolls every document out of the OLD cluster (6.x) and bulk-inserts
it into the NEW cluster (8.x).

Flow per index
──────────────
1.  Fetch + transform mapping/settings  (mapping_transformer)
2.  Create the index on 8.x             (skip if already exists)
3.  Scroll through 6.x in pages
4.  Bulk-insert each page into 8.x      (with retry)
5.  Final count comparison              (validation)
"""

import json
import os
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

from logger               import get_logger
from config               import MIGRATION
from es_client            import get_old_client, get_new_client
from mapping_transformer  import transform_index

logger = get_logger("reindex_engine")

_CHECKPOINT_LOCK = threading.Lock()


def _load_checkpoints() -> Dict[str, Dict[str, Any]]:
    path = MIGRATION.checkpoint_file
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        if isinstance(data, dict):
            return data
    except Exception as exc:
        logger.warning("Could not read checkpoint file '%s': %s", path, exc)
    return {}


def _save_checkpoints(data: Dict[str, Dict[str, Any]]):
    path = MIGRATION.checkpoint_file
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fp:
        json.dump(data, fp, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _get_checkpoint_docs(index_name: str) -> int:
    if not MIGRATION.resume_enabled:
        return 0
    with _CHECKPOINT_LOCK:
        checkpoints = _load_checkpoints()
        value = checkpoints.get(index_name, {}).get("docs_migrated", 0)
        try:
            return max(0, int(value))
        except Exception:
            return 0


def _update_checkpoint(index_name: str, docs_migrated: int, state: str,
                       error: str = ""):
    if not MIGRATION.resume_enabled:
        return
    with _CHECKPOINT_LOCK:
        checkpoints = _load_checkpoints()
        checkpoints[index_name] = {
            "docs_migrated": int(max(0, docs_migrated)),
            "state": state,
            "updated_at": int(time.time()),
            "last_error": error,
        }
        _save_checkpoints(checkpoints)


def _clear_checkpoint(index_name: str):
    if not MIGRATION.resume_enabled:
        return
    with _CHECKPOINT_LOCK:
        checkpoints = _load_checkpoints()
        if index_name in checkpoints:
            del checkpoints[index_name]
            _save_checkpoints(checkpoints)


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _build_bulk_body(index: str, hits: List[Dict]) -> str:
    """
    Build the newline-delimited JSON body for a _bulk request.
    Each doc becomes:
        {"index": {"_index": "<index>", "_id": "<id>"}}
        { … source … }
    """
    lines: List[str] = []
    for hit in hits:
        meta = {"index": {"_index": index, "_id": hit["_id"]}}
        lines.append(json.dumps(meta))
        lines.append(json.dumps(hit["_source"]))
    # _bulk requires a trailing newline
    return "\n".join(lines) + "\n"


def _retry(fn, max_retries: int = MIGRATION.max_retries,
           delay: int = MIGRATION.retry_delay_sec,
           retryable=None):
    """Generic retry wrapper with exponential back-off."""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if retryable is not None and not retryable(exc):
                raise
            wait = delay * (2 ** (attempt - 1))
            logger.warning("Attempt %d/%d failed: %s – retrying in %ds …",
                           attempt, max_retries, exc, wait)
            time.sleep(wait)
    raise last_exc


def _is_http_413(exc: Exception) -> bool:
    """Best-effort check for HTTP 413 Request Entity Too Large."""
    response = getattr(exc, "response", None)
    return bool(response is not None and getattr(response, "status_code", None) == 413)


def _bulk_with_fallback(new_client, index_name: str, docs: List[Dict],
                        min_chunk_size: int = MIGRATION.min_bulk_chunk_size) -> Dict:
    """
    Send bulk docs with retries. If a large chunk keeps timing out, split it
    recursively into smaller chunks until it succeeds or reaches min_chunk_size.
    """
    if not docs:
        return {"errors": False, "items": []}

    body = _build_bulk_body(index_name, docs)
    try:
        return _retry(
            lambda b=body: new_client.bulk(b),
            retryable=lambda exc: not _is_http_413(exc),
        )
    except Exception as exc:
        if len(docs) <= max(1, min_chunk_size):
            raise

        mid = len(docs) // 2
        left_docs = docs[:mid]
        right_docs = docs[mid:]
        logger.warning(
            "[%s] Bulk chunk of %d docs failed (%s). Retrying as %d + %d docs …",
            index_name,
            len(docs),
            exc,
            len(left_docs),
            len(right_docs),
        )

        left_result = _bulk_with_fallback(new_client, index_name, left_docs, min_chunk_size)
        right_result = _bulk_with_fallback(new_client, index_name, right_docs, min_chunk_size)

        return {
            "errors": left_result.get("errors") or right_result.get("errors"),
            "items": left_result.get("items", []) + right_result.get("items", []),
        }


# ──────────────────────────────────────────────
# Per-index migration
# ──────────────────────────────────────────────
def migrate_single_index(index_name: str) -> Dict:
    """
    Migrate ONE index from old → new.
    Returns a status dict with counts and any errors.
    """
    old = get_old_client()
    new = get_new_client()

    status = {
        "index":          index_name,
        "success":        False,
        "docs_migrated":  0,
        "docs_source":    0,
        "docs_dest":      0,
        "errors":         [],
        "start_time":     time.time(),
        "end_time":       None,
    }

    try:
        # ── 1. fetch mapping & settings from old cluster ──────────
        logger.info("[%s] Fetching mapping & settings …", index_name)
        raw_mapping  = old.get_mapping(index_name)
        raw_settings = old.get_settings(index_name)

        # unwrap the outer index key if present
        if index_name in raw_mapping:
            raw_mapping = raw_mapping[index_name].get("mappings", raw_mapping[index_name])
        if index_name in raw_settings:
            raw_settings = raw_settings[index_name].get("settings", {}).get("index", raw_settings[index_name])

        # ── 2. transform for 8.x ─────────────────────────────────
        mapping_8x, settings_8x = transform_index(index_name, raw_mapping, raw_settings)

        # ── 3. create index on new cluster (if missing) ──────────
        if new.index_exists(index_name):
            logger.info("[%s] Index already exists on new cluster – skipping creation.", index_name)
        else:
            logger.info("[%s] Creating index on new cluster …", index_name)
            create_body = {"mappings": mapping_8x}
            # Only pass settings if non-empty
            if settings_8x:
                create_body["settings"] = {"index": settings_8x}
            
            # Debug: log the create body
            import json
            logger.debug("[%s] Create body: %s", index_name, json.dumps(create_body, indent=2))
            
            new.create_index(index_name, create_body)

        # ── 4. scroll + bulk ─────────────────────────────────────
        logger.info("[%s] Starting scroll + bulk reindex …", index_name)
        resume_from = _get_checkpoint_docs(index_name)
        total_migrated = resume_from
        docs_to_skip = resume_from
        page_num       = 0

        if resume_from:
            logger.info("[%s] Resume enabled – skipping first %d already-migrated docs.",
                        index_name, resume_from)

        for scroll_id, hits in old.scroll_search(index_name, scroll_size=MIGRATION.scroll_size):
            page_num += 1
            if docs_to_skip:
                if docs_to_skip >= len(hits):
                    docs_to_skip -= len(hits)
                    logger.info("[%s] Page %d skipped due to checkpoint (%d docs left to skip) …",
                                index_name, page_num, docs_to_skip)
                    continue

                hits = hits[docs_to_skip:]
                logger.info("[%s] Page %d partially skipped %d docs due to checkpoint.",
                            index_name, page_num, docs_to_skip)
                docs_to_skip = 0

            # chunk into bulk_size batches
            for i in range(0, len(hits), MIGRATION.bulk_size):
                chunk = hits[i : i + MIGRATION.bulk_size]
                result = _bulk_with_fallback(new, index_name, chunk)

                # check for per-doc errors inside bulk response
                if result.get("errors"):
                    for item in result.get("items", []):
                        op   = next(iter(item))
                        info = item[op]
                        if "error" in info:
                            status["errors"].append({
                                "doc_id": info.get("_id"),
                                "error":  info["error"],
                            })
                            logger.warning("[%s] Bulk error on doc %s: %s",
                                           index_name, info.get("_id"), info["error"])

                total_migrated += len(chunk)
                status["docs_migrated"] = total_migrated
                _update_checkpoint(index_name, total_migrated, state="in_progress")

            logger.info("[%s] Page %d done – %d docs migrated so far …",
                        index_name, page_num, total_migrated)

        status["docs_migrated"] = total_migrated

        # ── 5. validation ────────────────────────────────────────
        logger.info("[%s] Validating counts …", index_name)
        status["docs_source"] = old.index_count(index_name)
        # give ES a moment to refresh
        time.sleep(2)
        status["docs_dest"]   = new.index_count(index_name)

        if status["docs_source"] == status["docs_dest"]:
            status["success"] = True
            _clear_checkpoint(index_name)
            logger.info("[%s] ✓ Migration complete. %d docs verified.",
                        index_name, status["docs_dest"])
        else:
            _update_checkpoint(index_name, total_migrated, state="count_mismatch")
            logger.warning(
                "[%s] ⚠ Count mismatch – source: %d  dest: %d",
                index_name, status["docs_source"], status["docs_dest"],
            )

    except Exception as exc:
        status["errors"].append(str(exc))
        _update_checkpoint(index_name, status.get("docs_migrated", _get_checkpoint_docs(index_name)),
                           state="failed", error=str(exc))
        logger.error("[%s] Migration FAILED:\n%s", index_name, traceback.format_exc())

    finally:
        status["end_time"] = time.time()

    return status


# ──────────────────────────────────────────────
# Parallel orchestrator
# ──────────────────────────────────────────────
def migrate_all_indices(indices: List[str] = None) -> List[Dict]:
    """
    Discover (or accept) all indices and migrate them in parallel.
    Returns a list of per-index status dicts.
    """
    old = get_old_client()

    if indices is None:
        indices = old.get_indices()
        logger.info("Discovered %d indices to migrate: %s", len(indices), indices)
    else:
        logger.info("Migrating %d specified indices: %s", len(indices), indices)

    results: List[Dict] = []

    with ThreadPoolExecutor(max_workers=MIGRATION.parallel_workers) as pool:
        future_to_index = {pool.submit(migrate_single_index, idx): idx for idx in indices}
        for future in as_completed(future_to_index):
            idx    = future_to_index[future]
            result = future.result()          # already caught internally
            results.append(result)

    return results