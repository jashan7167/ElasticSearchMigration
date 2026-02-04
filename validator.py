"""
validator.py
────────────
Post-migration validation.

Checks
──────
1.  Document-count parity  per index  (old vs new)
2.  Random-sample comparison          (spot-check actual docs)
3.  Cluster health on the new cluster
4.  Prints a full migration report
"""

import json
import random
import time
from typing import Any, Dict, List

from logger     import get_logger
from es_client  import get_old_client, get_new_client

logger = get_logger("validator")


# ──────────────────────────────────────────────
# Count comparison
# ──────────────────────────────────────────────
def compare_counts(indices: List[str]) -> List[Dict[str, Any]]:
    """Return per-index count comparison dicts."""
    old = get_old_client()
    new = get_new_client()
    report = []

    for idx in indices:
        old_count = old.index_count(idx)
        new_count = new.index_count(idx)
        match     = old_count == new_count
        status    = "✓ MATCH" if match else "⚠ MISMATCH"

        entry = {
            "index":     idx,
            "old_count": old_count,
            "new_count": new_count,
            "match":     match,
            "status":    status,
            "diff":      new_count - old_count,
        }
        report.append(entry)

        log_fn = logger.info if match else logger.warning
        log_fn("[%s] old=%d  new=%d  %s", idx, old_count, new_count, status)

    return report


# ──────────────────────────────────────────────
# Document-level spot-check
# ──────────────────────────────────────────────
def sample_docs(index: str, sample_size: int = 10) -> List[Dict[str, Any]]:
    """
    Pull `sample_size` random docs from the OLD cluster, then verify
    each one exists (by _id) in the NEW cluster with the same _source.
    """
    old = get_old_client()
    new = get_new_client()

    # fetch a pool of docs via match_all (take first N from a random offset)
    offset = random.randint(0, max(old.index_count(index) - sample_size, 0))
    resp   = old._get(f"/{index}/_search?size={sample_size}&from={offset}").json()
    docs   = resp.get("hits", {}).get("hits", [])

    results: List[Dict[str, Any]] = []
    for doc in docs:
        doc_id = doc["_id"]
        # fetch same doc from new cluster
        new_resp = new._get(f"/{index}/_doc/{doc_id}")
        if new_resp.status_code == 404:
            results.append({"doc_id": doc_id, "found": False, "match": False, "detail": "NOT FOUND in new cluster"})
            logger.warning("[%s] Doc %s – NOT FOUND in new cluster", index, doc_id)
            continue

        new_doc = new_resp.json()
        match   = doc["_source"] == new_doc.get("_source")
        results.append({
            "doc_id": doc_id,
            "found": True,
            "match": match,
            "detail": "OK" if match else "SOURCE MISMATCH",
        })
        log_fn = logger.debug if match else logger.warning
        log_fn("[%s] Doc %s – %s", index, doc_id, "OK" if match else "SOURCE MISMATCH")

    return results


# ──────────────────────────────────────────────
# Cluster health
# ──────────────────────────────────────────────
def check_new_cluster_health() -> Dict:
    new    = get_new_client()
    health = new.health()
    status = health.get("status", "unknown")
    logger.info("New cluster health: %s  (clusters=%s, nodes=%s)",
                status,
                health.get("cluster_name", "?"),
                health.get("number_of_nodes", "?"))
    return health


# ──────────────────────────────────────────────
# Full report
# ──────────────────────────────────────────────
def print_report(count_report: List[Dict],
                 sample_results: Dict[str, List[Dict]],
                 cluster_health: Dict,
                 migration_results: List[Dict] = None):
    """Pretty-print the full validation report to stdout + log."""

    divider = "=" * 70

    lines = [
        divider,
        "        ELASTICSEARCH MIGRATION VALIDATION REPORT",
        f"        Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        divider,
        "",
        "── CLUSTER HEALTH (new 8.x) ─────────────────────────────────────",
        f"   Status        : {cluster_health.get('status', '?').upper()}",
        f"   Cluster name  : {cluster_health.get('cluster_name', '?')}",
        f"   Nodes         : {cluster_health.get('number_of_nodes', '?')}",
        f"   Shards        : active={cluster_health.get('active_shards', '?')} "
        f"/ unassigned={cluster_health.get('unassigned_shards', '?')}",
        "",
        "── INDEX COUNT COMPARISON ────────────────────────────────────────",
        f"   {'Index':<35} {'Old':>8} {'New':>8} {'Diff':>6}  Status",
        "   " + "-" * 65,
    ]

    total_old = total_new = 0
    mismatches = 0
    for r in count_report:
        total_old  += r["old_count"]
        total_new  += r["new_count"]
        if not r["match"]:
            mismatches += 1
        lines.append(
            f"   {r['index']:<35} {r['old_count']:>8} {r['new_count']:>8} "
            f"{r['diff']:>+6}  {r['status']}"
        )

    lines += [
        "   " + "-" * 65,
        f"   {'TOTAL':<35} {total_old:>8} {total_new:>8} {total_new - total_old:>+6}  "
        f"{'✓ ALL MATCH' if mismatches == 0 else f'⚠ {mismatches} MISMATCH(ES)'}",
        "",
    ]

    # ── sample checks ──────────────────────────────────────────────
    lines.append("── DOCUMENT SAMPLE CHECKS ────────────────────────────────────────")
    total_sampled = total_matched = 0
    for idx, samples in sample_results.items():
        matched = sum(1 for s in samples if s["match"])
        total_sampled += len(samples)
        total_matched += matched
        lines.append(f"   {idx}: {matched}/{len(samples)} docs match")
        for s in samples:
            if not s["match"]:
                lines.append(f"      ⚠ doc {s['doc_id']} – {s['detail']}")

    lines.append(f"   Total: {total_matched}/{total_sampled} docs verified")
    lines.append("")

    # ── per-index migration timing (if available) ─────────────────
    if migration_results:
        lines.append("── MIGRATION TIMING ──────────────────────────────────────────────")
        lines.append(f"   {'Index':<35} {'Docs':>8}  {'Time (s)':>8}  Status")
        lines.append("   " + "-" * 65)
        for r in migration_results:
            elapsed = (r.get("end_time") or time.time()) - r.get("start_time", 0)
            status  = "✓ OK" if r["success"] else "✗ FAILED"
            lines.append(
                f"   {r['index']:<35} {r['docs_migrated']:>8}  {elapsed:>8.1f}  {status}"
            )
            if r["errors"]:
                for e in r["errors"][:3]:   # show max 3 errors
                    lines.append(f"      ✗ {e}")
        lines.append("")

    lines.append(divider)
    lines.append("  END OF REPORT")
    lines.append(divider)

    report_text = "\n".join(lines)
    print(report_text)
    logger.info("\n%s", report_text)

    return report_text