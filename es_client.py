"""
es_client.py - Thin wrappers around the official elasticsearch-py client
               for BOTH the old (6.x) and new (8.x) clusters.

The official client is version-aware:
  - elasticsearch==6.8.*  can talk to 6.x
  - elasticsearch==8.11.* can talk to 8.x

We install BOTH under different package names using pip extras / aliases.
See requirements.txt for the exact install commands.

If you cannot install two versions in one env, run the old-cluster helpers
in a separate venv or container.  The HTTP calls below are also plain
requests-based fallbacks for that scenario.
"""

import time
import requests
from typing import Any, Dict, List, Optional

from logger import get_logger
from config  import OldClusterConfig, NewClusterConfig, MIGRATION

logger = get_logger("es_client")


# ──────────────────────────────────────────────
# Generic HTTP helper  (works without the SDK)
# ──────────────────────────────────────────────
class ESHttpClient:
    """Plain-HTTP Elasticsearch client – no SDK dependency required."""

    def __init__(self, host: str, username: Optional[str] = None,
                 password: Optional[str] = None, timeout: int = 30):
        self.host    = host.rstrip("/")
        self.timeout = timeout
        self.auth    = (username, password) if username else None
        self.session = requests.Session()
        if self.auth:
            self.session.auth = self.auth
        self.session.headers.update({"Content-Type": "application/json"})

    # ── low-level verbs ──────────────────────────────────────────────
    def _get(self, path: str, **kw) -> requests.Response:
        return self.session.get(f"{self.host}{path}", timeout=self.timeout, **kw)

    def _put(self, path: str, json: Any = None, **kw) -> requests.Response:
        return self.session.put(f"{self.host}{path}", json=json,
                                timeout=self.timeout, **kw)

    def _post(self, path: str, json: Any = None, **kw) -> requests.Response:
        return self.session.post(f"{self.host}{path}", json=json,
                                 timeout=self.timeout, **kw)

    def _delete(self, path: str, **kw) -> requests.Response:
        return self.session.delete(f"{self.host}{path}", timeout=self.timeout, **kw)

    # ── cluster ──────────────────────────────────────────────────────
    def health(self) -> Dict:
        return self._get("/_cluster/health").json()

    def info(self) -> Dict:
        return self._get("/").json()

    # ── indices ──────────────────────────────────────────────────────
    def get_indices(self) -> List[str]:
        """Return list of index names, excluding system / skip indices."""
        resp = self._get("/_cat/indices?format=json").json()
        skip = set(MIGRATION.skip_indices)
        skip_prefixes = tuple(MIGRATION.skip_index_prefixes)
        return [
            i["index"]
            for i in resp
            if not i["index"].startswith(".")
            and i["index"] not in skip
            and (not skip_prefixes or not i["index"].startswith(skip_prefixes))
        ]

    def get_mapping(self, index: str) -> Dict:
        return self._get(f"/{index}/_mapping").json()

    def get_settings(self, index: str) -> Dict:
        return self._get(f"/{index}/_settings").json()

    def create_index(self, index: str, body: Dict) -> Dict:
        resp = self._put(f"/{index}", json=body)
        if not resp.ok:
            # Log detailed error from ES
            try:
                import json as jsonlib
                from logger import get_logger
                logger = get_logger("es_client")
                error_detail = resp.json()
                logger.error("Failed to create index %s. Status: %d. Error: %s", 
                           index, resp.status_code, jsonlib.dumps(error_detail, indent=2))
                logger.error("Request body was: %s", jsonlib.dumps(body, indent=2))
            except Exception as e:
                from logger import get_logger
                logger = get_logger("es_client")
                logger.error("Failed to create index %s. Status: %d. Response text: %s", 
                           index, resp.status_code, resp.text)
        resp.raise_for_status()
        return resp.json()

    def index_exists(self, index: str) -> bool:
        return self._get(f"/{index}").status_code == 200

    def index_count(self, index: str) -> int:
        return self._get(f"/{index}/_count").json().get("count", 0)

    # ── scroll / search ──────────────────────────────────────────────
    def scroll_search(self, index: str, scroll_size: int = 5000):
        """Generator – yields (scroll_id, hits) pages via the scroll API."""
        body = {"size": scroll_size, "query": {"match_all": {}}}
        resp = self._post(f"/{index}/_search?scroll=5m", json=body).json()
        scroll_id = resp.get("_scroll_id")
        hits      = resp.get("hits", {}).get("hits", [])

        while hits:
            yield scroll_id, hits
            resp  = self._post("/_search/scroll",
                               json={"scroll": "5m", "scroll_id": scroll_id}).json()
            scroll_id = resp.get("_scroll_id", scroll_id)
            hits      = resp.get("hits", {}).get("hits", [])

        # clear scroll
        if scroll_id:
            self._post("/_search/scroll/_clear", json={"scroll_id": scroll_id})

    # ── bulk ─────────────────────────────────────────────────────────
    def bulk(self, body: str) -> Dict:
        """Fire a raw bulk request (newline-delimited JSON string)."""
        resp = self.session.post(
            f"{self.host}/_bulk",
            data=body,
            timeout=MIGRATION.bulk_timeout_sec,
            headers={"Content-Type": "application/x-ndjson"},
        )
        resp.raise_for_status()
        return resp.json()

    # ── reindex ──────────────────────────────────────────────────────
    def reindex(self, source_index: str, dest_index: str,
                remote_host: Optional[str] = None,
                remote_auth: Optional[tuple] = None) -> Dict:
        """
        Trigger a server-side _reindex.  If remote_host is given the source
        is a remote cluster (reindex-from-remote).
        """
        source: Dict[str, Any] = {"index": source_index}
        if remote_host:
            source["remote"] = {"host": remote_host}
            if remote_auth:
                source["remote"]["username"] = remote_auth[0]
                source["remote"]["password"] = remote_auth[1]

        body = {"source": source, "dest": {"index": dest_index}}
        resp = self._post("/_reindex?wait_for_completion=false", json=body)
        resp.raise_for_status()
        return resp.json()                # contains task id

    def get_task(self, task_id: str) -> Dict:
        resp = self._get(f"/_tasks/{task_id}")
        resp.raise_for_status()
        return resp.json()


# ──────────────────────────────────────────────
# Pre-built clients from config
# ──────────────────────────────────────────────
def get_old_client(cfg: OldClusterConfig = None) -> ESHttpClient:
    cfg = cfg or __import__("config").OLD_CLUSTER
    return ESHttpClient(cfg.host, cfg.username, cfg.password, cfg.timeout)

def get_new_client(cfg: NewClusterConfig = None) -> ESHttpClient:
    cfg = cfg or __import__("config").NEW_CLUSTER
    return ESHttpClient(cfg.host, cfg.username, cfg.password, cfg.timeout)