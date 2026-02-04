#!/usr/bin/env python3
"""
import_from_uat.py - Export indices from UAT ES 6.4.2 and import to local

Usage: python3 import_from_uat.py <UAT_ES_URL> <INDEX_NAME>
Example: python3 import_from_uat.py http://uat-es-server:9200 my-index
"""

import sys
import json
import requests
import os
from pathlib import Path
from urllib.parse import urljoin

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 import_from_uat.py <UAT_ES_URL> <INDEX_NAME>")
        print("Example: python3 import_from_uat.py http://uat-server:9200 my-index")
        sys.exit(1)
    
    uat_es = sys.argv[1]
    index_name = sys.argv[2]
    local_es = "http://localhost:9201"
    temp_dir = Path(f"./uat_export/{index_name}")
    
    print("=" * 60)
    print("  Importing from UAT to Local ES 6.4.2")
    print("=" * 60)
    print(f"UAT ES:     {uat_es}")
    print(f"Index:      {index_name}")
    print(f"Local ES:   {local_es}")
    print(f"Temp dir:   {temp_dir}")
    print()
    
    # Create temp directory
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # ── Step 1: Export mapping ──────────────────────────────────────
    print("[1/5] Exporting mapping from UAT...")
    try:
        resp = requests.get(f"{uat_es}/{index_name}/_mapping")
        resp.raise_for_status()
        mapping_data = resp.json()
        mapping = mapping_data[index_name]["mappings"]
        
        with open(temp_dir / "mapping.json", "w") as f:
            json.dump(mapping, f, indent=2)
        print(f"✓ Mapping exported to {temp_dir}/mapping.json")
    except Exception as e:
        print(f"✗ Failed to export mapping: {e}")
        sys.exit(1)
    
    # ── Step 2: Export settings ──────────────────────────────────────
    print("[2/5] Exporting settings from UAT...")
    try:
        resp = requests.get(f"{uat_es}/{index_name}/_settings")
        resp.raise_for_status()
        settings_data = resp.json()
        settings = settings_data[index_name]["settings"]
        
        with open(temp_dir / "settings.json", "w") as f:
            json.dump(settings, f, indent=2)
        print(f"✓ Settings exported to {temp_dir}/settings.json")
    except Exception as e:
        print(f"✗ Failed to export settings: {e}")
        sys.exit(1)
    
    # ── Step 3: Export all documents using scroll ──────────────────────
    print("[3/5] Exporting documents from UAT...")
    try:
        # Initialize scroll
        resp = requests.post(
            f"{uat_es}/{index_name}/_search?scroll=5m&size=1000",
            json={"query": {"match_all": {}}},
            headers={"Content-Type": "application/json"}
        )
        resp.raise_for_status()
        scroll_data = resp.json()
        
        scroll_id = scroll_data["_scroll_id"]
        total_hits = scroll_data["hits"]["total"]
        if isinstance(total_hits, dict):
            total_hits = total_hits["value"]
        
        print(f"  Total documents: {total_hits}")
        
        # Save documents
        docs_file = temp_dir / "docs.jsonl"
        all_docs = []
        
        # First batch
        hits = scroll_data["hits"]["hits"]
        all_docs.extend(hits)
        fetched = len(hits)
        
        # Continue scrolling
        while len(hits) > 0:
            resp = requests.post(
                f"{uat_es}/_search/scroll",
                json={"scroll": "5m", "scroll_id": scroll_id},
                headers={"Content-Type": "application/json"}
            )
            resp.raise_for_status()
            scroll_data = resp.json()
            
            hits = scroll_data["hits"]["hits"]
            scroll_id = scroll_data["_scroll_id"]
            
            if len(hits) > 0:
                all_docs.extend(hits)
                fetched += len(hits)
                print(f"  Fetched: {fetched} / {total_hits}")
        
        # Clear scroll
        try:
            requests.delete(
                f"{uat_es}/_search/scroll",
                json={"scroll_id": scroll_id},
                headers={"Content-Type": "application/json"}
            )
        except:
            pass
        
        # Write all docs to file
        with open(docs_file, "w") as f:
            for doc in all_docs:
                f.write(json.dumps(doc) + "\n")
        
        print(f"✓ Exported {fetched} documents to {docs_file}")
        
    except Exception as e:
        print(f"✗ Failed to export documents: {e}")
        sys.exit(1)
    
    # ── Step 4: Create index on local ES ──────────────────────────────
    print("[4/5] Creating index on local ES...")
    try:
        # Check if index exists
        check_resp = requests.get(f"{local_es}/{index_name}")
        if check_resp.status_code == 200:
            response = input("⚠ Index already exists on local ES. Delete it first? (y/N): ")
            if response.lower() == 'y':
                requests.delete(f"{local_es}/{index_name}")
                print("  Deleted existing index")
            else:
                print("✗ Aborted")
                sys.exit(1)
        
        # Load mapping and settings
        with open(temp_dir / "mapping.json") as f:
            mapping = json.load(f)
        with open(temp_dir / "settings.json") as f:
            settings = json.load(f)
        
        # Clean settings - remove read-only/auto-managed settings
        if "index" in settings:
            index_settings = settings["index"]
            read_only_keys = [
                "creation_date", "uuid", "version", "provided_name",
                "version.created", "version.upgraded"
            ]
            for key in read_only_keys:
                index_settings.pop(key, None)
        
        # Create index
        create_body = {
            "mappings": mapping,
            "settings": settings
        }
        
        resp = requests.put(
            f"{local_es}/{index_name}",
            json=create_body,
            headers={"Content-Type": "application/json"}
        )
        resp.raise_for_status()
        print("✓ Index created on local ES")
        
    except Exception as e:
        print(f"✗ Failed to create index: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"  Response: {e.response.text}")
        sys.exit(1)
    
    # ── Step 5: Bulk import documents ──────────────────────────────────
    print("[5/5] Bulk importing documents to local ES...")
    try:
        bulk_data = []
        
        with open(temp_dir / "docs.jsonl") as f:
            for line in f:
                doc = json.loads(line.strip())
                
                # Bulk action line
                action = {
                    "index": {
                        "_index": index_name,
                        "_type": doc.get("_type", "doc"),
                        "_id": doc["_id"]
                    }
                }
                bulk_data.append(json.dumps(action))
                bulk_data.append(json.dumps(doc["_source"]))
        
        # Send bulk request
        bulk_body = "\n".join(bulk_data) + "\n"
        
        resp = requests.post(
            f"{local_es}/_bulk",
            data=bulk_body,
            headers={"Content-Type": "application/x-ndjson"}
        )
        resp.raise_for_status()
        bulk_response = resp.json()
        
        # Save response
        with open(temp_dir / "bulk_response.json", "w") as f:
            json.dump(bulk_response, f, indent=2)
        
        has_errors = bulk_response.get("errors", False)
        imported = len(bulk_response.get("items", []))
        
        if not has_errors:
            print(f"✓ Successfully imported {imported} documents")
        else:
            print(f"⚠ Bulk import completed with some errors")
            for item in bulk_response.get("items", []):
                if "error" in item.get("index", {}):
                    print(f"  Error: {item['index']['error']}")
        
        # Refresh index to make documents searchable
        requests.post(f"{local_es}/{index_name}/_refresh")
        
    except Exception as e:
        print(f"✗ Failed to bulk import: {e}")
        sys.exit(1)
    
    # ── Verification ──────────────────────────────────────────────────
    print()
    print("Verifying import...")
    try:
        resp = requests.get(f"{local_es}/{index_name}/_count")
        local_count = resp.json()["count"]
        
        print(f"  UAT docs:   {total_hits}")
        print(f"  Local docs: {local_count}")
        
        if local_count == total_hits:
            print("✓ Document counts match!")
        else:
            print("⚠ Document counts differ")
    except Exception as e:
        print(f"⚠ Could not verify: {e}")
    
    print()
    print("=" * 60)
    print("  Import complete!")
    print("=" * 60)
    print()
    print("Next steps:")
    print(f"  1. Verify data: curl 'http://localhost:9201/{index_name}/_search?pretty'")
    print("  2. Run migration: source venv/bin/activate && python3 main.py")
    print()

if __name__ == "__main__":
    main()
