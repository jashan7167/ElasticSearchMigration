# Elasticsearch Command Cheatsheet

> Quick reference for common Elasticsearch operations. Replace `localhost:9200` with your cluster URL.

---

## 📊 Cluster Info

```bash
# Cluster health
curl -s 'http://localhost:9200/_cluster/health?pretty'

# Cluster stats
curl -s 'http://localhost:9200/_cluster/stats?pretty'

# Node info
curl -s 'http://localhost:9200/_nodes?pretty'

# Version info
curl -s 'http://localhost:9200'
```

---

## 📁 Index Operations

### List Indices

```bash
# All indices (table format)
curl -s 'http://localhost:9200/_cat/indices?v'

# All indices (JSON)
curl -s 'http://localhost:9200/_cat/indices?format=json&pretty'

# Sorted by size (descending)
curl -s 'http://localhost:9200/_cat/indices?v&s=store.size:desc'

# Sorted by doc count
curl -s 'http://localhost:9200/_cat/indices?v&s=docs.count:desc'

# Only index names
curl -s 'http://localhost:9200/_cat/indices?h=index'

# Specific columns: index, docs, size
curl -s 'http://localhost:9200/_cat/indices?v&h=index,docs.count,store.size'
```

### Index Details

```bash
# Get mapping
curl -s 'http://localhost:9200/my-index/_mapping?pretty'

# Get settings
curl -s 'http://localhost:9200/my-index/_settings?pretty'

# Get both mapping + settings
curl -s 'http://localhost:9200/my-index?pretty'

# Index stats
curl -s 'http://localhost:9200/my-index/_stats?pretty'
```

### Create / Delete Index

```bash
# Create index (simple)
curl -X PUT 'http://localhost:9200/my-index'

# Create index with settings
curl -X PUT 'http://localhost:9200/my-index' -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1
  }
}'

# Create index with mapping
curl -X PUT 'http://localhost:9200/my-index' -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "name": { "type": "text" },
      "age": { "type": "integer" },
      "created": { "type": "date" }
    }
  }
}'

# Delete index
curl -X DELETE 'http://localhost:9200/my-index'

# Delete multiple indices
curl -X DELETE 'http://localhost:9200/index1,index2,index3'
```

---

## 📄 Document Operations

### Insert / Update

```bash
# Insert with auto-generated ID
curl -X POST 'http://localhost:9200/my-index/_doc' -H 'Content-Type: application/json' -d '{
  "name": "John Doe",
  "age": 30
}'

# Insert with specific ID
curl -X PUT 'http://localhost:9200/my-index/_doc/1' -H 'Content-Type: application/json' -d '{
  "name": "John Doe",
  "age": 30
}'

# Update document (partial)
curl -X POST 'http://localhost:9200/my-index/_update/1' -H 'Content-Type: application/json' -d '{
  "doc": {
    "age": 31
  }
}'

# Upsert (update or insert)
curl -X POST 'http://localhost:9200/my-index/_update/1' -H 'Content-Type: application/json' -d '{
  "doc": { "age": 31 },
  "doc_as_upsert": true
}'
```

### Get / Delete

```bash
# Get document by ID
curl -s 'http://localhost:9200/my-index/_doc/1?pretty'

# Get only _source (no metadata)
curl -s 'http://localhost:9200/my-index/_source/1?pretty'

# Check if document exists
curl -I 'http://localhost:9200/my-index/_doc/1'

# Delete document
curl -X DELETE 'http://localhost:9200/my-index/_doc/1'
```

### Bulk Operations

```bash
# Bulk insert (NDJSON format)
curl -X POST 'http://localhost:9200/_bulk' -H 'Content-Type: application/json' -d '
{"index": {"_index": "my-index", "_id": "1"}}
{"name": "John", "age": 30}
{"index": {"_index": "my-index", "_id": "2"}}
{"name": "Jane", "age": 25}
'

# Bulk with mixed operations
curl -X POST 'http://localhost:9200/_bulk' -H 'Content-Type: application/json' -d '
{"index": {"_index": "my-index", "_id": "1"}}
{"name": "John"}
{"delete": {"_index": "my-index", "_id": "2"}}
{"update": {"_index": "my-index", "_id": "3"}}
{"doc": {"age": 35}}
'
```

---

## 🔍 Search & Query

### Basic Search

```bash
# Get all documents (default 10)
curl -s 'http://localhost:9200/my-index/_search?pretty'

# Get more results
curl -s 'http://localhost:9200/my-index/_search?size=100&pretty'

# Search with query string
curl -s 'http://localhost:9200/my-index/_search?q=name:john&pretty'

# Search multiple indices
curl -s 'http://localhost:9200/index1,index2/_search?pretty'

# Search all indices
curl -s 'http://localhost:9200/_all/_search?pretty'
```

### Query DSL

```bash
# Match query
curl -s 'http://localhost:9200/my-index/_search?pretty' -H 'Content-Type: application/json' -d '{
  "query": {
    "match": { "name": "john" }
  }
}'

# Match all
curl -s 'http://localhost:9200/my-index/_search?pretty' -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} }
}'

# Term query (exact match)
curl -s 'http://localhost:9200/my-index/_search?pretty' -H 'Content-Type: application/json' -d '{
  "query": {
    "term": { "status": "active" }
  }
}'

# Range query
curl -s 'http://localhost:9200/my-index/_search?pretty' -H 'Content-Type: application/json' -d '{
  "query": {
    "range": {
      "age": { "gte": 18, "lte": 65 }
    }
  }
}'

# Bool query (AND/OR/NOT)
curl -s 'http://localhost:9200/my-index/_search?pretty' -H 'Content-Type: application/json' -d '{
  "query": {
    "bool": {
      "must": [
        { "match": { "name": "john" } }
      ],
      "filter": [
        { "range": { "age": { "gte": 18 } } }
      ],
      "must_not": [
        { "term": { "status": "inactive" } }
      ]
    }
  }
}'

# Wildcard query
curl -s 'http://localhost:9200/my-index/_search?pretty' -H 'Content-Type: application/json' -d '{
  "query": {
    "wildcard": { "name": "joh*" }
  }
}'
```

### Pagination & Sorting

```bash
# Pagination (from + size)
curl -s 'http://localhost:9200/my-index/_search?pretty' -H 'Content-Type: application/json' -d '{
  "from": 0,
  "size": 20,
  "query": { "match_all": {} }
}'

# Sorting
curl -s 'http://localhost:9200/my-index/_search?pretty' -H 'Content-Type: application/json' -d '{
  "sort": [
    { "created": "desc" },
    { "name": "asc" }
  ],
  "query": { "match_all": {} }
}'

# Only return specific fields
curl -s 'http://localhost:9200/my-index/_search?pretty' -H 'Content-Type: application/json' -d '{
  "_source": ["name", "age"],
  "query": { "match_all": {} }
}'
```

---

## 📈 Counts & Aggregations

### Document Count

```bash
# Count all docs in index
curl -s 'http://localhost:9200/my-index/_count'

# Count with query
curl -s 'http://localhost:9200/my-index/_count?pretty' -H 'Content-Type: application/json' -d '{
  "query": {
    "match": { "status": "active" }
  }
}'

# Count across all indices
curl -s 'http://localhost:9200/_count'
```

### Aggregations

```bash
# Terms aggregation (group by)
curl -s 'http://localhost:9200/my-index/_search?pretty' -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "by_status": {
      "terms": { "field": "status.keyword" }
    }
  }
}'

# Stats aggregation (min, max, avg, sum)
curl -s 'http://localhost:9200/my-index/_search?pretty' -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "age_stats": {
      "stats": { "field": "age" }
    }
  }
}'

# Date histogram
curl -s 'http://localhost:9200/my-index/_search?pretty' -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "by_month": {
      "date_histogram": {
        "field": "created",
        "calendar_interval": "month"
      }
    }
  }
}'

# Nested aggregation
curl -s 'http://localhost:9200/my-index/_search?pretty' -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "by_status": {
      "terms": { "field": "status.keyword" },
      "aggs": {
        "avg_age": { "avg": { "field": "age" } }
      }
    }
  }
}'
```

---

## 🔄 Scroll API (Large Datasets)

```bash
# Initial scroll request (keep context for 1 minute)
curl -s 'http://localhost:9200/my-index/_search?scroll=1m&pretty' -H 'Content-Type: application/json' -d '{
  "size": 1000,
  "query": { "match_all": {} }
}'

# Continue scrolling (use scroll_id from previous response)
curl -s 'http://localhost:9200/_search/scroll?pretty' -H 'Content-Type: application/json' -d '{
  "scroll": "1m",
  "scroll_id": "YOUR_SCROLL_ID_HERE"
}'

# Clear scroll context
curl -X DELETE 'http://localhost:9200/_search/scroll' -H 'Content-Type: application/json' -d '{
  "scroll_id": "YOUR_SCROLL_ID_HERE"
}'
```

---

## 🔧 Index Management

### Aliases

```bash
# List all aliases
curl -s 'http://localhost:9200/_aliases?pretty'

# Create alias
curl -X POST 'http://localhost:9200/_aliases' -H 'Content-Type: application/json' -d '{
  "actions": [
    { "add": { "index": "my-index-v1", "alias": "my-index" } }
  ]
}'

# Switch alias (zero-downtime)
curl -X POST 'http://localhost:9200/_aliases' -H 'Content-Type: application/json' -d '{
  "actions": [
    { "remove": { "index": "my-index-v1", "alias": "my-index" } },
    { "add": { "index": "my-index-v2", "alias": "my-index" } }
  ]
}'
```

### Reindex

```bash
# Reindex from one index to another
curl -X POST 'http://localhost:9200/_reindex?pretty' -H 'Content-Type: application/json' -d '{
  "source": { "index": "old-index" },
  "dest": { "index": "new-index" }
}'

# Reindex with query filter
curl -X POST 'http://localhost:9200/_reindex?pretty' -H 'Content-Type: application/json' -d '{
  "source": {
    "index": "old-index",
    "query": { "term": { "status": "active" } }
  },
  "dest": { "index": "new-index" }
}'

# Reindex from remote cluster
curl -X POST 'http://localhost:9200/_reindex?pretty' -H 'Content-Type: application/json' -d '{
  "source": {
    "remote": {
      "host": "http://remote-es:9200"
    },
    "index": "source-index"
  },
  "dest": { "index": "dest-index" }
}'
```

### Refresh & Flush

```bash
# Refresh index (make recent changes searchable)
curl -X POST 'http://localhost:9200/my-index/_refresh'

# Flush index (persist to disk)
curl -X POST 'http://localhost:9200/my-index/_flush'

# Force merge (optimize segments)
curl -X POST 'http://localhost:9200/my-index/_forcemerge?max_num_segments=1'
```

### Open / Close

```bash
# Close index (free resources, no operations allowed)
curl -X POST 'http://localhost:9200/my-index/_close'

# Open index
curl -X POST 'http://localhost:9200/my-index/_open'
```

---

## 🛠️ Mapping Operations

```bash
# Get mapping
curl -s 'http://localhost:9200/my-index/_mapping?pretty'

# Add new field to mapping (can't change existing fields!)
curl -X PUT 'http://localhost:9200/my-index/_mapping' -H 'Content-Type: application/json' -d '{
  "properties": {
    "new_field": { "type": "keyword" }
  }
}'

# Get field mapping
curl -s 'http://localhost:9200/my-index/_mapping/field/name?pretty'
```

---

## 🗑️ Delete Operations

```bash
# Delete by query
curl -X POST 'http://localhost:9200/my-index/_delete_by_query?pretty' -H 'Content-Type: application/json' -d '{
  "query": {
    "term": { "status": "deleted" }
  }
}'

# Delete all documents (keep index structure)
curl -X POST 'http://localhost:9200/my-index/_delete_by_query?pretty' -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} }
}'
```

---

## 📋 CAT APIs (Human-Readable)

```bash
# Indices
curl -s 'http://localhost:9200/_cat/indices?v'

# Shards
curl -s 'http://localhost:9200/_cat/shards?v'

# Nodes
curl -s 'http://localhost:9200/_cat/nodes?v'

# Allocation
curl -s 'http://localhost:9200/_cat/allocation?v'

# Health
curl -s 'http://localhost:9200/_cat/health?v'

# Master
curl -s 'http://localhost:9200/_cat/master?v'

# Pending tasks
curl -s 'http://localhost:9200/_cat/pending_tasks?v'

# Aliases
curl -s 'http://localhost:9200/_cat/aliases?v'

# Thread pool
curl -s 'http://localhost:9200/_cat/thread_pool?v'

# Segments
curl -s 'http://localhost:9200/_cat/segments/my-index?v'
```

---

## 🔐 Security (X-Pack)

```bash
# With basic auth
curl -u username:password 'http://localhost:9200/_cluster/health'

# With API key
curl -H "Authorization: ApiKey YOUR_API_KEY" 'http://localhost:9200/_cluster/health'
```

---

## 💡 Tips

| Tip | Example |
|-----|---------|
| Pretty print JSON | Add `?pretty` to URL |
| Silence curl progress | Use `-s` flag |
| Save output to file | `curl ... > output.json` |
| Read body from file | `curl -d @body.json ...` |
| Multiple indices | `index1,index2` or `index*` |
| Exclude indices | `_all,-index1` |

---

## 🆚 ES 6.x vs 8.x Differences

| Feature | ES 6.x | ES 8.x |
|---------|--------|--------|
| Types | `/_doc` or custom types | No types (types removed) |
| Mapping | `{"mappings": {"_doc": {...}}}` | `{"mappings": {...}}` |
| Document API | `/index/type/id` | `/index/_doc/id` |
| include_type_name | Required for 7.x compatibility | Not supported |
