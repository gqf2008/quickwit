---
title: Metrics
sidebar_position: 70
---

Quickwit exposes key metrics in the [Prometheus](https://prometheus.io/) format on the `/metrics` endpoint. You can use any front-end that supports Prometheus to examine the behavior of Quickwit visually.

## Cache Metrics

Quickwit exposes metrics for several cache components, including `fastfields`, `fd`, `partial_request`, `predicate`, `searcher_split`, and `splitfooter`. These metrics share the same structure.

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_cache_{cache_name}` | `in_cache_count` | Count of {cache_name} in cache | `gauge` |
| `quickwit_cache_{cache_name}` | `in_cache_num_bytes` | Number of {cache_name} bytes in cache | `gauge` |
| `quickwit_cache_{cache_name}` | `cache_hits_total` | Number of {cache_name} cache hits | `counter` |
| `quickwit_cache_{cache_name}` | `cache_hits_bytes` | Number of {cache_name} cache hits in bytes | `counter` |
| `quickwit_cache_{cache_name}` | `cache_misses_total` | Number of {cache_name} cache hits | `counter` |
| `quickwit_cache_{cache_name}` | `cache_evict_total` | Number of {cache_name} cache entry evicted | `counter` |
| `quickwit_cache_{cache_name}` | `cache_evict_bytes` | Number of {cache_name} cache entry evicted in bytes | `counter` |

## CLI Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit` | `allocated_num_bytes` | Number of bytes allocated memory, as reported by jemalloc. | `gauge` |

## Common Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit` | `write_bytes`| Number of bytes written by a given component in [`indexer`, `merger`, `deleter`, `split_downloader_{merge,delete}`] | [`index`, `component`] | `counter` |

## Indexing Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_indexing` | `processed_docs_total`| Number of processed docs by index, source and processed status in [`valid`, `schema_error`, `parse_error`, `transform_error`] | [`index`, `source`, `docs_processed_status`] | `counter` |
| `quickwit_indexing` | `processed_bytes`| Number of processed bytes by index, source and processed status in [`valid`, `schema_error`, `parse_error`, `transform_error`] | [`index`, `source`, `docs_processed_status`] | `counter` |
| `quickwit_indexing` | `available_concurrent_upload_permits`| Number of available concurrent upload permits by component in [`merger`, `indexer`] | [`component`] | `gauge` |
| `quickwit_indexing` | `ongoing_merge_operations`| Number of available concurrent upload permits by component in [`merger`, `indexer`]. | [`index`, `source`] | `gauge` |

## Ingest Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_ingest` | `docs_bytes_total` | Total size of the docs ingested, measured in ingester's leader, after validation and before persistence/replication | `counter` |
| `quickwit_ingest` | `docs_total` | Total number of the docs ingested, measured in ingester's leader, after validation and before persistence/replication | `counter` |
| `quickwit_ingest` | `queue_count` | Number of queues currently active | `counter` |

## Metastore Metrics

All metastore methods are monitored by the 3 metrics:

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_metastore` | `requests_total` | Number of requests | [`operation`, `index`] | `counter` |
| `quickwit_metastore` | `request_errors_total` | Number of failed requests | [`operation`, `index`] | `counter` |
| `quickwit_metastore` | `request_duration_seconds` | Duration of requests | [`operation`, `index`, `error`] | `histogram` |

Examples of operation names: `create_index`, `index_metadata`, `delete_index`, `stage_splits`, `publish_splits`, `list_splits`, `add_source`, ...

PostgreSQL-backed metastores also expose connection pool gauges:

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_metastore` | `active_connections` | Number of active PostgreSQL pool connections, including used and idle connections | `gauge` |
| `quickwit_metastore` | `idle_connections` | Number of idle PostgreSQL pool connections | `gauge` |
| `quickwit_metastore` | `acquire_connections` | Number of requests currently waiting to acquire a PostgreSQL pool connection | `gauge` |
| `quickwit_metastore` | `max_connections` | Maximum number of PostgreSQL pool connections configured per metastore node | `gauge` |

The file-backed metastore shared by several nodes (S3-compatible URI) exposes the contention of its
compare-and-swap write path and the maintenance its layouts need (folding a sharded slot or a manifest
stripe into a segment), and how often a reader had to skip a shard object it could not use:

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_metastore` | `file_backed_cas_conflicts_total` | Number of metadata writes that lost a compare-and-swap race and were replayed | `counter` |
| `quickwit_metastore` | `file_backed_cas_conflicts_exhausted_total` | Number of mutations that failed after exhausting their replay budget | `counter` |
| `quickwit_metastore` | `file_backed_replay_tolerated_splits_total` | Number of split state changes a replayed publish found already applied and accepted | `counter` |
| `quickwit_metastore` | `file_backed_shard_folds_total` | Number of split slots of the sharded layout folded into a segment | `counter` |
| `quickwit_metastore` | `file_backed_shard_fold_failures_total` | Number of folds of a sharded split slot that failed and were left for the next write | `counter` |
| `quickwit_metastore` | `file_backed_shard_stale_view_retries_total` | Number of reads of a sharded index that caught the split view moving and restarted | `counter` |
| `quickwit_metastore` | `file_backed_manifest_folds_total` | Number of manifest-layout stripes folded into a segment | `counter` |
| `quickwit_metastore` | `file_backed_manifest_fold_failures_total` | Number of folds of a manifest-layout stripe that failed and were left for the next write | `counter` |
| `quickwit_metastore` | `file_backed_shard_objects_skipped_total` | Number of manifest-layout shard objects a reader skipped (deleted, unreadable, unknown format, misnamed, or naming another shard); the index still loads without them | `counter` |
| `quickwit_metastore` | `file_backed_manifest_adoptions_total` | Number of times a listing read `manifest.json` to adopt the index and template sets another node may have changed | `counter` |

## Rest API Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit` | `http_requests_total` | Total number of HTTP requests received | `counter` |

## Search Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_search` | `split_search_outcome` | Number of local leaf split search outcomes by `category` (`success`, operational `error`, cache/pruning, or cancellation phase). Operational errors are classified by `error` (`create_reader`, `warmup`, `tantivy_search`, or `panic`). Retries are counted separately | `counter` |
| `quickwit_search` | `leaf_search_split_duration_secs` | Number of seconds required to run a leaf search over a single split. The timer starts after the semaphore is obtained | `histogram` |
| `quickwit_search` | `active_search_threads_count` | Number of threads in use in the CPU thread pool | `gauge` |

## Storage Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_storage` | `object_storage_gets_total` | Number of objects fetched | `counter` |
| `quickwit_storage` | `object_storage_puts_total` | Number of objects uploaded, conditional writes included. May differ from object_storage_requests_parts due to multipart upload | `counter` |
| `quickwit_storage` | `object_storage_puts_parts` | Number of object parts uploaded | `counter` |
| `quickwit_storage` | `object_storage_download_num_bytes` | Amount of data downloaded from an object storage | `counter` |
