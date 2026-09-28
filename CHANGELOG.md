# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Azure Blob Storage: support custom endpoints via `endpoint` and `endpoint_suffix` configuration options for sovereign clouds (#6624)
- Storage: an S3 request now gives up after **5 minutes** (`storage.s3.read_timeout` moves that bound, `0s`
  waits forever as before). The AWS SDK sets no read timeout, so a connection that went away without a reset —
  a NAT or a proxy that forgets the flow — held the request forever: a metastore call that never returns hangs
  the publisher or the GC instead of failing so that the retry and the replay can run. Measured: a request with
  no timeout sat in `mio::poll` for ten minutes with its connection `ESTABLISHED` and no bytes moving, while the
  endpoint answered a fresh connection in 0.22 s. The bound covers the request **up to its response headers,
  sending the body included** — measured against the pinned SDK, a healthy 32 MiB upload that takes 22 s fails
  with a 2 s bound — which is why the default is generous: it keeps the default 256 MiB split inside the bound
  down to roughly 7 Mbps. Widen it, or set `0s`, for a slower link. (walgit: `qw-s3-request-timeout`)
- Storage: conditional writes (`put_if_absent`, `put_if_version_matches`, `get_all_with_version`) with a
  `PreconditionFailed` error kind, and a Cloudflare R2 storage flavor (`storage.s3.flavor: r2`, alias
  `cloudflare`) that sets `region: auto`, path-style access and `Content-MD5` checksums. Verified against the
  real R2 endpoint: it enforces `If-None-Match`/`If-Match` and accepts both the `Content-MD5` and the default
  `crc32c` upload checksums. (walgit: `qw-dist-metastore-s3-r2`, `qw-r2-flavor-and-storage-polish`, `qw-real-r2-verification`)
  Note for implementors of `Storage`: the three methods have default implementations, so an existing
  implementation keeps compiling, but `ObjectMetadata` gains a public field and `StorageErrorKind` two
  variants, which breaks exhaustive `match`es and struct literals outside this repository.
- Metastore: a shared (S3-compatible) file-backed metastore now reports
  `quickwit_metastore_file_backed_cas_conflicts_total` (writes that lost a compare-and-swap race; the ones
  that ran out of the replay budget are counted again by the next counter) and `..._exhausted_total`
  (mutations that failed after the replay budget ran out), so contention and the
  mutations that ran out of budget are visible from Prometheus. A third counter, `..._replay_tolerated_splits_total`, counts the
  split state changes a replayed publish found already applied, so the tolerance granted by `is_replay` is
  visible instead of silent. (walgit: `qw-metastore-cas-observability`, `qw-metastore-replay-visibility`)
- Monitoring: the metastore Grafana dashboard now panels those counters — compare-and-swap conflicts, the
  mutations that failed after the replay budget, replayed publishes, manifest and shard folds with their failures, shard
  objects a reader skipped, and the manifest reads a listing pays for adoption — so a shared object-storage
  metastore's contention, and the mutations that ran out of the replay budget, are visible where an
  operator looks, not only in Prometheus.
  The same dashboard's three request panels and its instance variable had kept the pre-0.9 gRPC metric names,
  which no longer exist, so they showed nothing: they now query `quickwit_grpc_requests_total` and
  `quickwit_grpc_request_duration_seconds` with `service="metastore"`, the names the upgrade notes give.
  (walgit: `qw-metastore-dashboard-counters`)
- **Metastore: a sharded layout for indexes too large to rewrite on every publish.**
  `QW_METASTORE_SHARDED_LAYOUT=true` makes a node create indexes whose split metadata lives in one slot file
  per slot (`v2/splits/slots/00042.json`), with a view that names, per slot, the segment and the slot version
  folded into it. A publish rewrites only the slots it touched instead of the whole split map, and a reader
  fetches the view and the slot files whose version changed; segments are folded per slot against the view's
  generation, and two generations are kept so a reader holding an older view still resolves. The layout is
  recorded in the objects, so a node reads an index whichever layout created it, and the slot count is fixed
  when the index is created. Measured with the same workload at 180 splits — 64 slots and a fold threshold of
  8, before a fold has to rewrite a large segment, not the 256/512 defaults — a publish rewrites 1.6 KB → 5.6 KB
  against 6.5 KB → 123.9 KB for the single-object layout. A mutation commits the slots it touched **before**
  its index metadata, which is why a failure between the two costs duplicated documents rather than lost ones
  (see the commit-order entry under Fixed). (walgit: `qw-metastore-sharded-layout`,
  `qw-metastore-sharded-split-store`, `qw-sharded-partial-commit`, `qw-sharded-fold-on-s3`)
- **Metastore: a third layout for very large indexes, where the manifest holds references instead of the
  split map.** `QW_METASTORE_MANIFEST_LAYOUT=true` makes a node create indexes with one manifest per stripe
  (`v3/manifest-<stripe>.json`), an immutable WAL object per published batch and one segment per time bucket,
  so a read fetches the query's time window rather than the whole index and a publish rewrites only the splits
  it changes. The layout is recorded in the objects, so a node reads an index whichever layout created it, and
  the stripe count is sized from the writer count (`QW_METASTORE_MANIFEST_STRIPES`, default 32). Measured on a
  real R2 bucket: a publish writes 1 380 bytes, and 12 writers reach 3.8 publishes/s with 0.03 conflicts per
  publish on 32 stripes (1.4/s and 0.68 on eight). Segments and WAL objects are collected per stripe against
  that stripe's own fold generation, and a publish replayed after a partial commit finishes; one revision at a
  time has to serve a prefix in this layout, as for the shared metastore as a whole.
  (walgit: `qw-metastore-manifest-layout`, `qw-metastore-manifest-r2-cost`, `qw-metastore-manifest-fold-removal`,
  `qw-metastore-manifest-publish-replay`, `qw-metastore-manifest-gc-collection`)

### Changed
- **An S3-compatible file-backed metastore can be shared by several nodes.** Metadata writes reload the file
  together with its version and write it back with `If-Match`; a lost race is replayed within a bounded budget
  (16 attempts, the delay doubling from 10 ms to a 2 s cap) instead of overwriting the
  winner. The metastore probes the endpoint for conditional-write support at startup and refuses to run in
  shared mode when the endpoint would silently ignore the preconditions
  (`QW_METASTORE_ALLOW_UNSAFE_STORAGE=true` opts into single-writer mode on such an endpoint). `file://`,
  `gs://` and `azure://` metastores keep the single-writer behaviour, and the startup probe that decides
  between the two modes is covered by the same thread. (walgit: `qw-dist-metastore-s3-r2`,
  `qw-metastore-distributed-mode`)
  The escape hatch covers only what the probe can prove — an endpoint that accepts a conditional write it
  should have rejected. A storage that does not implement conditional writes, or a probe that could not run,
  still stops the node instead of silently starting in single-writer mode.
- Documentation: how to upgrade and roll back a cluster that shares an object-storage metastore (including the
  lost-update hazard of mixing versions), and what a metadata write costs in requests and latency.
  (walgit: `qw-metastore-rollback-drill`, `qw-metastore-perf-bench`)
- Documentation: for an index at the scale where the whole-index layouts stop fitting, the choice between
  the object-storage metastore and PostgreSQL is now written as a locality decision — object storage in the
  manifest layout when the nodes sit next to the bucket, PostgreSQL (or move the nodes) when they do not —
  with the round-trip arithmetic and the measured numbers behind it. (walgit: `qw-metastore-backend-guidance`)

### Fixed
- Metastore: a publish that the pipeline replays because its **first attempt committed and the response was
  lost** now finishes. The replay carries the checkpoint delta the first attempt applied; that delta is not
  incompatible, it is done, so the replay skips it instead of failing on it. Skipping it is only sound for
  the publish that is the replay's own, and the checkpoint cannot say whose delta moved it, so a replay
  proves it first: the splits it publishes are published already (only the caller's earlier attempt can have
  published them — an empty set proves nothing). On the ingest-v2 shard API the request must also carry the
  shard's publish token, which says the caller still holds the shard; it is not ownership of the delta by
  itself, because a shard that changed hands has a new token. A fresh request that re-sends an applied delta,
  a competing writer whose delta lands on the same position, and a writer that took the shard over and
  replays an overlapping delta all keep getting the precondition failure.
  (walgit: `qw-replay-tolerates-applied-delta`)
- Metastore: the sharded layout now commits the slots it touches **before** `root.json`, the object that
  carries the checkpoint. A slot commit that fails therefore leaves the checkpoint where it was — the
  slots that committed before the failure stay published, a mutation touches several of them — and the
  metastore's own retry finishes the mutation; the other order used to leave the
  checkpoint ahead of a split that was never published, so a single transient slot failure stranded that
  split's documents — nothing re-read the window, because the checkpoint said it had been read. A failure
  after the slots now leaves the splits published and the checkpoint behind them, which a replay finishes
  and which costs duplicated documents rather than missing ones.
  (walgit: `qw-sharded-partial-commit`)
- Metastore: a publish that the indexing pipeline replays — because an attempt's response was lost
  after it committed, or because the manifest layout's own replay stopped partway — can now finish.
  `PublishSplitsRequest` carries `is_replay`, set by the pipeline from its second attempt, and the
  metastore applies the steps the earlier attempt already applied instead of failing on them
  (a first attempt that publishes a published split still gets the precondition failure). On a
  five-node index the pipeline no longer faults and restarts on such a replay.
  (walgit: `qw-metastore-manifest-root-contention`)
- Metastore: a create that is replayed after losing a manifest race no longer answers `already exists` for the
  index file it wrote itself. Two nodes creating indexes at the same time used to hit that in about 5% of the
  creations, failing an operation that had actually succeeded (real R2: 80/80 creations after the fix).
  (walgit: `qw-metastore-create-replay-fix`)
- Metastore: a listing no longer fails because another node is in the middle of creating or deleting one of the
  indexes it is listing. An index in that state (which the listing's own snapshot already excludes) used to come
  back as an internal error and take the whole call with it — the metastore's `ListIndexStats` listing, and the
  split listing the compaction planner and the janitor run over every index. A listing that names no index now
  skips it, the way it already skips an index that is not there; a read that *does* name an index still reports
  the state, so a caller cannot mistake an unreadable index for an empty one. The same state's error also stopped
  printing `{index_id}` literally, and the metadata listing now recognises it at all.
  (walgit: `qw-transient-state-error-text`, `qw-listing-skips-transitioning`, `qw-splits-listing-skips-transitioning`)
- Metastore: the compare-and-swap replay budget went from 8 attempts with a 500 ms cap to 16 attempts capped at
  2 s; two nodes publishing into one index exhausted the old budget 11 times in two minutes.
  (walgit: `qw-metastore-retry-budget`)
- Indexing: publishing a split that lost a compare-and-swap, and staging one that hit a transient storage
  error, now retry instead of faulting the pipeline. Three nodes publishing into one index on a cross-region
  bucket finish a two-minute run with zero actor faults and no lost documents.
  (walgit: `qw-indexing-publish-conflict-retry`, `qw-indexing-stage-retry`)
- Indexing: a pipeline that keeps failing while the metastore or the storage is unreachable now backs off
  exponentially instead of restarting every second. (walgit: `qw-indexing-restart-backoff`)
- (Jaeger) Query resource attributes when Jaeger request carries tags



# [0.9.0]

### Breaking / Migration
- **Ingest V2 is now the default ingest path.** The `/api/v1/{index}/ingest` endpoint transparently routes to V2.
- The `rest_listen_port` top-level config field is deprecated; use `rest.listen_port` under the new `rest` block. The old field still works but emits a warning.
- Quickwit metrics now use `metrics-rs`: gRPC metric names use a `service` label instead of embedding the service in the metric name, and janitor GC metric names no longer carry the duplicated `quickwit_` prefix (#6374).
- Stemming is now restricted to the `multilang` cargo feature (#6085); the previously bundled generic stemmer is no longer available by default.
- The unused multilang tokenizer feature was removed (#6154).
- Metastore format: new `maturity` field and compaction columns on splits; PostgreSQL metastore migrates automatically on first start.
- Building from source now requires Rust 1.92 (#6432, #6545).

### Added
- Add Ingest V2 — now the default (#5600, #5566, #5463, #5375, #5350, #5252, #5202, #6078, #6185, #6203, #6207, #6217, #6249)
- Offload leaf-search work to AWS Lambda functions — searchers can farm out part of their workload to Lambda (#6157, #5c1e60f)
- Add experimental DataFusion query layer for Parquet metrics with SQL/Substrait execution, Arrow IPC streaming, and distributed workers (#6276)
- Extend the Parquet metrics pipeline with DDSketch, sorted-series and row-key metadata, zone-map pruning, partitioning, sketch split pagination, and Substrait execution metadata (#6248, #6257, #6290, #6292, #6295, #6340, #6347, #6348, #6349, #6363, #6364, #6368)
- Add column-major and streaming Parquet merge primitives, merge scheduler wiring, and a rollout flag for routing regular merges through the streaming engine (#6335, #6362, #6367, #6377, #6384, #6386, #6406, #6407, #6408, #6409, #6423, #6424, #6425, #6426, #6428, #6441)
- Add optional mTLS validation to the REST API and an optional dedicated health-check HTTP server for deployments that put the REST API behind mTLS (#6467, #6528)
- Add SQS source (#5374, #5335, #5148)
- Add Jaeger v2 support (#6023)
- Extract and propagate W3C `traceparent` header on incoming HTTP requests (#6224)
- Add distributed tracing to the gRPC stack (#6403)
- Support configurable OTLP exporter protocol for traces and logs (#6254)
- Export internal logs via OTLP exporter (#6142)
- Add `QW_LOG_FORMAT=DDG` JSON log formatter (#6215)
- Add ES-compatible endpoints for Trino connector support (#6168)
- Elasticsearch DSL: prefix and wildcard queries (#6000), `case_insensitive` parameter on supported queries (#6005), regexp shorthand, concatenate-fields exposure, and `text` → `keyword` mapping in `_mapping` (#6208), `index_filter` on field capabilities API (#6102), `ignore_unavailable` query parameter (#5971)
- Make Elasticsearch `TermsQuery` use `TermSetQuery` internally for better performance (#6151)
- Add composite aggregation (#6214) and aggregations alias (#6314)
- Add `skip_aggregation_finalization` to `SearchRequest` (#6145)
- Add `list_index_stats` endpoint (#6035)
- Add `validate_docs` ingest setting (#5984); validate doc-mapping updates (#5988)
- Support updating the doc mapper through the API (#5253)
- Add CORS debug mode (#5955)
- Add config to fail search when it targets too many splits (#6009)
- Differentiate leaf- and root-level search timeouts (#6255)
- Predicate cache in leaf search (#6024); skip CPU work when it cannot improve the result (#6001); propagate cancellation within leaf search (#6002)
- Expose search resource stats, CPU thread count, root phase wall times, and warmup memory currently in flight (#6416, #6514, #6533)
- Set `searcher.warmup_single_split_initial_allocation` default to 300 MB (#c34966c6)
- Rebalance shards when ingester status changes (#6185) and improved rebalance algorithm (#6018)
- Add object storage metrics for GCS (#5889)
- Add IO metrics and track bytes written to the WAL (#6429)
- Add configurable system prefix and separator for metrics (#6445)
- Expose per-shard load configuration and disable the Tokio LIFO slot by default (#5898, #5899)
- Redact sensitive information in developer API debug output (#6191)
- Disable control plane check for searcher (#5599, #5360)
- Partially implement `_elastic/_cluster/health` (#5595)
- Make Jaeger span attribute-to-tag conversion exhaustive (#5574)
- Use `content_length_limit` for ES bulk limit (#5573)
- Limit and monitor warmup memory usage (#5568)
- Add eviction metrics to caches (#5523)
- Record object storage request latencies (#5521)
- Throttle the janitor to prevent overloading the metastore (#5510)
- Prevent single split searches from different `leaf_search` from interleaving (#5509)
- Retry on S3 internal error (#5504); make more S3 errors retryable (#5384)
- Allow specifying OTEL index ID in header (#5503)
- Add a metric to count storage errors and their error code (#5497)
- Add support for concatenated fields (#4773, #5369, #5331)
- Add number of splits per root/leaf search histograms (#5472)
- Introduce a searcher config option to timeout get requests (#5467)
- Add fingerprint to task in cluster state (#5464)
- Enrich root/leaf search spans with number of docs and splits (#5450)
- Add some additional search metrics (#5447)
- Improve GC resilience and add metrics (#5420)
- Enable force shutdown with 2nd Ctrl+C (#5414)
- Add `request_timeout_secs` to searcher config (#5402)
- Memoize S3 client (#5377)
- Add more env var config for Postgres (#5365)
- Enable str fast field range queries (#5324)
- Allow querying non-existing fields (#5308)
- Add optional special handling for hex in code tokenizer (#5200)
- Add `UnicodeSegmenter` tokenizer and case-insensitive regex support for lowercasing tokenizers (#6277, #6454)
- Added a circuit breaker layer (#5134)
- Follow AWS hints for Lambda retries (#6195)
- Improve cluster sizing documentation for control plane, metastore and janitor (#6202)
- Various performance optimizations in Tantivy (https://github.com/quickwit-oss/tantivy/blob/main/CHANGELOG.md)

### Changed
- Parse datetimes and timestamps with leading and/or trailing whitespace (#5544)
- Restrict maturity period to retention (#5543)
- Wait for merge at end of local ingest (#5542)
- Log PostgreSQL metastore error (#5530)
- Update azure multipart policy (#5553)
- Stop relying on our own version of pulsar-rs (#5487)
- Handle nested OTLP values in attributes and log bodies (#5485)
- Improve merge pipeline finalization (#5475)
- Allow failed splits in root search (#5440)
- Batch delete from GC (#5404, #5380)
- Change default timestamps in OTEL logs (#5366)
- Only return root spans for Jaeger HTTP API (#5358)
- Share aggregation limit on node (#5357)
- Make regex lenient to start and end anchors (#6089) — *later reverted, see Fixed*
- Use correct precision level for fastfield-based term queries on datetime (#6027)
- Disable the Tokio LIFO slot and tune per-shard default load (#5898)
- Improve control-plane logging (#6003)
- Migrate Quickwit metrics to `metrics-rs` and move exporter setup into `quickwit-telemetry-exporters` (#6374)
- Move OTLP metrics export to `metrics-opentelemetry`, remove the temporality override, and add default retry policy to OTLP exporters (#6499, #6510, #6522, #6535)
- Use CRC32C instead of MD5 for object-storage checksums, and document the `disable_checksums` storage config parameter (#6325, #6442)
- Make `Storage::get_slice` zero-copy for single-segment bodies (#6336)
- Add Protobuf WAL mrecord serialization support for forward/backward-compatible schema evolution; legacy WAL writes remain supported (#6521)
- Improve search scheduling by using current node load, prioritizing result merging over split searches, and aborting split warmup when a required term is missing (#6390, #6511, #6512)
- Update Lambda release artifacts and automate Lambda binary builds in the release workflow (#6330, #6456, #6457, #6458)
- Upgraded Tantivy to 1e859fd with multiple search, aggregation, and tokenizer improvements (see Tantivy CHANGELOG) (#6327, #6360, #6365, #6380, #6444, #6495, #6516, #6532)

### Fixed
- Fix timestamp field bound precision and add range checks to floating-point timestamp parsing (#6201, #6525)
- Fix existence queries for nested fields (#5581)
- Fix lenient option with wildcard queries (#5575)
- Fix incompatible ES Java date format (#5462)
- Fix bulk api response order (#5434)
- Fix pulsar finalize (#5471)
- Fix pulsar URI scheme (#5470)
- Fix grafana searchers dashboard (#5455)
- Fix jaeger http endpoint (#5378)
- Fix file re-ingestion after EOF (#5330)
- Fix configuration interpolation (#5403)
- Fix jaeger duration parse error (#5518)
- Fix unit conversion in jaeger http search endpoint (#5519)
- Fix Kinesis source panic on resharding (#5912)
- Fix Azure multipart upload data corruption (#5919)
- Fix leaf list fields merging logic (#5908)
- Fix empty intermediate aggregation results merge (#5930)
- Fix error when running a scoring query with cache (#6025)
- Fix `f64` not working properly with concatenated fields (#6074)
- Fix wrong range result when datetime was inferred in JSON type (#6048)
- Fix infinite-loop OOM bug caused by rebalancing shards from unavailable ingesters (#6078)
- Fix index reincarnation routing bug (#6217)
- Fix bug when deploying a new routing table (#6249)
- Fix retiring/decommissioned indexer handling, shard ownership deletion, empty-shard indexing checks, and material indexer pool updates (#6427, #6504, #6509, #6518, #6553)
- Fix control-plane scheduling idempotency and unavailable-node reporting (#6503, #6524, #6551)
- Fix descending sorted-series encoding, null ordering in sorted-series keys, and maturity assignment on Parquet splits (#6338, #6343, #6399)
- Fix Parquet merge adapter and consumer edge cases around sorted input, sub-regions, and stronger verifiers (#6426, #6428)
- Fix OpenDAL GCS TLS regression, GCS put memory growth, single-part MD5 headers, S3 stalled-stream retryability, S3 dispatch retries, and localstack checksum compatibility (#6478, #6482, #6485, #6492, #6501, #6502, #6539, #6541)
- Fix Azure delete metrics for the Azure blob backend (#6469)
- Fix Lambda retry log storms and empty `LAMBDA_ZIP_PATH` handling (#6318, #6462)
- Fix gRPC telemetry exporters (#6558)
- Truncate split lists in error logs to keep large routing errors readable (#6315)
- Fix Chitchat gossip bug triggering excess gRPC traffic and bump chitchat (#6082, #6323)
- Revert over-eager anchor lenience in regex queries (#6089)
- `skip_aggregation_finalization` fixes for composite aggregations
- Jaeger: query resource attributes when the Jaeger request carries tags

### Removed
- Remove support for 2-digit years in Java datetime parser (#5596)
- Remove `DocMapper` trait (#5508)
- Remove standalone AWS Lambda deployment mode — the previous dedicated search/indexing Lambda binaries are gone (#5884). *AWS Lambda is still supported for search offloading; see the Added section.*
- Remove search stream endpoint (#5886)
- Remove mentions of the stream API from the docs (#5958)
- Remove the legacy telemetry crate and native-tls from transitive dependencies (#6431, #6537)
- Remove the legacy `/api/v1/{index}/ingest-v2` REST endpoint (V2 is now served from `/ingest` — see Breaking / Migration)
- Remove the unused multilang tokenizer feature (#6154)
- Restrict stemming to the `multilang` feature (#6085)

### Security
- Upgrade dependencies including `rustls-webpki` for RUSTSEC-2026-0104, OpenSSL, Python requests, and UI packages with known vulnerabilities (#6219, #6307, #6341, #6342, #6387)

# [0.8.1]

### Fixed

- Bug in the chitchat digest message serialization (chitchat#144)

## [0.8.0]

### Added

- Remove some noisy logs (#4447)
- Add `/{index}/_stats` and `/_stats` ES API (#4442)
- Use `search_after` in ES scroll API (#4280)
- Add support for wildcard exclusion in index patterns (#4458)
- Add `.` support in DSL indentifiers (#3989)
- Add cat indices ES API (#4465)
- Limit concurrent merges (#4473)
- Add Index Template API and auto create index (#4456) (only available with ingest V2)
- Add support for compressed ES `_bulk` requests (#4506)
- Add support for slash `/` character in field names (#4510)
- Handle SIGTERM shutdown signal (#4539)
- Add `start_timestamp` and `end_timestamp` filter to ES `_field_caps` API (#4547)
- Limit the number of merge pipelines that can be spawned concurrently (#4574)
- Add support for `_source_excludes` and `_source_includes` query parameters in ES API (#4572)
- Add gRPC metrics layer to clients and servers (#4591)
- Add additional cluster metrics (#4597)
- Add index patterns query param on GET `/indexes` endpoint (#4600)
- Add support for GCS file backed metastore (#4604)
- Add default search fields for OTEL traces index (#4602)
- Add support for delete index in ES API (#4606)
- Add a handler to dynamically change the log level (#4662)
- Add REST endpoint to parse a query into a query AST (#4652)
- Add postgresql index and use `IN` instead of many `OR` (#4670)
- Add support for `_source_excludes`, `_source_includes`, `extra_filters` in `_msearch` ES API (#4696)
- Handle `track_total_size` on request ES body (#4710)
- Add a metric for the number number of indexes (#4711)
- Add various performance optimizations in Quickwit and Tantivy

More details in tantivy's [changelog](https://github.com/quickwit-oss/tantivy/blob/main/CHANGELOG.md).

### Fixed

- Fix aggregation result on empty index (#4449)
- Fix Gzip file source (#4457)
- Rate limit noisy logs (#4483)
- Prevent the exponential backoff from overflowing after 64 attempts (#4501)
- Remove field presence in ES `_field_caps` API (#4492)
- Remove `source` in ES parameter, remove unsupported field `fields` in response (#4590)
- Fix aggregation `split_size` parameter, add docs and test (#4627)
- Various fixes in chitchat (gossip): more details in [chitchat commit history](https://github.com/quickwit-oss/chitchat/commits/main/?since=2024-01-08&until=2024-03-13)
- Various fixes in mrecordlog (WAL): more details in [mrecordlog commit history](https://github.com/quickwit-oss/mrecordlog/commits/main/?since=2024-01-08&until=2024-03-13)

### Changed

- (Breaking) [Add ZSTD compression to chitchat's Deltas](https://github.com/quickwit-oss/chitchat/pull/112)

### Removed

### Migration from 0.7.x to 0.8.0

To deploy Quickwit 0.8.0, you must either:
- **shutdown down** your cluster **entirely** before deploying, or
- **restart all** the nodes of your cluster after deploying.

Because we made some breaking changes in the gossip protocol (chitchat), nodes running different versions of Quickwit cannot communicate with each other and crash upon receiving messages that do not match their release version. The new protocol is now versioned, and future updates of the gossip protocol will be backward compatible.


## [0.7.1]

### Added

- Add es _count API (#4410)
- Add _elastic/_field_caps API (#4350)
- Make gRPC message size configurable (#4388)
- Add API endpoint to get some control-plan internal info (#4339)
- Add Google Cloud Storage Implementation available for storage paths starting with `gs://` (#4344)

### Changed

- Return 404 on index not found in ES Bulk API (#4425)
- Allow $ and @ characters in field names (#4413)

### Fixed
- Assign all sources/shards, even if this requires exceeding the indexer #4363
- Fix traces doc mapping (service name set as  fast) and update default otel logs index ID to `otel-logs-v0_7` (#4401)
- Fix parsing multi-line queries (#4409)
- Fix range query for optional fast field panics with Index out of bounds (#4362)

### Migration from 0.7.0 to 0.7.1

Quickwit 0.7.1 will create the new index `otel-logs-v0_7` which is now used by default when ingesting data with the OTEL gRPC and HTTP API.

In the traces index `otel-traces-v0_7`, the `service_name` field is now fast. No migration is done if `otel-traces-v0_7` already exists. If you want `service_name` field to be fast, you have to delete first the existing `otel-traces-v0_7` index or create your own index.

## [0.7.0]

### Added

- Elasticsearch-compatible API
  - Added scroll and search_after APIs and support for multi-index search queries
  - Added exists, multi-match, match phrase prefix, match bool prefix, bool queries
  - Added `_field_caps` API
- Added support for OTLP over HTTP API (Protobuf only) (#4335)
- Added Jaeger REST endpoints for Grafana tracing support (#4197)
- Added support for injecting custom HTTP headers and moved REST config parameters into REST config section (#4198)
- Added support for OTLP trace data in arbitrary sources
- Commit Kafka offsets on suggest truncate (#3638)
- Honor `auto.offset.reset` parameter in Kafka source (#4095)
- Added exact count optimization (#4019)
- Added stream splits gRPC (#4109)
- Adding a split cache in Searchers (#3857)
- Added `coerce` and `output_format` options for numeric fields (#3704)
- Added `PhraseMatchQuery` and `MultiMatchQuery` (#3727)
- Added Elasticsearch's `TermsQuery` (#3747)
- Added GCP PubSub source (#3720)
- Parse timestamp strings (#3639)
- Added Digital Ocean storage flavor (#3632)
- Added new tokenizers: `source_code_default`, `source_code`, `multilang` (#3647, #3655, #3608)


### Fixed

- Fixed dates in UI (#4277)
- Fixed duplicate splits planned on pipeline crash-respawn (#3854)
- Fixed sorting (#3799)

More details in tantivy's [changelog](https://github.com/quickwit-oss/tantivy/blob/main/CHANGELOG.md).

### Changed

- Improve OTEL traces index config (#4311)
  - OTEL endpoints are now using by default indexes `otel-logs-v0_7` and `otel-traces-v0_7` instead of `otel-logs-v0_6` and `otel-traces-v0_6`
  - OTEL indexes have more fields stored as "fast" and have Trace and Span ID bytes field in hex format

- Increased the gRPC payload limits from 10MiB to 20MiB (#4227)
- Reject malformed Elasticsearch API requests (#4175)
- Better logging when doc processing fails (#4323)
- Search performance improvements
- Indexing performance improvements

### Removed

### Migration from 0.6.x to 0.7

The format of the index and internal objects stored in the metastore of 0.7 is backward compatible with 0.6.

If you are using the OTEL indexes and ingesting data into indexes the `otel-logs-v0_6` and `otel-traces-v0_6`, you must stop indexing before upgrading.
Indeed, the first time you start Quickwit 0.7, it will update the doc mapping fields of Trace ID and Span ID of those two indexes by changing their input/output formats from base64 to hex. This is automatic: you don't have to perform any manual operation.
Quickwit 0.7 will create new indexes `otel-logs-v0_7` and `otel-traces-v0_7`, which are now used by default when ingesting data with the OTEL gRPC and HTTP API. The Jaeger gRPC and HTTP APIs will query both `otel-traces-v0_6` and `otel-traces-v0_7` by default.
It's possible to define the index ID you want to use for OTEL gRPC endpoints and Jaeger gRPC API by setting the request header `qw-otel-logs-index` or `qw-otel-traces-index` to the index ID you want to target.


## [0.6.1]

### Added
- Support of phrase prefix queries in the query language.

### Fixed
- Fix timestamp field which was not allowed when defined in an object mapping.
- Fix querying of integer on a JSON field (no document were returned).


## [0.6.0] - 2023-06-03

### Added
- Elasticsearch/Opensearch compatible API.
- New columnar format:
    - Fast fields can now have any cardinality (Optional, Multivalued, restricted). In fact cardinality is now only used to format the output.
    - Dynamic Fields are now fast fields.
- String fast fields now can be normalized.
- Various parameters of object storages can now be configured.
- The ingest API makes it possible to force a commit, or wait for a scheduled commit to occur.
- Ability to parse non-JSON data using VRL to extract some structure from documents.
- Object storage can now use the `virtual-hosted–style`.
- `date_histogram` aggregation.
- `percentiles` aggregation.
- Added support for Prefix Phrase query.
- Added support for range queries.
- The query language now supports different date formats.
- Added support for base16 input/output configuration for bytes field. You can search for bytes fields using base16 encoded values.
- Autotagging: fields used in the partition key are automatically added to tags.
- Added arm64 docker image.
- Added CORS configuration for the REST API.


### Fixed
- Major bug fix that required to restart quickwit when deleting and recreating an index with the same name.
- The number of concurrent GET requests to object stores is now limited. This fixes a bug observed with when requested a lot of documents from MinIO.
- Quickwit now searches into resource attributes when receiving a Jaeger request carrying tags
- Object storage can be figured to:
    - avoid Bulk delete API (workaround for Google Cloud Storage).
    - Use virtual-host style addresses (workaround for Alibaba Object Storage Service).
- Fix aggregation min doc_count empty merge bug.
- Fix: Sort order for term aggregations.
- Switch to ms in histogram for date type (aligning with ES).

### Improvements

- Search performance improvement.
- Aggregation performance improvement.
- Aggregation memory improvement.

More details in tantivy's [changelog](https://github.com/quickwit-oss/tantivy/blob/main/CHANGELOG.md).

### Changed
- Datetime now have up to a nanosecond precision.
- By default, quickwit now uses the node's hostname as the default node ID.
- By default, Quickwit is in dynamic mode and all dynamic fields are marked as fast fields.
- JSON field uses by default the raw tokanizer and is set to fast field.
- Various performance/compression improvements.
- OTEL indexes Trace ID and Span ID are now bytes fields.
- OTEL indexes stores timestamps with nanosecond precision.
- pan status is now indexed in the OTEL trace index.
- Default and raw tokenizers filter tokesn longer than 255 bytes instead of 40 bytes.


## [0.5.0] - 2023-03-16

### Added
- gRPC OpenTelemetry Protocol support for traces
- gRPC OpenTelemetry Protocol support for logs
- Control plane (indexing tasks scheduling)
- Ingest API rate limiter
- Pulsar source
- VRL transform for data sources
- REST API enhanced to fully manage indexes, sources, and splits
- OpenAPI specification and swagger UI for all REST available endpoints
- Large responses from REST API can be compressed
- Add bulk stage splits method to metastore
- MacOS M1 binary
- Doc mapping field names starting with `_` are now valid

### Fixed
- Fix UI index completion on search page
- Fix CLI index describe command to show stats on published splits
- Fix REST API to always return on error a body formatted as `{"message": "error message"}`
- Fixed REST status code when deleting unexisting index, source and when fetching splits on unexisting index

### Changed
- Source config schema (breaking or not? use serde rename to be not breaking?)
- RocksDB replaced by [mrecordlog](https://github.com/quickwit-oss/mrecordlog) to store ingest API queues records
- (Breaking) Indexing partition key new DSL
- (Breaking) Helm chart updated with the new CLI
- (Breaking) CLI indexes, sources, and splits commands use the REST API
- (Breaking) Index new format: you need to reindex all your data

## [0.4.0] - 2022-12-03

### Added
- Boolean, datetime, and IP address fields
- Chinese tokenizer
- Distributed indexing (Kafka only)
- gRPC metastore server
- Index partitioning
- Kubernetes
- Node config templating
- Prometheus metrics
- Retention policies
- REST API for CRUD operations on indexes/sources
- Support for Azure Blob Storage
- Support for BM25 document scoring
- Support for deletions
- Support for slop in phrase queries
- Support for snippeting

### Fixed
- Fixed cache misses during search fetch docs phase
- Fixed credentials leak in metastore URI
- Fixed GC scalability issues
- Fixed support for multi-source

### Changed
- Changed default docstore block size to 1 MiB and compression algorithm to ZSTD

- Quickwit now relies on sqlx rather than Diesel for PostgreSQL interactions.
Migrating from 0.3 should work as expected. Migrating from earlier version however is
not supported.

### Removed
- Removed support for i64 as timestamp field
- Removed support for sorting index by field

### Security
- Forbid access to paths with `..` at storage level

## [0.3.1] - 2022-06-22

### Added
- Add support for Google Cloud Storage
- Sort hits by timestamp desc by default in search UI
- Add `description` attribute to field mappings
- Display split state in output of `quickwit split list` command

### Fixed
- Clean up local split cache after index deletion
- Fix API URLs displayed for copy and paste in UI
- Fix custom S3 endpoint with trailing `/`
- Fix `quickwit index create` command with `--overwrite` option

## [0.3.0] - 2022-05-31

### Added
- Embedded UI for displaying search hits and cluster state
- Schemaless indexing with JSON field
- Ingest API (Elasticsearch-compatible)
- Aggregation queries
- Support for Amazon Kinesis

### Fixed
- Switched cluster membership algorithm from S.W.I.M. to Chitchat

### Removed
- u64 as date field

## [0.2.1] - 2022-02-28

### Added
- Query validation against index schema before dispatch to leaf nodes (#1109, @linxGnu)
- Support for custom S3 endpoint (#1108)
- Warm up terms and fastfields concurrently (#1147)

### Fixed
- Minor bug in leaf search stream (#1110)
- Default index root URI and metastore URI correctly default to data dir (#1140, @ddelemeny)

### Removed
- QW_ENV environment variable

### Security
- Compiled binaries with Rust 1.58.1, which fixes CVE-2022-21658

## [0.2.0] - 2022-01-12

## [0.1.0] - 2021-07-13
