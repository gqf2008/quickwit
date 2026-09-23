---
title: Version upgrade
sidebar_position: 4
---

## Migration from 0.6.x to 0.7.0

The format of the index and internal objects stored in the metastore of 0.7 is backward compatible with 0.6.

If you are using the OTEL indexes and ingesting data into indexes the `otel-logs-v0_6` and `otel-traces-v0_6`, you must stop indexing before upgrading. Indeed, the first time you start Quickwit 0.7, it will update the doc mapping fields of Trace ID and Span ID of those two indexes by changing their input/output formats from `base64` to `hex`. This is automatic: you don't have to perform any manual operation.

Quickwit 0.7 will also create the new index `otel-traces-v0_7`, which is now used by default when ingesting data with the OTEL gRPC and HTTP API. The Jaeger gRPC and HTTP APIs will query both `otel-traces-v0_6` and `otel-traces-v0_7` by default. It's possible to define the index ID you want to use for OTEL gRPC endpoints and Jaeger gRPC API by setting the request header `qw-otel-logs-index` or `qw-otel-traces-index` to the index ID you want to target.


## Migration from 0.7.0 to 0.7.1

Quickwit 0.7.1 will create the new index `otel-logs-v0_7` which is now used by default when ingesting data with the OTEL gRPC and HTTP API.

In the traces index `otel-traces-v0_7`, the `service_name` field is now `fast`. 
No migration is done if `otel-traces-v0_7` already exists. If you want `service_name` field to be `fast`, you have to delete first the existing `otel-traces-v0_7` index or you need to create your own index.

## Migration from 0.8 to 0.9

Quickwit 0.9 introduces a new ingestion service to power the ingest and bulk APIs (v2). The new ingest is enabled and used by default, even though the legacy one (v1) remains enabled to finish indexing residual data in the legacy write ahead logs. Note that `ingest_api.max_queue_disk_usage` is enforced on both ingest versions separately, which means that the cumulated disk usage might be up to twice this limit.

When upgrading to 0.9, we recommend to perform a full cluster restart.

<!--
Reasons:
- Ingested data into previously existing indexes on upgraded indexer nodes will not be picked by the indexing pipelines until the control plane is upgraded.
- The indexing plan is computed differently in 0.9, all pipelines will be restarted when upgrading the control plane.
- If you intend to enable compression for the ingest service (`ingest_api.grpc_compression_algorithm`), you must do so in two steps: first, upgrade the indexer nodes with compression disabled, then update the node configuration to enable compression, and finally restart the indexer nodes.
- Obscure bug raised in https://github.com/quickwit-oss/quickwit/issues/5787#issuecomment-2979470315
-->

Shutdown order:
1) indexers, searchers and janitor
2) control plane
3) metastores

Start up order:
1) metastores
2) control plane
3) indexers, searchers and janitor

## Shared object-storage metastore: upgrade and rollback

Several nodes can share one S3-compatible metastore prefix. Metadata writes then reload the file
together with its version and write it back with `If-Match`, replaying the mutation when another node
wrote first (see [Distributed deployments](../configuration/metastore-config.md#distributed-deployments)).

Versions that predate this behaviour keep the whole metastore state in memory and overwrite the
manifest with a plain `PUT`. **A prefix must therefore never be written by a pre-CAS binary and a CAS
binary at the same time**: the pre-CAS node's write silently drops everything the CAS node committed
in the meantime. This is not a theoretical risk, it was reproduced: a CAS node created an index
(`200 OK`) while a pre-CAS node was running on the same prefix, and the pre-CAS node's later write
left the manifest without that index — the index directory was still in the bucket, but the index had
vanished from the metastore.

### Upgrading

The metadata format itself did not change: both versions read and write the same
`manifest.json` / `[index_id]/metastore.json`. A pre-CAS node started on a prefix written by a CAS
node lists the same indexes and returns the same search hits, and documents published by it are later
read back by the CAS node.

Upgrade every node that writes to the metastore in the same maintenance window, or upgrade the nodes
one by one while making sure **at most one** of them writes at a time (for example, leave the other
nodes stopped). Rolling upgrades with mixed writers are safe only once every writer runs the CAS
code. Indexers are writers, and so is the janitor, which rewrites the index metadata when it
garbage-collects splits (and the manifest when it deletes indexes); a searcher-only node does not
write and can keep running throughout.

### Rolling back

1. Stop all nodes that write to the metastore prefix (see the shutdown order above).
2. Start the older binary, alone, on the same prefix. It reads the data written by the CAS version.
3. Keep that prefix single-writer for good. A pre-CAS binary has no locking either, so two of them
   sharing a prefix overwrite each other's manifest, exactly like the mixed case above.

Two things to watch out for:

- an older binary **cannot parse** a configuration using `storage.s3.flavor: r2` (the value did not
  exist yet), so remove that field before starting it;
- an older binary keeps the manifest it read at startup in memory: as soon as a CAS node commits
  anything after that read, the older node's next write drops it (the lost-update case above).

### Disabling the feature

Nothing in the metastore format forces CAS, so the feature can be turned off by moving the metastore:
point `metastore_uri` at a `file://` path, or at a URI type that does not take the shared write path
(`gs://`, `azure://`), and restart the nodes. Those metastores, and the S3 prefix you rolled back
from, stay single-writer: only one node may write the prefix. A node can also be started on a storage
that ignores conditional writes by setting `QW_METASTORE_ALLOW_UNSAFE_STORAGE=true` and running it as
the only writer; on an endpoint that does enforce conditional writes the variable has no effect.
