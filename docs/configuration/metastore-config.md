---
title: Metastore configuration
sidebar_position: 4
---

Quickwit needs a place to store meta-information about its indexes.

For instance:

- The index configuration.
- Meta-information about its splits. For instance, their IDs, the number of documents they contain, their sizes, their min/max timestamp, and the set of tags present in the split.
- The different sources checkpoints.
- Some extra information such as the index creation time.

The metastore is entirely defined by a single URI. One can set it by editing the `metastore_uri` parameter of the [node configuration file](./node-config.md) (often named `quickwit.yaml`).

Currently, Quickwit offers two implementations:

- **PostgreSQL**: recommended for distributed usage.
- **File-backed implementation**.

# PostgreSQL Metastore

We recommend the PostgreSQL metastore for any distributed usage.

The PostgreSQL metastore can be configured by setting a PostgreSQL URI in the `metastore_uri` parameter of the Quickwit configuration file. The URI takes the following format:

```
postgres://[user]:[password]@[host]:[port]/[dbname]
```

Some of those parameters can be omitted. The following PostgreSQL URIs are for instance valid:

```
postgres://localhost/mydb
postgres://user@localhost
postgres://user:secret@localhost
```

The database has to be created in advance.

On its first execution, Quickwit will transparently create the necessary tables.

Likewise, if you upgrade Quickwit to a version that includes some changes in the PostgreSQL schema, Quickwit will transparently operate the migration startup.

## PostgreSQL connection pool

Each Quickwit node running the `metastore` service maintains its own PostgreSQL connection pool. Database-backed metastore nodes admit at most `2 * metastore.postgres.max_connections` in-flight requests, so the default is `20`.

When sizing the pool, keep `metastore_nodes * metastore.postgres.max_connections` below the PostgreSQL connection limit with operational headroom. If Quickwit logs `pool timed out while waiting for an open connection`, check `quickwit_metastore_active_connections`, `quickwit_metastore_idle_connections`, and `quickwit_metastore_acquire_connections`.

# File-backed metastore

For convenience, Quickwit also makes it possible to store its metadata in files using a file-backed metastore. In that case, Quickwit will write one file per index.

The metastore is then configured by passing a [storage URI](storage-config#storage-uris) that will serve as the root of the metastore storage.

The metadata file associated with a given index will then be stored under

  `[storage_uri]/[index_id]/metastore.json`

For the moment, Quickwit supports two types of storage types:

- a local file system URI (e.g., `file:///opt/toto`). It is also valid to pass a file path directly (without file://). `/var/quickwit`. Relative paths will be resolved with respect to the current working directory.
- S3-compatible storage URI (e.g., `s3://my-bucket/some-path`). See the [storage config](storage-config) documentation to configure S3 or S3-compatible storage providers.

### Distributed deployments

An object-storage metastore is shared: several nodes may write the same prefix at the same time.
Before every metadata write, a node reloads the file it is about to change and writes the result back
with a conditional write (`If-Match` on the version it read). If another node wrote first, the
operation reloads the file and replays its change instead of overwriting it, so two nodes publishing
splits concurrently both keep their work.

This is automatic for S3-compatible URIs (`s3://`); there is nothing to configure. It requires a
backend that supports conditional writes — Amazon S3 (since November 2024), Cloudflare R2 and MinIO
do.

See [Shared object-storage metastore](../operating/shared-metastore.md) for the requirements,
operations and measured behaviour, and [upgrade and rollback](../operating/upgrades.md#shared-object-storage-metastore-upgrade-and-rollback) before
upgrading or downgrading a cluster: a prefix must never be written by an older (pre-CAS) node and a
CAS node at the same time, because the older node overwrites whatever the CAS node committed.

Quickwit does not take that on faith: at startup it writes a throwaway object twice with
`If-None-Match` and checks that the second write is rejected. If the endpoint accepts it anyway
(localstack 3.5.0 does, which would make a shared prefix lose updates silently), the node refuses to
start. To run such an endpoint in **single-writer** mode, set `QW_METASTORE_ALLOW_UNSAFE_STORAGE=true`;
the node then logs a warning and behaves like a `file://` metastore. Do not share its prefix.

Two cases remain single-node:

- a `gs://` or `azure://` metastore: those backends do not implement conditional writes in Quickwit
  yet, so a prefix shared by several processes would lose updates;
- a local-file (`file://`) metastore: a local file has no version to compare against.

The metastore logs which mode it started in.

#### Monitoring a shared metastore

A shared metastore exposes two counters on the `/metrics` endpoint:

| Metric | Meaning |
| ------ | ------- |
| `quickwit_metastore_file_backed_cas_conflicts_total` | Metadata writes that lost a compare-and-swap race (`412 Precondition Failed`) and were replayed against the fresh file. A conflict is normal and harmless: it only says another node wrote first. |
| `quickwit_metastore_file_backed_cas_conflicts_exhausted_total` | Writes that failed after exhausting the bounded replay budget (16 attempts, doubling from 10 ms up to a 2 s cap — under 20 s in total). The mutation was **not** applied. |

Suggested alerts:

- page on any increase of `quickwit_metastore_file_backed_cas_conflicts_exhausted_total`: it means a
  write was dropped under contention, and the caller (`create_index`, `publish_splits`, a source
  update, ...) got an error;
- warn when conflicts exceed 10% of the metastore write rate over a 5-minute window
  (`rate(quickwit_metastore_file_backed_cas_conflicts_total[5m])` against the write rate).
  Occasional conflicts are expected, and a sustained high ratio only means the nodes contend heavily
  on the same index; it is not by itself a correctness problem.

Recovery requires no operator action: once the contention or the endpoint outage ends, the nodes
converge by replaying their mutations. This was exercised against a metastore endpoint taken down
and brought back while under load (a metastore proxy killing its active connections on every mode
switch). Search returned errors while the metastore was unreachable and ingest kept acknowledging
documents into its local queue; after reconnection every one of the 256 acknowledged documents was
searchable, with no ingest failure and no readiness failure.

While the endpoint is unreachable, an indexing pipeline that cannot reach it backs off exponentially
(1 second doubling up to 10 minutes, and the delay is reset once the pipeline has been healthy for a
minute), so a recovering metastore is not hammered by pipelines restarting every second.

#### Cost and latency of the shared write path

Compare-and-swap costs one extra read per metadata write: the node reloads the file together with its
version before rewriting it, where a single-writer node writes straight from memory. Requests counted
on a real node talking to Cloudflare R2, for one index created and then deleted — one sample each,
and the GET totals also include unrelated reads that happen in the same window, so read them as a
shape rather than as constants:

| Operation | Single writer | Shared |
| --------- | ------------- | ------ |
| create index | 3 PUT | 3 PUT + 1 GET |
| delete index | 2 PUT + 1 DELETE | 2 PUT + 1 DELETE + the read of the file being deleted |

Latency sampled on one machine far from the bucket (median round trip 0.81 s, so the network
dominates — read these as a shape, not as constants):

| Workload (median) | Single writer | Shared |
| ----------------- | ------------- | ------ |
| create index | 1.6 s | 2.1 s |
| delete index | 1.3 s | 1.9 s |
| ingest acknowledgement → searchable | 7.8 s | 9.0 s |
| acknowledged documents / s, 1 node | 61 | 60 |
| acknowledged documents / s, 2 nodes | — | 122 |

Ingest acknowledgements do not wait for the metastore: splits are published asynchronously after the
acknowledgement, so sustained ingest throughput is essentially unchanged in this sample (61 → 60
acknowledged documents per second on one node, 122 with two nodes) while publication latency grows by
about one round trip. Two nodes publishing concurrently do not slow each other down; when they rewrite
the same manifest at the same moment the loser replays its mutation, which costs another round trip:
15 contended creates had a median of 3.0 s against 1.8–2.1 s uncontended, with 9 replays counted by
`quickwit_metastore_file_backed_cas_conflicts_total`.

Object storage bills per request, so sharing a metastore means one extra GET per metadata write on
top of the requests a single-writer node already makes.

### Polling configuration

By default, the File-Backed Metastore is only read once when you start a Quickwit process (searcher, indexer, ...).

You can also configure it to poll the File-Backed Metastore periodically to keep a fresh view of it. This is useful for a Searcher instance that needs to be aware of new splits published by an Indexer running in parallel.

To configure the polling interval (in seconds), add a URI fragment to the storage URI as follows: `s3://quickwit/my-indexes#polling_interval=30s`

:::note
The polling interval can be configured in seconds only; other units, such as minutes or hours, are not supported.
:::

:::tip
Amazon S3 charges $0.0004 per 1000 GET requests. Polling a metastore every 30 seconds costs $0.04 per month and index.
:::

### Examples

The following file-backed metastore URIs for instance are valid:

```markdown
s3://my-indexes
s3://quickwit/my-indexes
s3://quickwit/my-indexes#polling_interval=30s
file:///local/indices
file:///local/indices#polling_interval=30s
/local/indices
./quickwit-metastores
```

:::caution
Multiple instances can share one **S3-compatible** metastore prefix safely, as described in
[Distributed deployments](#distributed-deployments). A `file://`, `gs://` or `azure://` metastore is
still limited to a single writer: keep only one file-backed metastore instance running at all times
for those URIs.
:::
