---
title: Shared object-storage metastore
sidebar_position: 5
---

Several Quickwit nodes can share one S3-compatible metastore prefix. This page collects what the
feature requires, how to run it, and what was measured on real endpoints. Configuration details live
in [Metastore configuration](../configuration/metastore-config.md); the upgrade and rollback
procedure lives in [Version upgrade](upgrades.md).

## How it works

Every metadata write reloads the file together with its version and writes it back with `If-Match`.
Losing the race is normal — another node wrote first — so the operation replays within a bounded
budget (16 attempts, delays doubling from 5 ms to a 2 s cap) instead of overwriting the winner.
Contention and dropped writes are exported as
`quickwit_metastore_file_backed_cas_conflicts_total` and `..._exhausted_total`.

## Requirements

- The endpoint must **enforce conditional writes** (`If-None-Match` / `If-Match`). Amazon S3 does
  (since November 2024), Cloudflare R2 and MinIO do. Localstack 3.5.0 does not.
- At startup the node writes a throwaway object twice with `If-None-Match` and refuses to start in
  shared mode if the second write is accepted, because such a prefix would silently lose updates.
  `QW_METASTORE_ALLOW_UNSAFE_STORAGE=true` opts into single-writer mode on those endpoints; never
  share a prefix in that mode.
- `file://`, `gs://` and `azure://` metastores remain single-writer.

## Configuration

```yaml
metastore_uri: s3://my-bucket/metastore           # sharing is automatic for s3://
default_index_root_uri: s3://my-bucket/indexes
storage:
  s3:
    flavor: r2                                    # Cloudflare R2: region auto, path style, Content-MD5
```

A `#polling_interval=30s` fragment on the URI makes a node re-read the metastore periodically, which
searchers need to notice indexes created by other nodes.

## Operating it

- **Alerts**: page on any increase of `..._cas_conflicts_exhausted_total` (a write was dropped);
  warn when conflicts exceed ~10% of the metastore write rate over five minutes.
- **Cost**: sharing costs one extra read per metadata write (create: 3 PUT vs 3 PUT + 1 GET; delete:
  2 PUT + 1 DELETE plus the read of the file being deleted). With the default commit settings a
  500M-document day is roughly 50–200 metadata writes and about 1 MB of metadata per 30 days of
  retention — requests, not volume, are the cost.
- **Outages**: while the endpoint is unreachable, ingest keeps acknowledging into its local queue and
  the indexing pipeline backs off instead of restarting every second; reads are served from the
  node's cached metadata. Recovery is automatic.
- **Upgrade/rollback**: never run a pre-CAS binary and a CAS binary on the same prefix at the same
  time — the older node overwrites what the newer one committed. See [Version upgrade](upgrades.md).

## Measured on real endpoints (2026-09)

| Scenario | Result |
| -------- | ------ |
| 3 nodes publishing into one index on R2, 2 minutes | 32,880 acknowledged = 32,880 searchable, zero actor faults |
| GC/retention load, 3 nodes, ~14 minutes | zero actor faults, delete tasks progressing on every node |
| Metastore outage, 5 minutes, ingest continuing | 600/600 acknowledged during the outage, all 640 documents searchable 1.5 s after recovery |
| Rollback drill with a pre-CAS binary | data readable both ways; mixed versions silently lose updates (documented) |
| Metadata write latency, PostgreSQL vs R2 | delete p50 26 ms vs 2.14 s; publish p50 6.1 s vs 7.0 s; ingest 52 vs 63 acked/s (1 node) |

## Known limits

- Cross-region latency dominates: one round trip to the bucket used for these measurements was
  0.81 s, so run nodes close to the bucket.
- The janitor's `DeleteTaskPlanner` tripped the actor progress watchdog once in three three-node
  runs and did not reproduce under a GC-heavy soak; it stays a low-severity item to watch (a stalled
  planner means garbage collection lags, not data loss).

## Very large indexes: sharded splits

By default an index keeps **every split in one object**, and every publish rewrites that object. A
500M-document day is a few hundred splits and a metadata file of a few hundred kilobytes: fine. The
layout stops working when the arithmetic changes by orders of magnitude — 5·10¹² documents/day is
500 000 splits/day at the default split size, which at 30-day retention is ~15 M splits and ~12 GB
of metadata, rewritten in full by every publish, by every writer.

`QW_METASTORE_SHARDED_LAYOUT=true` makes a node create indexes in a sharded layout, under
`<index_id>/v2/`:

```
root.json                            # everything except the splits, its own compare-and-swap
splits/view.json                     # per-slot bookmark: folded sequence + segment + version
splits/slots/<slot>.json             # entries written since that slot was last folded
splits/segments/<slot>/<gen>-<id>.json  # folded snapshot of one slot
```

Splits are spread over 256 slots by `hash(split_id) % 256`. A publish rewrites only the slots it
touched, and only the entries those slots accumulated since their last fold (512 entries by
default), so the bytes a publish writes stop following the size of the index. Writers racing on
different slots both win; racing on the same slot is settled by a compare-and-swap on that slot file
alone.

The layout is opt-in and per index. A node reads whichever layout an index was created with — it
tries `<index_id>/metastore.json` first and only looks for a sharded root when that is missing — so
mixed fleets keep working and an index can be migrated by creating it again in the new layout.
It needs conditional writes; a node refuses to start with this layout on a storage that has none.

| Layout | Bytes rewritten by a publish | Measured |
| ------ | ---------------------------- | -------- |
| single object | all splits of the index | 6.5 KB → 123.9 KB while an index grows from 3 to 180 splits |
| sharded | the entries of the touched slots since their last fold | 1.6 KB → 5.6 KB over the same growth |

The single-object figure grows with the index (it *is* the index); the sharded one is bounded by the
fold threshold. Those two rows were measured with 64 slots and a fold threshold of 8, so that the
sawtooth fits in a unit test rather than with the 256/512 defaults. Extrapolated to the 12 GB index
above, the single-object layout writes 12 GB per publish against ~0.1–0.5 MB for the sharded one, and
the gap keeps widening with retention. The same measurement is asserted in `quickwit-metastore`'s
`sharded_layout::tests::test_sharded_writes_do_not_follow_the_index_size`, and the layout is also
covered on a real R2 bucket by `tests/s3_shared_metastore.rs`.

What has *not* changed: a reader still materialises the whole split map, so the read side of a
15 M-split index is as heavy as it was (one list request plus the segments and the changed slots,
but the segments are still large). Sizing the layout for a very large index — more slots, a smaller
fold threshold, or splitting the segments further — is the next step, not something this
configuration already solves.

A node that runs a shared prefix in single-writer mode (see `QW_METASTORE_ALLOW_UNSAFE_STORAGE`)
must not be pointed at an index in this layout: its write path assumes the single object and would
write one, shadowing the sharded objects rather than updating them.

One more consequence of not conflicting: with the single-object layout, two writers on the same
index almost always collide, so the loser replays and its cached copy of the index is refreshed as a
side effect. With the sharded layout writers usually do not collide, so a node's cached split map can
stay behind another node's writes until the next poll — the object store has every split, the cache
does not. That is the same contract as today (`#polling_interval` is what makes a node notice other
nodes' work), but the margin is thinner: configure polling wherever a node reads an index it also
writes, and expect a node that neither polls nor writes to keep the view it loaded.

### Sizing: when this layout is the wrong tool

The layout fixes *how much a write writes*. It does not change how much a read reads: the file-backed
metastore keeps the split map of an index in memory on every node, and reloads it in full before
every mutation. Both are `O(splits in the index)`, so a 15 M-split index means a ~12 GB in-memory
split map per node and a full reload per write, whatever the layout.

Measured on this machine (2026-09), comparing the two backends at the scale each is used at —
PostgreSQL over loopback, the file-backed metastore on RAM so that the numbers are the metastore's
own work rather than the network:

| Workload | File-backed, 50 000 splits | PostgreSQL, 1 000 000 splits |
| -------- | -------------------------- | ---------------------------- |
| publish one split into the index | 1.25–1.35 s (reload and rewrite of the whole index) | **8.4 ms** (one row update) |
| `list_splits` for one hour of a 30-day index | 7 ms, served from the in-memory copy of the whole index | **57 ms**, 776 KB read from the database |
| `list_splits` for the whole index | the node already holds it, and holds it for good | streams: first chunk in 4 ms, 37 s for all 1 M rows |
| Memory per node | the whole split map (~38 MB at 50 k splits, ~12 GB at 15 M) | none |

Both runs are in the repository (`quickwit-metastore/tests/file_backed_scale.rs` and
`quickwit-metastore/tests/postgres_scale.rs`); rerun them with `QW_TEST_SCALE_SPLITS` and
`QW_TEST_POSTGRES_URI`.

For a 5·10¹² documents/day index (500 000 splits/day, ~15 M splits at 30 days of retention) the
conclusion is that **PostgreSQL is the backend to use**: a publish touches one row, and a search reads
the rows of its time window through an index, independently of how many splits the index holds. The
object-storage metastore is the right tool when the point is to avoid operating a database and the
metadata stays in the single-digit-gigabyte range; past that it needs a split map that is queried and
mutated per time bucket instead of loaded whole, which is a different design rather than a setting.

The database part of that windowed read is small — `EXPLAIN (ANALYZE, BUFFERS)` on the 1 M-split index
shows a bitmap index scan over the window (3 112 index entries, 70 shared buffers) executing in
**0.2 ms**; the 57 ms above is the metastore serializing and the client decoding the 776 KB of split
metadata those rows carry. That is what "reads only the window" means in practice: the window's
metadata, not the index's.

Choosing PostgreSQL is not free — it is another component to run, back up and upgrade, and at this
scale it is a single point of failure unless it is itself made highly available. What the numbers
above say is narrower and firmer: an index whose metadata no longer fits the object-storage
metastore's whole-index model needs the query-and-row model, and PostgreSQL already implements it.

Sizing it from the same measurement: 1 M splits took 1.5 GB of table *and* indexes, so the 15 M
splits of that 30-day index are ~23 GB in the database — plan storage for the metadata of every index
you keep, and prefer retention (or one index per period) over a single index that lives forever.
Throughput is not the constraint: the index above accepted splits at ~18 000/s in batches, needs
5.8 publishes/s at 5·10¹² documents/day, and a single publish takes 8 ms, so one connection has
orders of magnitude of headroom. Watch the connection count instead: every node holds
`max_connections` connections (`metastore.postgresql` in the node config), so a large cluster wants
its `max_connections` per node kept small or a pooler in front.
