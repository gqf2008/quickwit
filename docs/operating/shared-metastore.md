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
| 5 nodes publishing into one manifest-layout index on R2, 2 minutes | 54,240 acknowledged = 54,240 searchable, zero actor faults, zero ERROR-level lines |
| GC/retention load, 3 nodes, ~14 minutes | zero actor faults, delete tasks progressing on every node |
| Metastore outage, 5 minutes, ingest continuing | 600/600 acknowledged during the outage, all 640 documents searchable 1.5 s after recovery |
| Rollback drill with a pre-CAS binary | data readable both ways; mixed versions silently lose updates (documented) |
| Metadata write latency, PostgreSQL vs R2 | delete p50 26 ms vs 2.14 s; publish p50 6.1 s vs 7.0 s; ingest 52 vs 63 acked/s (1 node) |

The search count in the manifest-layout row is the one a node reports while the index is still being
published and merged, and a node that polls a second later can count more documents than were
acknowledged while a merge is in flight; the number to compare against the acknowledged one is the
index's static state, which the row's two numbers are (33 published splits holding 54,240 documents
in that run). The other rows are from the same kind of run and the same harness.

## Known limits

- Cross-region latency dominates: one round trip to the bucket used for these measurements was
  0.81 s, so run nodes close to the bucket.
- The janitor's `DeleteTaskPlanner` used to trip the actor progress watchdog under load: its
  metastore and search calls are now awaited in the actor framework's protected zone, which is what
  those calls need (they are another actor's latency, not this one's), and the 5-node manifest-layout
  run above has no such fault. A protected call is no longer covered by this actor's watchdog, so a
  call that never returns relies on the downstream service to fail; if that shows up, bound it in
  the client rather than here.

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

The shared metastore test suite is generic over the metastore, so CI runs it twice: once on the
single-object layout and once with `QW_METASTORE_TEST_SHARDED_LAYOUT=true`, which makes the test
helper (`FileBackedMetastore::default_for_test`) create its indexes in the sharded layout. Locally:

```sh
cd quickwit
QW_METASTORE_TEST_SHARDED_LAYOUT=true cargo nextest run -p quickwit-metastore -E 'test(file_backed)'
```

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

Consequence of not conflicting, and how it is handled: with the single-object layout two writers on
the same index almost always collide, so the loser replays and its cached copy of the index is
refreshed as a side effect. With the sharded layout writers usually do not collide — winning the
compare-and-swap no longer proves that nobody else wrote in between — so a successful distributed
mutation drops the node's cached view instead of caching the snapshot it happened to hold. The next
read reloads from the object store and sees every writer's splits, at the cost of one full read per
write-then-read pair on top of the read the write itself needs. A node that only reads still needs
`#polling_interval` to notice other nodes' work, which is the contract it always had.

### Sizing: which backend to use, and when to stop using this one

The default and sharded layouts fix *how much a write writes*. They do not change how much a read
reads: the file-backed metastore keeps the split map of an index in memory on every node, and reloads
it in full before every mutation. Both are `O(splits in the index)`, so a 15 M-split index means a
~12 GB in-memory split map per node and a full reload per write, whatever the layout. **The manifest
layout is the one that breaks that**: a read costs the query's window, a write only the splits it
touches. What it costs instead is the round trip to the bucket — three of them per publish — so for a
large index the question is not "object storage or a database" in the abstract, it is where the nodes
are.

Measured on this machine (2026-09-25): the three object-storage layouts on RAM at 50 000 splits over
30 days, one hour queried (RAM so the numbers are the metastore's own work and not the network), and
PostgreSQL over loopback at 1 000 000 splits.

| Workload | Single object | Sharded (256 slots) | Manifest | PostgreSQL |
| -------- | ------------- | ------------------- | -------- | ---------- |
| `list_splits` for one hour of a 30-day index | 743 ms | 1 910 ms | **99 ms** | **57 ms**, 776 KB |
| publish one split into the index | 1 427 ms | 1 839 ms | **22 ms** | **8.4 ms** |
| staging throughput | 2 747/s | 2 817/s | **6 957/s** | ~18 000/s |
| Memory per node | the whole split map (~12 GB at 15 M) | the whole split map | none | none |
| `list_splits` for the whole index | the in-memory copy | the in-memory copy | the window's metadata | streams: 4 ms to the first chunk, 37 s for 1 M rows |

The same run at 200 000 splits — 4× the index, same machine and harness — says what the manifest
column buys: its windowed read is **74 ms** against 99 ms at 50 000, and its single publish **14 ms**
against 22 ms. Both are the same cost rather than 4×, because neither operation loads the index. Only
its bulk paths grow, and less than the index does (staging 9 033/s and publishing all of them 6 424/s,
3.1–3.3× the time for 4× the splits), while the other two layouts grow on every number (single object:
a 3.16 s windowed read and a 5.89 s publish; sharded: 4.59 s and 4.57 s).

Both runs are in the repository (`quickwit-metastore/tests/file_backed_scale.rs` measures all three
layouts in one run, `quickwit-metastore/tests/postgres_scale.rs` the database); rerun them with
`QW_TEST_SCALE_SPLITS` (50 000 or 200 000) and `QW_TEST_POSTGRES_URI`.

For a 5·10¹² documents/day index (500 000 splits/day, ~15 M splits at 30 days of retention) the
backend to use follows the locality of its nodes:

- **Nodes next to the bucket (same region, tens of milliseconds away): object storage, in the
  manifest layout.** It is the layout whose read and write cost does not grow with the index, it needs
  no second component to run, back up and keep highly available, and the round trip allows the rate: a
  publish is three storage calls, so ~16 publishes/s per node at a 20 ms round trip. That latency is a
  **model, not a measurement of this port** — it comes from the spike
  (`quickwit-metastore/tests/obj_layout_spike.rs`, which injects the round trip into RAM storage) and
  from the three-call arithmetic; no deployment in the same region as the nodes has been measured
  here, so measure the rate on the deployment's own bucket before relying on it.
- **Nodes away from the bucket: PostgreSQL, or move the nodes.** At the 0.81 s round trip the real
  bucket below measures from this machine those same three calls are ~2.4 s, so one node publishes
  0.4/s and twelve writers reach 3.8/s — under the 5.8 split publications/s this shape asks for (11.6
  metadata writes/s, counting the stage). A database answers in one round trip because the server owns
  the storage, and the database part of a windowed read is small: `EXPLAIN (ANALYZE, BUFFERS)` on the 1
  M-split index shows a bitmap index scan over the window (3 112 index entries, 70 shared buffers)
  executing in **0.2 ms**, so the 57 ms above is the metastore serializing and the client decoding
  776 KB of split metadata.

The 15 M splits of that index are ~23 GB in PostgreSQL (1 M splits took 1.5 GB of table *and*
indexes), so plan storage for the metadata of every index you keep and prefer retention, or one index
per period, over a single index that lives forever. On object storage the same metadata is the index's
segments and WAL objects in the bucket it already uses. If the choice is PostgreSQL, watch the
connection count rather than the throughput: every node holds `max_connections` connections
(`metastore.postgresql` in the node config), so a large cluster wants that kept small or a pooler in
front.

## Splits as manifests, segments and a WAL tail

`QW_METASTORE_MANIFEST_LAYOUT=true` makes a node create indexes in a third layout, which is the one
built for large indexes:

```
<index_id>/v3/manifest-<stripe>.json              mutable: references only, one compare-and-swap
<index_id>/v3/wal-<stripe>/<generation>-<id>.json immutable: one object per published batch
<index_id>/v3/segments/<stripe>/<bucket>/<generation>-<id>.json immutable: one per time bucket
<index_id>/v3/root.json                           metadata, sources, checkpoints, delete tasks
```

Segments and WAL objects live under the stripe that wrote them and are collected against that
stripe's own fold generation: a stripe keeps the two generations after its latest fold, which is the
window a reader that lost a race needs to finish fetching what the manifest it holds names. A stripe
that folds often therefore cannot collect what a stripe that folds rarely is still reading, and a
stripe that has never folded does not stop the stripes that have from collecting theirs.

Nodes sharing a prefix have to agree on what those generations mean: a node that counted them
differently would collect objects another node is still reading, so an index in this layout follows
the mixed-version rule above and is served by one revision at a time.

A publish appends one WAL object and commits one manifest: it costs what it touches, not what the
index holds. **Size the stripe count at or above the number of nodes that publish into one index**
(`QW_METASTORE_MANIFEST_STRIPES`, default 32): writers that hash to the same stripe contend, and each
conflict costs a full replay of that publish. Measured on a real bucket with 12 concurrent writers and
five publishes each (2026-09-25):

| Stripes | Conflicts | Conflicts per publish | Throughput |
| ------- | --------- | --------------------- | ---------- |
| 8 | 41 / 60 | 0.68 | 1.36 publishes/s |
| 32 | **2 / 60** | **0.03** | **3.81 publishes/s** |

Four writers saw no conflict at all with eight stripes (0 in 20 publishes), so the rule is a margin
over the writer count rather than a constant. The harness publishes split ids of its own shape
(`writer-<n>-<round>`), which do not hash like the ULIDs a deployment uses, so these counts are a
lower bound: size from the writer count, then recheck with the ids the index actually receives. More
stripes cost no *extra round trip* on the read side —
a read loads the manifests (in parallel, so they share one), prunes the time buckets the query cannot
touch, and fetches only the segments that remain plus the WAL tail, so it costs the query's window
rather than the index —
but they do multiply the number of requests a read makes, and creating an index puts one (also
parallel) object per stripe. With a *single* manifest the spike measured the write rate a
5·10¹² documents/day index needs failing once the round trip stops being same-zone, which is why the
count is sized from the writers rather than left at a constant.

Measured through the metastore API at 50 000 splits over 30 days, one hour queried, on RAM storage so
the numbers are the metastore's own work (2026-09-25): a windowed read takes **99 ms**, publishing one
split **22 ms**, and staging runs at **6 957 splits/s** — against 743 ms, 1.43 s and 2 747/s for the
default layout in the same run. The three-way comparison, and what it means for a large index, is
[above](#sizing-which-backend-to-use-and-when-to-stop-using-this-one). It is also the layout that does
not grow with the index: 4× the splits (200 000 against 50 000) leave a windowed read and a single
publish at the same cost — 74 ms and 14 ms — because neither loads the index, and only its bulk paths
grow, 3.1–3.3× for 4× the splits. What it does *not* change is the round trip to the bucket: a publish
is still three storage
calls plus the lookups of the splits it changes, so an index served from this layout wants its nodes
next to the bucket, like every other layout here.

On a real R2 bucket whose round trip is 0.81 s from this machine (2026-09-25), the same layout
measures: a publish writes **1 380 bytes** and takes p50 2.2 s to 6.1 s for a stage+publish pair (the
spread is the bucket's latency between runs, not the layout), and a read with only a start bound — so
nothing is pruned and it touches every bucket — is p50 0.7 s to 1.5 s. Writing is no longer the
problem — the old layout rewrites the whole index, ~12 GB per publish at 15 M splits — the round trips
are: a publish is three storage calls plus the lookups of the splits it changes, and a read is one per
object it touches (its manifests, segments and WAL objects are fetched in parallel).

That latency bounds the rate a single node can publish: three round trips per publish is ~2.4 s here,
so the 11.6 metadata writes/s that a 5·10¹² documents/day index implies (5.8 splits/s, staged and then
published) needs either nodes next to the bucket or ~28 writes in flight, and the 12 writers above
reached 3.8 publishes/s from this machine. The
measurement is opt-in and prints both the numbers and the writer count it used:
`QW_TEST_S3_MEASURE=1 QW_TEST_S3_STRIPES=<n> QW_TEST_S3_WRITERS=<n> cargo test -p quickwit-metastore
--features ci-test --test s3_shared_metastore -- --nocapture`.

The layout is opt-in per node and recorded in the objects, so a node reads an index whichever layout
created it, and `QW_METASTORE_TEST_MANIFEST_LAYOUT=true` runs the shared metastore suite on it locally
(CI runs it next to the other two).

Where its pruning pays off, and where it does not: the win comes from the query's *time* window, so an
index whose splits carry time ranges benefits in proportion to how narrow the queries are. Splits
without a time range belong to no bucket and are fetched by every query (the metastore's own predicate
returns them for every window), so an index of untimed splits gets this layout's write path and its
parallel reads, but none of its read pruning.
