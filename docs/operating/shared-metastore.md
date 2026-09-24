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
