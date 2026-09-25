# Split metadata on object storage: manifest plus segments

> Status: measured spike (batch-26). The layout is implemented in
> `quickwit-metastore/tests/obj_layout_spike.rs` and measured; the metastore does not use it yet.

## The question

PostgreSQL is the baseline, not the destination: the point of the object-storage metastore is to
avoid operating a database. So the question is whether an object-store metastore can serve the same
workload at a comparable cost — and the current file-backed layout cannot, because it keeps **every
split of an index in one mutable object**:

* a read materialises the whole split map (`list_splits` copies it into memory and filters),
* a mutation reloads and rewrites it,
* so 15 M splits means ~12 GB per node and a full reload per write, whatever the sharding.

## The shape that fixes it (from `objsearch`)

The neighbouring project `objsearch` stores vectors on the same kind of storage and does not have
this problem, because its mutable object holds **references, not data**:

| `objsearch` | what it holds | where |
| ----------- | ------------- | ----- |
| `manifest.json` | `epoch`, `segments: Vec<SegmentRef>`, `wal: Vec<WalRef>`, `index_cursor_seq` | `src/manifest.rs:366-382` |
| manifest commit | one compare-and-swap on that small object | `src/manifest.rs:678` |
| `wal/{seq}.json` | immutable, one object per group commit | SPEC §5.4 |
| `index/{segid}.seg` | immutable segment holding the documents | SPEC §5.1, §5.5 |
| read | load the manifest, then fetch the segments it needs | `src/engine.rs:2105,2175,2193` |
| measured read budget | 2-4 round trips **regardless of corpus size** | SPEC §7.4 |

## Measured for *split metadata*

The spike applies exactly that shape to split metadata — manifest of segment references, immutable
one-hour segments, WAL tail for un-folded publishes — and runs the workload the search path asks for
(`list_splits` for one hour of a 30-day index) at 200 000 and 1 000 000 splits. Storage is RAM and
every call is counted, so the numbers are storage work: round trips and bytes.

| | 200 000 splits | 1 000 000 splits |
| --- | -------------- | ---------------- |
| manifest | 721 segments, **38 KB** | 721 segments, **38 KB** |
| windowed read (last hour) | 3 round trips, 68 KB, 0.9 ms | 3 round trips, **189 KB, 2.3 ms** |
| publish (one split) | 3 round trips, **38 KB written**, 1.2 ms | 3 round trips, 38 KB written, 1.2 ms |
| whole-index read | 723 GETs (batchable into one logical round trip) | 723 GETs, 1.0 s |

The manifest does not grow with the index — it grows with the number of *time buckets* — and a
windowed read costs the window's metadata, not the index's.

For comparison, the same machine and the same workload:

| Workload | Current file-backed layout | PostgreSQL | This layout |
| -------- | -------------------------- | ---------- | ----------- |
| publish one split | 1.25–1.35 s at 50 k splits (reload + rewrite of the whole index) | 8.4 ms at 1 M | 1.2 ms at 1 M, 38 KB written |
| windowed read | from the in-memory whole index (38 MB at 50 k, ~12 GB at 15 M) | 57 ms, 776 KB at 1 M | **2.3 ms, 189 KB at 1 M** |
| memory per node | the whole split map | none | the window being read |

## What the object store still cannot do

The remaining cost is the object store's own latency: the spike needs **3 round trips** per publish
(read the manifest, write the WAL object, compare-and-swap the manifest) and 3 for a windowed read.
On the cross-region R2 prefix used earlier in this work a round trip is ~0.81 s, so a publish there
is ~2.4 s no matter how small the objects are; in-region it is tens of milliseconds. A database
answers in one round trip because the server owns the storage. That is the trade, and it is why the
earlier sizing note recommends PostgreSQL for a deployment that cannot put its nodes next to the
bucket.

Two things the spike does **not** settle, and that a production port has to:

1. **Contention on the one compare-and-swap.** At 5·10¹² documents/day the index sees ~11.6 metadata
   writes/s. One manifest per index is one CAS point; `objsearch` handles the same shape with group
   commit (one WAL object per window, `SPEC §6.2-6.4`). The port needs either that, or manifests
   striped per writer, plus a measurement of conflicts under N writers.
2. **The metastore above the storage.** `mutate_distributed` today loads the whole index, applies a
   closure and writes it back; `list_splits` collects everything matching. Those two call sites are
   what force `O(index)` — the layout alone does not remove them. The port has to turn the split
   path into operations (`stage`/`publish`/`mark for deletion`/`delete` as WAL records,
   `list_splits(query)` as manifest + pruned segments) before any of the numbers above become the
   metastore's numbers.

## Porting plan

1. **Layout module** (`file_backed/obj_layout.rs`): manifest, WAL objects, per-time-bucket segments,
   fold, and the windowed read — essentially the spike, hardened (format version, GC of superseded
   segments, torn-write handling as in `LESSON_条件写失败后不得清理自己写的对象.md`).
2. **Split operations** (`file_backed/`): `stage_splits`, `publish_splits`, `mark_splits_for_deletion`,
   `delete_splits` become WAL appends plus one manifest CAS in the sharded/distributed path, with the
   existing bounded replay. `FileBackedIndex` stays as the single-node, in-memory model.
3. **Read path**: `list_splits(query)` reads the manifest, prunes buckets by the query's time range
   (and by state), streams the matching segments plus the WAL tail, and never builds the whole map.
4. **Retention**: deleting a bucket is dropping references and deleting objects — no rewrite.
5. **Migration**: an existing single-object index is folded into segments once, by a reader that can
   write; the legacy object stays readable until the fold is committed.
6. **Evidence before wiring it in**: the same three measurements as above on a real bucket
   (round trips and bytes), a conflict-rate measurement at N writers, and the shared metastore suite
   run on the new layout the way it now runs on the sharded one.
