# Split metadata on object storage: manifest plus segments

> Status: implemented (batch-27) as the third layout of the file-backed metastore
> (`QW_METASTORE_MANIFEST_LAYOUT=true`,
> `quickwit-metastore/src/metastore/file_backed/manifest_layout.rs`). Reads are pruned by the query's
> window and the five split mutations read and write only the splits they touch; the shared metastore
> suite passes on it, and CI runs it next to the other two layouts. Measured growth is sub-linear
> rather than flat (4× the splits cost the query and the publish 2.4–2.9× more, from the WAL tail and
> the segment list). Still open: the real-bucket measurement of the port, sizing the stripe count in
> production, and the migration of an index that was created in an older layout.
>
> The original spike, kept for the numbers and the shape, is
> `quickwit-metastore/tests/obj_layout_spike.rs`.

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

## The one compare-and-swap, measured at the target rate

5·10¹² documents/day is ~11.6 metadata writes/s into one index, and an object store has no server to
serialise writers: the manifest compare-and-swap is the only ordering primitive. `objsearch` uses
one manifest per namespace, so the spike measured whether that carries the rate, and whether
striping the manifest across writers fixes it if it does not. Five writers offer 12 publishes/s
against an artificial round trip:

| Round trip | 1 manifest | 8 striped manifests |
| ---------- | ---------- | ------------------- |
| 20 ms (in-region) | 12.0/s published, **240 conflicts / 120 publishes** (2 per publish, 9 storage calls each) | **12.0/s, 0 conflicts, 3.0 storage calls per publish** |
| 200 ms | **1.6/s published** (target 12/s), 74 conflicts, **4 writes exhausted their replay budget** | 8.3/s, **0 conflicts**, 3.0 storage calls per publish |

So one manifest per index is *not* enough for this workload away from a same-zone bucket — it does
not reach the rate, and it starts dropping writes when the replay budget runs out — while eight
stripes hit the rate with no conflicts at all, at the minimum of three storage calls per publish
(read the manifest, write the WAL object, commit the manifest). Striping is therefore a requirement
of the port, not an optimisation; `objsearch`'s group commit is the alternative for a single-writer
process, and it is not available to independent Quickwit nodes.

Two things the spike still does **not** settle, and that a production port has to:

1. **How many stripes**, and how a reader finds them: eight was enough for five writers at 12/s, but
   the count has to be a function of the writer count and the round trip, and a reader has to
   discover the stripes cheaply (one `list`, or a fixed count known from the manifest).
2. **The metastore above the storage.** `mutate_distributed` today loads the whole index, applies a
   closure and writes it back; `list_splits` collects everything matching. Those two call sites are
   what force `O(index)` — the layout alone does not remove them. The port has to turn the split
   path into operations (`stage`/`publish`/`mark for deletion`/`delete` as WAL records,
   `list_splits(query)` as manifest + pruned segments) before any of the numbers above become the
   metastore's numbers.

## Porting plan

Landed in batch-27: the layout module, the pruned read path, and the subset mutation path (a
publish/can stage/mark/delete reads only the splits it names, through the id ranges the manifests carry,
and runs the same mutation closure as the other layouts). The rest of this plan is what is left.

1. **Layout module** (`file_backed/manifest_layout.rs`): manifest, WAL objects, per-time-bucket segments,
   fold, and the windowed read — essentially the spike, hardened (format version, GC of superseded
   segments, torn-write handling as in `LESSON_条件写失败后不得清理自己写的对象.md`). Segments and
   WAL objects belong to the stripe that wrote them, in its own directory, and are collected against
   that stripe's own fold generation: stripes fold at different rates, so a shared watermark either
   collects what a slow stripe is still reading or never collects at all while one stripe of the
   index has not folded yet.
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
