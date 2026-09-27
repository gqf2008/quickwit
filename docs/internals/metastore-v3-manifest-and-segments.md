# Split metadata on object storage: manifest plus segments

> Status: implemented (batch-27) as the third layout of the file-backed metastore
> (`QW_METASTORE_MANIFEST_LAYOUT=true`,
> `quickwit-metastore/src/metastore/file_backed/manifest_layout.rs`). Reads are pruned by the query's
> window and the five split mutations read and write only the splits they touch; the shared metastore
> suite passes on it, and CI runs it next to the other two layouts. Its single-operation costs do not
> grow with the index: 4× the splits (50 000 to 200 000, RAM, same harness) left a windowed read at
> 74 ms against 99 ms and a single publish at 14 ms against 22 ms, while the bulk paths that touch
> every split grew 3.1–3.3×. Measured on a real bucket: a publish writes 1 380 bytes, 12 writers conflict 0.03
> times per publish on 32 stripes and reach 3.8 publishes/s, and the fold path writes, reads and
> collects the segments it is supposed to (`tests/s3_shared_metastore.rs`,
> `docs/operating/shared-metastore.md`). Still open: the migration of an index that was created in an
> older layout.
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
| 20 ms (simulated in-region) | 12.0/s published, **240 conflicts / 120 publishes** (2 per publish, 9 storage calls each) | **12.0/s, 0 conflicts, 3.0 storage calls per publish** |
| 200 ms | **1.6/s published** (target 12/s), 74 conflicts, **4 writes exhausted their replay budget** | 8.3/s, **0 conflicts**, 3.0 storage calls per publish |

The run above injects the round trip into RAM storage rather than measuring a bucket, and its writers
are assigned to distinct stripes (`writer % stripes`), so its zeros are structural: read the
conflict numbers measured against a real bucket in
[Shared object-storage metastore](../operating/shared-metastore.md) instead of these.

So one manifest per index is *not* enough for this workload away from a same-zone bucket — it does
not reach the rate, and it starts dropping writes when the replay budget runs out — while eight
stripes hit the rate with no conflicts at all, at the minimum of three storage calls per publish
(read the manifest, write the WAL object, commit the manifest). Striping is therefore a requirement
of the port, not an optimisation; `objsearch`'s group commit is the alternative for a single-writer
process, and it is not available to independent Quickwit nodes.

Both things this spike could not settle are settled by the port that followed, so the spike is now
the first measurement of the shape rather than a list of what is missing:

1. **How many stripes**, and how a reader finds them: the count is set per index by
   `QW_METASTORE_MANIFEST_STRIPES` (default 32, sized from the writer count) and is recorded, with the
   bucket width, in the index's own layout, so a reader learns it from the index metadata instead of
   discovering it with a `list`.
2. **The metastore above the storage.** The five split mutations (stage, publish, mark for deletion,
   delete, and the delete-task opstamp update) no longer load the whole split map: they read the
   splits they name, through the manifests and the segments whose id range can hold them, publish what
   changed as WAL appends plus **one compare-and-swap per stripe each of their passes touches**, and
   read with the query's window (`manifest_layout.rs::publish_ops`, `ManifestLayout::list_splits`).
   The numbers above stay the *layout's* numbers — the spike counts storage calls, while the metastore
   also loads the index root and looks the splits up, so what the whole metastore costs is measured in
   `docs/operating/shared-metastore.md`, not here.

## Porting plan, item by item

The plan below was written before the port; this is where each item stands, so that the spike above is
not mistaken for the implementation. The operator's view of the same layout, with the numbers a
deployment sizes from, is `docs/operating/shared-metastore.md`.

1. **Layout module** (`file_backed/manifest_layout.rs`) — **landed**: manifest, WAL objects,
   per-time-bucket segments, fold and the windowed read, with a format version on the root, the
   manifests and the segments, checked when they are read; the torn-write rules of
   `LESSON_条件写失败后不得清理自己写的对象.md` (a publish that loses its compare-and-swap leaves the
   WAL object it wrote for a later fold to collect, it does not delete it); and the collection of
   superseded segments and WAL objects against the stripe's own fold generation: segments and WAL
   objects belong to the stripe that wrote them, in its own directory, because stripes fold at
   different rates and a shared watermark would either collect what a slow stripe is still reading or
   never collect at all while one stripe of the index has not folded yet.
2. **Split operations** (`file_backed/`) — **landed**: `stage_splits`, `publish_splits`,
   `mark_splits_for_deletion`, `delete_splits` and `update_splits_delete_opstamp` are WAL appends plus
   one compare-and-swap per stripe each of their passes touches, in the sharded/distributed path and
   under the existing bounded replay. A merge is the one mutation with more than one pass: the stripes
   that own the splits it replaces record the marking after the stripe carrying the product commits,
   and a single-product merge then clears the marks it wrote with one more compare-and-swap.
   A mutation that also changes what the index itself holds — its metadata, sources, checkpoints or
   delete tasks — pays the root's own compare-and-swap on top, and only when it changed.
   `FileBackedIndex` stays as the single-node, in-memory model.
3. **Read path** — **landed**: `list_splits(query)` reads the manifests (in parallel, so all stripes
   share one round trip), prunes the buckets the query's time range cannot touch, and fetches the
   segments that remain plus the WAL tail, so it never builds the whole map.
4. **Retention** — **landed as promised**: a delete appends a removal record and rewrites no segment;
   the bucket's segment is rewritten by the next fold that supersedes it, and what the layout collects
   is superseded segments and WAL objects, per stripe.
5. **Migration** — **still open**: an index created in an older layout stays in it. The layout is
   recorded in the index's own objects, so a node reads an index whichever layout created it and the
   layouts coexist in one prefix. What not migrating costs is what the other layouts cost: their reads
   materialise the whole split map, their mutations reload it and every node holds it in memory, so an
   index left alone keeps all of that and gets none of this layout's windowed reads. A deployment that
   wants this layout for an index that already exists rebuilds that index.
6. **Evidence before wiring it in** — **landed**: latency and bytes per publish on a real bucket, the
   conflict rate at N writers, and the fold path's writes, reads and collection, all from
   `quickwit-metastore/tests/s3_shared_metastore.rs`; the shared metastore suite runs on this layout
   next to the other two (`.github/workflows/ci.yml`, `QW_METASTORE_TEST_MANIFEST_LAYOUT`). The round
   trips stay counted by the spike above, which is where that number comes from.

One contract is invisible in every number above, and a merge is the operation that depends on it: in a
**multi-product** merge the products commit first and the markings after them, so from the commit of
the first product until the commit of the last marking a reader can return the same document twice —
it never hides one — while a single-product merge carries its marking in the same compare-and-swap as
the product it publishes. The mechanism, the two reader-side designs that were tried and rejected, and
the probes that measured each one are in
[`metastore-v3-merge-visibility.md`](metastore-v3-merge-visibility.md).
