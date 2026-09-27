# Metastore layout for very large indexes: shards, segments and compaction

> Status: design note (batch-21), partly implemented in batch-22 (branch
> `feat/metastore-sharded-store`): the slot write path, the per-slot fold, the view bookmark and the
> read path exist and are tested, and deletions are carried forward through a fold (the tombstone
> half). Still open: folding a slot *range* into one segment so a reader fetches fewer objects, a
> background compactor and the lease that would need, a grace beyond the two generations the GC
> keeps, migrating an existing legacy index, and a reader that does not materialise the whole split
> map. The implementation lives in
> `quickwit-metastore/src/metastore/file_backed/sharded_layout.rs`.

## Why the current layout does not scale

The file-backed metastore keeps one JSON object per index holding every split, and every metadata
write rewrites that whole object with a compare-and-swap. At the default 10 M documents per split,
and counting two metadata writes per split (one when it is staged and published, one when it is
deleted) with **~800 bytes of JSON per split** (a `SplitMetadata` measured on the bench indexes):
**5·10¹² documents/day is 500 000 splits/day = 5.79 splits/s ≈ 11.6 metadata writes/s**, and the
file they all rewrite grows with retention:

| Retention | Splits in one index | Metadata file | Bytes rewritten per publish |
| --------- | ------------------- | ------------- | --------------------------- |
| 7 days | 3.5 M | ~2.8 GB | ~2.8 GB |
| 30 days | 15 M | ~12 GB | ~12 GB |

Two independent problems: total rewrite cost per write, and the fact that every writer targets the same
object (measured: 3 nodes → 776 conflicts/3 min, 5 nodes → 2 583).

## Layout

```
metastore/
  manifest.json                     # index list, source configs, checkpoints. Small, low churn: keep
                                    # as today (single CAS file).
  <index_id>/
    v2/root.json                    # everything of the index except the splits, its own CAS
    v2/splits/view.json             # per-slot bookmark: folded sequence, segment, slot file version
    v2/splits/slots/<slot:05>.json  # entries written to that slot since it was last folded
    v2/splits/segments/<slot:05>/<generation:016>-<uuid>.json
                                    # immutable folded snapshot of one slot
```

Write path: a publish/stage/delete rewrites **one slot file per touched slot**, never the whole index.
Read path: read `view.json` once, then the segments it names plus the slot files whose version differs
from the bookmark — never all N slots.

## Protocols and invariants

1. **Shard key and shard size.** `hash(split_id) % N`. Two sizes matter and they are bounded
   differently: a **slot file** only holds the entries written since that slot was last folded, so
   the fold threshold bounds it (~400 KB at 512 entries of ~800 bytes) no matter how large the index
   is; a **segment** holds the whole content of one slot, so `N` bounds it (12 GB / 256 ≈ 47 MB).
   `N` therefore trades the number of objects a read has to fetch against the per-slot conflict
   probability (`1/N` per pair of concurrent writers: at `N = 256` and 11.6 writes/s, a pair of
   writers touches the same slot on ~0.05 writes/s — an order-of-magnitude estimate, not a measured
   collision rate). `N = 256` is the current default; sizing it for a 12 GB index is still open (a
   per-range segment would let a read fetch far fewer objects, see Next steps). Invariant: a slot
   file may be written by anyone, but only the writer holding its CAS version may commit it.
2. **Pointer switch.** Compaction writes the new segment, then CASes `view.json` (one write) to a new
   generation, and only then may the superseded segments be deleted. Invariant: every generation is a
   complete snapshot — readers never need to combine "old segments plus new segments" across a switch.
   Detection: a reader that fails to load a segment listed by the view retries; a view that lists a
   missing segment is a hard error, not a silent fallback.
3. **Folding is inline, and there is no lease yet.** Folding one slot is `previous segment + every
   entry of that slot file`, so it needs one slot file and one segment, never the whole index, and it
   can run inside the write that crossed the threshold. Two folders may then race: the view CAS
   settles it, and the loser deletes the segment it wrote. No lease is needed while folding is
   inline and per slot. It becomes necessary if a background compactor is ever added (to fold many
   slots without a writer, or to fold a slot range into one object) — that is open work, together
   with the invariant that two compactors may not publish overlapping generations.
4. **Tombstones.** Deletions (retention, delete tasks, merge replacements) are recorded as entries
   whose value is `None`, in the same slot as the split, and a fold carries them forward. Invariant:
   compaction may never resurrect a deleted split; a segment is only valid if, for every split it
   contains, the tombstones that were visible when it was built are also applied. Detection: split
   counts derived from the view must equal `list_splits` (the existing consistency tests can be
   extended to compare both views).
5. **GC grace.** Superseded segments are deleted only once a generation has moved two further
   generations ahead — the implementation keeps the last two generations of segments and deletes
   older ones right after a successful view CAS. Invariant: a reader holding generation *g* can
   always finish reading it (or restart against the newer view, which the staleness check forces).
   Detection: the deletion path is the only place that removes objects, and it is generation-gated.
   A time-based grace, longer than the searcher polling interval, would replace the two-generation
   rule if the read path ever holds a view for more than a generation; that is open.
6. **Format version and migration.** `view.json` carries a format version. A node that only knows the
   old format must refuse to write into a sharded prefix (the same style of refusal as the existing
   conditional-write probe). Migration: read the legacy single file, emit it as one segment, publish
   the first view, keep the legacy object until the grace period ends.

   Implemented so far: `root.json`, `view.json` and every segment carry `format_version = 1`, and a
   node that finds an unknown version refuses to interpret the object instead of guessing. Nothing
   migrates an existing legacy index yet — a sharded index is created sharded. Reading tries
   `<index_id>/metastore.json` first, which is the path every index created before this layout uses,
   and only looks for a sharded root when that object is missing; that is what keeps a mixed fleet
   working.

7. **Commit order: the slots first, the index metadata last.** A mutation writes the slot files it
   touched and then `root.json`, the object that carries the checkpoint among the index metadata. If
   the failure lands between the two, the splits are published and the checkpoint is behind them: a
   replay finishes the mutation (the splits are tolerated as already published, the delta is applied,
   the root commits), and a caller that never replays pays duplicated documents when the source is
   read again. The other order — root first — leaves the checkpoint *ahead* of a split that was never
   published, and nothing then re-reads that window: it costs the documents themselves. This is the
   same choice the manifest layout makes, for the same reason.

## Read path

1. Load `root.json` (1 GET), which is the index without its splits.
2. Load `view.json` (1 GET). It names, per slot, the segment to use and the version of the slot file
   that was folded into it.
3. Load the segments the view names — one object per slot that holds data, which is the read cost
   this design does *not* yet reduce.
4. `list` the slot prefix (1 request, paginated at 1000 keys) and load only the slot files whose
   version differs from the bookmark; apply their entries after `folded_seq`, removals last. A slot
   file written against a newer generation than the view means the view is stale: restart the read.

Searchers already poll the metastore rather than reading it per query, and a slot that has been
folded and not written since is skipped without downloading it. The steady state is still
`1 + 1 + segments + changed slot files` GETs per poll, and `segments` grows with the number of
populated slots until a read can be served from a cache or from a coarser object.

## Failure modes

| Failure | Expected behaviour |
| ------- | ------------------ |
| Writer crashes after writing a slot file, before any fold | Entries are in the slot file; the next write or fold picks them up (`test_sharded_layout_round_trip`) |
| Writer commits the slots, then fails before `root.json` | The splits are published and the checkpoint is behind them (`test_a_root_commit_failure_leaves_the_splits_published`): a replay finishes the mutation, and a caller that never replays costs duplicated documents, never missing ones (invariant 7) |
| A slot commit fails before `root.json` is written | The slots that committed before it stay published and the checkpoint has not moved; the metastore's own retry finishes the mutation (`test_a_slot_commit_failure_is_replayed_by_the_metastore`; a mutation touches several slots, so "nothing is published" only holds when the first one fails) |
| Writer crashes mid-fold, after the segment, before the view CAS | The segment is unreferenced; the view still names the previous one, and the next write to that slot folds again (`fold_slot` deletes its segment when the view CAS loses) |
| Two writers fold the same slot at once | The view CAS settles it; the loser deletes the segment it wrote |
| Reader holds an old view while a fold (and its GC) runs | Two generations of segments are kept, and a slot file from a newer generation forces the reader to restart (`test_sharded_layout_conflicts_only_on_the_same_slot` covers the conflicting writers, the GC rule is asserted by construction) |
| Endpoint ignores conditional writes | A node configured for the layout refuses to start; a legacy index on such an endpoint keeps the existing single-writer behaviour |
| A mutation forgets to record the split it touched | The write silently drops it: this is why every split mutation in `FileBackedIndex` has to record into `touched_split_ids`, and why the reviewer is asked to enumerate them |

## Alternatives considered

- **Tolerating an already-applied checkpoint delta by its positions, instead of ordering the
  commits.** A publish that is being replayed carries the delta its earlier attempt applied, and
  applying it again is refused as incompatible; skipping it when the checkpoint already sits at the
  delta's end position would let the write that failed after the root finish. It was implemented,
  reviewed and rejected: the checkpoint is a shared watermark and cannot say *whose* delta moved it,
  so any competing writer whose delta happens to end on the same position would be answered with a
  success — the same documents indexed twice, which is the one thing the compatibility check exists
  to prevent. A shard's publish token does not separate the writers either: a shard that changes
  hands carries the new holder's token. What a replay may skip is therefore gated on proof that the
  publish is its own (the splits it publishes are published already) or on the request changing no
  split state at all; the measurements and the two counterexamples are in the ledger thread
  `qw-replay-tolerates-applied-delta`.

- **PostgreSQL metastore**: row-level updates, no whole-file rewrite, 26 ms writes and zero conflicts in
  the same benchmark where R2 needed 2.14 s. It is the pragmatic answer when a database may be
  operated; the sharded layout preserves the "no extra component" property of object storage.
- **Many small indexes** (hourly/daily): works today with no format change, at the cost of cross-index
  query convenience and more metastore reads.

## Next steps

1. Measured instead of simulated (batch-22): bytes rewritten per publish now come from diffing a
   listing by object version, on both layouts. An index growing to 180 splits rewrites 6.5 KB → 123.9
   KB per publish on the single-object layout against 1.6 KB → 5.6 KB on the sharded one — measured
   with 64 slots and a fold threshold of 8 so that the sawtooth fits in a unit test, not with the
   256/512 defaults, and before a fold has to rewrite a large segment.
2. Still to measure at scale: cold-read GETs and conflict rate with 256 slots and hundreds of
   writers, plus the fold cost of a slot that holds millions of splits.
3. Remaining implementation work: fold the segments per slot range instead of per slot (so a reader
   does not have to fetch one object per slot), a compaction lease, and an incremental reader that
   keeps what it has instead of materialising the whole split map.
