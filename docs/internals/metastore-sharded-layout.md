# Metastore layout for very large indexes: shards, segments and compaction

> Status: design note (batch-21). No code implements this yet.

## Why the current layout does not scale

The file-backed metastore keeps one JSON object per index holding every split, and every metadata
write rewrites that whole object with a compare-and-swap. At the default 10 M documents per split,
**5·10¹² documents/day is 500 000 splits/day ≈ 11.6 metadata writes/s**, and the file they all
rewrite grows with retention:

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
    view.json                       # "current view" pointer: format version + ordered list of segments
                                    # + generation. Written only when the segment set changes.
    shards/<hash>/<generation>.json # live deltas: split metadata appended by hash(split_id) % N
    segments/<segment_id>.json      # immutable consolidated files produced by compaction
```

Write path: a publish/stage/delete touches **one shard file** (its own object), never the whole index.
Read path: read `view.json` once, then the segments it lists plus the deltas that appeared after the
generation it names — never N shards.

## Protocols and invariants

1. **Shard key and shard size.** `hash(split_id) % N`. Sizing rule: keep a shard file in the
   single-digit MB range and `N ≥ 10 ×` concurrent writers per index. For the numbers above,
   `N = 1024` gives ~12 MB/shard at 30-day retention, a per-write conflict probability of ~2 % and
   ~0.26 conflicts/s, which the existing bounded replay absorbs. Invariant: a shard file may be
   written by anyone, but only the writer holding the shard's CAS version may commit it.
2. **Pointer switch.** Compaction writes the new segment, then CASes `view.json` (one write) to a new
   generation, and only then may the superseded segments be deleted. Invariant: every generation is a
   complete snapshot — readers never need to combine "old segments plus new segments" across a switch.
   Detection: a reader that fails to load a segment listed by the view retries; a view that lists a
   missing segment is a hard error, not a silent fallback.
3. **Compactor lease.** Only one compactor per index may run: it takes a lease object
   (CAS-protected, with an expiry) before producing a segment. Invariant: two compactors may not
   publish overlapping generations; the loser discards its segment. Detection: the pointer CAS fails,
   the loser logs and exits.
4. **Tombstones.** Deletions (retention, delete tasks, merge replacements) are recorded as tombstones
   in a shard, and compaction must carry them forward. Invariant: compaction may never resurrect a
   deleted split; a segment is only valid if, for every split it contains, the tombstones that were
   visible when it was built are also applied. Detection: split counts derived from the view must
   equal `list_splits` (the existing consistency tests can be extended to compare both views).
5. **GC grace.** Superseded segments and shards are deleted only after a grace period longer than the
   searcher's polling interval, or once the reference count for the generation they belong to drops to
   zero. Invariant: a reader holding generation *g* can always finish reading it. Detection: the
   deletion path is the only place that removes objects and it must be given a generation older than
   the grace period.
6. **Format version and migration.** `view.json` carries a format version. A node that only knows the
   old format must refuse to write into a sharded prefix (the same style of refusal as the existing
   conditional-write probe). Migration: read the legacy single file, emit it as one segment, publish
   the first view, keep the legacy object until the grace period ends.

## Read path

1. Load `view.json` (1 GET). If it is missing, fall back to the legacy single-file layout.
2. Load the segments it lists (bounded by design, target ≤ 3 for a hot index; more for a cold one).
3. Load the deltas with a generation greater than the view's, if any (usually a handful).
4. Merge in memory, applying tombstones last.

Searchers already poll the metastore rather than reading it per query, so the steady-state cost is
`1 + segments + deltas` GETs per poll, independent of N.

## Failure modes to test before implementing

| Failure | Expected behaviour |
| ------- | ------------------ |
| Writer crashes after writing a shard, before the view switch | Shard is a delta; the next writer re-reads and commits it |
| Compactor dies mid-segment | Segment object is incomplete and unreferenced; the lease expires and a new compactor rebuilds it |
| Two compactors race | Pointer CAS fails for the loser, which deletes its own segment |
| Reader holds an old view while GC runs | Grace period covers the reader; deletion is generation-gated |
| Endpoint ignores conditional writes | Same refusal as today (no silent degradation) |

## Alternatives considered

- **PostgreSQL metastore**: row-level updates, no whole-file rewrite, 26 ms writes and zero conflicts in
  the same benchmark where R2 needed 2.14 s. It is the pragmatic answer when a database may be
  operated; the sharded layout preserves the "no extra component" property of object storage.
- **Many small indexes** (hourly/daily): works today with no format change, at the cost of cross-index
  query convenience and more metastore reads.

## Next steps

1. Replay spike: synthesise the split stream for 5·10¹² docs/day and measure bytes rewritten per write,
   conflict rate and cold-read GETs for (a) the current layout, (b) this design, (c) PostgreSQL.
2. Only then implement, behind a format version, with the legacy path kept readable.
