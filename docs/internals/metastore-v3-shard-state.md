# Splits as manifests, take two: getting the shard state out of the root

> Status: design, revised after an independent design review; step 1 of it is implemented
> (`test_a_publish_that_marked_a_replaced_split_and_lost_the_other_stripe_replays`). The rest is
> tracked as `qw-metastore-manifest-root-contention`, whose issue carries the run and the logs.

## What the run found

Five nodes, one index, 120 s of ingest against a real bucket (Cloudflare R2, 0.81 s round trip from
the node that ran it), then publication and merges. 53 240 documents were acknowledged with zero
ingest errors, the searches ended 1 180 documents *larger* than that, and the logs carry four
`actor-fault`s. They are **two different defects**, not one:

- **three `Publisher` faults** with `precondition failed for index shared: the index metadata was
  modified concurrently` on the first attempt and `splits are not staged` on the retries: the
  compare-and-swap on `v3/root.json` lost its budget, the mutation's splits had already committed,
  and the publisher's retry — a *fresh* request — cannot republish them;
- **one `MergePublisher` fault** — the same hot object as the three above, reached through a mutation
  that carries no checkpoint delta. Its ten markings and the publication of the replacing split
  commit together, so the only replay point after them is `store_root`, and that is where the first
  request failed. What the publisher then reports is that state in two halves: *attempt 1* is the
  first request's own error, `precondition failed for splits <the ten replaced splits>: splits are
  not deletable`, which its internal replay hit when it met the splits it had already marked, and the
  *fresh* retries after it report `splits are not staged` for the replacing split, because the
  publication is durable too. Step 1 below lets the first request's replay finish; step 2 is what a
  fresh retry needs, and it has to feed both steps. (A second node met the same shape as it was being
  shut down, so the four faults undercount the occurrences.)

The three `Publisher` faults need three conditions together, and all three hold:

1. **one mutation, two commit points** — the splits commit per stripe, the root after;
2. **the caller's retry cannot finish it** — publishing an already published split is
   `FailedPrecondition`, asserted by the shared suite for every backend;
3. **the root is a hot object** — every node's commit writes it (checkpoint and shard state live
   there); five nodes commit about every two seconds and a root compare-and-swap costs two round
   trips, 1.6 s here, so a writer's window almost always contains someone else's write. The internal
   budget (16 attempts, backoff capped at 2 s) lost every time; an attempt wins a few percent of the
   time, so a budget that is reliable is minutes long, which the ingest pipeline cannot pay. And the
   write is not even conditional on a change: `store_root` skips the compare-and-swap only when the
   serialized bytes are identical to the ones it read, and the state it serializes contains hash maps
   whose iteration order differs between processes, so the same state from two nodes looks different.
   Making that serialization deterministic is a cheap win of its own; it does not remove this
   condition, because an ordinary publish does change the checkpoint.

The single-object and sharded layouts do not have this failure: their publish and their checkpoint go
into one compare-and-swap, so a lost race means *nothing* happened and the retry is safe.

## The order to fix it in

1. **Make the marked-split step idempotent for a replay** (done): a mutation replayed after part of
   its own commit finds the splits it replaced already marked, which is the state it is asking for.
   A fresh request keeps the strict contract, and a split in any other non-deletable state is still
   an error.
2. **Let the caller say it is a replay.** `publish_splits` already tolerates an already published
   split within one request (attempt > 1). The publisher's retry is a *new* request and cannot say
   so. A field on the request, defaulting to false and set by the pipeline from its second attempt,
   carries that across requests, keeps the shared suite's assertion for everyone else (it does not
   set the field), and is far smaller than moving state. It has to be honoured by *both* tolerances
   — the already published split and the already marked replaced split — or a merge replay stops at
   the second one. It is what makes a half-applied mutation *finishable*; on its own it does not help
   while the root keeps losing, because the checkpoint is the payload of the commit that keeps
   failing.
3. **Measure the read cost of moving the state** on a real bucket before writing the code, and decide
   there whether a reader loads shard objects eagerly or on demand. Today the shard state rides along
   with the root, and the readers that pay for the root are: every read that materialises an index
   (`index_metadata`, `list_indexes_metadata`, the control plane's reload — all through `load_index`),
   the mutations that are not split mutations (shard life cycle, sources, delete tasks — also through
   `load_index`), and a v3 split mutation, which reads the root plus the stripes it touches. Moving
   the state means each of those also reads one object per shard involved, unless step 3 decides to
   load them on demand; step 3 counts both, and not `list_shards` alone.
4. **Then move the state**, if the numbers say the read cost is affordable. That is what removes
   condition 3 rather than making it rarer.

## The design for step 4

The v3 layout already splits the split map out of the root and gives every stripe one writer. This
does the same for the state that changes on every commit.

```
<index_id>/v3/root.json                        metadata, source definitions, delete tasks
<index_id>/v3/shards/<source>/<shard>.json     one shard's state and its checkpoint position
<index_id>/v3/manifest-<stripe>.json           unchanged
<index_id>/v3/wal-<stripe>/…                   unchanged
<index_id>/v3/segments/<stripe>/<bucket>/…     unchanged
```

`<source>` is the source id (already validated as an identifier) and `<shard>` a shard id (a ULID);
both are path-safe. Each object holds what `Shards` holds for that shard today: the `Shard` state
(publish token, doc and split counters, timestamps) and that shard's checkpoint position. For an
ingest-v2 source the checkpoint's partition id *is* the shard id, so a delta names the object
directly — the three-part `queue_id` is the key inside the shard, not the file name.

**What this buys, precisely.** The writers that collide today are the nodes publishing *different
shards of the same source*: they stop colliding, because each writes its own object, the way they
already do not collide on their stripes. It does not make the shard object single-writer in the
absolute sense — the control plane's shard operations (`open_shards`, `acquire_shards`,
`delete_shards`, `prune_shards`) write the same objects, and one publish can touch several shards —
so the honest claim is "contention drops from every commit on one shared object to shard-life-cycle
events on that shard's object", and the acceptance run has to show it. A mutation that fails
*between* the stripe commit and the shard commit is still half-applied; what makes it finishable is
step 2.

**What moves and what does not.** Only sources that use the shard API keep their state in `Shards`;
a source that does not keeps its checkpoint in `metadata.checkpoint`, inside the root, and its
publishes keep writing the root. Either those move too (keyed per source instead of per shard) or
the design says plainly that they are not covered — that decision belongs to step 4.

**Reads.** `list_shards` lists the source's directory and fetches its objects in parallel; a
mutation reads only the shards its request names. Every read that materialises the index —
`index_metadata`, `list_indexes_metadata`, the control plane's reload — also needs the shard state,
so unless step 3 decides to load it on demand, each of them gains a list plus one read per shard.
That, and not only `list_shards`, is the cost step 3 measures and the reason step 4 waits for it.

**Compatibility.** A root written before the change carries the state, so reads take the root's copy
as a base and let the objects override it; writes move the state into the objects and clear the
root's copy. One revision at a time serves an index in this layout (already the rule in the
operating guide); an older node reading a new root would see no shards, which is exactly why that
rule matters here. An index migrates lazily: the first write to a shard writes its object, and until
then the root's copy is what a reader sees.

**Deleted with their owner.** `delete_shards`, `prune_shards` and `delete_source` remove the objects
they own; `delete_index` already removes the whole prefix. Nothing folds or collects these objects
individually.

## Acceptance

- The five-node run, same harness, same bucket, same 120 s of ingest: **zero actor faults and the
  hit count equal to the acknowledged count** (today: 4 faults, +1 180 hits). Step 1 removes one way
  a merge faults — a replay that stopped at the marking step — but a merge that keeps losing the root
  still ends with a fresh retry that cannot finish, and neither do the three `Publisher` faults.
  Step 2 is what makes those finishable; step 4 is what stops them from happening.
- The metastore suite on the three layouts, plus: a replay that meets an already marked replaced
  split (step 1, done), a failpoint on the shard-object commit that a replay carrying the step-2
  field has to survive, an `acquire_shards` racing a publish on the same shard, and — once step 4
  lands — an assertion that a publish whose only persisted change is a shard's state does not write
  `root.json`.
