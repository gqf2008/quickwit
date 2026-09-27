// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Metrics for the shared (compare-and-swap) write path of the file-backed metastore.
//!
//! These counters are the only way to see contention from the outside: every conflict means another
//! node wrote the same object first, and an exhausted replay means a mutation failed after its
//! retry budget ran out.

use quickwit_metrics::{LazyCounter, lazy_counter};

/// Number of metadata writes that lost a compare-and-swap race, whether they were replayed after it
/// or ran out of budget and gave up (those are counted again by [`CAS_CONFLICTS_EXHAUSTED_TOTAL`]).
pub(super) static CAS_CONFLICTS_TOTAL: LazyCounter = lazy_counter!(
    name: "file_backed_cas_conflicts_total",
    description: "Number of file-backed metastore writes that lost a compare-and-swap race (HTTP \
                  412) against the object they were updating. A conflict that does not exhaust the \
                  replay budget is replayed against the fresh object; the ones that run out of \
                  budget are counted again by file_backed_cas_conflicts_exhausted_total. Sustained \
                  growth means several nodes are writing the same metastore prefix.",
    subsystem: "metastore",
);

/// Number of mutations that failed after exhausting their replay budget: the caller got an error,
/// and what the mutation left behind depends on the layout and on which commit ran out (the
/// single-object layout loses the whole write, the sharded one can have written the index root,
/// and the manifest one can have committed part of its splits — the stripes that committed before
/// the one that ran out — or all of them when the index metadata is what ran out).
pub(super) static CAS_CONFLICTS_EXHAUSTED_TOTAL: LazyCounter = lazy_counter!(
    name: "file_backed_cas_conflicts_exhausted_total",
    description: "Number of file-backed metastore mutations that failed after exhausting their \
                  compare-and-swap replay budget, so the caller got an error. Any increase should \
                  be paged on. What such a mutation left behind depends on the layout and on which \
                  commit ran out: the single-object layout lost the whole write; the sharded \
                  layout writes the index root before the slots it touches, so an exhausted slot \
                  commit can leave that earlier write in place; and a manifest-layout mutation \
                  can have committed part of its splits (the stripes that committed before the \
                  one that ran out) or all of them, when it is the index metadata that ran out — \
                  and a replay of that publish finishes it.",
    subsystem: "metastore",
);

/// Number of split state changes a publish being replayed accepted as already applied.
///
/// A mutation is replayed for one of two reasons: a caller's second attempt, whose first one may
/// have committed while its response was lost, or the manifest layout finishing its own commit one
/// stripe at a time. Both reach the mutation with the tolerance asked for; a *fresh* request that
/// publishes an already-published split is still refused. Without the counter the tolerance would
/// be invisible, and a client that keeps re-sending publishes it already got an acknowledgement for
/// would look like normal traffic.
///
/// The checkpoint delta such a replay carries is accepted the same way — applied already is done,
/// not incompatible — and is not counted here: only the split state changes are.
pub(super) static REPLAY_TOLERATED_SPLITS_TOTAL: LazyCounter = lazy_counter!(
    name: "file_backed_replay_tolerated_splits_total",
    description: "Number of split state changes that a replayed publish found already applied and \
                  accepted. The replay is either a caller's second attempt, whose first one \
                  committed before its response was lost, or the manifest layout finishing its own \
                  multi-step commit. Growth with no matching retry in the pipeline means callers are \
                  re-sending publishes that already succeeded. The checkpoint delta a replay carries \
                  is accepted the same way and is not counted here; a fresh request that re-sends an \
                  applied delta is still refused.",
    subsystem: "metastore",
);

/// Number of split slots folded into a segment.
///
/// Folding is what keeps a slot file bounded: it only ever holds the entries written since the last
/// fold. A sharded index whose writes keep flowing but whose fold counter stands still has slot
/// files growing without bound, which is the one maintenance failure of the layout.
pub(super) static SHARD_FOLDS_TOTAL: LazyCounter = lazy_counter!(
    name: "file_backed_shard_folds_total",
    description: "Number of split slots of the sharded layout folded into a segment.",
    subsystem: "metastore",
);

/// Number of folds that failed and will be retried by the next write to the slot.
///
/// The write that triggered the fold is already durable, so this is maintenance falling behind
/// rather than data loss — but a counter that keeps rising with `..._shard_folds_total` standing
/// still means slot files are growing.
pub(super) static SHARD_FOLD_FAILURES_TOTAL: LazyCounter = lazy_counter!(
    name: "file_backed_shard_fold_failures_total",
    description: "Number of folds of a sharded split slot that failed and were left for the next \
                  write to retry. Sustained growth without folds means slot files are growing.",
    subsystem: "metastore",
);

/// Number of reads that caught the split view moving and restarted.
///
/// A fold racing a reader is normal; a rate that tracks the read rate means reads keep landing on a
/// view that is already obsolete, which is a latency problem rather than a correctness one.
pub(super) static SHARD_STALE_VIEW_RETRIES_TOTAL: LazyCounter = lazy_counter!(
    name: "file_backed_shard_stale_view_retries_total",
    description: "Number of reads of a sharded index that caught the split view moving and \
                  restarted from the top.",
    subsystem: "metastore",
);

/// Number of folds of a manifest-layout stripe.
pub(super) static MANIFEST_FOLDS_TOTAL: LazyCounter = lazy_counter!(
    name: "file_backed_manifest_folds_total",
    description: "Number of manifest-layout stripes folded into a segment. Folding is what keeps a \
                  read's segment list short and the WAL tail small.",
    subsystem: "metastore",
);

/// Number of folds of a manifest-layout stripe that failed and were left for the next write.
///
/// The publish that triggered the fold is already durable, so this is maintenance falling behind:
/// a rising rate with the fold counter standing still means WAL tails and read costs are growing.
pub(super) static MANIFEST_FOLD_FAILURES_TOTAL: LazyCounter = lazy_counter!(
    name: "file_backed_manifest_fold_failures_total",
    description: "Number of folds of a manifest-layout stripe that failed and were left for the \
                  next write to retry.",
    subsystem: "metastore",
);

/// Number of times a listing read the manifest to adopt the index and template sets.
///
/// A listing adopts the index set and the template set from the manifest, which is what lets a
/// long-running node see what another node created; the cost is one read of `manifest.json` per
/// such call. This counter is how that cost is measured before deciding whether to add a TTL or a
/// conditional read.
pub(super) static MANIFEST_ADOPTIONS_TOTAL: LazyCounter = lazy_counter!(
    name: "file_backed_manifest_adoptions_total",
    description: "Number of times a file-backed metastore listing read `manifest.json` to adopt the \
                  index set and the template set another node may have changed. One per listing \
                  that walks the index set, plus the template readers.",
    subsystem: "metastore",
);

/// Number of shard objects a reader had to skip.
///
/// A shard object is skipped when it disappeared between the listing and the read (the normal fate
/// of a retired shard), when the node cannot parse it, when its format is unknown to this revision,
/// when it is not named like a shard at all, or when it names another shard than its path does.
/// Skipping keeps one shard's problem from hiding the whole index, so this counter is how an
/// operator sees that it is happening: a rate that tracks the read rate means the prefix holds
/// objects this revision cannot use.
pub(super) static SHARD_OBJECTS_SKIPPED_TOTAL: LazyCounter = lazy_counter!(
    name: "file_backed_shard_objects_skipped_total",
    description: "Number of manifest-layout shard objects a reader skipped because the object had \
                  disappeared, could not be parsed, had an unknown format, was not named like a \
                  shard, or named another shard than its path. The index still loads without them.",
    subsystem: "metastore",
);
