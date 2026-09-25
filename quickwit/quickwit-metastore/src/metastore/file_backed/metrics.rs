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
//! node wrote the same object first, and an exhausted replay means a mutation was dropped after its
//! retry budget ran out.

use quickwit_metrics::{LazyCounter, lazy_counter};

/// Number of metadata writes that lost a compare-and-swap race and were replayed.
pub(super) static CAS_CONFLICTS_TOTAL: LazyCounter = lazy_counter!(
    name: "file_backed_cas_conflicts_total",
    description: "Number of file-backed metastore writes that lost a compare-and-swap race (HTTP \
                  412) and were replayed. Sustained growth means several nodes are writing the \
                  same metastore prefix.",
    subsystem: "metastore",
);

/// Number of mutations that failed after exhausting their replay budget.
pub(super) static CAS_CONFLICTS_EXHAUSTED_TOTAL: LazyCounter = lazy_counter!(
    name: "file_backed_cas_conflicts_exhausted_total",
    description: "Number of file-backed metastore mutations that failed after exhausting their \
                  compare-and-swap replay budget. Any increase means a write was dropped under \
                  contention and should be paged on.",
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
