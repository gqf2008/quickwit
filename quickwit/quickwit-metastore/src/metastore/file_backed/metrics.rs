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
