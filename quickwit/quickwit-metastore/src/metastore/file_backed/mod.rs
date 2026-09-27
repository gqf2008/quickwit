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

//! Module for [`FileBackedMetastore`]. It is public so that the crate `quickwit-backward-compat`
//! can import `FileBackedIndex` and run backward-compatibility tests. You should not have to
//! import anything from here directly.

pub mod file_backed_index;
mod file_backed_metastore_factory;
mod index_id_matcher;
mod index_template_matcher;
mod lazy_file_backed_index;
pub(crate) mod manifest;
mod manifest_layout;
mod metrics;
mod sharded_layout;
mod state;
mod store_operations;

use core::fmt;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::future::try_join_all;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use quickwit_common::ServiceStream;
use quickwit_common::uri::Protocol;
use quickwit_config::IndexTemplate;
use quickwit_proto::metastore::{
    AcquireShardsRequest, AcquireShardsResponse, AddSourceRequest, CreateIndexRequest,
    CreateIndexResponse, CreateIndexTemplateRequest, DeleteIndexRequest,
    DeleteIndexTemplatesRequest, DeleteMetricsSplitsRequest, DeleteQuery, DeleteShardsRequest,
    DeleteShardsResponse, DeleteSketchSplitsRequest, DeleteSourceRequest, DeleteSplitsRequest,
    DeleteTask, EmptyResponse, EntityKind, FindIndexTemplateMatchesRequest,
    FindIndexTemplateMatchesResponse, GetClusterIdentityRequest, GetClusterIdentityResponse,
    GetIndexTemplateRequest, GetIndexTemplateResponse, IndexMetadataFailure,
    IndexMetadataFailureReason, IndexMetadataRequest, IndexMetadataResponse, IndexStats,
    IndexTemplateMatch, IndexesMetadataRequest, IndexesMetadataResponse, LastDeleteOpstampRequest,
    LastDeleteOpstampResponse, ListDeleteTasksRequest, ListDeleteTasksResponse,
    ListIndexStatsRequest, ListIndexStatsResponse, ListIndexTemplatesRequest,
    ListIndexTemplatesResponse, ListIndexesMetadataRequest, ListIndexesMetadataResponse,
    ListMetricsSplitsRequest, ListMetricsSplitsResponse, ListShardsRequest, ListShardsResponse,
    ListSketchSplitsRequest, ListSketchSplitsResponse, ListSplitsRequest, ListSplitsResponse,
    ListStaleSplitsRequest, MarkMetricsSplitsForDeletionRequest,
    MarkSketchSplitsForDeletionRequest, MarkSplitsForDeletionRequest, MetastoreError,
    MetastoreResult, MetastoreService, MetastoreServiceStream, OpenShardSubrequest,
    OpenShardsRequest, OpenShardsResponse, PruneShardsRequest, PublishMetricsSplitsRequest,
    PublishSketchSplitsRequest, PublishSplitsRequest, ResetSourceCheckpointRequest,
    StageMetricsSplitsRequest, StageSketchSplitsRequest, StageSplitsRequest, ToggleSourceRequest,
    UpdateIndexRequest, UpdateSourceRequest, UpdateSplitsDeleteOpstampRequest,
    UpdateSplitsDeleteOpstampResponse, serde_utils,
};
use quickwit_proto::types::{IndexId, IndexUid, SplitId};
use quickwit_storage::{ObjectVersion, Storage, StorageErrorKind};
use time::OffsetDateTime;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use tracing::{debug, info, instrument, warn};
use ulid::Ulid;
use uuid::Uuid;

use self::file_backed_index::FileBackedIndex;
pub use self::file_backed_metastore_factory::FileBackedMetastoreFactory;
use self::index_id_matcher::IndexIdMatcher;
use self::index_template_matcher::IndexTemplateMatcher;
use self::lazy_file_backed_index::LazyFileBackedIndex;
use self::manifest::{
    MANIFEST_FILE_NAME, load_manifest_with_version, load_or_create_manifest, save_manifest,
    save_manifest_if_version_matches,
};
use self::sharded_layout::DEFAULT_NUM_SLOTS;
use self::state::MetastoreState;
pub use self::store_operations::IndexLayout;
use self::store_operations::{
    create_index_file, delete_index, index_exists, load_index, load_index_with_version, put_index,
    put_index_if_version_matches,
};

/// Environment variable that lets an operator run a metastore on an endpoint that *ignores*
/// conditional writes. The metastore then falls back to single-writer mode instead of refusing to
/// start.
///
/// Hidden contract: this only covers what the startup probe can prove -- an endpoint that accepts a
/// conditional write it should have rejected. A storage that does not implement conditional writes,
/// or a probe that failed to run, still stops the node: those are a configuration mistake and a
/// connectivity problem, not an unsafe endpoint, and a warning would hide them.
pub const ALLOW_UNSAFE_STORAGE_ENV_KEY: &str = "QW_METASTORE_ALLOW_UNSAFE_STORAGE";

/// Environment variable that makes this node create indexes in the sharded split layout.
///
/// The layout is opt-in per node and per index: a node reads whichever layout an index was created
/// with (`<index_id>/metastore.json` or `<index_id>/v2/root.json`), and this variable only decides
/// what *new* indexes are created with. See
/// [`sharded_layout`](crate::metastore::file_backed::sharded_layout).
pub const SHARDED_LAYOUT_ENV_KEY: &str = "QW_METASTORE_SHARDED_LAYOUT";

/// Environment variable that makes this node create indexes in the manifest layout.
///
/// That layout keeps the split map out of the mutable object: splits live in immutable segments,
/// referenced by striped manifests, so a read costs the query's window and a write costs the
/// batches it publishes (see [`manifest_layout`](crate::metastore::file_backed::manifest_layout)).
/// Like the sharded layout it is opt-in and recorded in the objects, so a node reads an index
/// whichever layout created it.
pub const MANIFEST_LAYOUT_ENV_KEY: &str = "QW_METASTORE_MANIFEST_LAYOUT";

/// Width of a time bucket, in seconds, for indexes created in the manifest layout.
const MANIFEST_LAYOUT_BUCKET_SECS: i64 = 3_600;

/// Number of manifests (compare-and-swap points) an index created in the manifest layout has.
///
/// Striping is a requirement, not an optimisation: with one manifest the spike measured the write
/// rate a 5·10¹² documents/day index needs failing away from a same-zone round trip, and two
/// conflicts per publish. The count is also a sizing rule, measured on a real bucket: writers that
/// hash to the same stripe contend, so four writers saw 0 conflicts per publish on eight stripes,
/// twelve writers 0.68 with eight stripes and 0.03 with 32 — each conflict costing a replay of the
/// publish. Those counts come from split ids the test harness generates, which do not hash like the
/// ULIDs a deployment publishes, so they are a lower bound; the table, and what each number
/// measures, is in `docs/operating/shared-metastore.md`. The default leaves room for a burst of
/// writers over the stripe count, because a read fetches all the manifests at once and pays for
/// them in one round trip, not one per stripe.
const MANIFEST_LAYOUT_NUM_STRIPES: usize = 32;

/// Environment variable that sets the stripe count of indexes this node creates in the manifest
/// layout. Keep it at or above the number of nodes that publish into one index.
pub const MANIFEST_LAYOUT_STRIPES_ENV_KEY: &str = "QW_METASTORE_MANIFEST_STRIPES";

/// Environment variable that makes the metastore test suite run on the sharded layout.
///
/// The suite is generic over the metastore implementation, so the layout cannot be a type parameter
/// without a wrapper for every method. Running the same suite twice, once with this variable set,
/// gives the sharded layout the same coverage as the historical one.
pub const SHARDED_LAYOUT_TEST_ENV_KEY: &str = "QW_METASTORE_TEST_SHARDED_LAYOUT";

/// Environment variable that makes the metastore test suite run on the manifest layout.
pub const MANIFEST_LAYOUT_TEST_ENV_KEY: &str = "QW_METASTORE_TEST_MANIFEST_LAYOUT";

/// Number of times a mutation is replayed before the metastore gives up on a compare-and-swap race.
///
/// Raised from 8 to 16: each attempt costs a round trip to the object store, so on a cross-region
/// endpoint (~2 s) the old budget only covered a few seconds of contention. With two writers
/// publishing into the same index, a two-minute run exhausted the 8 attempts 11 times and faulted
/// the publisher; the same run finishes with no exhausted replay at 16 attempts.
const DISTRIBUTED_MAX_ATTEMPTS: usize = 16;

/// Upper bound of the delay between two replays of a mutation.
///
/// Raised from 500 ms to 2 s together with [`DISTRIBUTED_MAX_ATTEMPTS`]: the budget has to cover
/// several object-store round trips, and a contended index must still not stall a request for long.
/// The worst case (16 attempts) stays under 20 s.
const DISTRIBUTED_RETRY_BACKOFF_CAP_MILLIS: u64 = 2_000;

/// Delay before replaying a mutation that lost a compare-and-swap race.
///
/// Exponential with a cap, plus jitter so that two nodes that keep colliding do not stay in
/// lockstep. The cap keeps a contended index from stalling a request for long.
fn distributed_retry_backoff(attempt: usize) -> Duration {
    // Double the delay per attempt, and cap it so a contended index cannot stall a request for
    // long. Jitter keeps two nodes that keep colliding out of lockstep.
    let exponent = attempt.min(31) as u32;
    let base_millis = (5u64 << exponent).min(DISTRIBUTED_RETRY_BACKOFF_CAP_MILLIS);
    let jitter_millis = rand::random::<u64>() % 10;
    Duration::from_millis(base_millis + jitter_millis)
}

/// Returns whether `error` is a compare-and-swap conflict, i.e. another node wrote first.
fn is_manifest_conflict(error: &MetastoreError) -> bool {
    matches!(error, MetastoreError::FailedPrecondition { .. })
}

/// Records one lost compare-and-swap race, and whether the caller is giving up because of it.
fn record_cas_conflict(giving_up: bool) {
    metrics::CAS_CONFLICTS_TOTAL.inc();
    if giving_up {
        metrics::CAS_CONFLICTS_EXHAUSTED_TOTAL.inc();
    }
}

/// Stripe count for indexes this node creates: the default, or what the operator asked for.
fn manifest_layout_num_stripes() -> usize {
    std::env::var(MANIFEST_LAYOUT_STRIPES_ENV_KEY)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|num_stripes| *num_stripes > 0)
        .unwrap_or(MANIFEST_LAYOUT_NUM_STRIPES)
}

/// Window to prune with, derived from a query's time bounds.
///
/// The metastore's predicate keeps a split when its time range overlaps the query's, so the window
/// has to contain every such split and may contain more: the caller filters again afterwards.
/// Bounds are inclusive on both sides in `FilterRange`, hence the off-by-one adjustments.
fn pruning_window(query: &ListSplitsQuery) -> (i64, i64) {
    let window_start = match query.time_range.start {
        Bound::Unbounded => i64::MIN,
        Bound::Included(start) => start,
        // A split whose end is strictly after `start` has an end of at least `start + 1`.
        Bound::Excluded(start) => start.saturating_add(1),
    };
    let window_end = match query.time_range.end {
        Bound::Unbounded => i64::MAX,
        Bound::Included(end) => end.saturating_add(1),
        Bound::Excluded(end) => end,
    };
    (window_start, window_end)
}

/// Builds the error raised when the metastore storage cannot safely be shared.
fn unsafe_storage_error(storage: &dyn Storage, reason: &str) -> MetastoreError {
    MetastoreError::Internal {
        message: "the metastore storage does not enforce conditional writes".to_string(),
        cause: format!(
            "`{}` cannot back a shared metastore because {reason}. Sharing a metastore prefix \
             with such a storage would silently lose updates. Use a storage that enforces \
             preconditions (AWS S3, Cloudflare R2, MinIO), or set \
             {ALLOW_UNSAFE_STORAGE_ENV_KEY}=true to run this node in single-writer mode.",
            storage.uri()
        ),
    }
}

/// Builds the error raised when the storage cannot express a conditional write at all.
///
/// Hidden contract: [`ALLOW_UNSAFE_STORAGE_ENV_KEY`] deliberately does **not** cover this case. The
/// variable is for an endpoint that accepts a conditional write it should have rejected -- an
/// unsafe *endpoint*, which the operator has proven they know about. A storage that does not
/// implement conditional writes is a configuration mistake, and answering it with a warning plus
/// single-writer mode would turn "this node cannot be shared" into a line in a startup log.
fn storage_without_conditional_writes_error(storage: &dyn Storage) -> MetastoreError {
    MetastoreError::Internal {
        message: "the metastore storage does not implement conditional writes".to_string(),
        cause: format!(
            "`{}` cannot back a metastore shared by several nodes because it does not implement \
             conditional writes, so there is no precondition to enforce. Point the metastore at a \
             storage that implements them (AWS S3, Cloudflare R2, MinIO), or keep a single writer \
             and use a `file://` metastore. {ALLOW_UNSAFE_STORAGE_ENV_KEY}=true does not apply here: \
             it covers an endpoint that accepts a conditional write it should reject, not a storage \
             that cannot express one.",
            storage.uri()
        ),
    }
}

/// What the startup probe learned about the storage backing a file-backed metastore.
///
/// The four outcomes are kept apart because [`ALLOW_UNSAFE_STORAGE_ENV_KEY`] only covers one of
/// them. Downgrading any other failure to single-writer mode would hide a configuration mistake or
/// a connectivity problem behind a warning, so the node refuses to start instead.
enum ConditionalWriteSupport {
    /// The storage rejected a conditional write whose precondition did not hold.
    Enforced,
    /// The storage accepted a conditional write it should have rejected, and would therefore lose
    /// updates if several nodes shared the prefix.
    EndpointIgnoresPreconditions(String),
    /// The storage does not implement conditional writes.
    NotImplemented,
    /// The probe could not run to completion.
    ProbeFailed(MetastoreError),
}

impl ConditionalWriteSupport {
    /// The error to return when this outcome means the node may not share the prefix.
    fn into_metastore_error(self, storage: &dyn Storage) -> MetastoreError {
        match self {
            Self::Enforced => {
                unreachable!("a storage that enforces preconditions has no error to report")
            }
            Self::EndpointIgnoresPreconditions(reason) => unsafe_storage_error(storage, &reason),
            Self::NotImplemented => storage_without_conditional_writes_error(storage),
            Self::ProbeFailed(error) => error,
        }
    }
}
use super::{
    AddSourceRequestExt, CreateIndexRequestExt, IndexMetadataResponseExt,
    IndexesMetadataResponseExt, ListIndexesMetadataResponseExt, ListParquetSplitsRequestExt,
    ListParquetSplitsResponseExt, ListSplitsRequestExt, ListSplitsResponseExt,
    PublishParquetSplitsRequestExt, PublishSplitsRequestExt, STREAM_SPLITS_CHUNK_SIZE,
    StageParquetSplitsRequestExt, StageSplitsRequestExt, UpdateIndexRequestExt,
    UpdateSourceRequestExt,
};
use crate::checkpoint::IndexCheckpointDelta;
use crate::{IndexMetadata, ListSplitsQuery, MetastoreServiceExt, Split, SplitState};

/// Status of an index tracked by the metastore.
pub(crate) enum LazyIndexStatus {
    /// The index is being created but its metadata have yet to be written on the storage.
    Creating,
    /// The index is created and available.
    Active(LazyFileBackedIndex),
    /// The index is being deleted and but its index metadata file has not yet been removed from
    /// storage.
    Deleting,
}

#[derive(Debug)]
pub(crate) enum MutationOccurred<T> {
    Yes(T),
    No(T),
}

impl From<bool> for MutationOccurred<()> {
    fn from(mutation_occurred: bool) -> Self {
        if mutation_occurred {
            Self::Yes(())
        } else {
            Self::No(())
        }
    }
}

/// A metastore implementation that stores all the metadata associated to each index
/// into as many files and stores a map of indexes
/// (index_id, index_status) in a dedicated file `manifest.json`.
///
/// A `LazyIndexStatus` describes the lifecycle of an index: `LazyIndexStatus::Creating` and
/// `LazyIndexStatus::Deleting` are transitioning states that indicates that the index is not
/// yet available. On the contrary, the `LazyIndexStatus::Active` status indicates the index is
/// ready to be fetched and updated.
///
/// Transitioning states are useful to track inconsistencies between the in-memory and on-disk data
/// structures when error(s) occur during index creations and deletions:
/// - `Creating` indicates that the metastore updated the manifest file with this state but not yet
///   the index metadata file;
/// - `Deleting` indicates that the metastore updated the manifest file with this state but the
///   index metadata file is not yet deleted.
///
/// !!! Important note: the indexes map manifest does not
/// guarantee exhaustivity: an index metadata file can be on the storage
/// but not present in the states map. As the map is incomplete, the metastore
/// does not rely on it to check index existence, this leads to following
/// implementations:
/// - on creation, the metastore always checks if an index metadata file is already present on the
///   storage even if the index is not in the indexes map;
/// - on get/update of an index, same story, the metastore checks if index is on the storage and if
///   present, the index is loaded in the map and returned /modified;
/// - on deletion, same story, the metastore deletes an index metadata file present on the storage
///   even if the index is not in the map.
///
/// !!! Important note 2: it is strongly advised to restrict the `FileBackedMetastore`
/// usage to the following use cases:
/// - testing;
/// - single-node environment;
/// - multiple-nodes environment with only one writer and readers. In this case, you must be very
///   cautious and ensure that your readers are really readers.
#[derive(Clone)]
pub struct FileBackedMetastore {
    state: Arc<RwLock<MetastoreState>>,
    storage: Arc<dyn Storage>,
    polling_interval_opt: Option<Duration>,
    /// Whether several nodes may write this metastore concurrently.
    ///
    /// Set for S3-compatible storage (AWS S3, Cloudflare R2, MinIO), which supports the
    /// conditional writes this mode is built on: reload the file before every mutation and
    /// write it back with `If-Match`. Local files, RAM, and the GCS/Azure backends stay
    /// single-node, because claiming to share without conditional writes would turn a lost
    /// update into a silent one.
    distributed: bool,
    /// Layout new indexes are created with. See [`IndexLayout`].
    index_layout: IndexLayout,
}

impl fmt::Debug for FileBackedMetastore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileBackedMetastore")
            .field("storage_uri", self.storage.uri())
            .field("polling_interval_opt", &self.polling_interval_opt)
            .finish()
    }
}

impl FileBackedMetastore {
    /// Creates a [`FileBackedMetastore`] for tests.
    #[doc(hidden)]
    pub fn for_test(storage: Arc<dyn Storage>) -> Self {
        Self {
            state: Default::default(),
            storage,
            polling_interval_opt: None,
            distributed: false,
            index_layout: IndexLayout::SingleObject,
        }
    }

    /// Returns whether this metastore is in distributed mode (see [`Self::distributed`]).
    pub fn is_distributed(&self) -> bool {
        self.distributed
    }

    /// Forces distributed mode on or off.
    ///
    /// Production code derives this from the metastore URI protocol; tests use it to exercise the
    /// compare-and-swap path against in-memory storage.
    pub fn set_distributed(&mut self, distributed: bool) {
        self.distributed = distributed;
    }

    /// Returns the layout new indexes are created with.
    pub fn index_layout(&self) -> IndexLayout {
        self.index_layout
    }

    /// Sets the layout new indexes are created with.
    ///
    /// Production code reads it once from [`SHARDED_LAYOUT_ENV_KEY`]; tests use it to exercise both
    /// layouts against the same in-memory storage.
    pub fn set_index_layout(&mut self, index_layout: IndexLayout) {
        self.index_layout = index_layout;
    }

    /// Sets the polling interval.
    ///
    /// Only newly accessed indexes will be affected by the change of this setting.
    pub fn set_polling_interval(&mut self, polling_interval_opt: Option<Duration>) {
        self.polling_interval_opt = polling_interval_opt;
    }

    /// Verifies that the storage really enforces conditional writes.
    ///
    /// The shared write path is only safe when the endpoint rejects a write whose precondition does
    /// not hold. Some S3-compatible implementations accept `If-None-Match: *` and overwrite the
    /// object anyway (localstack 3.5.0 does), which turns compare-and-swap into an unconditional
    /// write and loses updates silently -- the one outcome this mode must never produce. A normal
    /// write cannot reveal that, so we check it once, with a throwaway object, before serving any
    /// metadata.
    async fn check_storage_enforces_conditional_writes(
        storage: &dyn Storage,
    ) -> ConditionalWriteSupport {
        let probe_path: std::path::PathBuf =
            format!(".quickwit-conditional-write-probe-{}", Ulid::new()).into();
        let check_result = match storage
            .put_if_absent(&probe_path, Box::new(b"probe".to_vec()))
            .await
        {
            Ok(_) => {
                match storage
                    .put_if_absent(&probe_path, Box::new(b"probe again".to_vec()))
                    .await
                {
                    Err(error) if error.kind() == StorageErrorKind::PreconditionFailed => {
                        ConditionalWriteSupport::Enforced
                    }
                    Err(error) => ConditionalWriteSupport::ProbeFailed(MetastoreError::Internal {
                        message: format!(
                            "failed to probe the metastore storage located at `{}`",
                            storage.uri()
                        ),
                        cause: error.to_string(),
                    }),
                    Ok(_) => ConditionalWriteSupport::EndpointIgnoresPreconditions(
                        "the endpoint accepted a second write carrying `If-None-Match: *`"
                            .to_string(),
                    ),
                }
            }
            Err(error) if error.kind() == StorageErrorKind::Unsupported => {
                ConditionalWriteSupport::NotImplemented
            }
            Err(error) => ConditionalWriteSupport::ProbeFailed(MetastoreError::Internal {
                message: format!(
                    "failed to probe the metastore storage located at `{}`",
                    storage.uri()
                ),
                cause: error.to_string(),
            }),
        };
        // Best effort: never leave the probe object behind, and never fail startup over its
        // cleanup.
        if let Err(error) = storage.delete(&probe_path).await {
            warn!(
                path = %probe_path.display(),
                "failed to delete the conditional-write probe object: {error}"
            );
        }
        check_result
    }

    /// Return the underlying storage.
    ///
    /// This is only build in tests to verify the metastore did indeed store what it should.
    /// It shouldn't be relied uppon elsewhere as to not break abstractions.
    #[cfg(test)]
    pub fn storage(&self) -> Arc<dyn Storage> {
        self.storage.clone()
    }

    /// Compare-and-swap conflicts recorded so far by this process.
    ///
    /// Exposed for the integration measurements that run against a real endpoint: a conflict
    /// counter is the only way to tell contention from latency there, and the metric registry
    /// is not part of the metastore's API.
    #[cfg(any(test, feature = "ci-test", feature = "testsuite"))]
    #[doc(hidden)]
    pub fn cas_conflicts_total(&self) -> u64 {
        metrics::CAS_CONFLICTS_TOTAL.get()
    }

    /// Creates a [`FileBackedMetastore`] for a specified storage, immediately loading the manifest
    /// file.
    pub async fn try_new(
        storage: Arc<dyn Storage>,
        polling_interval_opt: Option<Duration>,
    ) -> MetastoreResult<Self> {
        let allow_unsafe_storage =
            quickwit_common::get_bool_from_env(ALLOW_UNSAFE_STORAGE_ENV_KEY, false);
        Self::try_new_with_options(storage, polling_interval_opt, allow_unsafe_storage).await
    }

    /// Same as [`Self::try_new`], with the conditional-write policy passed in instead of read from
    /// the environment.
    ///
    /// `allow_unsafe_storage` lets the metastore run in single-writer mode on a storage that
    /// ignores conditional writes; without it, such a storage is refused (see
    /// [`Self::check_storage_enforces_conditional_writes`]).
    pub async fn try_new_with_options(
        storage: Arc<dyn Storage>,
        polling_interval_opt: Option<Duration>,
        allow_unsafe_storage: bool,
    ) -> MetastoreResult<Self> {
        let manifest = load_or_create_manifest(&*storage).await?;
        let state =
            MetastoreState::try_from_manifest(storage.clone(), manifest, polling_interval_opt)?;
        // Sharing a metastore safely needs conditional writes, and today only the S3-compatible
        // backend implements them (which covers AWS S3, Cloudflare R2 and MinIO). Azure and GCS
        // keep the single-node path: entering distributed mode without conditional writes would
        // make every metadata write fail instead of merely being unsafe, and failing is not a
        // better answer than not claiming a capability we do not have.
        let mut distributed = storage.uri().protocol() == Protocol::S3;
        if distributed {
            match Self::check_storage_enforces_conditional_writes(&*storage).await {
                ConditionalWriteSupport::Enforced => {}
                ConditionalWriteSupport::EndpointIgnoresPreconditions(reason)
                    if allow_unsafe_storage =>
                {
                    warn!(
                        metastore_uri = %storage.uri(),
                        "the metastore storage does not enforce conditional writes; \
                         {ALLOW_UNSAFE_STORAGE_ENV_KEY}=true, so this node runs in single-writer \
                         mode and must not share its metastore prefix: {reason}"
                    );
                    distributed = false;
                }
                unsupported => return Err(unsupported.into_metastore_error(&*storage)),
            }
        }
        if distributed {
            info!(
                metastore_uri = %storage.uri(),
                "file-backed metastore is shared between nodes; metadata writes are \
                 compare-and-swap and may be replayed when another node writes first"
            );
        } else {
            debug!(
                metastore_uri = %storage.uri(),
                "file-backed metastore is single-node; metadata writes assume one writer"
            );
        }
        // The sharded layout is built on conditional writes, so it only makes sense where the
        // distributed path runs; asking for it on a storage that cannot compare-and-swap would
        // publish indexes this node cannot write back.
        let manifest_layout_requested =
            quickwit_common::get_bool_from_env(MANIFEST_LAYOUT_ENV_KEY, false);
        let sharded_layout_requested =
            quickwit_common::get_bool_from_env(SHARDED_LAYOUT_ENV_KEY, false);
        if manifest_layout_requested && sharded_layout_requested {
            return Err(MetastoreError::Internal {
                message: "two metastore layouts were requested at once".to_string(),
                cause: format!(
                    "{MANIFEST_LAYOUT_ENV_KEY} and {SHARDED_LAYOUT_ENV_KEY} cannot both be true"
                ),
            });
        }
        let index_layout = if manifest_layout_requested || sharded_layout_requested {
            if !distributed {
                return Err(MetastoreError::Internal {
                    message: "this metastore layout requires conditional writes".to_string(),
                    cause: format!(
                        "a split layout was requested but `{}` does not support the \
                         compare-and-swap this layout is built on",
                        storage.uri()
                    ),
                });
            }
            if manifest_layout_requested {
                IndexLayout::ManifestSegments {
                    bucket_secs: MANIFEST_LAYOUT_BUCKET_SECS,
                    num_stripes: manifest_layout_num_stripes(),
                }
            } else {
                IndexLayout::Sharded {
                    num_slots: DEFAULT_NUM_SLOTS,
                }
            }
        } else {
            IndexLayout::SingleObject
        };
        let metastore = Self {
            state: Arc::new(RwLock::new(state)),
            storage,
            polling_interval_opt,
            distributed,
            index_layout,
        };
        Ok(metastore)
    }

    async fn mutate<T>(
        &self,
        index_uid: &IndexUid,
        mutate_fn: impl Fn(&mut FileBackedIndex) -> MetastoreResult<MutationOccurred<T>>,
    ) -> MetastoreResult<T> {
        self.mutate_replaying(index_uid, false, |index, _is_replay| mutate_fn(index))
            .await
    }

    /// Same as [`Self::mutate`], for a mutation whose steps a replay has to tolerate.
    ///
    /// `caller_replay` says the caller is replaying a request of its own (the pipeline's second
    /// attempt after a lost response); an attempt after the first one is a replay too, because the
    /// previous attempt may have applied part of the mutation before it failed. The closure is told
    /// which of the two it is, so a publish can accept the splits and the checkpoint delta an
    /// earlier attempt already applied instead of failing on them.
    async fn mutate_replaying<T>(
        &self,
        index_uid: &IndexUid,
        caller_replay: bool,
        mutate_fn: impl Fn(&mut FileBackedIndex, bool) -> MetastoreResult<MutationOccurred<T>>,
    ) -> MetastoreResult<T> {
        if self.distributed {
            return self
                .mutate_distributed_replaying(index_uid, caller_replay, mutate_fn)
                .await;
        }
        let index_id = &index_uid.index_id;
        let mut locked_index = self.get_locked_index(index_id).await?;
        if locked_index.index_uid() != index_uid {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_id.to_string(),
            }));
        }
        let mut index = locked_index.clone();

        let value = match mutate_fn(&mut index, caller_replay)? {
            MutationOccurred::Yes(value) => value,
            MutationOccurred::No(value) => {
                return Ok(value);
            }
        };
        locked_index.set_recently_modified();

        let put_result = put_index(&*self.storage, &index).await;
        match put_result {
            Ok(()) => {
                *locked_index = index;
                Ok(value)
            }
            Err(error) => {
                // For some of the error type here, we cannot know for sure
                // whether the content was written or not.
                //
                // Just to be sure, let's discard the cache.
                let mut state_wlock_guard = self.state.write().await;

                // At this point, we hold both locks.
                state_wlock_guard.indexes.insert(
                    index_id.to_string(),
                    LazyIndexStatus::Active(LazyFileBackedIndex::new(
                        self.storage.clone(),
                        index_id.to_string(),
                        self.polling_interval_opt,
                        None,
                    )),
                );
                locked_index.discarded = true;
                Err(error)
            }
        }
    }

    /// Compare-and-swap variant of [`Self::mutate`] for nodes that share one metastore prefix.
    ///
    /// The cached index is deliberately bypassed: another node may have published splits since we
    /// last read, and a mutation applied to a stale snapshot would either drop that work or
    /// resurrect splits it deleted. Every attempt therefore re-reads the index together with its
    /// version and writes back with `If-Match`; losing that race means somebody else wrote first,
    /// so we reload and replay instead of overwriting them.
    async fn mutate_distributed_replaying<T>(
        &self,
        index_uid: &IndexUid,
        caller_replay: bool,
        mutate_fn: impl Fn(&mut FileBackedIndex, bool) -> MetastoreResult<MutationOccurred<T>>,
    ) -> MetastoreResult<T> {
        let index_id = &index_uid.index_id;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let (mut index, version_opt) =
                load_index_with_version(&*self.storage, index_id).await?;
            if index.index_uid() != index_uid {
                return Err(MetastoreError::NotFound(EntityKind::Index {
                    index_id: index_id.to_string(),
                }));
            }
            let Some(version) = version_opt else {
                return Err(MetastoreError::Internal {
                    message: "distributed metastore requires a storage that versions objects"
                        .to_string(),
                    cause: format!(
                        "storage `{}` returned no object version for `{index_id}`",
                        self.storage.uri()
                    ),
                });
            };
            // Attempt 2 onwards replays an attempt that may have committed part of the mutation
            // before it failed, so the mutation is told to tolerate what is already applied.
            let is_replay = attempt > 1 || caller_replay;
            let value = match mutate_fn(&mut index, is_replay)? {
                MutationOccurred::Yes(value) => value,
                MutationOccurred::No(value) => {
                    // Nothing to write, but the read still refreshed our cached view.
                    self.replace_cached_index(index_id, index).await;
                    return Ok(value);
                }
            };
            match put_index_if_version_matches(&*self.storage, &mut index, &version).await {
                Ok(_) => {
                    // The snapshot we wrote is ours, but winning the compare-and-swap does not
                    // prove nobody else wrote in between: with the sharded layout in particular
                    // two writers usually touch different slots and both win. Caching the snapshot
                    // we happen to hold would then serve a view that is missing the other writer's
                    // splits until the next poll. Drop it instead, so the next read reloads from
                    // the storage, which is what the poller would have done anyway.
                    self.discard_cached_index(index_id).await;
                    return Ok(value);
                }
                Err(MetastoreError::FailedPrecondition { .. })
                    if attempt < DISTRIBUTED_MAX_ATTEMPTS =>
                {
                    debug!(
                        index_id,
                        attempt, "index metadata changed concurrently, replaying the mutation"
                    );
                    record_cas_conflict(false);
                    tokio::time::sleep(distributed_retry_backoff(attempt)).await;
                    continue;
                }
                Err(error) => {
                    if is_manifest_conflict(&error) {
                        // The replay budget ran out on the last attempt.
                        record_cas_conflict(true);
                    }
                    self.discard_cached_index(index_id).await;
                    return Err(error);
                }
            }
        }
    }

    /// Replaces the cached view of an index with the state that was just read or written.
    async fn replace_cached_index(&self, index_id: &str, index: FileBackedIndex) {
        let mut state_wlock_guard = self.state.write().await;
        if matches!(
            state_wlock_guard.indexes.get(index_id),
            Some(LazyIndexStatus::Active(_))
        ) {
            state_wlock_guard.indexes.insert(
                index_id.to_string(),
                LazyIndexStatus::Active(LazyFileBackedIndex::new(
                    self.storage.clone(),
                    index_id.to_string(),
                    self.polling_interval_opt,
                    Some(index),
                )),
            );
        }
    }

    /// Drops the cached view of an index, so that the next read reloads it from storage.
    async fn discard_cached_index(&self, index_id: &str) {
        let mut state_wlock_guard = self.state.write().await;
        if matches!(
            state_wlock_guard.indexes.get(index_id),
            Some(LazyIndexStatus::Active(_))
        ) {
            state_wlock_guard.indexes.insert(
                index_id.to_string(),
                LazyIndexStatus::Active(LazyFileBackedIndex::new(
                    self.storage.clone(),
                    index_id.to_string(),
                    self.polling_interval_opt,
                    None,
                )),
            );
        }
    }

    /// Adopts what the manifest has and this node does not: the indexes, and the index templates
    /// that go with them.
    ///
    /// A node reloads the whole manifest when it writes, so a node that never writes would never
    /// list an index another node created (nor use a template it added): the janitor and the
    /// listing callers of a long-running node would not see it, and the requests it never plans for
    /// would fail. Listing what the metastore holds is exactly the moment to notice one, and the
    /// manifest is small.
    ///
    /// The manifest is the index set and the template set, so both are taken from it whole: an
    /// index or a template another node deleted stops being listed here, and one another node
    /// changed takes its new state. An index that is active keeps the object it already has.
    ///
    /// Single-node mode is the no-op it is for every other reload: the cached state is the only
    /// state there is.
    async fn adopt_indexes_from_manifest(&self) -> MetastoreResult<()> {
        if !self.distributed {
            return Ok(());
        }
        let mut state_wlock_guard = self.state.write().await;
        // The manifest is read under the lock: a writer reloads and rewrites it while holding it,
        // so reading before the lock could rebuild the state from a manifest that is
        // already gone.
        metrics::MANIFEST_ADOPTIONS_TOTAL.inc();
        let (manifest, _) = load_manifest_with_version(&*self.storage).await?;
        // The manifest is the index set, so this rebuilds it rather than only adding what is
        // missing: an index this node listed while another node was still creating it would
        // otherwise stay `Creating` for ever (the create finishes in the manifest, and adding only
        // missing keys never replaces what is already there), and an index another node deleted
        // would stay in the listing. An index that is already active keeps the object it has, so
        // the rebuild does not drop a cache or restart its poller; the layout cache and the
        // metastore's identity are untouched.
        let mut indexes: HashMap<IndexId, LazyIndexStatus> =
            HashMap::with_capacity(manifest.indexes.len());
        for (index_id, index_status) in manifest.indexes {
            let lazy_index_status = match index_status {
                manifest::IndexStatus::Creating => LazyIndexStatus::Creating,
                manifest::IndexStatus::Deleting => LazyIndexStatus::Deleting,
                manifest::IndexStatus::Active => {
                    match state_wlock_guard.indexes.remove(&index_id) {
                        Some(active @ LazyIndexStatus::Active(_)) => active,
                        _ => LazyIndexStatus::Active(LazyFileBackedIndex::new(
                            self.storage.clone(),
                            index_id.clone(),
                            self.polling_interval_opt,
                            None,
                        )),
                    }
                }
            };
            indexes.insert(index_id, lazy_index_status);
        }
        // Templates travel in the same manifest, so a node that never wrote would also miss a
        // template another node added — or keep the old patterns of one it overwrote, which is what
        // the control plane creates indexes from. They are the manifest's set, like the indexes:
        // replacing them wholesale is what drops one another node deleted.
        let templates = manifest.templates;
        let templates_changed = templates != state_wlock_guard.templates;
        let template_matcher = if templates_changed {
            Some(IndexTemplateMatcher::try_from_index_templates(
                templates.values(),
            )?)
        } else {
            None
        };
        // Everything is built before anything is assigned, so a failure leaves the state as it was.
        state_wlock_guard.indexes = indexes;
        if templates_changed {
            state_wlock_guard.templates = templates;
            state_wlock_guard.template_matcher = template_matcher.expect("built above");
        }
        Ok(())
    }

    /// Reloads the manifest into `state_wlock_guard` when this metastore is shared with other
    /// nodes.
    ///
    /// In single-node mode the cached state is authoritative and this is a no-op. In distributed
    /// mode the cached view may already be behind another node's write, and a decision taken on it
    /// would be written back as if it were current.
    ///
    /// On success `manifest_version_opt` holds the version the next write has to match.
    async fn reload_manifest_if_distributed(
        &self,
        state_wlock_guard: &mut MetastoreState,
        manifest_version_opt: &mut Option<ObjectVersion>,
    ) -> MetastoreResult<()> {
        if !self.distributed {
            return Ok(());
        }
        let (manifest, loaded_version_opt) = load_manifest_with_version(&*self.storage).await?;
        let Some(version) = loaded_version_opt else {
            return Err(MetastoreError::Internal {
                message: "distributed metastore requires a storage that versions objects"
                    .to_string(),
                cause: format!(
                    "storage `{}` returned no version for the manifest",
                    self.storage.uri()
                ),
            });
        };
        *state_wlock_guard = MetastoreState::try_from_manifest(
            self.storage.clone(),
            manifest,
            self.polling_interval_opt,
        )?;
        *manifest_version_opt = Some(version);
        Ok(())
    }

    /// Writes the manifest held by `state_wlock_guard`.
    ///
    /// In distributed mode the write is a compare-and-swap against the version recorded by
    /// [`Self::reload_manifest_if_distributed`]; losing the race returns
    /// [`MetastoreError::FailedPrecondition`] so the caller can reload and replay.
    async fn save_manifest_cas(
        &self,
        state_wlock_guard: &MetastoreState,
        manifest_version_opt: &mut Option<ObjectVersion>,
        attempt: usize,
    ) -> MetastoreResult<()> {
        let manifest = state_wlock_guard.as_manifest();
        if !self.distributed {
            return save_manifest(&*self.storage, &manifest).await;
        }
        let version = manifest_version_opt
            .take()
            .ok_or_else(|| MetastoreError::Internal {
                message: "distributed metastore requires a manifest version".to_string(),
                cause: "the manifest was not reloaded before being written".to_string(),
            })?;
        match save_manifest_if_version_matches(&*self.storage, &manifest, &version).await {
            Ok(new_version) => {
                *manifest_version_opt = new_version;
                Ok(())
            }
            Err(error) => {
                if is_manifest_conflict(&error) {
                    record_cas_conflict(attempt >= DISTRIBUTED_MAX_ATTEMPTS);
                }
                Err(error)
            }
        }
    }

    /// Returns whether the index file already on the storage was written by this very
    /// [`FileBackedMetastore::create_index`] request.
    ///
    /// `create_index` replays its whole attempt when it loses a manifest compare-and-swap, and the
    /// index file written during the previous attempt is still there on the replay. That file must
    /// not be mistaken for another node's index: it carries the incarnation id generated for this
    /// request, which is what tells the two apart.
    async fn index_file_matches_request(
        &self,
        index_id: &str,
        index_uid: &IndexUid,
    ) -> MetastoreResult<bool> {
        match load_index(&*self.storage, index_id).await {
            Ok(stored_index) => Ok(*stored_index.index_uid() == *index_uid),
            // The file vanished between the existence check and this read; the caller retries.
            Err(MetastoreError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Compare-and-swap variant of `delete_index` for nodes that share one metastore prefix.
    ///
    /// Same two-step shape as the single-node version (mark the index `Deleting`, delete its file,
    /// then drop it from the manifest), but every manifest write is a compare-and-swap against a
    /// freshly loaded manifest, and a lost race replays the whole operation.
    async fn delete_index_distributed(
        &self,
        request: &DeleteIndexRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_id = request.index_uid().index_id.clone();
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut state_wlock_guard = self.state.write().await;
            let mut manifest_version_opt = None;
            self.reload_manifest_if_distributed(&mut state_wlock_guard, &mut manifest_version_opt)
                .await?;

            // If the index is neither in the manifest nor on the storage, it does not exist.
            if !state_wlock_guard.indexes.contains_key(&index_id)
                && !index_exists(&*self.storage, &index_id).await?
            {
                return Err(MetastoreError::NotFound(EntityKind::Index {
                    index_id: index_id.clone(),
                }));
            }
            state_wlock_guard
                .indexes
                .insert(index_id.clone(), LazyIndexStatus::Deleting);

            if let Err(error) = self
                .save_manifest_cas(&state_wlock_guard, &mut manifest_version_opt, attempt)
                .await
            {
                state_wlock_guard.indexes.remove(&index_id);
                if is_manifest_conflict(&error) && attempt < DISTRIBUTED_MAX_ATTEMPTS {
                    drop(state_wlock_guard);
                    tokio::time::sleep(distributed_retry_backoff(attempt)).await;
                    continue;
                }
                return Err(error);
            }

            let delete_result = delete_index(&*self.storage, &index_id).await;

            if matches!(
                &delete_result,
                Ok(()) | Err(MetastoreError::NotFound(EntityKind::Index { .. }))
            ) {
                state_wlock_guard.indexes.remove(&index_id);
                // The layout cache outlives the index otherwise, and an index recreated with the
                // same id would be written with the dead one's parameters.
                state_wlock_guard.manifest_layouts.remove(&index_id);
                if let Err(error) = self
                    .save_manifest_cas(&state_wlock_guard, &mut manifest_version_opt, attempt)
                    .await
                {
                    state_wlock_guard
                        .indexes
                        .insert(index_id.clone(), LazyIndexStatus::Deleting);
                    if is_manifest_conflict(&error) && attempt < DISTRIBUTED_MAX_ATTEMPTS {
                        drop(state_wlock_guard);
                        tokio::time::sleep(distributed_retry_backoff(attempt)).await;
                        continue;
                    }
                    return Err(error);
                }
            }
            return delete_result.map(|_| EmptyResponse {});
        }
    }

    async fn read<T, F>(&self, index_uid: &IndexUid, view: F) -> MetastoreResult<T>
    where F: FnOnce(&FileBackedIndex) -> MetastoreResult<T> {
        self.read_any(
            index_uid.index_id.as_str(),
            Some(index_uid.incarnation_id),
            view,
        )
        .await
    }

    /// Reads the index metadata given an `index_id`. The difference with `read` it that
    /// this function does necessarily take a incarnation id, so that it is less strict.
    async fn read_any<T>(
        &self,
        index_id: &str,
        incarnation_id_opt: Option<Ulid>,
        view: impl FnOnce(&FileBackedIndex) -> MetastoreResult<T>,
    ) -> MetastoreResult<T> {
        let locked_index = self.get_locked_index(index_id).await?;
        if let Some(incarnation_id) = incarnation_id_opt
            && locked_index.index_uid().incarnation_id != incarnation_id
        {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_id.to_string(),
            }));
        }
        view(&locked_index)
    }

    /// Returns a valid locked index.
    ///
    /// This function guarantees that it has not been
    /// marked as discarded.
    async fn get_locked_index(
        &self,
        index_id: &str,
    ) -> MetastoreResult<OwnedMutexGuard<FileBackedIndex>> {
        loop {
            let index = self.index(index_id).await?;
            let locked_index = index.lock_owned().await;

            if !locked_index.discarded {
                return Ok(locked_index);
            }
        }
    }

    /// Returns a FileBackedIndex for the given index_id.
    ///
    /// If `index_id` is in a transitioning state `Creating` or `Deleting`, it will
    /// trigger an error.
    /// If `index_id` is not yet in `per_index_metastores` map,
    /// a fetch to the storage will be initiated and might trigger an error.
    ///
    /// For a given index_id, only copies of the same index_view are returned.
    async fn index(&self, index_id: &str) -> MetastoreResult<Arc<Mutex<FileBackedIndex>>> {
        {
            // Happy path!
            // If the object is already in our cache then we just return a copy
            let inner_rlock_guard = self.state.read().await;
            if let Some(index_state) = inner_rlock_guard.indexes.get(index_id) {
                return get_index_mutex(index_id, index_state).await;
            }
        }
        // At this point we do not hold our mutex, so we need to do a little dance
        // to make sure we return the same instance.
        //
        // If there is an error here, note we do not return right away.
        // That's because we want to observe the property that after one success
        // all subsequent calls will succeed.
        let index_result = load_index(&*self.storage, index_id).await;

        // Here we retake the lock, still no io ongoing.
        let mut state_wlock_guard = self.state.write().await;

        // At this point, some other client might have added another instance of the Metadataet in
        // the map. We want to avoid two copies to exist in the application, so we keep only
        // one.
        if let Some(index_state) = state_wlock_guard.indexes.get(index_id) {
            return get_index_mutex(index_id, index_state).await;
        }

        // We need to instantiate a `LazyFileBackedIndex` that will hold the mutex
        // and take care of spawning the polling if needed.
        let index = index_result?;
        let lazy_index = LazyFileBackedIndex::new(
            self.storage.clone(),
            index_id.to_string(),
            self.polling_interval_opt,
            Some(index),
        );
        let index_mutex = lazy_index.get().await?;
        state_wlock_guard
            .indexes
            .insert(index_id.to_string(), LazyIndexStatus::Active(lazy_index));
        Ok(index_mutex)
    }

    async fn index_metadata_inner(
        &self,
        index_id_opt: Option<IndexId>,
        index_uid_opt: Option<IndexUid>,
    ) -> Result<IndexMetadata, (MetastoreError, Option<IndexId>, Option<IndexUid>)> {
        let index_id = if let Some(index_id) = &index_id_opt {
            index_id
        } else if let Some(index_uid) = &index_uid_opt {
            &index_uid.index_id
        } else {
            let message = "invalid request: neither `index_id` nor `index_uid` is set".to_string();
            let metastore_error = MetastoreError::Internal {
                message,
                cause: "".to_string(),
            };
            return Err((metastore_error, index_id_opt, index_uid_opt));
        };
        let index_metadata = match self
            .read_any(index_id, None, |index| Ok(index.metadata().clone()))
            .await
        {
            Ok(index_metadata) => index_metadata,
            Err(metastore_error) => {
                return Err((metastore_error, index_id_opt, index_uid_opt));
            }
        };
        if let Some(index_uid) = &index_uid_opt
            && index_metadata.index_uid != *index_uid
        {
            let metastore_error = MetastoreError::NotFound(EntityKind::Index {
                index_id: index_id.to_string(),
            });
            return Err((metastore_error, index_id_opt, index_uid_opt));
        }
        Ok(index_metadata)
    }

    /// Stats of one index.
    ///
    /// A manifest-layout index's cached view holds no split map, so its stats come from the
    /// layout's splits; the other layouts compute them from the index the cache holds.
    async fn index_stats_of(&self, index_id: &str) -> MetastoreResult<IndexStats> {
        let Some(layout) = self.manifest_layout_of(index_id).await? else {
            return self
                .read_any(index_id, None, |index| index.get_stats())
                .await;
        };
        let (root_info, _, _) = layout.load_root_metadata_only(&*self.storage).await?;
        let index_uid = root_info.index.index_uid().clone();
        let splits = layout.load_split_map(&*self.storage).await?;
        Ok(file_backed_index::index_stats_from_splits(
            &index_uid,
            splits.iter(),
        ))
    }

    async fn list_splits_aux(
        &self,
        index_id_with_incarnation_id_opts: &[(IndexId, Option<Ulid>)],
        list_splits_query: ListSplitsQuery,
    ) -> MetastoreResult<Vec<Split>> {
        let mut splits_per_index = Vec::with_capacity(index_id_with_incarnation_id_opts.len());
        for (index_id, incarnation_id_opt) in index_id_with_incarnation_id_opts {
            // An index stored in the manifest layout is read from its segments, pruned by the
            // query's window, instead of being materialised: that is the whole point of the layout.
            match self
                .manifest_layout_list_splits(index_id, *incarnation_id_opt, &list_splits_query)
                .await
            {
                Ok(Some(splits)) => {
                    splits_per_index.push(splits);
                    continue;
                }
                Ok(None) => {}
                Err(MetastoreError::NotFound(_)) => continue,
                Err(error) => return Err(error),
            }
            match self
                .read_any(index_id, *incarnation_id_opt, |index| {
                    index.list_splits(&list_splits_query)
                })
                .await
            {
                Ok(splits) => {
                    splits_per_index.push(splits);
                }
                Err(MetastoreError::NotFound(_)) => {
                    // If the index does not exist, we just skip it.
                    continue;
                }
                Err(error) => return Err(error),
            }
        }

        let limit = list_splits_query.limit.unwrap_or(usize::MAX);
        let offset = list_splits_query.offset.unwrap_or_default();

        let merged_results = splits_per_index
            .into_iter()
            .kmerge_by(|lhs, rhs| list_splits_query.sort_by.compare(lhs, rhs).is_lt())
            .skip(offset)
            .take(limit)
            .collect();

        Ok(merged_results)
    }

    /// Reads the splits of a manifest-layout index straight from its segments and WAL tail.
    ///
    /// Returns `Ok(None)` when the index is not stored in that layout, so the caller falls back to
    /// the in-memory path. The window passed to the layout is a superset of what the query can
    /// return: it is derived from the query's time bounds, and splits without a time range live in
    /// a bucket no window prunes.
    async fn manifest_layout_list_splits(
        &self,
        index_id: &str,
        incarnation_id_opt: Option<Ulid>,
        list_splits_query: &ListSplitsQuery,
    ) -> MetastoreResult<Option<Vec<Split>>> {
        // Through the cache, so an index that uses another layout pays the probe once per process
        // rather than once per query.
        let Some(layout) = self.manifest_layout_of(index_id).await? else {
            return Ok(None);
        };
        // The root carries the incarnation the request may name, so it is read either way.
        let (root_info, _, _) = layout.load_root_metadata_only(&*self.storage).await?;
        if let Some(incarnation_id) = incarnation_id_opt
            && root_info.index.index_uid().incarnation_id != incarnation_id
        {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_id.to_string(),
            }));
        }
        let (window_start, window_end) = pruning_window(list_splits_query);
        let splits = layout
            .list_splits(&*self.storage, window_start, window_end)
            .await?;
        let mut splits: Vec<Split> = splits
            .into_iter()
            .filter(|split| file_backed_index::split_query_predicate(&split, list_splits_query))
            .collect();
        splits.sort_unstable_by(|left, right| list_splits_query.sort_by.compare(left, right));
        Ok(Some(splits))
    }

    /// Applies a mutation that only touches the splits with the given ids.
    ///
    /// On the layouts that keep the split map in one object the whole index is loaded, because that
    /// is where the map lives. On the manifest layout only the named splits are read (through
    /// their stripe's manifests and the segments whose id range can hold them), the *same*
    /// mutation closure runs, and the splits it changed are published as operations — so a
    /// publish costs what it touches rather than what the index holds.
    async fn mutate_splits<T>(
        &self,
        index_uid: &IndexUid,
        split_ids: &[SplitId],
        mutate_fn: impl Fn(&mut FileBackedIndex, bool) -> MetastoreResult<MutationOccurred<T>>,
        caller_replay: bool,
    ) -> MetastoreResult<T> {
        let index_id = &index_uid.index_id;
        let Some(layout) = self.manifest_layout_of(index_id).await? else {
            // The other layouts write the whole index in one compare-and-swap, so a mutation never
            // observes a partial commit of *itself*. A caller that says it is replaying still gets
            // the same tolerance: its commit may have landed while the response was lost.
            return self
                .mutate_replaying(index_uid, caller_replay, |index, is_replay| {
                    mutate_fn(index, is_replay)
                })
                .await;
        };
        let mut attempt = 0;
        loop {
            attempt += 1;
            // Attempt 2 onwards replays a mutation that got partway: this layout commits one stripe
            // at a time, so splits of the stripes already committed are published. The closure is
            // told, because refusing them would fail an RPC whose publication already happened.
            let is_replay = attempt > 1 || caller_replay;
            let (root_info, root_version, root_bytes) = layout.load_root(&*self.storage).await?;
            let loaded_shard_objects = root_info.shard_objects;
            let mut index = root_info.index;
            if index.index_uid() != index_uid {
                return Err(MetastoreError::NotFound(EntityKind::Index {
                    index_id: index_id.to_string(),
                }));
            }
            // Only the splits this mutation can touch, never the whole map.
            let touched_state = layout.get_splits_by_id(&*self.storage, split_ids).await?;
            index.put_splits(touched_state);
            let value = match mutate_fn(&mut index, is_replay)? {
                MutationOccurred::Yes(value) => value,
                MutationOccurred::No(value) => {
                    // The index here holds only the splits this mutation looked at, so it is not a
                    // view of the index and must not be cached as one.
                    return Ok(value);
                }
            };
            let ops: Vec<manifest_layout::SplitOp> = index
                .take_touched_split_ids()
                .into_iter()
                .map(|split_id| manifest_layout::SplitOp {
                    split: index.split_opt(&split_id).cloned(),
                    split_id,
                })
                .collect();
            match layout.publish_ops(&*self.storage, ops).await {
                Ok(()) => {}
                Err(MetastoreError::FailedPrecondition { .. })
                    if attempt < DISTRIBUTED_MAX_ATTEMPTS =>
                {
                    record_cas_conflict(false);
                    tokio::time::sleep(distributed_retry_backoff(attempt)).await;
                    continue;
                }
                Err(error) => {
                    if is_manifest_conflict(&error) {
                        record_cas_conflict(true);
                    }
                    self.discard_cached_index(index_id).await;
                    return Err(error);
                }
            }
            // The rest of the index (metadata, sources, checkpoints, delete tasks) keeps its own
            // small compare-and-swap; it is only written when it actually changed.
            if let Err(error) = layout
                .store_root(
                    &*self.storage,
                    &mut index,
                    &loaded_shard_objects,
                    &root_bytes,
                    &root_version,
                )
                .await
            {
                if is_manifest_conflict(&error) && attempt < DISTRIBUTED_MAX_ATTEMPTS {
                    record_cas_conflict(false);
                    tokio::time::sleep(distributed_retry_backoff(attempt)).await;
                    continue;
                }
                // Giving up on the index metadata is the case an operator has to be able to see:
                // the splits of this mutation are committed by now (they commit
                // before the root), so the caller gets an error for a mutation whose split work
                // already landed, and a caller that does not replay cannot tell that state from a
                // mutation nothing happened to. A replay of the same publish does finish it — the
                // already-published splits are tolerated, which is what `is_replay` is for.
                // Leaving this uncounted is what let a real 5-node run exhaust
                // the budget with `cas_conflicts_exhausted_total` still at zero.
                if is_manifest_conflict(&error) {
                    record_cas_conflict(true);
                }
                self.discard_cached_index(index_id).await;
                return Err(error);
            }
            self.discard_cached_index(index_id).await;
            return Ok(value);
        }
    }

    /// Layout of an index when it is stored as manifests, segments and a WAL tail.
    ///
    /// Cached per index: the layout of an index does not change over its life, and a write should
    /// not pay two round trips to learn what it already knows.
    async fn manifest_layout_of(
        &self,
        index_id: &str,
    ) -> MetastoreResult<Option<manifest_layout::ManifestLayout>> {
        {
            let state_rlock_guard = self.state.read().await;
            // `Some(None)` means "probed, and this index is not in that layout": without it every
            // read of every index that uses another layout would pay a HEAD for the probe.
            if let Some(layout_opt) = state_rlock_guard.manifest_layouts.get(index_id) {
                return Ok(layout_opt.clone());
            }
        }
        let probe = manifest_layout::ManifestLayout::new(
            index_id,
            MANIFEST_LAYOUT_BUCKET_SECS,
            MANIFEST_LAYOUT_NUM_STRIPES,
        );
        if !probe.exists(&*self.storage).await? {
            let mut state_wlock_guard = self.state.write().await;
            state_wlock_guard
                .manifest_layouts
                .insert(index_id.to_string(), None);
            return Ok(None);
        }
        let (root_info, _, _) = probe.load_root_metadata_only(&*self.storage).await?;
        let layout = manifest_layout::ManifestLayout::new(
            index_id,
            root_info.bucket_secs,
            root_info.num_stripes,
        );
        let mut state_wlock_guard = self.state.write().await;
        state_wlock_guard
            .manifest_layouts
            .insert(index_id.to_string(), Some(layout.clone()));
        Ok(Some(layout))
    }

    /// Returns the list of splits for the given request.
    /// No error is returned if any of the requested `index_uid` does not exist.
    async fn list_splits_inner(&self, request: ListSplitsRequest) -> MetastoreResult<Vec<Split>> {
        let mut list_splits_query = request.deserialize_list_splits_query()?;

        let index_id_incarnation_id_opts: Vec<(IndexId, Option<Ulid>)> =
            if let Some(index_uids) = list_splits_query.index_uids.take() {
                index_uids
                    .into_iter()
                    .map(|index_uid| (index_uid.index_id, Some(index_uid.incarnation_id)))
                    .collect()
            } else {
                // We do not have an explicit list of index_uids with the query, so we search for
                // all indexes — which is the moment to notice one another node created (see
                // `adopt_indexes_from_manifest`); the compaction planner reads this way.
                self.adopt_indexes_from_manifest().await?;
                let inner_rlock_guard = self.state.read().await;
                inner_rlock_guard
                    .indexes
                    .iter()
                    .filter_map(|(index_id, index_state)| match index_state {
                        LazyIndexStatus::Active(_) => Some(index_id),
                        _ => None,
                    })
                    .map(|index_id| (index_id.clone(), None))
                    .collect()
            };

        self.list_splits_aux(&index_id_incarnation_id_opts, list_splits_query)
            .await
    }

    /// Helper used for testing to obtain the data associated with the given index.
    #[cfg(test)]
    async fn get_index(&self, index_uid: &IndexUid) -> MetastoreResult<FileBackedIndex> {
        self.read(index_uid, |index| Ok(index.clone())).await
    }
}

#[async_trait]
impl MetastoreService for FileBackedMetastore {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.storage.exists(Path::new(MANIFEST_FILE_NAME)).await?;
        Ok(())
    }

    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        vec![self.storage.uri().clone()]
    }

    // -------------------------------------------------------------------------------
    // Mutations over the high-level index.

    #[instrument(name = "metastore.file_backed.create_index", skip_all)]
    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse> {
        let index_config = request.deserialize_index_config()?;
        let source_configs = request.deserialize_source_configs()?;

        let mut index_metadata = IndexMetadata::new(index_config);

        for source_config in source_configs {
            index_metadata.add_source(source_config)?;
        }
        let index_uid = index_metadata.index_uid.clone();
        let index_id = &index_uid.index_id;

        let index_metadata_json = serde_utils::to_json_str(&index_metadata)?;
        let index = FileBackedIndex::from(index_metadata);

        let mut attempt = 0;
        loop {
            attempt += 1;

            let mut state_wlock_guard = self.state.write().await;
            let mut manifest_version_opt = None;
            self.reload_manifest_if_distributed(&mut state_wlock_guard, &mut manifest_version_opt)
                .await?;

            // Checking if index already exists is a bit tedious:
            // - first we check the index state: if it's `Active`, return `IndexAlreadyExists`
            //   error, and if it's `Creating` or `Deleting`, it's ok to override them as these are
            //   transitioning states.
            // - if the index is not in the index states map, we still need to check the storage as
            //   we don't want to override an existing metadata file.
            // The file may also be the one this request wrote during a previous attempt: the create
            // is replayed when the manifest compare-and-swap loses a race, and the file it already
            // wrote must not turn that replay into an "already exists" error.
            let mut index_file_needs_to_be_written = true;
            if let Some(index_status) = state_wlock_guard.indexes.get(index_id) {
                if let LazyIndexStatus::Active(_) = index_status {
                    return Err(MetastoreError::AlreadyExists(EntityKind::Index {
                        index_id: index_id.to_string(),
                    }));
                }
            } else if index_exists(&*self.storage, index_id).await? {
                if self.distributed {
                    if self
                        .index_file_matches_request(index_id, &index_uid)
                        .await?
                    {
                        index_file_needs_to_be_written = false;
                    } else if attempt < DISTRIBUTED_MAX_ATTEMPTS {
                        // Another node may have created the index file after we read the manifest,
                        // and its manifest write may not have landed yet. Reload instead of
                        // reporting an inconsistency; if the file is still there afterwards, the
                        // index exists.
                        drop(state_wlock_guard);
                        tokio::time::sleep(distributed_retry_backoff(attempt)).await;
                        continue;
                    } else {
                        return Err(MetastoreError::AlreadyExists(EntityKind::Index {
                            index_id: index_id.to_string(),
                        }));
                    }
                } else {
                    return Err(MetastoreError::Internal {
                        message: format!("index {index_id} cannot be created"),
                        cause: format!(
                            "index {index_id} is not present in the manifest file but its file \
                             `{index_id}/metastore.json` is on the storage"
                        ),
                    });
                }
            }
            // Set state to `Creating` and rollback on metastore error.
            state_wlock_guard
                .indexes
                .insert(index_id.clone(), LazyIndexStatus::Creating);

            if let Err(error) = self
                .save_manifest_cas(&state_wlock_guard, &mut manifest_version_opt, attempt)
                .await
            {
                state_wlock_guard.indexes.remove(index_id);
                if is_manifest_conflict(&error) && attempt < DISTRIBUTED_MAX_ATTEMPTS {
                    drop(state_wlock_guard);
                    tokio::time::sleep(distributed_retry_backoff(attempt)).await;
                    continue;
                }
                return Err(error);
            }

            // The index file has to be created exactly once: two nodes racing on the same
            // `create_index` must not be able to overwrite one another's metadata.
            if !self.distributed {
                put_index(&*self.storage, &index).await?;
            } else if index_file_needs_to_be_written {
                match create_index_file(&*self.storage, &index, self.index_layout).await {
                    Ok(_) => {}
                    Err(MetastoreError::AlreadyExists(_))
                    | Err(MetastoreError::FailedPrecondition { .. }) => {
                        if self
                            .index_file_matches_request(index_id, &index_uid)
                            .await?
                        {
                            // The file is this request's own write: the previous attempt got that
                            // far before losing the manifest race. This is not a conflict, the
                            // manifest update below is what is still missing.
                        } else {
                            state_wlock_guard.indexes.remove(index_id);
                            if attempt < DISTRIBUTED_MAX_ATTEMPTS {
                                drop(state_wlock_guard);
                                tokio::time::sleep(distributed_retry_backoff(attempt)).await;
                                continue;
                            }
                            return Err(MetastoreError::AlreadyExists(EntityKind::Index {
                                index_id: index_id.to_string(),
                            }));
                        }
                    }
                    Err(error) => {
                        state_wlock_guard.indexes.remove(index_id);
                        return Err(error);
                    }
                }
            }

            state_wlock_guard.indexes.insert(
                index_id.clone(),
                LazyIndexStatus::Active(LazyFileBackedIndex::new(
                    self.storage.clone(),
                    index_id.clone(),
                    self.polling_interval_opt,
                    Some(index.clone()),
                )),
            );
            // Set state to `Active` and rollback on metastore error.
            if let Err(error) = self
                .save_manifest_cas(&state_wlock_guard, &mut manifest_version_opt, attempt)
                .await
            {
                state_wlock_guard
                    .indexes
                    .insert(index_id.clone(), LazyIndexStatus::Creating);
                if is_manifest_conflict(&error) && attempt < DISTRIBUTED_MAX_ATTEMPTS {
                    drop(state_wlock_guard);
                    tokio::time::sleep(distributed_retry_backoff(attempt)).await;
                    continue;
                }
                return Err(error);
            }

            return Ok(CreateIndexResponse {
                index_uid: index_uid.into(),
                index_metadata_json,
            });
        }
    }

    #[instrument(name = "metastore.file_backed.update_index", skip_all, fields(index_uid = %request.index_uid()))]
    async fn update_index(
        &self,
        request: UpdateIndexRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        let index_uid = request.index_uid();
        let doc_mapping = request.deserialize_doc_mapping()?;
        let indexing_settings = request.deserialize_indexing_settings()?;
        let ingest_settings = request.deserialize_ingest_settings()?;
        let search_settings = request.deserialize_search_settings()?;
        let retention_policy_opt = request.deserialize_retention_policy()?;

        let index_metadata = self
            .mutate(index_uid, |index| {
                let mutation_occurred = index.update_index_config(
                    doc_mapping.clone(),
                    indexing_settings.clone(),
                    ingest_settings.clone(),
                    search_settings.clone(),
                    retention_policy_opt.clone(),
                )?;
                let index_metadata = index.metadata().clone();

                if mutation_occurred {
                    Ok(MutationOccurred::Yes(index_metadata))
                } else {
                    Ok(MutationOccurred::No(index_metadata))
                }
            })
            .await?;
        IndexMetadataResponse::try_from_index_metadata(&index_metadata)
    }

    #[instrument(name = "metastore.file_backed.delete_index", skip_all, fields(index_uid = %request.index_uid()))]
    async fn delete_index(&self, request: DeleteIndexRequest) -> MetastoreResult<EmptyResponse> {
        if self.distributed {
            return self.delete_index_distributed(&request).await;
        }
        // We pick the outer lock here, so that we enter a critical section.
        let mut state_wlock_guard = self.state.write().await;

        let index_id = &request.index_uid().index_id;
        // If index is neither in `per_index_metastores_wlock` nor on the storage, it does not
        // exist.
        if !state_wlock_guard.indexes.contains_key(index_id)
            && !index_exists(&*self.storage, index_id).await?
        {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_id.to_string(),
            }));
        }
        // Set state to `Deleting` and keep the previous state in memory in case we need to insert
        // if an error occurs.
        let index_state_opt = state_wlock_guard
            .indexes
            .insert(index_id.to_string(), LazyIndexStatus::Deleting);
        let manifest = state_wlock_guard.as_manifest();
        // On a put error, reinsert the previous state if any.
        if let Err(error) = save_manifest(&*self.storage, &manifest).await {
            if let Some(index_state) = index_state_opt {
                state_wlock_guard
                    .indexes
                    .insert(index_id.to_string(), index_state);
            } else {
                state_wlock_guard.indexes.remove(index_id);
            }
            return Err(error);
        }

        let delete_result = delete_index(&*self.storage, index_id).await;

        if matches!(
            &delete_result,
            Ok(()) | Err(MetastoreError::NotFound(EntityKind::Index { .. }))
        ) {
            state_wlock_guard.indexes.remove(index_id);
            let manifest = state_wlock_guard.as_manifest();

            if let Err(error) = save_manifest(&*self.storage, &manifest).await {
                state_wlock_guard
                    .indexes
                    .insert(index_id.to_string(), LazyIndexStatus::Deleting);
                return Err(error);
            }
        }
        delete_result.map(|_| EmptyResponse {})
    }

    // -------------------------------------------------------------------------------
    // Mutations over a single index

    #[instrument(name = "metastore.file_backed.stage_splits", skip_all, fields(index_uid = %request.index_uid()))]
    async fn stage_splits(&self, request: StageSplitsRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();
        let splits_metadata = request.deserialize_splits_metadata()?;
        let staged_split_ids: Vec<SplitId> = splits_metadata
            .iter()
            .map(|split_metadata| split_metadata.split_id.clone())
            .collect();

        self.mutate_splits(
            &index_uid,
            &staged_split_ids,
            |index, _is_replay| {
                let mut failed_split_ids = Vec::new();

                for split_metadata in splits_metadata.clone() {
                    match index.stage_split(split_metadata) {
                        Ok(()) => {}
                        Err(MetastoreError::FailedPrecondition {
                            entity: EntityKind::Split { split_id },
                            ..
                        }) => {
                            failed_split_ids.push(split_id);
                        }
                        Err(error) => return Err(error),
                    };
                }
                if !failed_split_ids.is_empty() {
                    let entity = EntityKind::Splits {
                        split_ids: failed_split_ids,
                    };
                    let message = "splits are not staged".to_string();
                    Err(MetastoreError::FailedPrecondition { entity, message })
                } else {
                    Ok(MutationOccurred::Yes(()))
                }
            },
            false,
        )
        .await?;
        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.publish_splits", skip_all, fields(index_uid = %request.index_uid()))]
    async fn publish_splits(
        &self,
        request: PublishSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_checkpoint_delta: Option<IndexCheckpointDelta> =
            request.deserialize_index_checkpoint()?;
        let index_uid = request.index_uid().clone();
        let mut touched_split_ids: Vec<SplitId> = request
            .staged_split_ids
            .iter()
            .map(|split_id| SplitId::from(split_id.as_str()))
            .collect();
        touched_split_ids.extend(
            request
                .replaced_split_ids
                .iter()
                .map(|split_id| SplitId::from(split_id.as_str())),
        );
        // A publish is replayed either inside this request (the manifest layout commits one stripe
        // at a time, so its own retry can find half of its publication done) or by the caller,
        // which says so on the request: its earlier attempt may have committed while its
        // response was lost. Both tolerances below follow from that one flag. A caller that
        // publishes a split that is already published *without* being a replay still gets
        // the hard error it always got.
        let caller_replay = request.is_replay;
        self.mutate_splits(
            &index_uid,
            &touched_split_ids,
            |index, is_replay| {
                index.publish_splits_with_retry_tolerance(
                    request.staged_split_ids.clone(),
                    request.replaced_split_ids.clone(),
                    index_checkpoint_delta.clone(),
                    request.publish_token_opt.clone().map(|token| token.into()),
                    is_replay,
                )?;
                Ok(MutationOccurred::Yes(()))
            },
            caller_replay,
        )
        .await?;
        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.mark_splits_for_deletion", skip_all, fields(index_uid = %request.index_uid()))]
    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();
        let marked_split_ids: Vec<SplitId> = request
            .split_ids
            .iter()
            .map(|split_id| SplitId::from(split_id.as_str()))
            .collect();

        self.mutate_splits(
            &index_uid,
            &marked_split_ids,
            |index, _is_replay| {
                index
                    .mark_splits_for_deletion(
                        request.split_ids.clone(),
                        &[
                            SplitState::Staged,
                            SplitState::Published,
                            SplitState::MarkedForDeletion,
                        ],
                        false,
                        // The states this call accepts already include the marked one, so it is
                        // idempotent without the replay tolerance.
                        false,
                    )
                    .map(MutationOccurred::from)
            },
            false,
        )
        .await?;
        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.delete_splits", skip_all, fields(index_uid = %request.index_uid()))]
    async fn delete_splits(&self, request: DeleteSplitsRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();
        let deleted_split_ids: Vec<SplitId> = request
            .split_ids
            .iter()
            .map(|split_id| SplitId::from(split_id.as_str()))
            .collect();

        self.mutate_splits(
            &index_uid,
            &deleted_split_ids,
            |index, _is_replay| {
                index.delete_splits(request.split_ids.clone())?;
                Ok(MutationOccurred::Yes(EmptyResponse {}))
            },
            false,
        )
        .await?;
        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.add_source", skip_all, fields(index_uid = %request.index_uid()))]
    async fn add_source(&self, request: AddSourceRequest) -> MetastoreResult<EmptyResponse> {
        let source_config = request.deserialize_source_config()?;
        let index_uid = request.index_uid();

        self.mutate(index_uid, |index| {
            index.add_source(source_config.clone())?;
            Ok(MutationOccurred::Yes(()))
        })
        .await?;
        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.update_source", skip_all, fields(index_uid = %request.index_uid()))]
    async fn update_source(&self, request: UpdateSourceRequest) -> MetastoreResult<EmptyResponse> {
        let source_config = request.deserialize_source_config()?;
        let index_uid = request.index_uid();

        self.mutate(index_uid, |index| {
            let mutation_occurred = index.update_source(source_config.clone())?;
            Ok(MutationOccurred::from(mutation_occurred))
        })
        .await?;
        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.toggle_source", skip_all, fields(index_uid = %request.index_uid(), source_id = %request.source_id))]
    async fn toggle_source(&self, request: ToggleSourceRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid();

        self.mutate(index_uid, |index| {
            index
                .toggle_source(&request.source_id, request.enable)
                .map(MutationOccurred::from)
        })
        .await?;
        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.delete_source", skip_all, fields(index_uid = %request.index_uid(), source_id = %request.source_id))]
    async fn delete_source(&self, request: DeleteSourceRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid();

        self.mutate(index_uid, |index| {
            index.delete_source(&request.source_id)?;
            Ok(MutationOccurred::Yes(()))
        })
        .await?;
        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.reset_source_checkpoint", skip_all, fields(index_uid = %request.index_uid(), source_id = %request.source_id))]
    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid();

        self.mutate(index_uid, |index| {
            index
                .reset_source_checkpoint(&request.source_id)
                .map(MutationOccurred::from)
        })
        .await?;
        Ok(EmptyResponse {})
    }

    // -------------------------------------------------------------------------------
    // Read-only accessors

    /// Streams of splits for the given request.
    /// No error is returned if any of the requested `index_uid` does not exist.
    #[instrument(name = "metastore.file_backed.list_splits", skip_all)]
    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
        let splits = self.list_splits_inner(request).await?;
        let splits_responses: Vec<MetastoreResult<ListSplitsResponse>> = splits
            .chunks(STREAM_SPLITS_CHUNK_SIZE)
            .map(|chunk| ListSplitsResponse::try_from_splits(chunk.to_vec()))
            .collect();
        let splits_responses_stream = Box::pin(futures::stream::iter(splits_responses));
        Ok(ServiceStream::new(splits_responses_stream))
    }

    #[instrument(name = "metastore.file_backed.list_index_stats", skip_all, fields(index_id_patterns = ?request.index_id_patterns))]
    async fn list_index_stats(
        &self,
        request: ListIndexStatsRequest,
    ) -> MetastoreResult<ListIndexStatsResponse> {
        let index_id_matcher =
            IndexIdMatcher::try_from_index_id_patterns(&request.index_id_patterns)?;
        // Listing stats is a listing: adopt what another node created, the way the index listing
        // does, before reading the cached set.
        self.adopt_indexes_from_manifest().await?;
        let index_ids: Vec<IndexId> = {
            let inner_rlock_guard = self.state.read().await;
            inner_rlock_guard
                .indexes
                .iter()
                .filter_map(|(index_id, index_state)| match index_state {
                    LazyIndexStatus::Active(_) if index_id_matcher.is_match(index_id) => {
                        Some(index_id)
                    }
                    _ => None,
                })
                .cloned()
                .collect()
        };

        let mut index_read_futures = FuturesUnordered::new();
        for index_id in index_ids {
            let index_read_future = async move { self.index_stats_of(&index_id).await };
            index_read_futures.push(index_read_future);
        }

        let mut index_stats = Vec::new();
        while let Some(index_read_result) = index_read_futures.next().await {
            match index_read_result {
                Ok(stats) => index_stats.push(stats),
                Err(MetastoreError::NotFound(_)) => {
                    // If the index does not exist, we just skip it.
                    continue;
                }
                Err(error) => return Err(error),
            }
        }

        Ok(ListIndexStatsResponse { index_stats })
    }

    #[instrument(name = "metastore.file_backed.list_stale_splits", skip_all, fields(index_uid = %request.index_uid()))]
    async fn list_stale_splits(
        &self,
        request: ListStaleSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse> {
        let list_splits_query = ListSplitsQuery::for_index(request.index_uid().clone())
            .with_delete_opstamp_lt(request.delete_opstamp)
            .with_split_state(SplitState::Published)
            .retain_mature(OffsetDateTime::now_utc())
            .sort_by_staleness()
            .with_limit(request.num_splits as usize);
        let list_splits_request =
            ListSplitsRequest::try_from_list_splits_query(&list_splits_query)?;
        let splits = self.list_splits_inner(list_splits_request).await?;
        ListSplitsResponse::try_from_splits(splits)
    }

    #[instrument(name = "metastore.file_backed.index_metadata", skip(self))]
    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        let index_metadata = self
            .index_metadata_inner(request.index_id, request.index_uid)
            .await
            .map_err(|(metastore_error, _index_id_opt, _index_uid_opt)| metastore_error)?;
        let response = IndexMetadataResponse::try_from_index_metadata(&index_metadata)?;
        Ok(response)
    }

    #[instrument(name = "metastore.file_backed.indexes_metadata", skip_all, fields(num_subrequests = request.subrequests.len()))]
    async fn indexes_metadata(
        &self,
        request: IndexesMetadataRequest,
    ) -> MetastoreResult<IndexesMetadataResponse> {
        let mut indexes_metadata: Vec<IndexMetadata> =
            Vec::with_capacity(request.subrequests.len());
        let mut failures: Vec<IndexMetadataFailure> = Vec::new();

        let mut index_metadata_futures = FuturesUnordered::new();

        for subrequest in request.subrequests {
            let metastore = self.clone();
            let index_metadata_future = async move {
                metastore
                    .index_metadata_inner(subrequest.index_id, subrequest.index_uid)
                    .await
            };
            index_metadata_futures.push(index_metadata_future);
        }
        while let Some(index_metadata_result) = index_metadata_futures.next().await {
            match index_metadata_result {
                Ok(index_metadata) => indexes_metadata.push(index_metadata),
                Err((MetastoreError::NotFound(_), index_id, index_uid)) => {
                    let failure = IndexMetadataFailure {
                        index_id,
                        index_uid,
                        reason: IndexMetadataFailureReason::NotFound as i32,
                    };
                    failures.push(failure)
                }
                // All other errors are considered internal errors.
                Err((_metastore_error, index_id, index_uid)) => {
                    let failure = IndexMetadataFailure {
                        index_id,
                        index_uid,
                        reason: IndexMetadataFailureReason::Internal as i32,
                    };
                    failures.push(failure)
                }
            }
        }
        let response =
            IndexesMetadataResponse::try_from_indexes_metadata(indexes_metadata, failures).await?;
        Ok(response)
    }

    #[instrument(name = "metastore.file_backed.list_indexes_metadata", skip_all, fields(index_id_patterns = ?request.index_id_patterns))]
    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> MetastoreResult<ListIndexesMetadataResponse> {
        // Done in two steps:
        // 1) Get index IDs and release the lock on `per_index_metastores`.
        // 2) Get each index metadata. Note that each get will take a read lock on
        // `per_index_metastores`. Lock is released in 1) to let a concurrent task/thread to
        // take a write lock on `per_index_metastores`.
        let index_id_matcher =
            IndexIdMatcher::try_from_index_id_patterns(&request.index_id_patterns)?;
        // A node that has not written since another node created an index does not have it in its
        // cached state; the listing is where that is noticed (see `adopt_indexes_from_manifest`).
        self.adopt_indexes_from_manifest().await?;
        let inner_rlock_guard = self.state.read().await;
        let index_ids: Vec<IndexId> = inner_rlock_guard
            .indexes
            .iter()
            .filter_map(|(index_id, index_state)| match index_state {
                LazyIndexStatus::Active(_) if index_id_matcher.is_match(index_id) => Some(index_id),
                _ => None,
            })
            .cloned()
            .collect();
        drop(inner_rlock_guard);

        let metastore = self.clone();
        let indexes_metadata: Vec<IndexMetadata> = try_join_all(
            index_ids
                .into_iter()
                .map(|index_id| get_index_metadata(metastore.clone(), index_id)),
        )
        .await?
        .into_iter()
        .flatten()
        .collect();
        let response =
            ListIndexesMetadataResponse::try_from_indexes_metadata(indexes_metadata).await?;
        Ok(response)
    }

    // Shard API

    #[instrument(name = "metastore.file_backed.open_shards", skip_all, fields(num_subrequests = request.subrequests.len()))]
    async fn open_shards(&self, request: OpenShardsRequest) -> MetastoreResult<OpenShardsResponse> {
        let mut response = OpenShardsResponse {
            subresponses: Vec::with_capacity(request.subrequests.len()),
        };
        // We must group the subrequests by `index_uid` to mutate each index only once, since each
        // mutation triggers an IO.
        let per_index_uid_subrequests: HashMap<IndexUid, Vec<OpenShardSubrequest>> = request
            .subrequests
            .into_iter()
            .into_group_map_by(|subrequest| subrequest.index_uid().clone());

        for (index_uid, subrequests) in per_index_uid_subrequests {
            let subresponses = self
                .mutate(&index_uid, |index| index.open_shards(subrequests.clone()))
                .await?;
            response.subresponses.extend(subresponses);
        }
        Ok(response)
    }

    #[instrument(name = "metastore.file_backed.acquire_shards", skip_all, fields(index_uid = %request.index_uid()))]
    async fn acquire_shards(
        &self,
        request: AcquireShardsRequest,
    ) -> MetastoreResult<AcquireShardsResponse> {
        let index_uid = request.index_uid().clone();
        let response = self
            .mutate(&index_uid, |index| index.acquire_shards(request.clone()))
            .await?;
        Ok(response)
    }

    #[instrument(name = "metastore.file_backed.delete_shards", skip_all, fields(index_uid = %request.index_uid()))]
    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> MetastoreResult<DeleteShardsResponse> {
        let index_uid = request.index_uid().clone();
        let response = self
            .mutate(&index_uid, |index| index.delete_shards(request.clone()))
            .await?;
        Ok(response)
    }

    #[instrument(name = "metastore.file_backed.prune_shards", skip_all, fields(index_uid = %request.index_uid()))]
    async fn prune_shards(&self, request: PruneShardsRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();
        self.mutate(&index_uid, |index| index.prune_shards(request.clone()))
            .await?;
        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.list_shards", skip_all, fields(num_subrequests = request.subrequests.len()))]
    async fn list_shards(&self, request: ListShardsRequest) -> MetastoreResult<ListShardsResponse> {
        let mut subresponses = Vec::with_capacity(request.subrequests.len());

        for subrequest in request.subrequests {
            let index_uid = subrequest.index_uid().clone();
            let subresponse = self
                .read(&index_uid, |index| index.list_shards(subrequest))
                .await?;
            subresponses.push(subresponse);
        }
        let response = ListShardsResponse { subresponses };
        Ok(response)
    }

    // -------------------------------------------------------------------------------
    // Delete tasks

    #[instrument(name = "metastore.file_backed.last_delete_opstamp", skip_all, fields(index_uid = %request.index_uid()))]
    async fn last_delete_opstamp(
        &self,
        request: LastDeleteOpstampRequest,
    ) -> MetastoreResult<LastDeleteOpstampResponse> {
        let last_delete_opstamp = self
            .read(request.index_uid(), |index| Ok(index.last_delete_opstamp()))
            .await?;
        Ok(LastDeleteOpstampResponse::new(last_delete_opstamp))
    }

    #[instrument(name = "metastore.file_backed.create_delete_task", skip_all, fields(index_uid = %delete_query.index_uid()))]
    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        let index_uid = delete_query.index_uid().clone();
        let delete_task = self
            .mutate(&index_uid, |index| {
                index
                    .create_delete_task(delete_query.clone())
                    .map(MutationOccurred::Yes)
            })
            .await?;
        Ok(delete_task)
    }

    #[instrument(name = "metastore.file_backed.update_splits_delete_opstamp", skip_all, fields(index_uid = %request.index_uid()))]
    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        let index_uid = request.index_uid();
        let opstamped_split_ids: Vec<SplitId> = request
            .split_ids
            .iter()
            .map(|split_id| SplitId::from(split_id.as_str()))
            .collect();

        self.mutate_splits(
            index_uid,
            &opstamped_split_ids,
            |index, _is_replay| {
                let split_ids_str = request
                    .split_ids
                    .iter()
                    .map(|split_id| split_id.as_str())
                    .collect::<Vec<_>>();
                index
                    .update_splits_delete_opstamp(&split_ids_str, request.delete_opstamp)
                    .map(MutationOccurred::from)
            },
            false,
        )
        .await?;
        Ok(UpdateSplitsDeleteOpstampResponse {})
    }

    #[instrument(name = "metastore.file_backed.list_delete_tasks", skip_all, fields(index_uid = %request.index_uid()))]
    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse> {
        let index_uid = request.index_uid();

        let delete_tasks = self
            .read(index_uid, |index| {
                Ok(index.list_delete_tasks(request.opstamp_start))
            })
            .await??;
        let response = ListDeleteTasksResponse { delete_tasks };
        Ok(response)
    }

    // Index Template API

    #[instrument(name = "metastore.file_backed.create_index_template", skip(self))]
    async fn create_index_template(
        &self,
        request: CreateIndexTemplateRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_template: IndexTemplate =
            serde_utils::from_json_str(&request.index_template_json)?;
        let template_id = index_template.template_id.clone();

        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut state_wlock_guard = self.state.write().await;
            let mut manifest_version_opt = None;
            self.reload_manifest_if_distributed(&mut state_wlock_guard, &mut manifest_version_opt)
                .await?;

            let evicted_template_opt = match state_wlock_guard.templates.entry(template_id.clone())
            {
                Entry::Vacant(entry) => {
                    entry.insert(index_template.clone());
                    None
                }
                Entry::Occupied(mut entry) if request.overwrite => {
                    let evicted_template = entry.insert(index_template.clone());
                    Some(evicted_template)
                }
                Entry::Occupied(_) => {
                    return Err(MetastoreError::AlreadyExists(EntityKind::IndexTemplate {
                        template_id: template_id.clone(),
                    }));
                }
            };
            if let Err(error) = state_wlock_guard.template_matcher.insert(&index_template) {
                if let Some(evicted_template) = evicted_template_opt {
                    state_wlock_guard
                        .templates
                        .insert(evicted_template.template_id.clone(), evicted_template);
                } else {
                    state_wlock_guard.templates.remove(&template_id);
                }
                return Err(error);
            }

            if let Err(error) = self
                .save_manifest_cas(&state_wlock_guard, &mut manifest_version_opt, attempt)
                .await
            {
                if is_manifest_conflict(&error) && attempt < DISTRIBUTED_MAX_ATTEMPTS {
                    drop(state_wlock_guard);
                    tokio::time::sleep(distributed_retry_backoff(attempt)).await;
                    continue;
                }
                // Rollback on error.
                if let Some(evicted_template) = evicted_template_opt {
                    state_wlock_guard
                        .template_matcher
                        .insert(&evicted_template)
                        .expect("evicted template should be valid");
                    state_wlock_guard
                        .templates
                        .insert(evicted_template.template_id.clone(), evicted_template);
                } else {
                    state_wlock_guard.templates.remove(&template_id);
                    state_wlock_guard.template_matcher.remove(&template_id);
                }
                return Err(error);
            }
            return Ok(EmptyResponse {});
        }
    }

    #[instrument(name = "metastore.file_backed.get_index_template", skip(self))]
    async fn get_index_template(
        &self,
        request: GetIndexTemplateRequest,
    ) -> MetastoreResult<GetIndexTemplateResponse> {
        // Templates live in the manifest with the indexes: a node that never wrote would not see
        // one another node added (see `adopt_indexes_from_manifest`).
        self.adopt_indexes_from_manifest().await?;
        let inner_rlock_guard = self.state.read().await;
        let index_template = inner_rlock_guard
            .templates
            .get(&request.template_id)
            .ok_or({
                MetastoreError::NotFound(EntityKind::IndexTemplate {
                    template_id: request.template_id,
                })
            })?;
        let index_template_json = serde_utils::to_json_str(index_template)?;
        let response = GetIndexTemplateResponse {
            index_template_json,
        };
        Ok(response)
    }

    #[instrument(name = "metastore.file_backed.find_index_template_matches", skip(self))]
    async fn find_index_template_matches(
        &self,
        request: FindIndexTemplateMatchesRequest,
    ) -> MetastoreResult<FindIndexTemplateMatchesResponse> {
        // The control plane creates indexes from the template this returns, so a stale view does
        // not fail loudly: it creates the index without the template another node added.
        self.adopt_indexes_from_manifest().await?;
        let inner_rlock_guard = self.state.read().await;

        let mut matches = Vec::new();

        for index_id in request.index_ids {
            if let Some(template_id) = inner_rlock_guard
                .template_matcher
                .find_match(&index_id)
                .clone()
            {
                let index_template = inner_rlock_guard
                    .templates
                    .get(&template_id)
                    .expect("template should exist");
                let index_template_json = serde_utils::to_json_str(index_template)?;
                let index_template_match = IndexTemplateMatch {
                    index_id,
                    template_id,
                    index_template_json,
                };
                matches.push(index_template_match);
            };
        }
        let response = FindIndexTemplateMatchesResponse { matches };
        Ok(response)
    }

    #[instrument(name = "metastore.file_backed.list_index_templates", skip_all)]
    async fn list_index_templates(
        &self,
        _request: ListIndexTemplatesRequest,
    ) -> MetastoreResult<ListIndexTemplatesResponse> {
        self.adopt_indexes_from_manifest().await?;
        let inner_rlock_guard = self.state.read().await;

        let index_templates_json: Vec<String> = inner_rlock_guard
            .templates
            .values()
            .map(serde_utils::to_json_str)
            .collect::<MetastoreResult<_>>()?;
        let response = ListIndexTemplatesResponse {
            index_templates_json,
        };
        Ok(response)
    }

    #[instrument(name = "metastore.file_backed.delete_index_templates", skip(self))]
    async fn delete_index_templates(
        &self,
        request: DeleteIndexTemplatesRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut evicted_templates = Vec::with_capacity(request.template_ids.len());
            let mut state_wlock_guard = self.state.write().await;
            let mut manifest_version_opt = None;
            self.reload_manifest_if_distributed(&mut state_wlock_guard, &mut manifest_version_opt)
                .await?;

            for template_id in &request.template_ids {
                if let Some(evicted_template) = state_wlock_guard.templates.remove(template_id) {
                    evicted_templates.push(evicted_template);
                    state_wlock_guard.template_matcher.remove(template_id);
                }
            }

            if let Err(error) = self
                .save_manifest_cas(&state_wlock_guard, &mut manifest_version_opt, attempt)
                .await
            {
                if is_manifest_conflict(&error) && attempt < DISTRIBUTED_MAX_ATTEMPTS {
                    drop(state_wlock_guard);
                    tokio::time::sleep(distributed_retry_backoff(attempt)).await;
                    continue;
                }
                // Rollback on error.
                for evicted_template in evicted_templates {
                    state_wlock_guard
                        .template_matcher
                        .insert(&evicted_template)
                        .expect("evicted template should be valid");
                    state_wlock_guard
                        .templates
                        .insert(evicted_template.template_id.clone(), evicted_template);
                }
                return Err(error);
            }
            return Ok(EmptyResponse {});
        }
    }

    // Get cluster identity api

    // this returns a constant uuid. on first call, it generate said uuid if it doesn't already
    // exists
    #[instrument(name = "metastore.file_backed.get_cluster_identity", skip_all)]
    async fn get_cluster_identity(
        &self,
        _: GetClusterIdentityRequest,
    ) -> MetastoreResult<GetClusterIdentityResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut state_wlock_guard = self.state.write().await;
            let mut manifest_version_opt = None;
            self.reload_manifest_if_distributed(&mut state_wlock_guard, &mut manifest_version_opt)
                .await?;

            if state_wlock_guard.identity.is_nil() {
                state_wlock_guard.identity = Uuid::new_v4();

                if let Err(error) = self
                    .save_manifest_cas(&state_wlock_guard, &mut manifest_version_opt, attempt)
                    .await
                {
                    state_wlock_guard.identity = Uuid::nil();
                    if is_manifest_conflict(&error) && attempt < DISTRIBUTED_MAX_ATTEMPTS {
                        // Another node minted the identity first; reload and adopt theirs.
                        drop(state_wlock_guard);
                        tokio::time::sleep(distributed_retry_backoff(attempt)).await;
                        continue;
                    }
                    return Err(error);
                }
            }

            return Ok(GetClusterIdentityResponse {
                uuid: state_wlock_guard.identity.hyphenated().to_string(),
            });
        }
    }

    // Metrics Splits API

    #[instrument(name = "metastore.file_backed.stage_metrics_splits", skip_all, fields(index_uid = %request.index_uid()))]
    async fn stage_metrics_splits(
        &self,
        request: StageMetricsSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();
        let splits_metadata = request.deserialize_splits_metadata()?;

        if splits_metadata.is_empty() {
            return Ok(EmptyResponse {});
        }

        self.mutate(&index_uid, |index| {
            let mutated = index.stage_metrics_splits(splits_metadata.clone())?;
            if mutated {
                Ok(MutationOccurred::Yes(()))
            } else {
                Ok(MutationOccurred::No(()))
            }
        })
        .await?;

        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.publish_metrics_splits", skip_all, fields(index_uid = %request.index_uid()))]
    async fn publish_metrics_splits(
        &self,
        request: PublishMetricsSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_checkpoint_delta: Option<IndexCheckpointDelta> =
            request.deserialize_index_checkpoint()?;
        let index_uid = request.index_uid().clone();
        let staged_split_ids = request.staged_split_ids;
        let replaced_split_ids = request.replaced_split_ids;
        let publish_token_opt = request.publish_token_opt;

        self.mutate(&index_uid, |index| {
            let mutated = index.publish_metrics_splits(
                &staged_split_ids,
                &replaced_split_ids,
                index_checkpoint_delta.clone(),
                publish_token_opt.clone().map(|token| token.into()),
            )?;
            if mutated {
                Ok(MutationOccurred::Yes(()))
            } else {
                Ok(MutationOccurred::No(()))
            }
        })
        .await?;

        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.list_metrics_splits", skip_all, fields(index_uid = %request.index_uid()))]
    async fn list_metrics_splits(
        &self,
        request: ListMetricsSplitsRequest,
    ) -> MetastoreResult<ListMetricsSplitsResponse> {
        use crate::metastore::ParquetSplitRecord;

        let index_uid = request.index_uid().clone();
        let query = request.deserialize_query()?;

        let stored_splits = self
            .read(&index_uid, |index| Ok(index.list_metrics_splits(&query)))
            .await?;

        let split_records: Vec<ParquetSplitRecord> = stored_splits
            .into_iter()
            .map(|s| ParquetSplitRecord {
                state: s.state,
                update_timestamp: s.update_timestamp,
                metadata: s.metadata,
            })
            .collect();

        ListMetricsSplitsResponse::try_from_splits(&split_records)
    }

    #[instrument(name = "metastore.file_backed.mark_metrics_splits_for_deletion", skip_all, fields(index_uid = %request.index_uid()))]
    async fn mark_metrics_splits_for_deletion(
        &self,
        request: MarkMetricsSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();
        let split_ids = request.split_ids;

        if split_ids.is_empty() {
            return Ok(EmptyResponse {});
        }

        self.mutate(&index_uid, |index| {
            let mutated = index.mark_metrics_splits_for_deletion(&split_ids)?;
            if mutated {
                Ok(MutationOccurred::Yes(()))
            } else {
                Ok(MutationOccurred::No(()))
            }
        })
        .await?;

        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.delete_metrics_splits", skip_all, fields(index_uid = %request.index_uid()))]
    async fn delete_metrics_splits(
        &self,
        request: DeleteMetricsSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();
        let split_ids = request.split_ids;

        if split_ids.is_empty() {
            return Ok(EmptyResponse {});
        }

        self.mutate(&index_uid, |index| {
            let mutated = index.delete_metrics_splits(&split_ids)?;
            if mutated {
                Ok(MutationOccurred::Yes(()))
            } else {
                Ok(MutationOccurred::No(()))
            }
        })
        .await?;

        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.stage_sketch_splits", skip_all, fields(index_uid = %request.index_uid()))]
    async fn stage_sketch_splits(
        &self,
        request: StageSketchSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();
        let splits_metadata = request.deserialize_splits_metadata()?;

        if splits_metadata.is_empty() {
            return Ok(EmptyResponse {});
        }

        self.mutate(&index_uid, |index| {
            let mutated = index.stage_sketch_splits(splits_metadata.clone())?;
            if mutated {
                Ok(MutationOccurred::Yes(()))
            } else {
                Ok(MutationOccurred::No(()))
            }
        })
        .await?;

        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.publish_sketch_splits", skip_all, fields(index_uid = %request.index_uid()))]
    async fn publish_sketch_splits(
        &self,
        request: PublishSketchSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_checkpoint_delta: Option<IndexCheckpointDelta> =
            request.deserialize_index_checkpoint()?;
        let index_uid = request.index_uid().clone();
        let staged_split_ids = request.staged_split_ids;
        let replaced_split_ids = request.replaced_split_ids;
        let publish_token_opt = request.publish_token_opt;

        self.mutate(&index_uid, |index| {
            let mutated = index.publish_sketch_splits(
                &staged_split_ids,
                &replaced_split_ids,
                index_checkpoint_delta.clone(),
                publish_token_opt.clone().map(|token| token.into()),
            )?;
            if mutated {
                Ok(MutationOccurred::Yes(()))
            } else {
                Ok(MutationOccurred::No(()))
            }
        })
        .await?;

        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.list_sketch_splits", skip_all, fields(index_uid = %request.index_uid()))]
    async fn list_sketch_splits(
        &self,
        request: ListSketchSplitsRequest,
    ) -> MetastoreResult<ListSketchSplitsResponse> {
        use crate::metastore::ParquetSplitRecord;

        let index_uid = request.index_uid().clone();
        let query = request.deserialize_query()?;

        let stored_splits = self
            .read(&index_uid, |index| Ok(index.list_sketch_splits(&query)))
            .await?;

        let split_records: Vec<ParquetSplitRecord> = stored_splits
            .into_iter()
            .map(|s| ParquetSplitRecord {
                state: s.state,
                update_timestamp: s.update_timestamp,
                metadata: s.metadata,
            })
            .collect();

        ListSketchSplitsResponse::try_from_splits(&split_records)
    }

    #[instrument(name = "metastore.file_backed.mark_sketch_splits_for_deletion", skip_all, fields(index_uid = %request.index_uid()))]
    async fn mark_sketch_splits_for_deletion(
        &self,
        request: MarkSketchSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();
        let split_ids = request.split_ids;

        if split_ids.is_empty() {
            return Ok(EmptyResponse {});
        }

        self.mutate(&index_uid, |index| {
            let mutated = index.mark_sketch_splits_for_deletion(&split_ids)?;
            if mutated {
                Ok(MutationOccurred::Yes(()))
            } else {
                Ok(MutationOccurred::No(()))
            }
        })
        .await?;

        Ok(EmptyResponse {})
    }

    #[instrument(name = "metastore.file_backed.delete_sketch_splits", skip_all, fields(index_uid = %request.index_uid()))]
    async fn delete_sketch_splits(
        &self,
        request: DeleteSketchSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();
        let split_ids = request.split_ids;

        if split_ids.is_empty() {
            return Ok(EmptyResponse {});
        }

        self.mutate(&index_uid, |index| {
            let mutated = index.delete_sketch_splits(&split_ids)?;
            if mutated {
                Ok(MutationOccurred::Yes(()))
            } else {
                Ok(MutationOccurred::No(()))
            }
        })
        .await?;

        Ok(EmptyResponse {})
    }
}

impl MetastoreServiceExt for FileBackedMetastore {}

async fn get_index_mutex(
    index_id: &str,
    lazy_index_status: &LazyIndexStatus,
) -> MetastoreResult<Arc<Mutex<FileBackedIndex>>> {
    match lazy_index_status {
        LazyIndexStatus::Active(lazy_index) => lazy_index.get().await,
        LazyIndexStatus::Creating => Err(MetastoreError::Internal {
            message: format!("index `{index_id}` cannot be retrieved"),
            cause: "index `{index_id}` is in transitioning state `creating` and this should not \
                    happened. either recreate or delete it"
                .to_string(),
        }),
        LazyIndexStatus::Deleting => Err(MetastoreError::Internal {
            message: format!("index `{index_id}` cannot be retrieved"),
            cause: "index `{index_id}` is in transitioning state `deleting` and this should not \
                    happened. try to delete it again"
                .to_string(),
        }),
    }
}

async fn get_index_metadata(
    metastore: FileBackedMetastore,
    index_id: IndexId,
) -> MetastoreResult<Option<IndexMetadata>> {
    let request = IndexMetadataRequest::for_index_id(index_id);
    let index_metadata_result = metastore
        .index_metadata(request)
        .await
        .and_then(|response| response.deserialize_index_metadata());
    match index_metadata_result {
        Ok(index_metadata) => Ok(Some(index_metadata)),
        Err(MetastoreError::NotFound { .. }) => Ok(None),
        Err(MetastoreError::Internal { message, cause }) => {
            // Indexes can be in transient states `Creating` or `Deleting`.
            // It is fine to ignore those errors.
            if message.contains("transient state") {
                Ok(None)
            } else {
                Err(MetastoreError::Internal { message, cause })
            }
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
#[async_trait]
impl crate::tests::DefaultForTest for FileBackedMetastore {
    async fn default_for_test() -> Self {
        use quickwit_storage::RamStorage;
        let mut metastore = FileBackedMetastore::try_new(Arc::new(RamStorage::default()), None)
            .await
            .unwrap();
        // The whole metastore test suite is run twice: once on the historical layout and once, with
        // this environment variable set, on the sharded one. The layout of an index is recorded in
        // the objects themselves, so the same tests can exercise both without knowing about it.
        if quickwit_common::get_bool_from_env(SHARDED_LAYOUT_TEST_ENV_KEY, false) {
            metastore.set_distributed(true);
            metastore.set_index_layout(IndexLayout::Sharded { num_slots: 8 });
        }
        if quickwit_common::get_bool_from_env(MANIFEST_LAYOUT_TEST_ENV_KEY, false) {
            metastore.set_distributed(true);
            metastore.set_index_layout(IndexLayout::ManifestSegments {
                bucket_secs: MANIFEST_LAYOUT_BUCKET_SECS,
                num_stripes: MANIFEST_LAYOUT_NUM_STRIPES,
            });
        }
        metastore
    }
}

#[cfg(test)]
mod tests {

    use std::ops::RangeInclusive;
    use std::path::Path;
    use std::sync::Arc;

    use futures::executor::block_on;
    use quickwit_common::uri::{Protocol, Uri};
    use quickwit_config::{INGEST_V2_SOURCE_ID, IndexConfig, SourceConfig};
    use quickwit_proto::ingest::{Shard, ShardState};
    use quickwit_proto::metastore::{DeleteQuery, MetastoreError};
    use quickwit_proto::types::{Position, ShardId, SourceId};
    use quickwit_query::query_ast::qast_helper;
    use quickwit_storage::{LocalFileStorage, MockStorage, RamStorage, Storage, StorageErrorKind};
    use rand::RngExt;
    use tests::manifest::{IndexStatus, Manifest};
    use time::OffsetDateTime;
    use tokio::time::Duration;

    use super::store_operations::{metastore_filepath, put_index_given_index_id};
    use super::*;
    use crate::metastore::MetastoreServiceStreamSplitsExt;
    use crate::tests::DefaultForTest;
    use crate::tests::shard::ReadWriteShardsForTest;
    use crate::{IndexMetadata, ListSplitsQuery, SplitMetadata, SplitState, metastore_test_suite};

    #[async_trait]
    impl ReadWriteShardsForTest for FileBackedMetastore {
        async fn insert_shards(
            &self,
            index_uid: &IndexUid,
            source_id: &SourceId,
            shards: Vec<Shard>,
        ) {
            self.mutate(index_uid, |index| {
                index.insert_shards(source_id, shards.clone());
                Ok(MutationOccurred::Yes(()))
            })
            .await
            .unwrap();
        }

        async fn list_all_shards(&self, index_uid: &IndexUid, source_id: &SourceId) -> Vec<Shard> {
            self.read(index_uid, |index| {
                let shards = index.list_all_shards(source_id);
                Ok(shards)
            })
            .await
            .unwrap()
        }
    }

    metastore_test_suite!(crate::FileBackedMetastore);

    async fn list_published_split_ids_with(
        metastore: &FileBackedMetastore,
        index_uid: &IndexUid,
    ) -> MetastoreResult<Vec<String>> {
        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(crate::SplitState::Published);
        let mut splits: Vec<String> = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
            .await?
            .collect_split_ids()
            .await?
            .into_iter()
            .map(String::from)
            .collect();
        splits.sort();
        Ok(splits)
    }

    /// A mutation that only changes a shard writes that shard's own object and leaves the root
    /// alone.
    ///
    /// This is the property the five-node run needs: before the shard state moved out of the root,
    /// every node publishing any shard of any source wrote the one hot root object, lost the
    /// compare-and-swap and replayed. The root's version is what tells the two apart — a skipped
    /// compare-and-swap leaves it where it was.
    ///
    /// The same holds for the shard API the control plane drives: `open_shards` has to leave an
    /// object behind, or the next reader of the index sees a source without its shards, and the
    /// nodes that are not the one that opened it cannot ingest at all.
    #[tokio::test]
    async fn test_a_shard_mutation_does_not_touch_the_root() {
        use quickwit_proto::metastore::{OpenShardSubrequest, OpenShardsRequest};

        let storage = Arc::new(RamStorage::default());
        let mut metastore = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        metastore.set_distributed(true);
        metastore.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });

        let index_id = "test-shard-objects";
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let index_uid = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
            .await
            .unwrap()
            .index_uid()
            .clone();
        let source_id = SourceId::from(INGEST_V2_SOURCE_ID);
        metastore
            .add_source(AddSourceRequest {
                index_uid: Some(index_uid.clone()),
                source_config_json: serde_json::to_string(&SourceConfig::ingest_v2()).unwrap(),
            })
            .await
            .unwrap();

        let root_path = Path::new("test-shard-objects/v3/root.json");
        let (_, root_version_before) = storage.get_all_with_version(root_path).await.unwrap();
        // The shard object is named after its source, which for an ingest-v2 source is
        // `_ingest-source`.
        let shard_path = format!("test-shard-objects/v3/shards/{source_id}/01J0SHARD.json");

        // A shard opened through the API the control plane uses, before any test-only insertion.
        let opened = metastore
            .open_shards(OpenShardsRequest {
                subrequests: vec![OpenShardSubrequest {
                    index_uid: Some(index_uid.clone()),
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from("01J0OPENED")),
                    ingester_id: "test-ingester".to_string(),
                    ..Default::default()
                }],
            })
            .await
            .unwrap();
        let opened_shard_id = opened.subresponses[0]
            .open_shard
            .as_ref()
            .unwrap()
            .shard_id()
            .clone();
        let opened_path =
            format!("test-shard-objects/v3/shards/{source_id}/{opened_shard_id}.json");
        assert!(
            storage.exists(Path::new(&opened_path)).await.unwrap(),
            "open_shards has to persist the shard state: {opened_path} is missing"
        );

        let shard = |publish_position_inclusive: Position| Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from("01J0SHARD")),
            shard_state: ShardState::Open as i32,
            ingester_id: "test-ingester".to_string(),
            publish_position_inclusive: Some(publish_position_inclusive),
            ..Default::default()
        };
        metastore
            .insert_shards(&index_uid, &source_id, vec![shard(Position::Beginning)])
            .await;
        assert!(
            storage.exists(Path::new(&shard_path)).await.unwrap(),
            "the shard has an object of its own"
        );
        let (_, root_version_after_open) = storage.get_all_with_version(root_path).await.unwrap();
        assert_eq!(
            root_version_before, root_version_after_open,
            "opening a shard must not write the root"
        );
        let object_after_open = storage.get_all(Path::new(&shard_path)).await.unwrap();

        // A second mutation of the same shard: the object follows it, the root does not.
        metastore
            .insert_shards(&index_uid, &source_id, vec![shard(Position::offset(7u64))])
            .await;
        let object_after_publish = storage.get_all(Path::new(&shard_path)).await.unwrap();
        assert_ne!(
            object_after_open.as_slice(),
            object_after_publish.as_slice(),
            "the shard object carries the new state"
        );
        let (_, root_version_after_publish) =
            storage.get_all_with_version(root_path).await.unwrap();
        assert_eq!(
            root_version_before, root_version_after_publish,
            "publishing into a shard must not write the root"
        );
        let shards = metastore.list_all_shards(&index_uid, &source_id).await;
        let updated = shards
            .iter()
            .find(|shard| shard.shard_id() == ShardId::from("01J0SHARD"))
            .expect("the shard the test inserted");
        assert_eq!(
            updated.publish_position_inclusive,
            Some(Position::offset(7u64)),
            "the state read back is the one in the object"
        );
        assert_eq!(
            shards.len(),
            2,
            "the shard opened through the API and the inserted one are both in the index"
        );
    }

    /// An index a node adopted while it was still being created becomes visible when the create
    /// finishes.
    ///
    /// A create takes more than one round trip (the manifest says `Creating`, the index file is
    /// written, the manifest says `Active`), and a listing that lands in the middle adopts the
    /// index as `Creating`. Adopting only what is missing would leave it there for ever, because
    /// nothing else on a node that never writes replaces what its state already holds.
    #[tokio::test]
    async fn test_an_index_adopted_while_it_was_creating_becomes_visible() {
        use super::manifest::{IndexStatus, load_manifest_with_version, save_manifest};

        let storage = Arc::new(RamStorage::default());
        let mut node_a = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        node_a.set_distributed(true);
        node_a.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });
        let mut node_b = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        node_b.set_distributed(true);
        node_b.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });

        let index_id = "test-created-window";
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        node_a
            .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
            .await
            .unwrap();
        let listed_by_b = |metastore: FileBackedMetastore| async move {
            metastore
                .list_indexes_metadata(ListIndexesMetadataRequest::all())
                .await
                .unwrap()
                .deserialize_indexes_metadata()
                .await
                .unwrap()
        };

        // A listing that lands in the middle of the create sees it as `Creating`.
        let (mut manifest, _) = load_manifest_with_version(&*storage).await.unwrap();
        manifest
            .indexes
            .insert(index_id.to_string(), IndexStatus::Creating);
        save_manifest(&*storage, &manifest).await.unwrap();
        assert!(
            listed_by_b(node_b.clone()).await.is_empty(),
            "an index that is still being created is not listed"
        );

        // The create finishes, and the node that never wrote has to see it.
        let (mut manifest, _) = load_manifest_with_version(&*storage).await.unwrap();
        manifest
            .indexes
            .insert(index_id.to_string(), IndexStatus::Active);
        save_manifest(&*storage, &manifest).await.unwrap();
        let listed = listed_by_b(node_b.clone()).await;
        assert!(
            listed
                .iter()
                .any(|metadata| metadata.index_id() == index_id),
            "the index stayed invisible after the create finished: {listed:?}"
        );
    }

    /// A node that has not written since another node added a template still matches it.
    ///
    /// Templates live in the same manifest as the indexes, and the control plane creates an index
    /// from the template this lookup returns. A stale view would not fail loudly: it would create
    /// the index without the template another node added.
    #[tokio::test]
    async fn test_a_node_that_never_wrote_still_matches_a_template_another_node_added() {
        use quickwit_proto::metastore::{
            CreateIndexTemplateRequest, FindIndexTemplateMatchesRequest,
        };

        let storage = Arc::new(RamStorage::default());
        let mut node_a = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        node_a.set_distributed(true);
        node_a.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });
        // Started before the template exists, and it never lists anything.
        let mut node_b = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        node_b.set_distributed(true);
        node_b.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });
        // One node per reader under test, each started before the template exists and each only
        // calling its own reader: a call to another reader would fill the cache for it.
        let mut node_c = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        node_c.set_distributed(true);
        node_c.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });
        let mut node_d = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        node_d.set_distributed(true);
        node_d.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });

        let template_id = "test-late-template";
        let template =
            quickwit_config::IndexTemplate::for_test(template_id, &["test-template-*"], 60);
        node_a
            .create_index_template(CreateIndexTemplateRequest {
                index_template_json: serde_json::to_string(&template).unwrap(),
                overwrite: false,
            })
            .await
            .unwrap();

        let matches = node_b
            .find_index_template_matches(FindIndexTemplateMatchesRequest {
                index_ids: vec!["test-template-index".to_string()],
            })
            .await
            .unwrap()
            .matches;
        assert_eq!(
            matches.len(),
            1,
            "the node has to adopt the template another node added: {matches:?}"
        );
        assert_eq!(matches[0].template_id, template_id);

        // The two management readers adopt as well, each on a node that has never read anything.
        let fetched = node_c
            .get_index_template(GetIndexTemplateRequest {
                template_id: template_id.to_string(),
            })
            .await
            .unwrap();
        assert!(
            fetched.index_template_json.contains(template_id),
            "reading one template has to adopt it: {}",
            fetched.index_template_json
        );
        let listed = node_d
            .list_index_templates(ListIndexTemplatesRequest::default())
            .await
            .unwrap();
        assert!(
            listed
                .index_templates_json
                .iter()
                .any(|json| json.contains(template_id)),
            "listing the templates has to adopt them: {:?}",
            listed.index_templates_json
        );
    }

    /// A node that has not written since another node created an index still lists it.
    ///
    /// The index set lives in the metastore's `manifest.json`, and a long-running node only
    /// reloaded it when it wrote: a node that never wrote would not list an index another node
    /// created, so its control plane never planned it and its requests for it failed. Two nodes on
    /// one storage are the shape to test, and the second one here never writes.
    #[tokio::test]
    async fn test_a_node_that_never_wrote_still_lists_an_index_another_node_created() {
        let storage = Arc::new(RamStorage::default());
        let mut node_a = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        node_a.set_distributed(true);
        node_a.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });
        let mut node_b = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        node_b.set_distributed(true);
        node_b.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });
        // Two more nodes, started before the index exists like the one above: their caches stay
        // empty until the listing under test fills them, which is what makes the assertions about
        // `list_index_stats` and the listing that names no index load-bearing.
        let mut node_c = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        node_c.set_distributed(true);
        node_c.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });
        let mut node_d = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        node_d.set_distributed(true);
        node_d.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });

        let listed_by_b = |metastore: FileBackedMetastore| async move {
            metastore
                .list_indexes_metadata(ListIndexesMetadataRequest::all())
                .await
                .unwrap()
                .deserialize_indexes_metadata()
                .await
                .unwrap()
        };
        let adoptions_before =
            crate::metastore::file_backed::metrics::MANIFEST_ADOPTIONS_TOTAL.get();
        assert!(
            listed_by_b(node_b.clone()).await.is_empty(),
            "the node starts with an empty metastore"
        );

        let index_id = "test-late-adoption";
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let index_uid = node_a
            .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
            .await
            .unwrap()
            .index_uid()
            .clone();

        let listed = listed_by_b(node_b.clone()).await;
        assert!(
            listed
                .iter()
                .any(|metadata| metadata.index_id() == index_id),
            "a node that never wrote has to adopt the index: {listed:?}"
        );
        assert!(
            crate::metastore::file_backed::metrics::MANIFEST_ADOPTIONS_TOTAL.get()
                - adoptions_before
                >= 1,
            "the read a listing pays for adopting has to be visible outside the logs"
        );
        // A node whose cache has never been filled adopts through the stats listing too, and the
        // compaction planner reads splits through the listing that names no index. Both get their
        // own fresh node: a listing on this one would have filled the cache for them.
        let stats = node_c
            .list_index_stats(ListIndexStatsRequest {
                index_id_patterns: vec![index_id.to_string()],
            })
            .await
            .unwrap();
        assert!(
            stats.index_stats.iter().any(|stats| stats
                .index_uid
                .as_ref()
                .map(|uid| uid.index_id.as_str())
                == Some(index_id)),
            "listing stats has to adopt the index too: {:?}",
            stats.index_stats
        );
        // A split to find, published before `node_d` reads anything.
        let split_metadata = SplitMetadata::for_test(SplitId::from("split-adoption"));
        node_a
            .stage_splits(
                StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                    .unwrap(),
            )
            .await
            .unwrap();
        node_a
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid),
                staged_split_ids: vec!["split-adoption".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let split_ids = node_d
            .list_splits(
                ListSplitsRequest::try_from_list_splits_query(&ListSplitsQuery::for_all_indexes())
                    .unwrap(),
            )
            .await
            .unwrap()
            .collect_split_ids()
            .await
            .unwrap();
        assert!(
            split_ids
                .iter()
                .any(|split_id| split_id == &SplitId::from("split-adoption")),
            "the listing that names no index has to adopt the index and find its split: \
             {split_ids:?}"
        );
    }

    /// A publish whose shard-object write is lost is replayed and finishes, rather than leaving a
    /// split that no shard state describes.
    ///
    /// A publish commits its stripe first (the split), then the state of the shards it touched,
    /// then the root. The hook fails that middle write: the split is committed by then, so the
    /// caller's retry has to finish the mutation — that is the case the acceptance list of
    /// `docs/internals/metastore-v3-shard-state.md` names, and the one the publisher's `is_replay`
    /// field exists for.
    #[tokio::test]
    async fn test_a_lost_shard_object_write_is_replayed() {
        use super::manifest_layout::test_hooks;
        use crate::checkpoint::{IndexCheckpointDelta, PartitionId, SourceCheckpointDelta};

        let storage = Arc::new(RamStorage::default());
        let mut metastore = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        metastore.set_distributed(true);
        metastore.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });

        let index_id = "test-shard-replay";
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let index_uid = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
            .await
            .unwrap()
            .index_uid()
            .clone();
        let source_id = SourceId::from(INGEST_V2_SOURCE_ID);
        metastore
            .add_source(AddSourceRequest {
                index_uid: Some(index_uid.clone()),
                source_config_json: serde_json::to_string(&SourceConfig::ingest_v2()).unwrap(),
            })
            .await
            .unwrap();

        // A shard of the ingest-v2 source, opened through the API the control plane uses.
        let opened = metastore
            .open_shards(OpenShardsRequest {
                subrequests: vec![OpenShardSubrequest {
                    index_uid: Some(index_uid.clone()),
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from("01J0REPLAY")),
                    ingester_id: "test-ingester".to_string(),
                    publish_token: Some("test-publish-token".to_string()),
                    ..Default::default()
                }],
            })
            .await
            .unwrap();
        assert!(
            opened.subresponses[0].open_shard.is_some(),
            "the shard the test opens has to exist"
        );

        // A split to publish, and the checkpoint delta that moves the shard's publish position.
        let split_metadata = SplitMetadata::for_test(SplitId::from("split-replay"));
        metastore
            .stage_splits(
                StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut source_delta = SourceCheckpointDelta::default();
        source_delta
            .record_partition_delta(
                PartitionId::from("01J0REPLAY"),
                Position::Beginning,
                Position::offset(1u64),
            )
            .unwrap();
        let delta_json = serde_json::to_string(&IndexCheckpointDelta {
            source_id: source_id.clone(),
            source_delta,
        })
        .unwrap();

        // Fail the first write of the shard state; the replay has to finish the publish.
        let injections_before = test_hooks::shard_object_write_injections();
        test_hooks::fail_next_shard_object_writes(index_id, 1);
        metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: vec!["split-replay".to_string()],
                index_checkpoint_delta_json_opt: Some(delta_json.clone()),
                publish_token_opt: Some("test-publish-token".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            test_hooks::shard_object_write_injections(),
            injections_before + 1,
            "the injection has to have fired for this test to mean anything"
        );

        let mut pages = storage.list(Path::new("test-shard-replay/v3/shards"));
        let mut paths = Vec::new();
        while let Some(Ok(page)) = futures::StreamExt::next(&mut pages).await {
            for metadata in page {
                paths.push(metadata.path);
            }
        }
        assert!(
            paths.iter().any(|path| path.ends_with("01J0REPLAY.json")),
            "the replay has to leave the shard state on its object: {paths:?}"
        );
        assert_eq!(
            list_published_split_ids_with(&metastore, &index_uid)
                .await
                .unwrap(),
            vec!["split-replay".to_string()],
            "the split the first attempt committed is the one the index has"
        );
        let shards = metastore.list_all_shards(&index_uid, &source_id).await;
        assert_eq!(shards.len(), 1, "the index has the one shard");
        assert_eq!(
            shards[0].publish_position_inclusive,
            Some(Position::offset(1u64)),
            "the shard state the replay committed is the one on its object"
        );
    }

    // Hook of the layout itself: the suite above runs on whichever layout the environment selects,
    // so this test pins that the selection actually reaches the storage.
    #[tokio::test]
    async fn test_sharded_layout_is_used_when_configured() {
        let storage = Arc::new(RamStorage::default());
        let mut metastore = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        metastore.set_distributed(true);
        metastore.set_index_layout(IndexLayout::Sharded { num_slots: 4 });

        let index_id = "test-sharded-layout";
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        assert!(
            storage
                .exists(Path::new("test-sharded-layout/v2/root.json"))
                .await
                .unwrap(),
            "the index should have been created in the sharded layout"
        );
        assert!(
            !storage
                .exists(Path::new("test-sharded-layout/metastore.json"))
                .await
                .unwrap(),
            "the sharded layout must not also write the single metadata file"
        );
        assert!(metastore.index_exists(index_id).await.unwrap());
        metastore
            .delete_index(DeleteIndexRequest {
                index_uid: Some(index_uid),
            })
            .await
            .unwrap();
        assert!(!metastore.index_exists(index_id).await.unwrap());
    }

    /// Hook of the manifest layout, plus the case the layout cache can get wrong: an index deleted
    /// and recreated under the same id has to be written with the *new* layout parameters, not the
    /// ones its predecessor had.
    #[tokio::test]
    async fn test_manifest_layout_is_used_when_configured() {
        let storage = Arc::new(RamStorage::default());
        let mut metastore = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        metastore.set_distributed(true);
        metastore.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });

        let index_id = "test-manifest-layout";
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let index_uid = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
            .await
            .unwrap()
            .index_uid()
            .clone();
        assert!(
            storage
                .exists(Path::new("test-manifest-layout/v3/root.json"))
                .await
                .unwrap(),
            "the index should have been created in the manifest layout"
        );

        let split_metadata = SplitMetadata::for_test(SplitId::from("split-0"));
        metastore
            .stage_splits(
                StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                    .unwrap(),
            )
            .await
            .unwrap();
        metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: vec!["split-0".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            list_published_split_ids_with(&metastore, &index_uid)
                .await
                .unwrap(),
            vec!["split-0".to_string()]
        );

        // Delete and recreate under the same id: the cached layout of the dead index must not be
        // reused.
        metastore
            .delete_index(DeleteIndexRequest {
                index_uid: Some(index_uid),
            })
            .await
            .unwrap();
        let index_uid = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
            .await
            .unwrap()
            .index_uid()
            .clone();
        metastore
            .stage_splits(
                StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                    .unwrap(),
            )
            .await
            .unwrap();
        metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: vec!["split-0".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            list_published_split_ids_with(&metastore, &index_uid)
                .await
                .unwrap(),
            vec!["split-0".to_string()]
        );
    }

    #[tokio::test]
    async fn test_metastore_connectivity_and_endpoints() {
        let metastore = FileBackedMetastore::default_for_test().await;
        metastore.check_connectivity().await.unwrap();
        assert_eq!(metastore.endpoints()[0].protocol(), Protocol::Ram);
    }
    /// The manifest layout commits one stripe at a time, so a mutation can be replayed after part
    /// of its own publication happened. The state that replay sees — some requested splits
    /// already `Published`, the rest still `Staged` — is indistinguishable from another writer
    /// having published them, so the mutation is told it is a replay and tolerates them there.
    ///
    /// Without that, the publish fails permanently: the first attempt committed one stripe, and the
    /// replay refuses the splits it published itself, so an RPC whose work is half done can never
    /// finish. The test injects the failure on the second stripe, then asserts the publish finishes
    /// and both splits are published.
    ///
    /// Publishing a split that is already published *without* being a replay stays the hard error
    /// it always was, on every layout.
    #[tokio::test]
    async fn test_a_publish_that_lost_one_stripe_replays_to_success() {
        use crate::metastore::file_backed::manifest_layout::{ManifestLayout, test_hooks};

        let index_id = "test-publish-replay";
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let storage = Arc::new(RamStorage::default());
        let mut metastore = FileBackedMetastore::try_new(storage, None).await.unwrap();
        metastore.set_distributed(true);
        metastore.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 3_600,
            num_stripes: 4,
        });
        let layout = ManifestLayout::new(index_id, 3_600, 4);
        let index_uid = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
            .await
            .unwrap()
            .index_uid()
            .clone();

        // One split per stripe, so the publish commits two manifests and can fail on the second.
        let split_for_stripe = |stripe: usize| {
            (0..)
                .map(|candidate| {
                    SplitMetadata::for_test(SplitId::from(format!("split-{candidate}")))
                })
                .find(|split_metadata| layout.stripe_of(split_metadata.split_id.as_str()) == stripe)
                .unwrap()
        };
        let split_a = split_for_stripe(0);
        let split_b = split_for_stripe(1);
        for split_metadata in [&split_a, &split_b] {
            metastore
                .stage_splits(
                    StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split_metadata)
                        .unwrap(),
                )
                .await
                .unwrap();
        }
        // Stripes commit in ascending order, so failing stripe 1 lets stripe 0 land first.
        test_hooks::fail_next_commit_for_stripe(1);
        metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: vec![split_a.split_id.to_string(), split_b.split_id.to_string()],
                ..Default::default()
            })
            .await
            .expect("the replay has to finish the publication the first attempt started");
        let mut expected = vec![split_a.split_id.to_string(), split_b.split_id.to_string()];
        expected.sort();
        assert_eq!(
            list_published_split_ids_with(&metastore, &index_uid)
                .await
                .unwrap(),
            expected
        );

        // The control: publishing an already-published split, not as a replay, is still an error.
        let error = metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid),
                staged_split_ids: vec![split_a.split_id.to_string()],
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(
            matches!(error, MetastoreError::FailedPrecondition { .. }),
            "publishing a published split outside a replay must still fail: {error:?}"
        );
    }

    /// A root that never commits has to be counted as an exhausted budget.
    ///
    /// The manifest layout commits the splits of a mutation before it commits the index metadata,
    /// so when the metadata is what keeps losing the caller gets an error after its split work
    /// already landed: the splits are published, the index metadata update is not, and only a
    /// replay of the same publish finishes it (the already-published splits are tolerated). This
    /// is the failure a real five-node run against a bucket 0.81 s away hit, and the counter
    /// stayed at zero because only the split path recorded its exhaustion.
    #[tokio::test]
    async fn test_a_root_that_never_commits_is_counted_as_exhausted() {
        use quickwit_config::{SourceConfig, SourceParams};

        use crate::metastore::file_backed::manifest_layout::test_hooks;
        use crate::metastore::file_backed::metrics::CAS_CONFLICTS_EXHAUSTED_TOTAL;

        // Process-global counters and parallel tests: the assertion is a lower bound.
        let exhausted_before = CAS_CONFLICTS_EXHAUSTED_TOTAL.get();

        let index_id = "test-root-exhaustion";
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let source_id = "test-source";
        let storage = Arc::new(RamStorage::default());
        let mut metastore = FileBackedMetastore::try_new(storage, None).await.unwrap();
        metastore.set_distributed(true);
        metastore.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 3_600,
            num_stripes: 4,
        });
        let split_metadata = SplitMetadata::for_test(SplitId::from("split-0"));
        let index_uid = metastore
            .create_index(
                CreateIndexRequest::try_from_index_and_source_configs(
                    &index_config,
                    &[SourceConfig::for_test(source_id, SourceParams::void())],
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .index_uid()
            .clone();
        metastore
            .stage_splits(
                StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                    .unwrap(),
            )
            .await
            .unwrap();

        // Every commit of the index metadata fails, the way a mesh of writers at a high round trip
        // keeps winning the race; the splits themselves commit.
        test_hooks::fail_next_root_commits(index_id, DISTRIBUTED_MAX_ATTEMPTS as u32);
        let checkpoint_delta = IndexCheckpointDelta::for_test(source_id, 0..10);
        let error = metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid),
                staged_split_ids: vec![split_metadata.split_id.to_string()],
                index_checkpoint_delta_json_opt: Some(
                    serde_json::to_string(&checkpoint_delta).unwrap(),
                ),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(
            matches!(error, MetastoreError::FailedPrecondition { .. }),
            "an index metadata that never commits must report the conflict: {error:?}"
        );
        assert!(
            CAS_CONFLICTS_EXHAUSTED_TOTAL.get() - exhausted_before >= 1,
            "giving up on the index metadata has to be visible to operators"
        );
    }

    /// A publish whose first attempt committed, replayed because its response was lost.
    ///
    /// The replay carries the checkpoint delta the first attempt applied. Applying it again is
    /// refused as incompatible, so a replay that does not tolerate it fails a mutation that is
    /// already done — on both layouts, since the delta is written in the index metadata either way.
    #[tokio::test]
    async fn test_a_replay_of_a_committed_publish_finishes() {
        for layout in [
            IndexLayout::Sharded { num_slots: 8 },
            IndexLayout::ManifestSegments {
                bucket_secs: 3_600,
                num_stripes: 4,
            },
        ] {
            let index_id = format!("test-replay-committed-{}", index_id_suffix(&layout));
            let (metastore, index_uid, request) = staged_publish_fixture(&index_id, layout).await;

            // The first attempt commits; its response is lost before the caller sees it.
            metastore.publish_splits(request(false)).await.unwrap();

            // The pipeline replays the same request, which the metastore has to finish rather than
            // refuse on the checkpoint delta it already applied.
            metastore
                .publish_splits(request(true))
                .await
                .expect("a replay of a committed publish has to finish");

            let splits = list_splits_of(&metastore, &index_uid).await;
            assert_eq!(splits.len(), 1);
            assert_eq!(splits[0].split_state, SplitState::Published);
        }
    }

    /// The tolerance is for replays only: a fresh request that re-sends an applied delta is still
    /// refused, so a caller bug stays visible instead of being answered with a success.
    #[tokio::test]
    async fn test_a_fresh_request_with_an_applied_delta_is_still_refused() {
        let index_id = "test-fresh-applied-delta";
        let (metastore, _index_uid, request) = staged_publish_fixture(
            index_id,
            IndexLayout::ManifestSegments {
                bucket_secs: 3_600,
                num_stripes: 4,
            },
        )
        .await;
        metastore.publish_splits(request(false)).await.unwrap();

        let error = metastore
            .publish_splits(request(false))
            .await
            .expect_err("a fresh request with an applied delta has to be refused");
        assert!(
            matches!(error, MetastoreError::FailedPrecondition { .. }),
            "the refusal has to be a precondition failure: {error:?}"
        );
        assert!(
            error.to_string().contains("checkpoint delta"),
            "the refusal has to name the checkpoint delta: {error}"
        );
    }

    /// Known gap, not a fix: the sharded layout writes the index root (the checkpoint among it)
    /// before the slots it touches, so a slot commit that fails leaves the checkpoint moved and the
    /// split unpublished. The metastore's own retry cannot finish it, because the delta it carries
    /// is applied already and this request has no proof the publish is its own: its split is *not*
    /// published, which is exactly what the failure left behind.
    ///
    /// Fixing it needs a decision the ledger holds
    /// (`qw-replay-tolerates-applied-delta`, parked as needs-human): either commit the slots first,
    /// so the failure leaves the checkpoint untouched, or record a writer identity the checkpoint
    /// format cannot carry today. This test pins the state so that a change to it is deliberate.
    #[tokio::test]
    async fn test_a_slot_commit_failure_after_the_root_is_a_known_gap() {
        use crate::metastore::file_backed::manifest_layout::test_hooks;

        let index_id = "test-slot-commit-after-root";
        let (metastore, index_uid, request) =
            staged_publish_fixture(index_id, IndexLayout::Sharded { num_slots: 8 }).await;

        test_hooks::fail_next_slot_commits(index_id, 1);
        let error = metastore
            .publish_splits(request(false))
            .await
            .expect_err("the slot commit fails after the root, and the retry cannot finish it");
        assert!(
            error.to_string().contains("incompatible checkpoint delta"),
            "the failure to pin is the checkpoint delta the retry cannot apply: {error}"
        );

        let splits = list_splits_of(&metastore, &index_uid).await;
        assert_eq!(splits.len(), 1);
        assert_eq!(
            splits[0].split_state,
            SplitState::Staged,
            "the split the failed slot commit left behind stays staged until the gap is fixed"
        );

        let replay = metastore.publish_splits(request(true)).await;
        assert!(
            replay.is_err(),
            "a caller replay does not finish it either: {replay:?}"
        );
    }

    /// The shard API path (ingest-v2) tolerates the delta of a replay too, and it proves the
    /// publish is its own with the shard's publish token: a replay may skip the delta, but a
    /// request that carries a different token — another writer's publish — may not.
    #[tokio::test]
    async fn test_the_shard_api_replay_tolerates_its_own_delta_only() {
        use crate::checkpoint::{PartitionId, SourceCheckpointDelta};

        let storage = Arc::new(RamStorage::default());
        let mut metastore = FileBackedMetastore::try_new(storage, None).await.unwrap();
        metastore.set_distributed(true);
        metastore.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });
        let index_id = "test-shard-api-delta-replay";
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let index_uid = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
            .await
            .unwrap()
            .index_uid()
            .clone();
        let source_id = SourceId::from(INGEST_V2_SOURCE_ID);
        metastore
            .add_source(AddSourceRequest {
                index_uid: Some(index_uid.clone()),
                source_config_json: serde_json::to_string(&SourceConfig::ingest_v2()).unwrap(),
            })
            .await
            .unwrap();
        let shard_id = "01J0TOKEN";
        metastore
            .open_shards(OpenShardsRequest {
                subrequests: vec![OpenShardSubrequest {
                    index_uid: Some(index_uid.clone()),
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from(shard_id)),
                    ingester_id: "test-ingester".to_string(),
                    publish_token: Some("test-publish-token".to_string()),
                    ..Default::default()
                }],
            })
            .await
            .unwrap();
        let split_metadata = SplitMetadata::for_test(SplitId::from("split-0"));
        metastore
            .stage_splits(
                StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                    .unwrap(),
            )
            .await
            .unwrap();
        let delta = IndexCheckpointDelta {
            source_id: source_id.clone(),
            source_delta: SourceCheckpointDelta::from_partition_delta(
                PartitionId::from(shard_id),
                Position::Beginning,
                Position::offset(1u64),
            )
            .unwrap(),
        };
        let delta_json = serde_json::to_string(&delta).unwrap();
        let request = |is_replay: bool, token: &str| PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_metadata.split_id.to_string()],
            index_checkpoint_delta_json_opt: Some(delta_json.clone()),
            publish_token_opt: Some(token.to_string()),
            is_replay,
            ..Default::default()
        };

        // The first attempt commits; its response is lost, and the caller replays it.
        metastore
            .publish_splits(request(false, "test-publish-token"))
            .await
            .unwrap();
        metastore
            .publish_splits(request(true, "test-publish-token"))
            .await
            .expect("a replay of this writer's own publish has to finish");

        // Another writer (a different token) whose delta ends on the same position: the token is
        // what says the delta is not its own, so it keeps getting refused even as a "replay".
        let error = metastore
            .publish_splits(request(true, "another-publish-token"))
            .await
            .expect_err("another writer's delta has to be refused");
        assert!(
            matches!(error, MetastoreError::InvalidPublishToken { .. }),
            "the refusal has to name the publish token: {error:?}"
        );
    }

    /// A shard can change hands: `acquire_shards` replaces its publish token, so a token match
    /// only says which writer holds the shard now, not whose delta moved the checkpoint. A writer
    /// that took the shard over therefore cannot replay an overlapping delta into a success — the
    /// proof is the same on this path as on the classic one: the splits of the request are
    /// published already.
    #[tokio::test]
    async fn test_a_shard_takeover_does_not_tolerate_an_overlapping_delta() {
        use crate::checkpoint::{PartitionId, SourceCheckpointDelta};

        let storage = Arc::new(RamStorage::default());
        let mut metastore = FileBackedMetastore::try_new(storage, None).await.unwrap();
        metastore.set_distributed(true);
        metastore.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 60,
            num_stripes: 2,
        });
        let index_id = "test-shard-takeover-delta";
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let index_uid = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
            .await
            .unwrap()
            .index_uid()
            .clone();
        let source_id = SourceId::from(INGEST_V2_SOURCE_ID);
        metastore
            .add_source(AddSourceRequest {
                index_uid: Some(index_uid.clone()),
                source_config_json: serde_json::to_string(&SourceConfig::ingest_v2()).unwrap(),
            })
            .await
            .unwrap();
        let shard_id = ShardId::from("01J0TAKEOVER");
        metastore
            .open_shards(OpenShardsRequest {
                subrequests: vec![OpenShardSubrequest {
                    index_uid: Some(index_uid.clone()),
                    source_id: source_id.clone(),
                    shard_id: Some(shard_id.clone()),
                    ingester_id: "test-ingester".to_string(),
                    publish_token: Some("aaa-token".to_string()),
                    ..Default::default()
                }],
            })
            .await
            .unwrap();
        let split_a = SplitMetadata::for_test(SplitId::from("split-a"));
        let split_b = SplitMetadata::for_test(SplitId::from("split-b"));
        for split in [&split_a, &split_b] {
            metastore
                .stage_splits(
                    StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split).unwrap(),
                )
                .await
                .unwrap();
        }
        let delta = |from_offset: u64| IndexCheckpointDelta {
            source_id: source_id.clone(),
            source_delta: SourceCheckpointDelta::from_partition_delta(
                PartitionId::from(shard_id.as_str()),
                Position::offset(from_offset),
                Position::offset(9u64),
            )
            .unwrap(),
        };

        // Writer 1 publishes up to position 9.
        metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: vec![split_a.split_id.to_string()],
                index_checkpoint_delta_json_opt: Some(
                    serde_json::to_string(&IndexCheckpointDelta {
                        source_id: source_id.clone(),
                        source_delta: SourceCheckpointDelta::from_partition_delta(
                            PartitionId::from(shard_id.as_str()),
                            Position::Beginning,
                            Position::offset(9u64),
                        )
                        .unwrap(),
                    })
                    .unwrap(),
                ),
                publish_token_opt: Some("aaa-token".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        // The shard changes hands, and the new holder replays an overlapping delta.
        let acquired = metastore
            .acquire_shards(AcquireShardsRequest {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_ids: vec![shard_id.clone()],
                publish_token: "zzz-token".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(acquired.acquired_shards.len(), 1);

        let error = metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: vec![split_b.split_id.to_string()],
                index_checkpoint_delta_json_opt: Some(serde_json::to_string(&delta(4)).unwrap()),
                publish_token_opt: Some("zzz-token".to_string()),
                is_replay: true,
                ..Default::default()
            })
            .await
            .expect_err("a takeover does not make the previous writer's delta its own");
        assert!(
            !error.to_string().is_empty(),
            "the refusal has to say what happened: {error:?}"
        );

        let splits = list_splits_of(&metastore, &index_uid).await;
        let published: Vec<String> = splits
            .into_iter()
            .filter(|split| split.split_state == SplitState::Published)
            .map(|split| split.split_id().to_string())
            .collect();
        assert_eq!(published, vec!["split-a".to_string()]);
    }

    /// An empty set of staged splits is no proof that the publish is the replay's own, so it must
    /// not open the delta tolerance on its own.
    #[tokio::test]
    async fn test_an_empty_staged_set_is_not_a_proof_of_its_own_publish() {
        let index_id = "test-empty-staged-no-proof";
        let (metastore, _index_uid, request) = staged_publish_fixture(
            index_id,
            IndexLayout::ManifestSegments {
                bucket_secs: 3_600,
                num_stripes: 4,
            },
        )
        .await;
        metastore.publish_splits(request(false)).await.unwrap();

        let mut empty = request(true);
        empty.staged_split_ids.clear();
        let error = metastore
            .publish_splits(empty)
            .await
            .expect_err("an empty staged set has to keep the delta strict");
        assert!(
            error.to_string().contains("checkpoint delta"),
            "the refusal has to name the checkpoint delta: {error}"
        );
    }

    /// The tolerance is for the replay of a publish that is provably its own. A competing writer
    /// whose delta lands on the same position carries a different delta, and must keep getting the
    /// precondition failure: its split is not published, so answering it with a success would index
    /// the same documents twice.
    #[tokio::test]
    async fn test_a_competing_writers_overlapping_delta_is_still_refused() {
        use quickwit_config::{SourceConfig, SourceParams};

        use crate::checkpoint::{PartitionId, SourceCheckpointDelta};

        let index_id = "test-competing-writer-delta";
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let source_id = "test-source";
        let storage = Arc::new(RamStorage::default());
        let mut metastore = FileBackedMetastore::try_new(storage, None).await.unwrap();
        metastore.set_distributed(true);
        metastore.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 3_600,
            num_stripes: 4,
        });
        let split_a = SplitMetadata::for_test(SplitId::from("split-a"));
        let split_b = SplitMetadata::for_test(SplitId::from("split-b"));
        let index_uid = metastore
            .create_index(
                CreateIndexRequest::try_from_index_and_source_configs(
                    &index_config,
                    &[SourceConfig::for_test(source_id, SourceParams::void())],
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .index_uid()
            .clone();
        for split in [&split_a, &split_b] {
            metastore
                .stage_splits(
                    StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split).unwrap(),
                )
                .await
                .unwrap();
        }

        // Writer A publishes (Beginning .. 9] with its own split.
        let delta_a = IndexCheckpointDelta::for_test(source_id, 0..10);
        metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: vec![split_a.split_id.to_string()],
                index_checkpoint_delta_json_opt: Some(serde_json::to_string(&delta_a).unwrap()),
                ..Default::default()
            })
            .await
            .unwrap();

        // Writer B's delta ends on the same position but starts later: the checkpoint is already
        // there, and yet the publish is not B's — its split is still staged.
        let delta_b = IndexCheckpointDelta {
            source_id: source_id.to_string(),
            source_delta: SourceCheckpointDelta::from_partition_delta(
                PartitionId::from(""),
                Position::offset(5u64),
                Position::offset(9u64),
            )
            .unwrap(),
        };
        let error = metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: vec![split_b.split_id.to_string()],
                index_checkpoint_delta_json_opt: Some(serde_json::to_string(&delta_b).unwrap()),
                is_replay: true,
                ..Default::default()
            })
            .await
            .expect_err("a competing writer's delta has to be refused, replay or not");
        assert!(
            matches!(error, MetastoreError::FailedPrecondition { .. }),
            "the refusal has to be a precondition failure: {error:?}"
        );

        let splits = list_splits_of(&metastore, &index_uid).await;
        let published: Vec<String> = splits
            .into_iter()
            .filter(|split| split.split_state == SplitState::Published)
            .map(|split| split.split_id().to_string())
            .collect();
        assert_eq!(
            published,
            vec!["split-a".to_string()],
            "only the writer that owns the delta publishes"
        );
    }

    fn index_id_suffix(layout: &IndexLayout) -> &'static str {
        match layout {
            IndexLayout::ManifestSegments { .. } => "manifest",
            _ => "sharded",
        }
    }

    /// One index, one staged split, and a publish request that can be sent as a first attempt or as
    /// a replay (the same request, the way the pipeline retries it).
    async fn staged_publish_fixture(
        index_id: &str,
        layout: IndexLayout,
    ) -> (
        FileBackedMetastore,
        IndexUid,
        impl Fn(bool) -> PublishSplitsRequest + use<>,
    ) {
        use quickwit_config::{SourceConfig, SourceParams};

        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let source_id = "test-source";
        let storage = Arc::new(RamStorage::default());
        let mut metastore = FileBackedMetastore::try_new(storage, None).await.unwrap();
        metastore.set_distributed(true);
        metastore.set_index_layout(layout);
        let split_metadata = SplitMetadata::for_test(SplitId::from("split-0"));
        let index_uid = metastore
            .create_index(
                CreateIndexRequest::try_from_index_and_source_configs(
                    &index_config,
                    &[SourceConfig::for_test(source_id, SourceParams::void())],
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .index_uid()
            .clone();
        metastore
            .stage_splits(
                StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                    .unwrap(),
            )
            .await
            .unwrap();
        let checkpoint_delta = IndexCheckpointDelta::for_test(source_id, 0..10);
        let request_uid = index_uid.clone();
        let request = move |is_replay: bool| PublishSplitsRequest {
            index_uid: Some(request_uid.clone()),
            staged_split_ids: vec![split_metadata.split_id.to_string()],
            index_checkpoint_delta_json_opt: Some(
                serde_json::to_string(&checkpoint_delta).unwrap(),
            ),
            is_replay,
            ..Default::default()
        };
        (metastore, index_uid, request)
    }

    async fn list_splits_of(metastore: &FileBackedMetastore, index_uid: &IndexUid) -> Vec<Split> {
        metastore
            .list_splits(
                ListSplitsRequest::try_from_list_splits_query(&ListSplitsQuery::for_index(
                    index_uid.clone(),
                ))
                .unwrap(),
            )
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap()
    }

    /// The counterpart of the test above: a caller that does replay finishes the mutation its
    /// root commit gave up on.
    ///
    /// The splits are committed before the index metadata, so a mutation that exhausts the budget
    /// on the metadata has published its splits already. The pipeline's second attempt carries
    /// `is_replay`, and that is what lets it finish: the splits it is about to publish are already
    /// published, which a fresh request would be refused for but a replay tolerates.
    #[tokio::test]
    async fn test_a_replay_finishes_a_mutation_whose_root_commit_exhausted_the_budget() {
        use quickwit_config::{SourceConfig, SourceParams};

        use crate::metastore::file_backed::manifest_layout::test_hooks;

        let index_id = "test-root-exhaustion-replay";
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let source_id = "test-source";
        let storage = Arc::new(RamStorage::default());
        let mut metastore = FileBackedMetastore::try_new(storage, None).await.unwrap();
        metastore.set_distributed(true);
        metastore.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 3_600,
            num_stripes: 4,
        });
        let split_metadata = SplitMetadata::for_test(SplitId::from("split-0"));
        let index_uid = metastore
            .create_index(
                CreateIndexRequest::try_from_index_and_source_configs(
                    &index_config,
                    &[SourceConfig::for_test(source_id, SourceParams::void())],
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .index_uid()
            .clone();
        metastore
            .stage_splits(
                StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                    .unwrap(),
            )
            .await
            .unwrap();
        let checkpoint_delta = IndexCheckpointDelta::for_test(source_id, 0..10);
        let publish_request = |is_replay: bool| PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_metadata.split_id.to_string()],
            index_checkpoint_delta_json_opt: Some(
                serde_json::to_string(&checkpoint_delta).unwrap(),
            ),
            is_replay,
            ..Default::default()
        };

        // Every commit of the index metadata fails: the splits commit, the metadata does not, and
        // the first attempt gives up after its budget.
        test_hooks::fail_next_root_commits(index_id, DISTRIBUTED_MAX_ATTEMPTS as u32);
        let first_attempt = metastore.publish_splits(publish_request(false)).await;
        assert!(
            matches!(
                first_attempt,
                Err(MetastoreError::FailedPrecondition { .. })
            ),
            "the first attempt has to report the metadata conflict: {first_attempt:?}"
        );

        // The pipeline's second attempt replays the same request, and it has to finish the
        // mutation rather than fail on the split it published before.
        metastore
            .publish_splits(publish_request(true))
            .await
            .expect("a replay has to finish a mutation whose splits are already published");

        let listed = metastore
            .list_splits(
                ListSplitsRequest::try_from_list_splits_query(&ListSplitsQuery::for_index(
                    index_uid,
                ))
                .unwrap(),
            )
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].split_state, SplitState::Published);
    }

    /// A merge marks the splits it replaced and publishes the split that replaces them. When the
    /// first attempt commits the marking and loses the publication, the replay has to be able to
    /// finish: the replaced split is already marked, which is exactly the state the replay asks
    /// for.
    ///
    /// The real five-node run met this as a `MergePublisher` fault,
    /// `precondition failed for splits <id>: splits are not deletable`: the marking step refused an
    /// already marked split *before* the branch that skips one, so the replay could never finish
    /// and the pipeline restarted instead.
    #[tokio::test]
    async fn test_a_publish_that_marked_a_replaced_split_and_lost_the_other_stripe_replays() {
        use crate::metastore::file_backed::manifest_layout::{ManifestLayout, test_hooks};

        let index_id = "test-replaced-split-replay";
        let num_stripes = 8;
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let storage = Arc::new(RamStorage::default());
        let mut metastore = FileBackedMetastore::try_new(storage, None).await.unwrap();
        metastore.set_distributed(true);
        metastore.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 3_600,
            num_stripes,
        });
        let layout = ManifestLayout::new(index_id, 3_600, num_stripes);
        let index_uid = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
            .await
            .unwrap()
            .index_uid()
            .clone();

        // One split per stripe, in ascending stripe order: the first attempt commits the marking of
        // the replaced split before it reaches the stripe of the split that replaces it.
        let split_for_stripe = |prefix: &str, stripe: usize| {
            (0..)
                .map(|candidate| {
                    SplitMetadata::for_test(SplitId::from(format!("{prefix}-{candidate}")))
                })
                .find(|split_metadata| layout.stripe_of(split_metadata.split_id.as_str()) == stripe)
                .unwrap()
        };
        let replaced_split = split_for_stripe("replaced", 0);
        let new_split = split_for_stripe("new", 1);
        for split_metadata in [&replaced_split, &new_split] {
            metastore
                .stage_splits(
                    StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split_metadata)
                        .unwrap(),
                )
                .await
                .unwrap();
        }
        // The split a merge replaces has to be published first.
        metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: vec![replaced_split.split_id.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        // Lose the publication of the new split: the marking of the replaced one is already
        // committed when the replay runs.
        test_hooks::fail_next_commit_for_stripe(layout.stripe_of(new_split.split_id.as_str()));
        metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: vec![new_split.split_id.to_string()],
                replaced_split_ids: vec![replaced_split.split_id.to_string()],
                ..Default::default()
            })
            .await
            .expect("the replay has to finish the publication the first attempt started");

        assert_eq!(
            list_published_split_ids_with(&metastore, &index_uid)
                .await
                .unwrap(),
            vec![new_split.split_id.to_string()],
            "the replacing split is the published one"
        );
        use crate::ListSplitsQuery;
        let marked = metastore
            .list_splits(
                ListSplitsRequest::try_from_list_splits_query(
                    &ListSplitsQuery::for_index(index_uid.clone())
                        .with_split_states([SplitState::MarkedForDeletion]),
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .collect_split_ids()
            .await
            .unwrap();
        assert_eq!(
            marked,
            vec![replaced_split.split_id.to_string()],
            "the replaced split stays marked for deletion"
        );

        // The same request, sent again by the caller rather than replayed inside one request: the
        // publisher does that when an attempt's response was lost. Without the flag the metastore
        // refuses it (the split it publishes is already published); with it the replayed request is
        // what finishes the mutation.
        let replayed_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![new_split.split_id.to_string()],
            replaced_split_ids: vec![replaced_split.split_id.to_string()],
            ..Default::default()
        };
        let error = metastore
            .publish_splits(replayed_request.clone())
            .await
            .unwrap_err();
        assert!(
            matches!(error, MetastoreError::FailedPrecondition { .. }),
            "a fresh publish of a published split must still fail: {error:?}"
        );
        metastore
            .publish_splits(PublishSplitsRequest {
                is_replay: true,
                ..replayed_request
            })
            .await
            .expect("a replayed publish must finish the mutation it replays");
    }

    /// The tolerance a caller buys with `is_replay` has to be visible, and it has to stay bounded
    /// to what the flag says: the counter moves when a replay stops at a step its earlier
    /// attempt applied, and the same publish without the flag is still refused.
    #[tokio::test]
    async fn test_a_replay_that_stops_on_applied_steps_is_counted() {
        use crate::metastore::file_backed::metrics::REPLAY_TOLERATED_SPLITS_TOTAL;

        // Process-global counter and parallel tests: the assertion is a lower bound.
        let tolerated_before = REPLAY_TOLERATED_SPLITS_TOTAL.get();

        let index_id = "test-tolerated-replay-counted";
        let index_config = IndexConfig::for_test(index_id, &format!("ram:///indexes/{index_id}"));
        let storage = Arc::new(RamStorage::default());
        let mut metastore = FileBackedMetastore::try_new(storage, None).await.unwrap();
        metastore.set_distributed(true);
        let index_uid = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
            .await
            .unwrap()
            .index_uid()
            .clone();
        let split_metadata = SplitMetadata::for_test(SplitId::from("split-0"));
        metastore
            .stage_splits(
                StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                    .unwrap(),
            )
            .await
            .unwrap();
        let publish_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_metadata.split_id.to_string()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_request.clone())
            .await
            .unwrap();

        let error = metastore
            .publish_splits(publish_request.clone())
            .await
            .unwrap_err();
        assert!(
            matches!(error, MetastoreError::FailedPrecondition { .. }),
            "a publish of a published split without the flag must still fail: {error:?}"
        );
        // The counter is process-global and other tests publish with the tolerance, so this test
        // cannot assert that the refused publish left it untouched: under `cargo test` the two
        // happen in the same process. The refusal is checked above, and that a refused publish
        // cannot be counted is structural -- the increment sits inside the branch that only the
        // tolerance opens.
        metastore
            .publish_splits(PublishSplitsRequest {
                is_replay: true,
                ..publish_request
            })
            .await
            .expect("a replayed publish must finish the mutation it replays");
        assert!(
            REPLAY_TOLERATED_SPLITS_TOTAL.get() > tolerated_before,
            "the split a replay found already published must be counted"
        );
    }

    #[tokio::test]
    async fn test_file_backed_metastore_connectivity_fails_if_states_file_does_not_exist() {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        // `try_new` asks the storage for its URI to decide whether several nodes may share it.
        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .times(3)
            .returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(1)
            .returning(move |path, put_payload| {
                assert!(path == Path::new("manifest.json"));
                block_on(ram_storage_clone.put(path, put_payload))
            });
        let metastore = FileBackedMetastore::try_new(Arc::new(mock_storage), None)
            .await
            .unwrap();

        metastore.check_connectivity().await.unwrap();
    }

    #[tokio::test]
    async fn test_file_backed_metastore_index_exists() {
        let index_id = "test-index";
        let mut metastore = FileBackedMetastore::default_for_test().await;
        assert!(!metastore.index_exists(index_id).await.unwrap());

        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        metastore.create_index(create_index_request).await.unwrap();

        assert!(metastore.index_exists(index_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_index() {
        let metastore = FileBackedMetastore::default_for_test().await;

        // Create index
        let index_id = "test-index";
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        // Open index and check its metadata
        let created_index = metastore.get_index(&index_uid).await.unwrap();
        assert_eq!(created_index.index_id(), index_config.index_id);
        assert_eq!(
            created_index.metadata().index_uri(),
            &index_config.index_uri
        );

        // Check index is returned by list indexes.
        let indexes_metadata = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap()
            .deserialize_indexes_metadata()
            .await
            .unwrap();
        assert_eq!(indexes_metadata.len(), 1);

        // Open a non-existent index.
        let metastore_error = metastore
            .get_index(&IndexUid::new_with_random_ulid("index-does-not-exist"))
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::NotFound { .. }));

        // Open a index with a different incarnation_id.
        let metastore_error = metastore
            .get_index(&IndexUid::new_with_random_ulid(index_id))
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::NotFound { .. }));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_storage_failing() {
        // The file-backed metastore should not update its internal state if the storage fails.
        let mut mock_storage = MockStorage::default();

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();

        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(4)
            .returning(move |path, put_payload| {
                assert!(
                    path == Path::new("manifest.json") || path == metastore_filepath("test-index")
                );
                block_on(ram_storage_clone.put(path, put_payload))
            });
        mock_storage
            .expect_get_all()
            .times(1)
            .returning(move |path| block_on(ram_storage.get_all(path)));
        mock_storage.expect_put().times(1).returning(|_uri, _| {
            Err(StorageErrorKind::Io
                .with_error(anyhow::anyhow!("Oops. Some network problem maybe?")))
        });
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        let index_config = IndexConfig::for_test("test-index", "ram:///indexes/test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        let split_id = "split-one";
        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id.into(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            create_timestamp: current_timestamp,
            ..Default::default()
        };
        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        // publish split fails
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id.to_string()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();

        let list_splits_query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Published);
        let list_splits_request =
            ListSplitsRequest::try_from_list_splits_query(&list_splits_query).unwrap();
        let splits = metastore
            .list_splits(list_splits_request)
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert!(splits.is_empty());

        let list_splits_query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
        let list_splits_request =
            ListSplitsRequest::try_from_list_splits_query(&list_splits_query).unwrap();
        let splits = metastore
            .list_splits(list_splits_request)
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert!(!splits.is_empty());
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_index_checks_for_inconsistent_index_id()
    -> MetastoreResult<()> {
        let storage = Arc::new(RamStorage::default());
        let index_id = "test-index";
        let index_metadata =
            IndexMetadata::for_test("my-inconsistent-index", "ram:///indexes/test-index");

        // Put inconsistent index and manifest into storage.
        let index = FileBackedIndex::from(index_metadata);
        put_index_given_index_id(&*storage, &index, index_id).await?;
        let mut manifest = Manifest::default();
        manifest
            .indexes
            .insert(index_id.to_string(), IndexStatus::Active);
        save_manifest(&*storage, &manifest).await.unwrap();

        let metastore = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();

        // Getting index with inconsistent index ID should raise an error.
        let metastore_error = metastore
            .get_index(&IndexUid::new_with_random_ulid(index_id))
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn test_file_backed_metastore_write_directly_visible() -> MetastoreResult<()> {
        let metastore = FileBackedMetastore::default_for_test().await;

        let index_config = IndexConfig::for_test("test-index", "ram:///indexes/test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let create_index_response = metastore.create_index(create_index_request).await.unwrap();
        let index_uid: IndexUid = create_index_response.index_uid().clone();

        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert!(splits.is_empty());

        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "split1".into(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            ..Default::default()
        };
        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await?;

        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert_eq!(splits.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_file_backed_metastore_polling() -> MetastoreResult<()> {
        let storage = Arc::new(RamStorage::default());

        let metastore_write = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        let polling_interval = Duration::from_millis(20);
        let metastore_read = FileBackedMetastore::try_new(storage, Some(polling_interval))
            .await
            .unwrap();

        let index_config = IndexConfig::for_test("test-index", "ram:///indexes/test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let create_index_response = metastore_write
            .create_index(create_index_request)
            .await
            .unwrap();
        let index_uid: IndexUid = create_index_response.index_uid().clone();

        let splits = metastore_write
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert!(splits.is_empty());

        let splits = metastore_read
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert!(splits.is_empty());

        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "split1".into(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            ..Default::default()
        };
        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                .unwrap();
        metastore_write.stage_splits(stage_splits_request).await?;

        let splits = metastore_read
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert!(splits.is_empty());

        for _ in 0..10 {
            tokio::time::sleep(polling_interval).await;

            let splits = metastore_read
                .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
                .await
                .unwrap()
                .collect_splits()
                .await
                .unwrap();
            if !splits.is_empty() {
                return Ok(());
            }
        }
        panic!("The metastore should have been updated.");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_file_backed_metastore_race_condition() {
        let metastore = FileBackedMetastore::default_for_test().await;

        let index_config = IndexConfig::for_test("test-index", "ram:///indexes/test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let create_index_response = metastore.create_index(create_index_request).await.unwrap();
        let index_uid: IndexUid = create_index_response.index_uid().clone();

        // Stage splits in multiple threads
        let mut handles = Vec::new();
        let mut random_generator = rand::rng();
        for i in 1..=20 {
            let sleep_duration = Duration::from_millis(random_generator.random_range(0..=200));
            let metastore = metastore.clone();
            let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
            let handle = tokio::spawn({
                let index_uid = index_uid.clone();
                async move {
                    let split_metadata = SplitMetadata {
                        footer_offsets: 1000..2000,
                        split_id: format!("split-{i}").into(),
                        num_docs: 1,
                        uncompressed_docs_size_in_bytes: 2,
                        time_range: Some(RangeInclusive::new(0, 99)),
                        create_timestamp: current_timestamp,
                        ..Default::default()
                    };
                    // stage split
                    let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
                        index_uid.clone(),
                        &split_metadata,
                    )
                    .unwrap();
                    metastore.stage_splits(stage_splits_request).await.unwrap();

                    tokio::time::sleep(sleep_duration).await;

                    // publish split
                    let split_id = format!("split-{i}");
                    let publish_splits_request = PublishSplitsRequest {
                        index_uid: Some(index_uid.clone()),
                        staged_split_ids: vec![split_id.to_string()],
                        ..Default::default()
                    };
                    metastore
                        .publish_splits(publish_splits_request)
                        .await
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        futures::future::try_join_all(handles).await.unwrap();

        let list_splits_query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Published);
        let list_splits_request =
            ListSplitsRequest::try_from_list_splits_query(&list_splits_query).unwrap();
        let splits = metastore
            .list_splits(list_splits_request)
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();

        // Make sure that all 20 splits are in `Published` state.
        assert_eq!(splits.len(), 20);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_file_backed_metastore_list_indexes_race_condition() {
        let metastore = FileBackedMetastore::default_for_test().await;
        let mut index_uids = Vec::new();
        for idx in 0..10 {
            let index_uid = IndexUid::new_with_random_ulid(&format!("test-index-{idx}"));
            let index_config =
                IndexConfig::for_test(&index_uid.index_id, "ram:///indexes/test-index");
            let create_index_request =
                CreateIndexRequest::try_from_index_config(&index_config).unwrap();
            let index_uid: IndexUid = metastore
                .create_index(create_index_request)
                .await
                .unwrap()
                .index_uid()
                .clone();
            index_uids.push(index_uid);
        }
        // Delete indexes + call to list_indexes_metadata.
        let mut handles = Vec::new();
        for index_uid in index_uids {
            let delete_request = DeleteIndexRequest {
                index_uid: Some(index_uid.clone()),
            };
            {
                let metastore = metastore.clone();
                let handle = tokio::spawn(async move {
                    metastore
                        .list_indexes_metadata(ListIndexesMetadataRequest::all())
                        .await
                        .unwrap();
                });
                handles.push(handle);
            }
            {
                let metastore = metastore.clone();
                let handle = tokio::spawn(async move {
                    metastore.delete_index(delete_request).await.unwrap();
                });
                handles.push(handle);
            }
        }
        tokio::time::timeout(
            Duration::from_secs(2),
            futures::future::try_join_all(handles),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn test_file_backed_metastore_create_index_when_storage_failing_on_indexes_states_put() {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let index_id = "test-index";

        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));
        mock_storage.expect_exists().returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(1)
            .returning(move |path, _| {
                assert!(path == Path::new("manifest.json"));
                Err(StorageErrorKind::Io
                    .with_error(anyhow::anyhow!("Oops. Some network problem maybe?")))
            });
        mock_storage
            .expect_get_all()
            .times(1)
            .returning(move |path| block_on(ram_storage.get_all(path)));

        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");

        // Create index.
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let metastore_error = metastore
            .create_index(create_index_request)
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Try fetch the not created index.
        let created_index_error = metastore
            .get_index(&IndexUid::new_with_random_ulid(index_id))
            .await
            .unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::NotFound { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_create_index_when_storage_failing_before_metadata_put() {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        let ram_storage_clone_2 = ram_storage.clone();
        let index_id = "test-index";
        let index_uid = IndexUid::new_with_random_ulid(index_id);

        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(4)
            .returning(move |path, put_payload| {
                assert!(
                    path == Path::new("manifest.json") || path == metastore_filepath("test-index")
                );
                if path == Path::new("manifest.json") {
                    return block_on(ram_storage_clone.put(path, put_payload));
                }
                Err(StorageErrorKind::Io
                    .with_error(anyhow::anyhow!("Oops. Some network problem maybe?")))
            });
        mock_storage
            .expect_get_all()
            .times(1)
            .returning(move |path| block_on(ram_storage.get_all(path)));
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");

        // Create index
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let metastore_error = metastore
            .create_index(create_index_request)
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Let's fetch the index, we expect an internal error as the index state is in `Creating`
        // state.
        let created_index_error = metastore.get_index(&index_uid.clone()).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::Internal { .. }
        ));
        // Check index state is in `Creating` in the manifest file.
        let storage = Arc::new(ram_storage_clone_2.clone());
        let manifest = load_or_create_manifest(&*storage).await.unwrap();
        assert!(matches!(
            *manifest.indexes.get(index_id).unwrap(),
            IndexStatus::Creating
        ));
        // Let's delete the index to clean states.
        let delete_request = DeleteIndexRequest {
            index_uid: Some(index_uid.clone()),
        };
        let deleted_index_error = metastore.delete_index(delete_request).await.unwrap_err();
        assert!(matches!(
            deleted_index_error,
            MetastoreError::NotFound { .. }
        ));
        let manifest = load_or_create_manifest(&*storage).await.unwrap();
        assert!(!manifest.indexes.contains_key(index_id));
        // Now we can expect an `IndexDoesNotExist` error.
        let created_index_error = metastore.get_index(&index_uid).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::NotFound { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_create_index_when_storage_failing_before_last_indexes_states_put()
     {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        let index_id = "test-index";
        let mut indexes_json_valid_put = 1;

        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(3)
            .returning(move |path, put_payload| {
                assert!(
                    path == Path::new("manifest.json") || path == metastore_filepath("test-index")
                );
                if path == Path::new("manifest.json") {
                    if indexes_json_valid_put == 0 {
                        return Err(StorageErrorKind::Io.with_error(anyhow::anyhow!(
                            "oops. perhaps there are some network problems"
                        )));
                    }
                    indexes_json_valid_put -= 1;
                }
                block_on(ram_storage_clone.put(path, put_payload))
            });
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");

        // Create index
        let metastore_error = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Let's fetch the index, we expect an internal error as the index state is in `Creating`
        // state.
        let created_index_error = metastore
            .get_index(&IndexUid::new_with_random_ulid(index_id))
            .await
            .unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::Internal { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_delete_index_when_storage_failing_before_metadata_delete() {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        let index_id = "test-index";
        let index_uid = IndexUid::new_with_random_ulid(index_id);
        let index_metadata =
            IndexMetadata::for_test(&index_uid.index_id, "ram:///indexes/test-index");
        let index = FileBackedIndex::from(index_metadata);
        put_index_given_index_id(&ram_storage, &index, &index_uid.index_id)
            .await
            .unwrap();

        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(true));
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_delete()
            .returning(|_| {
                Err(StorageErrorKind::Io
                    .with_error(anyhow::anyhow!("Oops. Some network problem maybe?")))
            });
        mock_storage
            .expect_put()
            .times(1)
            .returning(move |path, put_payload| block_on(ram_storage_clone.put(path, put_payload)));
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        // Delete index
        let delete_request = DeleteIndexRequest {
            index_uid: Some(index_uid.clone()),
        };
        let metastore_error = metastore.delete_index(delete_request).await.unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Let's fetch the index, we expect an internal error as the index state is in `Deleting`
        // state.
        let created_index_error = metastore.get_index(&index_uid).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::Internal { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_delete_index_storage_failing_before_last_indexes_states_put()
     {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        let index_id = "test-index";
        let index_uid = IndexUid::new_with_random_ulid(index_id);
        let index_metadata =
            IndexMetadata::for_test(&index_uid.index_id, "ram:///indexes/test-index");
        let index = FileBackedIndex::from(index_metadata);
        put_index_given_index_id(&ram_storage, &index, &index_uid.index_id)
            .await
            .unwrap();
        let mut indexes_json_valid_put = 1;
        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(true));
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_delete()
            .returning(|_| Ok(()));
        mock_storage
            .expect_put()
            .times(2)
            .returning(move |path, put_payload| {
                assert!(path == Path::new("manifest.json"));
                if path == Path::new("manifest.json") {
                    if indexes_json_valid_put == 0 {
                        return Err(StorageErrorKind::Io.with_error(anyhow::anyhow!(
                            "oops. perhaps there are some network problems"
                        )));
                    }
                    indexes_json_valid_put -= 1;
                }
                block_on(ram_storage_clone.put(path, put_payload))
            });
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        // Delete index
        let delete_request = DeleteIndexRequest {
            index_uid: Some(index_uid.clone()),
        };
        let metastore_error = metastore.delete_index(delete_request).await.unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Let's fetch the index, we expect an internal error as the index state is in `Deleting`
        // state.
        let created_index_error = metastore.get_index(&index_uid).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::Internal { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_list_indexes() -> MetastoreResult<()> {
        let index_id_creating = "test-index--creating";
        let index_id_alive = "testing-index--alive";
        let index_id_unregistered = "test-index--unregistered";
        let index_id_deleting = "test-index--deleting";

        let index_metadata_alive =
            IndexMetadata::for_test(index_id_alive, "ram:///indexes/test-index--alive");
        let index_metadata_unregistered = IndexMetadata::for_test(
            index_id_unregistered,
            "ram:///indexes/test-index--unregistered",
        );

        // Put index states into storage.
        let ram_storage = Arc::new(RamStorage::default());
        let mut manifest = Manifest::default();
        manifest
            .indexes
            .insert(index_id_creating.to_string(), IndexStatus::Creating);
        manifest
            .indexes
            .insert(index_id_alive.to_string(), IndexStatus::Active);
        manifest
            .indexes
            .insert(index_id_deleting.to_string(), IndexStatus::Deleting);
        save_manifest(&*ram_storage, &manifest).await.unwrap();

        let index_alive = FileBackedIndex::from(index_metadata_alive);
        let index_alive_unregistered = FileBackedIndex::from(index_metadata_unregistered);
        let index_uid_alive = index_alive.index_uid();
        let index_uid_unregistered = index_alive_unregistered.index_uid();

        // Put indexes metadatas.
        put_index_given_index_id(&*ram_storage, &index_alive, index_id_alive).await?;
        put_index_given_index_id(
            &*ram_storage,
            &index_alive_unregistered,
            index_id_unregistered,
        )
        .await?;

        // Fetch alive indexes metadatas.
        let metastore = FileBackedMetastore::try_new(ram_storage.clone(), None)
            .await
            .unwrap();
        let indexes_metadata = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap()
            .deserialize_indexes_metadata()
            .await
            .unwrap();
        assert_eq!(indexes_metadata.len(), 1);

        // Fetch the index metadata not registered in index states json.
        metastore
            .get_index(&index_uid_unregistered.clone())
            .await
            .unwrap();

        // Now list indexes return 2 indexes metadatas as the metastore is now aware of
        // 2 alive indexes.
        let indexes_metadata = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap()
            .deserialize_indexes_metadata()
            .await
            .unwrap();
        assert_eq!(indexes_metadata.len(), 2);

        // Let's delete indexes.
        let delete_request = DeleteIndexRequest {
            index_uid: Some(index_uid_alive.clone()),
        };
        metastore.delete_index(delete_request).await.unwrap();

        let delete_request = DeleteIndexRequest {
            index_uid: Some(index_uid_unregistered.clone()),
        };
        metastore.delete_index(delete_request).await.unwrap();
        let indexes_metadata = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap()
            .deserialize_indexes_metadata()
            .await
            .unwrap();
        assert!(indexes_metadata.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_monotically_increasing_stamps_by_index() {
        let storage = RamStorage::default();
        let metastore = FileBackedMetastore::try_new(Arc::new(storage.clone()), None)
            .await
            .unwrap();
        let index_id = "test-index-increasing-stamps-by-index";
        let index_config = IndexConfig::for_test(
            index_id,
            "ram:///indexes/test-index-increasing-stamps-by-index",
        );
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let create_index_response = metastore.create_index(create_index_request).await.unwrap();
        let index_uid = create_index_response.index_uid;

        let delete_query = DeleteQuery {
            start_timestamp: None,
            end_timestamp: None,
            index_uid,
            query_ast: serde_json::to_string(&qast_helper("harry potter", &["body"])).unwrap(),
        };

        let delete_task_1 = metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();
        assert_eq!(delete_task_1.opstamp, 1);
        let delete_task_2 = metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();
        assert_eq!(delete_task_2.opstamp, 2);

        // Create metastore with data already in the storage.
        let new_metastore = FileBackedMetastore::try_new(Arc::new(storage), None)
            .await
            .unwrap();
        let delete_task_3 = new_metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();
        assert_eq!(delete_task_3.opstamp, 3);

        // Create delete tasks on new index.
        let index_id_2 = "test-index-increasing-stamps-by-index-2";
        let index_config = IndexConfig::for_test(
            index_id_2,
            "ram:///indexes/test-index-increasing-stamps-by-index-2",
        );
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let create_index_response = metastore.create_index(create_index_request).await.unwrap();
        let index_uid = create_index_response.index_uid;

        let delete_query = DeleteQuery {
            start_timestamp: None,
            end_timestamp: None,
            index_uid,
            query_ast: serde_json::to_string(&qast_helper("harry potter", &["body"])).unwrap(),
        };
        let delete_task_4 = metastore.create_delete_task(delete_query).await.unwrap();
        assert_eq!(delete_task_4.opstamp, 1);
    }

    #[tokio::test]
    async fn test_create_index_template_rollback() {
        let mut mock_storage = MockStorage::default();

        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));

        mock_storage
            .expect_put()
            .once()
            .returning(|path, _payload| {
                assert_eq!(path, Path::new(MANIFEST_FILE_NAME));
                Ok(())
            });

        mock_storage
            .expect_put()
            .once()
            .returning(|path, _payload| {
                assert_eq!(path, Path::new(MANIFEST_FILE_NAME));
                let io_error = StorageErrorKind::Io.with_error(anyhow::anyhow!("IO error"));
                Err(io_error)
            });

        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        let index_template = IndexTemplate::for_test("test-template", &["test-index-foo*"], 100);
        let index_template_json = serde_json::to_string(&index_template).unwrap();
        let create_index_template_request = CreateIndexTemplateRequest {
            index_template_json,
            overwrite: false,
        };
        metastore
            .create_index_template(create_index_template_request)
            .await
            .unwrap();
        {
            let state = metastore.state.read().await;
            assert_eq!(state.templates.len(), 1);
            state.template_matcher.find_match("test-index-foo").unwrap();
        }
        let index_template = IndexTemplate::for_test("test-template", &["test-index-bar*"], 100);
        let index_template_json = serde_json::to_string(&index_template).unwrap();
        let create_index_template_request = CreateIndexTemplateRequest {
            index_template_json,
            overwrite: true,
        };
        metastore
            .create_index_template(create_index_template_request)
            .await
            .unwrap_err();
        {
            let state = metastore.state.read().await;
            assert_eq!(state.templates.len(), 1);
            state.template_matcher.find_match("test-index-foo").unwrap();
        }
    }

    #[tokio::test]
    async fn test_delete_index_templates_rollback() {
        let mut mock_storage = MockStorage::default();

        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));

        mock_storage
            .expect_put()
            .once()
            .returning(|path, _payload| {
                assert_eq!(path, Path::new(MANIFEST_FILE_NAME));
                Ok(())
            });

        mock_storage
            .expect_put()
            .once()
            .returning(|path, _payload| {
                assert_eq!(path, Path::new(MANIFEST_FILE_NAME));
                let io_error = StorageErrorKind::Io.with_error(anyhow::anyhow!("IO error"));
                Err(io_error)
            });

        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        let index_template = IndexTemplate::for_test("test-template", &["test-index-foo*"], 100);
        let index_template_json = serde_json::to_string(&index_template).unwrap();
        let create_index_template_request = CreateIndexTemplateRequest {
            index_template_json,
            overwrite: false,
        };
        metastore
            .create_index_template(create_index_template_request)
            .await
            .unwrap();
        {
            let state = metastore.state.read().await;
            assert_eq!(state.templates.len(), 1);
            state.template_matcher.find_match("test-index-foo").unwrap();
        }
        let delete_index_templates_request = DeleteIndexTemplatesRequest {
            template_ids: vec![index_template.template_id],
        };
        metastore
            .delete_index_templates(delete_index_templates_request)
            .await
            .unwrap_err();
        {
            let state = metastore.state.read().await;
            assert_eq!(state.templates.len(), 1);
            state.template_matcher.find_match("test-index-foo").unwrap();

            assert!(
                state
                    .template_matcher
                    .find_match("test-index-bar")
                    .is_none()
            );
        }
    }

    /// A node that read the index before another node published must not overwrite that work.
    ///
    /// This is exactly the state a shared metastore gets into: node A and node B both look at the
    /// index, then A publishes, then B publishes from the view it loaded earlier. With the
    /// single-node path (read the cached index, then overwrite the file) B's write erases A's
    /// split; with the distributed path B's compare-and-swap fails, B reloads, and both splits
    /// survive.
    #[tokio::test]
    async fn test_distributed_metastore_does_not_lose_updates_from_a_stale_cache()
    -> anyhow::Result<()> {
        let storage = Arc::new(RamStorage::default());
        let mut metastore_a = FileBackedMetastore::try_new(storage.clone(), None).await?;
        metastore_a.set_distributed(true);
        let mut metastore_b = FileBackedMetastore::try_new(storage.clone(), None).await?;
        metastore_b.set_distributed(true);
        assert!(metastore_a.is_distributed() && metastore_b.is_distributed());

        let index_id = "test-distributed-stale-cache";
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(index_id, &index_uri);
        let create_index_request = CreateIndexRequest::try_from_index_config(&index_config)?;
        let index_uid: IndexUid = metastore_a
            .create_index(create_index_request)
            .await?
            .index_uid()
            .clone();

        async fn stage_and_publish_split(
            metastore: &FileBackedMetastore,
            index_uid: &IndexUid,
            split_id: &str,
        ) -> anyhow::Result<()> {
            let split_metadata = SplitMetadata {
                footer_offsets: 0..10,
                split_id: split_id.to_string().into(),
                num_docs: 1,
                time_range: Some(RangeInclusive::new(0, 99)),
                ..Default::default()
            };
            let stage_splits_request =
                StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)?;
            metastore.stage_splits(stage_splits_request).await?;
            let publish_splits_request = PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: vec![split_id.to_string()],
                ..Default::default()
            };
            metastore.publish_splits(publish_splits_request).await?;
            Ok(())
        }

        async fn list_published_split_ids(
            metastore: &FileBackedMetastore,
            index_uid: &IndexUid,
        ) -> anyhow::Result<Vec<String>> {
            let list_splits_query = ListSplitsQuery::for_index(index_uid.clone())
                .with_split_state(SplitState::Published);
            let list_splits_request =
                ListSplitsRequest::try_from_list_splits_query(&list_splits_query)?;
            let mut split_ids: Vec<String> = metastore
                .list_splits(list_splits_request)
                .await?
                .collect_splits()
                .await?
                .iter()
                .map(|split| split.split_id().to_string())
                .collect();
            split_ids.sort();
            Ok(split_ids)
        }

        // Both nodes look at the index before either of them writes.
        assert!(
            list_published_split_ids(&metastore_a, &index_uid)
                .await?
                .is_empty()
        );
        assert!(
            list_published_split_ids(&metastore_b, &index_uid)
                .await?
                .is_empty()
        );

        stage_and_publish_split(&metastore_a, &index_uid, "a-split-0").await?;
        // B still holds the view it loaded above, where A's split does not exist.
        stage_and_publish_split(&metastore_b, &index_uid, "b-split-0").await?;

        // A reader that never wrote must see both splits.
        let mut metastore_c = FileBackedMetastore::try_new(storage.clone(), None).await?;
        metastore_c.set_distributed(true);
        assert_eq!(
            list_published_split_ids(&metastore_c, &index_uid).await?,
            vec!["a-split-0".to_string(), "b-split-0".to_string()],
            "the second writer must not erase the first writer's split"
        );
        Ok(())
    }

    /// A storage that accepts `If-None-Match: *` unconditionally is what localstack 3.5.0 does, and
    /// running in shared mode against it loses updates silently. The startup probe must catch it:
    /// refuse by default, and fall back to single-writer mode when explicitly allowed.
    #[tokio::test]
    async fn test_distributed_metastore_refuses_storage_that_ignores_preconditions()
    -> anyhow::Result<()> {
        fn mock_storage_ignoring_preconditions() -> MockStorage {
            let ram_storage = Arc::new(RamStorage::default());
            let mut mock_storage = MockStorage::default();
            mock_storage
                .expect_uri()
                .return_const(Uri::for_test("s3://test-bucket/indexes"));
            let ram_storage_clone = ram_storage.clone();
            mock_storage
                .expect_put()
                .returning(move |path, payload| block_on(ram_storage_clone.put(path, payload)));
            let ram_storage_clone = ram_storage.clone();
            mock_storage
                .expect_exists()
                .returning(move |path| block_on(ram_storage_clone.exists(path)));
            let ram_storage_clone = ram_storage.clone();
            mock_storage
                .expect_get_all_with_version()
                .returning(move |path| block_on(ram_storage_clone.get_all_with_version(path)));
            // The probe writes twice; a store that enforces preconditions rejects the second write,
            // this one accepts it.
            mock_storage
                .expect_put_if_absent()
                .returning(|path, payload| {
                    Ok(Some(ObjectVersion::new(format!(
                        "unconditional-{}-{}",
                        path.display(),
                        payload.len()
                    ))))
                });
            mock_storage.expect_delete().returning(|_| Ok(()));
            mock_storage
        }

        let error =
            FileBackedMetastore::try_new(Arc::new(mock_storage_ignoring_preconditions()), None)
                .await
                .expect_err("a storage that ignores preconditions must not be shared");
        assert!(
            error.to_string().contains("conditional writes"),
            "the refusal must name the capability gap, got: {error}"
        );

        let metastore = FileBackedMetastore::try_new_with_options(
            Arc::new(mock_storage_ignoring_preconditions()),
            None,
            true,
        )
        .await?;
        assert!(
            !metastore.is_distributed(),
            "with `allow_unsafe_storage` the metastore must fall back to single-writer mode"
        );
        Ok(())
    }

    /// A storage that cannot express a conditional write is a configuration mistake, not an unsafe
    /// endpoint. The escape hatch was written for the latter, so it must not cover this: turning it
    /// into a warning would start a node in single-writer mode on a prefix several nodes use.
    #[tokio::test]
    async fn test_distributed_metastore_refuses_storage_without_conditional_writes()
    -> anyhow::Result<()> {
        fn mock_storage_without_conditional_writes() -> MockStorage {
            let mut mock_storage = MockStorage::default();
            mock_storage
                .expect_uri()
                .return_const(Uri::for_test("s3://test-bucket/indexes"));
            mock_storage.expect_exists().returning(|_| Ok(false));
            mock_storage.expect_put().returning(|_, _| Ok(()));
            mock_storage.expect_put_if_absent().returning(|_, _| {
                Err(StorageErrorKind::Unsupported
                    .with_error(anyhow::anyhow!("this storage has no conditional writes")))
            });
            mock_storage.expect_delete().returning(|_| Ok(()));
            mock_storage
        }

        let error = FileBackedMetastore::try_new_with_options(
            Arc::new(mock_storage_without_conditional_writes()),
            None,
            true,
        )
        .await
        .expect_err("a storage without conditional writes must not start, even when allowed");
        let message = error.to_string();
        assert!(
            message.contains("does not implement conditional writes"),
            "the refusal must name the missing capability, got: {message}"
        );
        assert!(
            message.contains("does not apply here"),
            "the refusal must say the escape hatch does not cover this case, got: {message}"
        );
        Ok(())
    }

    /// A probe that could not run says nothing about preconditions. Downgrading it to single-writer
    /// mode would let a connectivity or credential problem decide the write path.
    #[tokio::test]
    async fn test_distributed_metastore_refuses_to_start_when_the_probe_fails() -> anyhow::Result<()>
    {
        fn mock_storage_whose_probe_fails() -> MockStorage {
            let mut mock_storage = MockStorage::default();
            mock_storage
                .expect_uri()
                .return_const(Uri::for_test("s3://test-bucket/indexes"));
            mock_storage.expect_exists().returning(|_| Ok(false));
            mock_storage.expect_put().returning(|_, _| Ok(()));
            mock_storage.expect_put_if_absent().returning(|_, _| {
                Err(StorageErrorKind::Internal.with_error(anyhow::anyhow!("connection refused")))
            });
            mock_storage.expect_delete().returning(|_| Ok(()));
            mock_storage
        }

        let error = FileBackedMetastore::try_new_with_options(
            Arc::new(mock_storage_whose_probe_fails()),
            None,
            true,
        )
        .await
        .expect_err("a probe that could not run must not be downgraded to single-writer mode");
        assert!(
            error.to_string().contains("failed to probe"),
            "the refusal must say the probe failed rather than name a capability gap, got: {error}"
        );
        Ok(())
    }

    /// Pins the replay budget of a lost compare-and-swap race.
    ///
    /// Each attempt costs a round trip to the object store, so the budget has to cover several of
    /// them on a slow endpoint (the cross-region R2 bucket used for the measurements answers in
    /// about two seconds), while a contended index must still not stall a request for minutes.
    #[test]
    fn test_distributed_retry_budget() {
        let total: Duration = (1..DISTRIBUTED_MAX_ATTEMPTS)
            .map(distributed_retry_backoff)
            .sum();
        assert!(
            total >= Duration::from_secs(15),
            "the replay budget must cover a slow object store, got {total:?}"
        );
        assert!(
            total <= Duration::from_secs(20),
            "a contended index must not stall a request for minutes, got {total:?}"
        );
    }

    /// A compare-and-swap that never wins must stop after a bounded number of attempts and report
    /// the conflict, rather than retrying forever or reporting success.
    #[tokio::test]
    async fn test_distributed_metastore_gives_up_after_bounded_conflicts() -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crate::metastore::file_backed::metrics::{
            CAS_CONFLICTS_EXHAUSTED_TOTAL, CAS_CONFLICTS_TOTAL,
        };

        // Counters are process-global and tests run in parallel, so the assertions below are lower
        // bounds: another test bumping the same counter only makes the observed delta larger.
        let conflicts_before = CAS_CONFLICTS_TOTAL.get();
        let exhausted_before = CAS_CONFLICTS_EXHAUSTED_TOTAL.get();

        let ram_storage = Arc::new(RamStorage::default());
        let mut mock_storage = MockStorage::default();
        // An `s3://` URI makes the metastore take the shared write path.
        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("s3://test-bucket/indexes"));

        let ram_storage_clone = ram_storage.clone();
        mock_storage
            .expect_put()
            .returning(move |path, payload| block_on(ram_storage_clone.put(path, payload)));
        let ram_storage_clone = ram_storage.clone();
        mock_storage
            .expect_exists()
            .returning(move |path| block_on(ram_storage_clone.exists(path)));
        let ram_storage_clone = ram_storage.clone();
        mock_storage
            .expect_put_if_absent()
            .returning(move |path, payload| {
                block_on(ram_storage_clone.put_if_absent(path, payload))
            });
        // The startup conditional-write probe cleans up after itself.
        mock_storage.expect_delete().returning(|_| Ok(()));
        let ram_storage_clone = ram_storage.clone();
        mock_storage
            .expect_get_all_with_version()
            .returning(move |path| block_on(ram_storage_clone.get_all_with_version(path)));

        let conflicts = Arc::new(AtomicUsize::new(0));
        let conflicts_clone = conflicts.clone();
        mock_storage
            .expect_put_if_version_matches()
            .returning(move |_path, _payload, _version| {
                conflicts_clone.fetch_add(1, Ordering::SeqCst);
                Err(StorageErrorKind::PreconditionFailed
                    .with_error(anyhow::anyhow!("injected conflict")))
            });

        let metastore = FileBackedMetastore::try_new(Arc::new(mock_storage), None).await?;
        assert!(metastore.is_distributed());

        let index_config = IndexConfig::for_test(
            "test-injected-conflicts",
            "s3://test-bucket/indexes/test-injected-conflicts",
        );
        let create_index_request = CreateIndexRequest::try_from_index_config(&index_config)?;
        let error = metastore
            .create_index(create_index_request)
            .await
            .expect_err("a compare-and-swap that never wins must not report success");

        assert!(
            matches!(error, MetastoreError::FailedPrecondition { .. }),
            "expected a precondition failure, got: {error}"
        );
        assert_eq!(
            conflicts.load(Ordering::SeqCst),
            DISTRIBUTED_MAX_ATTEMPTS,
            "the retry loop must stop after a bounded number of attempts"
        );
        assert!(
            CAS_CONFLICTS_TOTAL.get() - conflicts_before >= DISTRIBUTED_MAX_ATTEMPTS as u64,
            "every lost compare-and-swap race must be counted"
        );
        assert!(
            CAS_CONFLICTS_EXHAUSTED_TOTAL.get() - exhausted_before >= 1,
            "running out of retries must be visible to operators"
        );
        Ok(())
    }

    /// Builds a shared metastore over a RAM storage, with a hook on the manifest compare-and-swap.
    ///
    /// An `s3://` URI puts the metastore on the shared write path. `put_if_version_matches` is
    /// forwarded to the RAM storage except when `fail_on_call` says otherwise, which is how the
    /// tests below make a specific manifest write lose a race.
    fn shared_metastore_over_ram(
        ram_storage: Arc<RamStorage>,
        fail_on_call: Option<usize>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    ) -> MockStorage {
        use std::sync::atomic::Ordering;

        let mut mock_storage = MockStorage::default();
        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("s3://test-bucket/indexes"));
        let ram = ram_storage.clone();
        mock_storage
            .expect_put()
            .returning(move |path, payload| block_on(ram.put(path, payload)));
        let ram = ram_storage.clone();
        mock_storage
            .expect_exists()
            .returning(move |path| block_on(ram.exists(path)));
        let ram = ram_storage.clone();
        mock_storage
            .expect_put_if_absent()
            .returning(move |path, payload| block_on(ram.put_if_absent(path, payload)));
        let ram = ram_storage.clone();
        mock_storage
            .expect_get_all_with_version()
            .returning(move |path| block_on(ram.get_all_with_version(path)));
        let ram = ram_storage.clone();
        mock_storage
            .expect_get_all()
            .returning(move |path| block_on(ram.get_all(path)));
        // The startup conditional-write probe cleans up after itself.
        mock_storage.expect_delete().returning(|_| Ok(()));
        let ram = ram_storage.clone();
        mock_storage
            .expect_put_if_version_matches()
            .returning(move |path, payload, version| {
                let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if fail_on_call == Some(call) {
                    return Err(StorageErrorKind::PreconditionFailed
                        .with_error(anyhow::anyhow!("injected conflict")));
                }
                block_on(ram.put_if_version_matches(path, payload, version))
            });
        mock_storage
    }

    /// A create that loses a manifest compare-and-swap is replayed, and the replay finds the index
    /// file written by the previous attempt. Reporting `AlreadyExists` there would fail a create
    /// that actually happened, which is what a two-node cluster used to see (2 creates out of 40).
    #[tokio::test]
    async fn test_distributed_create_index_survives_a_replayed_manifest_conflict()
    -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let ram_storage = Arc::new(RamStorage::default());
        let calls = Arc::new(AtomicUsize::new(0));
        // Call 1 is the manifest write that marks the index `Creating`, call 2 the one that marks
        // it `Active`: failing call 2 replays the create with the index file already written.
        let mock_storage = shared_metastore_over_ram(ram_storage.clone(), Some(2), calls.clone());
        let metastore = FileBackedMetastore::try_new(Arc::new(mock_storage), None).await?;
        assert!(metastore.is_distributed());

        let index_config = IndexConfig::for_test(
            "test-create-replayed",
            "s3://test-bucket/indexes/test-create-replayed",
        );
        let create_index_request = CreateIndexRequest::try_from_index_config(&index_config)?;
        let response = metastore
            .create_index(create_index_request)
            .await
            .expect("a replayed create must not be reported as an existing index");
        assert_eq!(response.index_uid().index_id, "test-create-replayed");
        assert!(
            calls.load(Ordering::SeqCst) >= 3,
            "the create should have been replayed after the injected conflict"
        );
        // The index file is there, and so is its manifest entry.
        assert!(
            block_on(ram_storage.exists(&metastore_filepath("test-create-replayed"))).unwrap(),
            "the index file must exist after the create"
        );
        let indexes_metadata = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await?
            .deserialize_indexes_metadata()
            .await?;
        assert!(
            indexes_metadata
                .iter()
                .any(|index| index.index_id() == "test-create-replayed"),
            "the manifest must list the index that was just created"
        );
        Ok(())
    }

    /// The other direction: an index file written by *another* node (a different incarnation id)
    /// must still be reported as an existing index instead of being adopted.
    #[tokio::test]
    async fn test_distributed_create_index_does_not_adopt_another_nodes_file() -> anyhow::Result<()>
    {
        use std::sync::atomic::AtomicUsize;

        let ram_storage = Arc::new(RamStorage::default());
        // Another node wrote the index file; its manifest write has not landed (yet).
        let index_config = IndexConfig::for_test(
            "test-create-other-node",
            "s3://test-bucket/indexes/test-create-other-node",
        );
        let other_index = FileBackedIndex::from(IndexMetadata::new(index_config.clone()));
        let other_index_bytes = serde_utils::to_json_bytes_pretty(&other_index)?;
        block_on(ram_storage.put(
            &metastore_filepath("test-create-other-node"),
            Box::new(other_index_bytes),
        ))?;

        let calls = Arc::new(AtomicUsize::new(0));
        let mock_storage = shared_metastore_over_ram(ram_storage.clone(), None, calls);
        let metastore = FileBackedMetastore::try_new(Arc::new(mock_storage), None).await?;
        assert!(metastore.is_distributed());

        let create_index_request = CreateIndexRequest::try_from_index_config(&index_config)?;
        let error = metastore
            .create_index(create_index_request)
            .await
            .expect_err("another node's index file must not be adopted");
        assert!(
            matches!(error, MetastoreError::AlreadyExists(_)),
            "expected `AlreadyExists`, got: {error}"
        );
        Ok(())
    }

    /// A backend without conditional writes must fail loudly in distributed mode.
    ///
    /// Falling back to an unconditional write would "work" and silently lose updates, which is the
    /// one outcome a shared metastore must never produce.
    #[tokio::test]
    async fn test_distributed_metastore_rejects_storage_without_conditional_writes()
    -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let uri: Uri = format!("file://{}", temp_dir.path().display())
            .parse()
            .unwrap();
        let storage: Arc<dyn Storage> = Arc::new(LocalFileStorage::from_uri(&uri)?);
        let mut metastore = FileBackedMetastore::try_new(storage, None).await?;
        assert!(
            !metastore.is_distributed(),
            "a local-file metastore must stay single-node"
        );

        // Force the shared mode onto a backend that cannot version an object.
        metastore.set_distributed(true);
        let index_config = IndexConfig::for_test(
            "test-no-conditional-writes",
            "file:///indexes/test-no-conditional-writes",
        );
        let create_index_request = CreateIndexRequest::try_from_index_config(&index_config)?;
        let error = metastore
            .create_index(create_index_request)
            .await
            .expect_err("a storage without conditional writes must not accept a shared write");
        let message = error.to_string();
        assert!(
            message.contains("conditional writes"),
            "the error must name the capability gap, got: {message}"
        );
        Ok(())
    }
}
