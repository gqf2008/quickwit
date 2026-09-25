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

//! Split metadata stored as striped manifests, immutable WAL objects and time-bucketed segments.
//!
//! The single-object layout and the sharded layout both keep the split map of an index in memory:
//! a read materialises it, a mutation reloads it, so both cost `O(splits in the index)`. This
//! layout stores references instead of data, the way the neighbouring project `objsearch` stores
//! vectors (`src/manifest.rs`: `segments: Vec<SegmentRef>`, `wal: Vec<WalRef>`, one
//! compare-and-swap on that small object), and the measured shape is in
//! `docs/internals/metastore-v3-manifest-and-segments.md`:
//!
//! ```text
//! <index_id>/v3/manifest-<stripe:03>.json      mutable, one compare-and-swap point per stripe
//! <index_id>/v3/wal-<stripe:03>/<generation:020>-<uuid>.json   immutable, one per published batch
//! <index_id>/v3/segments/<stripe:03>/<bucket:012>/<generation:020>-<uuid>.json
//!                                              immutable, one per time bucket of one stripe
//! ```
//!
//! A publish appends one WAL object and commits one manifest (three storage calls: read, write,
//! compare-and-swap). A fold turns the WAL tail of a stripe into one segment per time bucket it
//! touched. A read loads the manifests, prunes the buckets outside the query's time window, and
//! fetches only the segments that remain, plus the WAL tail — so its cost follows the window, not
//! the index. `num_stripes` is not an optimisation but a requirement: with one manifest the target
//! write rate (~11.6/s at 5·10¹² documents/day) cannot be sustained beyond a same-zone round trip,
//! and writes start exhausting their replay budget (measured in the spike, 200 ms round trip).
//!
//! Invariants:
//!
//! 1. Only the writer holding a stripe's manifest version commits it; losing is an error the caller
//!    replays, never an overwrite.
//! 2. A segment and a WAL object are immutable and are named after the *fold generation* of the
//!    stripe that owns them — its own directory, its own counter — so garbage collection is a pure
//!    function of the name and that stripe's manifest (never of a wall clock). A WAL object is
//!    named after the fold that will take it over, which is what makes its name comparable with the
//!    generation a later fold collects up to.
//! 3. An unreferenced object is never deleted on the failure path: a conditional write that was
//!    committed but whose response was lost is indistinguishable from a lost race
//!    (`LESSON_条件写失败后不得清理自己写的对象.md`), so only the generation-gated GC removes
//!    anything.
//! 4. A read is a snapshot: it loads a manifest and then fetches the immutable objects that
//!    manifest names, so it cannot observe a torn view. It may observe an older or a newer epoch
//!    than the caller wrote, never a mixture of the two.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use futures::TryStreamExt;
use quickwit_proto::ingest::Shard;
use quickwit_proto::metastore::{MetastoreError, MetastoreResult, serde_utils};
use quickwit_proto::types::{ShardId, SourceId, SplitId};
use quickwit_storage::{ObjectVersion, OwnedBytes, Storage, StorageErrorKind};
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

use super::file_backed_index::FileBackedIndex;
use crate::Split;

/// Version of the objects written by this layout. A reader refuses what it does not know.
pub(crate) const MANIFEST_LAYOUT_FORMAT_VERSION: u32 = 1;

/// Format version of the object that carries one shard's state.
pub(crate) const SHARD_OBJECT_FORMAT_VERSION: u32 = 1;

/// Every shard, as it was read from its own object: source, shard, state.
pub(crate) type ShardObjects = BTreeMap<SourceId, BTreeMap<ShardId, Shard>>;

/// One shard's state, stored on its own so that publishing it does not write the root.
///
/// The source's checkpoint is rebuilt from the shards' publish positions, exactly as the root's
/// inline copy is read back today, so the object holds the shard and nothing else.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ShardObject {
    format_version: u32,
    shard: Shard,
}

/// Generations of segments kept after a fold, so a reader that lost a race can still finish.
const SEGMENT_GRACE_GENERATIONS: u64 = 2;

/// Test-only way to make a stripe's manifest commit fail once, so a test can build the state a
/// replay faces: part of a mutation committed, the rest not.
#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::Mutex;

    static FAIL_NEXT_COMMIT_FOR_STRIPE: Mutex<Option<usize>> = Mutex::new(None);
    static FAIL_NEXT_ROOT_COMMITS: Mutex<Option<(String, u32)>> = Mutex::new(None);

    /// Fails the next manifest commit of `stripe`, once.
    pub(crate) fn fail_next_commit_for_stripe(stripe: usize) {
        *FAIL_NEXT_COMMIT_FOR_STRIPE.lock().unwrap() = Some(stripe);
    }

    /// Fails the next `attempts` commits of the root of `index_id`, so a test can drive the
    /// index-metadata compare-and-swap (the one shared object) into exhausting its replay budget.
    ///
    /// The hook names the index it is armed for: the state is a process-global, so a runner that
    /// keeps every test in one process (plain `cargo test`) would otherwise let another test's
    /// mutation consume the injections.
    pub(crate) fn fail_next_root_commits(index_id: &str, attempts: u32) {
        *FAIL_NEXT_ROOT_COMMITS.lock().unwrap() = Some((index_id.to_string(), attempts));
    }

    pub(super) fn take_root_commit_failure(index_id: &str) -> bool {
        let mut guard = FAIL_NEXT_ROOT_COMMITS.lock().unwrap();
        let Some((armed_index_id, attempts)) = guard.as_mut() else {
            return false;
        };
        if armed_index_id != index_id || *attempts == 0 {
            return false;
        }
        *attempts -= 1;
        true
    }

    pub(super) fn take_failure_for(stripe: usize) -> bool {
        let mut guard = FAIL_NEXT_COMMIT_FOR_STRIPE.lock().unwrap();
        if *guard == Some(stripe) {
            *guard = None;
            true
        } else {
            false
        }
    }
}

/// Bucket of the splits that carry no time range. A time window never prunes it away.
const UNTIMED_BUCKET: i64 = i64::MIN;

/// A stripe folds its WAL tail once it holds this many unpublished batches.
///
/// Folding is what keeps a read's WAL tail short, and it costs one segment rewrite per bucket the
/// tail touched, so the threshold trades read cost against fold cost.
const FOLD_WAL_THRESHOLD: usize = 32;

/// A stripe folds its WAL tail once it holds this many unpublished operations.
///
/// The object count alone is not enough: a batch can be large (a publish of thousands of splits),
/// and then a handful of objects already carry most of the index. The threshold is on what a read
/// would have to fetch, in entries, not on how many objects carry them.
const FOLD_WAL_THRESHOLD_OPS: usize = 1_000;

/// One operation on one split: its new value, or its removal.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SplitOp {
    pub split_id: SplitId,
    /// `None` records that the split is gone.
    pub split: Option<Split>,
}

/// A batch of operations published together: one immutable object.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct WalBatch {
    format_version: u32,
    ops: Vec<SplitOp>,
}

/// A segment the manifest points at, with the split-id range it covers.
///
/// The range is what makes a split id findable without scanning every segment of a stripe, the way
/// a sorted run in an LSM tree lets a point lookup skip the runs that cannot hold the key.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SegmentRef {
    key: String,
    min_split_id: SplitId,
    max_split_id: SplitId,
    /// Earliest `time_range.start` of the splits in the segment, or `None` when it holds only
    /// splits without a time range.
    ///
    /// Pruning uses this span and not the bucket: a split is placed in the bucket of its *start*,
    /// so its range can reach into later buckets, and pruning by bucket would drop a split
    /// that overlaps the query.
    #[serde(default)]
    min_time_range_start: Option<i64>,
    /// Latest `time_range.end` of the splits in the segment.
    #[serde(default)]
    max_time_range_end: Option<i64>,
    /// Whether the segment holds splits with no time range, which no window prunes away.
    #[serde(default)]
    has_untimed_splits: bool,
    num_splits: usize,
}

/// The mutable state of one stripe: references, never splits.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct StripeManifest {
    format_version: u32,
    /// Commit counter of this stripe; bumped by every compare-and-swap.
    epoch: u64,
    /// Fold counter of this stripe; bumped by every fold.
    ///
    /// Segments and WAL objects are named after *this* and not after `epoch`: the grace that keeps
    /// them alive for a reader is a number of folds, and one commit is not a useful unit for it
    /// (at 11.6 writes/s two commits are a sixth of a second, while a reader a bucket away
    /// needs seconds to fetch what a manifest names). Both are collected against the generation of
    /// their own stripe, since that is the manifest a reader would have taken them from.
    #[serde(default)]
    fold_generation: u64,
    /// Segment currently serving each time bucket.
    segments: BTreeMap<i64, SegmentRef>,
    /// WAL objects not yet folded into segments.
    wal: Vec<String>,
    /// Number of operations those objects carry, so a fold triggers on size and not only on count.
    #[serde(default)]
    wal_ops: usize,
    /// Bucket width in seconds; a layout constant recorded here so a reader can prune.
    bucket_secs: i64,
}

impl StripeManifest {
    fn new(bucket_secs: i64) -> Self {
        Self {
            format_version: MANIFEST_LAYOUT_FORMAT_VERSION,
            epoch: 0,
            fold_generation: 0,
            segments: BTreeMap::new(),
            wal: Vec::new(),
            wal_ops: 0,
            bucket_secs,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SplitSegment {
    format_version: u32,
    bucket: i64,
    splits: Vec<Split>,
}

/// The part of an index that is *not* its split map: metadata, sources, checkpoints, delete tasks.
///
/// It is small and low-churn, so it keeps the single-object compare-and-swap while the splits live
/// in manifests, WAL objects and segments.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ManifestRoot {
    format_version: u32,
    /// Layout parameters, recorded with the index so a reader does not need the writer's config.
    bucket_secs: i64,
    num_stripes: usize,
    index: FileBackedIndex,
}

/// Split metadata of one index, stored as manifests, WAL objects and segments.
#[derive(Clone, Debug)]
pub(crate) struct ManifestLayout {
    index_id: String,
    bucket_secs: i64,
    num_stripes: usize,
}

/// What a write needs to commit an index stored in this layout: the root as it was read, and the
/// layout parameters (bucket width, stripe count) that the objects themselves record.
#[derive(Clone, Debug)]
pub(crate) struct ManifestWriteContext {
    pub layout: ManifestLayout,
    pub root_version: ObjectVersion,
    pub root_bytes: OwnedBytes,
    /// The shard states the root's index was completed with, so a write can tell which objects
    /// changed and which are obsolete.
    pub shard_objects: ShardObjects,
}

/// What the root says about the index: its non-split state and the layout parameters.
#[derive(Clone, Debug)]
pub(crate) struct ManifestRootInfo {
    pub index: FileBackedIndex,
    pub bucket_secs: i64,
    pub num_stripes: usize,
    /// The shard states read from their own objects, as they were found.
    pub shard_objects: ShardObjects,
}

/// Splits an object path under the shard prefix into the source and the shard it names.
fn shard_object_ids(prefix: &Path, path: &Path) -> Option<(SourceId, ShardId)> {
    let relative = path.strip_prefix(prefix).ok()?;
    let mut components = relative.components();
    let source_id = components.next()?.as_os_str().to_str()?;
    let file_name = components.next()?.as_os_str().to_str()?;
    if components.next().is_some() {
        return None;
    }
    let shard_id = file_name.strip_suffix(".json")?;
    Some((source_id.to_string(), ShardId::from(shard_id)))
}

fn internal(message: impl Into<String>, cause: impl Into<String>) -> MetastoreError {
    MetastoreError::Internal {
        message: message.into(),
        cause: cause.into(),
    }
}

fn map_storage_error(index_id: &str, error: quickwit_storage::StorageError) -> MetastoreError {
    super::store_operations::convert_error(index_id, error)
}

impl ManifestLayout {
    pub(crate) fn new(index_id: &str, bucket_secs: i64, num_stripes: usize) -> Self {
        assert!(bucket_secs > 0, "the bucket width has to be positive");
        assert!(num_stripes > 0, "the layout needs at least one stripe");
        Self {
            index_id: index_id.to_string(),
            bucket_secs,
            num_stripes,
        }
    }

    /// Stripe a split belongs to. FNV-1a over the id, spelled out because it is part of the layout.
    pub(crate) fn stripe_of(&self, split_id: &str) -> usize {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in split_id.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x1000_0000_01b3);
        }
        (hash % self.num_stripes as u64) as usize
    }

    fn prefix(&self) -> PathBuf {
        Path::new(&self.index_id).join("v3")
    }

    pub(crate) fn root_path(&self) -> PathBuf {
        self.prefix().join("root.json")
    }

    fn manifest_path(&self, stripe: usize) -> PathBuf {
        self.prefix().join(format!("manifest-{stripe:03}.json"))
    }

    fn wal_path(&self, stripe: usize, generation: u64, object_id: &str) -> PathBuf {
        self.wal_prefix(stripe)
            .join(format!("{generation:020}-{object_id}.json"))
    }

    fn wal_prefix(&self, stripe: usize) -> PathBuf {
        self.prefix().join(format!("wal-{stripe:03}"))
    }

    /// Segments are named under the stripe that wrote them.
    ///
    /// The stripe is part of the path because the only state a segment needs to be collected
    /// against is the fold generation of its own stripe: a reader holds the manifests of every
    /// stripe, so the age that matters for one stripe's segment is how many times *that* stripe has
    /// folded since. Keeping them together would force one watermark on stripes that fold at
    /// different rates — either collecting what a slow stripe is still reading, or never collecting
    /// anything because some stripe of the index has never folded at all.
    fn segment_path(&self, stripe: usize, bucket: i64, epoch: u64, object_id: &str) -> PathBuf {
        self.segment_stripe_prefix(stripe)
            .join(format!("{bucket:012}"))
            .join(format!("{epoch:020}-{object_id}.json"))
    }

    fn segment_stripe_prefix(&self, stripe: usize) -> PathBuf {
        self.prefix().join("segments").join(format!("{stripe:03}"))
    }

    fn shards_prefix(&self) -> PathBuf {
        self.prefix().join("shards")
    }

    fn source_shards_prefix(&self, source_id: &SourceId) -> PathBuf {
        self.shards_prefix().join(source_id)
    }

    /// Where one shard's state lives. Source ids and shard ids are path-safe identifiers.
    pub(crate) fn shard_path(&self, source_id: &SourceId, shard_id: &ShardId) -> PathBuf {
        self.source_shards_prefix(source_id)
            .join(format!("{shard_id}.json"))
    }

    /// Bucket a split belongs to.
    ///
    /// A split without a time range matches every query (that is what the metastore's predicate
    /// does), so it goes to a bucket that no window prunes away.
    pub(crate) fn bucket_of(&self, split: &Split) -> i64 {
        let Some(time_range) = &split.split_metadata.time_range else {
            return UNTIMED_BUCKET;
        };
        time_range.start().div_euclid(self.bucket_secs)
    }

    /// Creates the empty manifests of an index. Fails if one already exists.
    ///
    /// The manifests are created together: one round trip per stripe in sequence would make
    /// creating an index cost as many round trips as it has stripes (32 of them by default),
    /// which on a bucket that is a round trip away is the difference between instant and half a
    /// minute.
    pub(crate) async fn create(&self, storage: &dyn Storage) -> MetastoreResult<()> {
        let body = serde_utils::to_json_bytes(&StripeManifest::new(self.bucket_secs))?;
        futures::future::try_join_all((0..self.num_stripes).map(|stripe| {
            let body = body.clone();
            async move {
                match storage
                    .put_if_absent(&self.manifest_path(stripe), Box::new(body))
                    .await
                {
                    Ok(_) => Ok(()),
                    // An existing manifest is this node's own from an interrupted create: the
                    // content is fixed, so it is the same object.
                    Err(error) if error.kind() == StorageErrorKind::PreconditionFailed => Ok(()),
                    Err(error) => Err(map_storage_error(&self.index_id, error)),
                }
            }
        }))
        .await?;
        Ok(())
    }

    /// Whether the index exists in this layout.
    pub(crate) async fn exists(&self, storage: &dyn Storage) -> MetastoreResult<bool> {
        storage
            .exists(&self.root_path())
            .await
            .map_err(|error| map_storage_error(&self.index_id, error))
    }

    /// Creates the root and the empty manifests of an index. Fails if the root already exists.
    pub(crate) async fn create_index(
        &self,
        storage: &dyn Storage,
        index: &FileBackedIndex,
    ) -> MetastoreResult<()> {
        let mut index_without_splits = index.clone();
        // An index is created without shards in practice, but the state must not be left behind in
        // the root either: it belongs to the shard objects, and a root that carried a copy would
        // make the next publish write it.
        let splits = index_without_splits.take_splits();
        let source_shards = index_without_splits.take_source_shards();
        let root = ManifestRoot {
            format_version: MANIFEST_LAYOUT_FORMAT_VERSION,
            bucket_secs: self.bucket_secs,
            num_stripes: self.num_stripes,
            index: index_without_splits,
        };
        let root_bytes = serde_utils::to_json_bytes_pretty(&root)?;
        let mut restore_splits = root.index;
        restore_splits.put_splits(splits);
        restore_splits.put_source_shards(source_shards);
        self.sync_shard_objects(storage, &restore_splits, &ShardObjects::new())
            .await?;
        // Manifests first, root last: the root is what makes the index exist, so an interrupted
        // create leaves a state a retry can finish instead of an index that exists but cannot be
        // read.
        self.create(storage).await?;
        storage
            .put_if_absent(&self.root_path(), Box::new(root_bytes))
            .await
            .map_err(|error| map_storage_error(&self.index_id, error))?;
        Ok(())
    }

    /// Reads the root: the index without its splits, with the version to compare-and-swap against.
    pub(crate) async fn load_root(
        &self,
        storage: &dyn Storage,
    ) -> MetastoreResult<(ManifestRootInfo, ObjectVersion, OwnedBytes)> {
        self.load_root_with_shards(storage, true).await
    }

    /// Reads the root without the shard objects.
    ///
    /// The readers that only name the index (its stats, a listing that filters by a time window)
    /// do not need the shard state, and reading it would add a list and one read per shard to a
    /// path that was just measured at 645 ms for a 4 000-split index.
    pub(crate) async fn load_root_metadata_only(
        &self,
        storage: &dyn Storage,
    ) -> MetastoreResult<(ManifestRootInfo, ObjectVersion, OwnedBytes)> {
        self.load_root_with_shards(storage, false).await
    }

    async fn load_root_with_shards(
        &self,
        storage: &dyn Storage,
        load_shard_objects: bool,
    ) -> MetastoreResult<(ManifestRootInfo, ObjectVersion, OwnedBytes)> {
        let path = self.root_path();
        let (bytes, version_opt) = storage
            .get_all_with_version(&path)
            .await
            .map_err(|error| map_storage_error(&self.index_id, error))?;
        let root: ManifestRoot = serde_utils::from_json_bytes(&bytes)?;
        if root.format_version != MANIFEST_LAYOUT_FORMAT_VERSION {
            return Err(internal(
                "index uses an unknown metadata layout version",
                format!(
                    "`{}` says `{}`, this node understands `{}`",
                    path.display(),
                    root.format_version,
                    MANIFEST_LAYOUT_FORMAT_VERSION
                ),
            ));
        }
        let version = version_opt.ok_or_else(|| {
            internal(
                "this layout needs a storage that versions objects",
                format!("no version for `{}`", path.display()),
            )
        })?;
        let shard_objects = if load_shard_objects {
            self.load_shard_objects(storage).await?
        } else {
            ShardObjects::new()
        };
        let mut index = root.index;
        if load_shard_objects {
            index.put_shard_objects(&shard_objects);
        }
        let info = ManifestRootInfo {
            index,
            shard_objects,
            bucket_secs: root.bucket_secs,
            num_stripes: root.num_stripes,
        };
        Ok((info, version, bytes))
    }

    /// Every shard whose state was written to its own object, keyed by source.
    ///
    /// A root written before the change still carries its shards inline; those are the base the
    /// objects are merged over, and a write moves them into objects of their own.
    pub(crate) async fn load_shard_objects(
        &self,
        storage: &dyn Storage,
    ) -> MetastoreResult<ShardObjects> {
        let prefix = self.shards_prefix();
        let mut pages = storage.list(&prefix);
        let mut paths = Vec::new();
        loop {
            let page = pages.try_next().await.map_err(|error| {
                internal("failed to list the shards of the index", error.to_string())
            })?;
            let Some(page) = page else {
                break;
            };
            for metadata in page {
                paths.push(metadata.path);
            }
        }
        let mut shard_objects = ShardObjects::new();
        for path in paths {
            let (source_id, shard_id) = shard_object_ids(&prefix, &path).ok_or_else(|| {
                internal(
                    "unexpected object in the shard state of the index",
                    format!("`{}` is not `<source>/<shard>.json`", path.display()),
                )
            })?;
            let bytes = storage
                .get_all(&path)
                .await
                .map_err(|error| map_storage_error(&self.index_id, error))?;
            let object: ShardObject = serde_utils::from_json_bytes(&bytes)?;
            if object.format_version != SHARD_OBJECT_FORMAT_VERSION {
                return Err(internal(
                    "unknown shard state format",
                    format!(
                        "`{}` says `{}`, this node understands `{}`",
                        path.display(),
                        object.format_version,
                        SHARD_OBJECT_FORMAT_VERSION
                    ),
                ));
            }
            if object.shard.shard_id() != shard_id {
                return Err(internal(
                    "a shard object names another shard",
                    format!("`{}` holds `{}`", path.display(), object.shard.shard_id()),
                ));
            }
            shard_objects
                .entry(source_id)
                .or_default()
                .insert(shard_id, object.shard);
        }
        Ok(shard_objects)
    }

    /// Writes the shard objects a mutation changed or created, and answers with the objects that
    /// are no longer needed.
    ///
    /// A shard that is in the index but not in `loaded` has never been written to its own object —
    /// either it is new or an older root carried it inline — so it is written too. The obsolete
    /// objects are returned rather than deleted: their deletion only makes sense once the root
    /// commit went through, because until then a reader may still resolve the shard through the
    /// root.
    async fn sync_shard_objects(
        &self,
        storage: &dyn Storage,
        index: &FileBackedIndex,
        loaded: &ShardObjects,
    ) -> MetastoreResult<Vec<PathBuf>> {
        let current: BTreeMap<(SourceId, ShardId), Shard> = index
            .shard_states()
            .into_iter()
            .map(|(source_id, shard_id, shard)| ((source_id, shard_id), shard))
            .collect();
        for ((source_id, shard_id), shard) in &current {
            let unchanged = matches!(
                loaded
                    .get(source_id)
                    .and_then(|per_shard| per_shard.get(shard_id)),
                Some(loaded_shard) if loaded_shard == shard
            );
            if unchanged {
                continue;
            }
            let object = ShardObject {
                format_version: SHARD_OBJECT_FORMAT_VERSION,
                shard: shard.clone(),
            };
            let bytes = serde_utils::to_json_bytes_pretty(&object)?;
            storage
                .put(&self.shard_path(source_id, shard_id), Box::new(bytes))
                .await
                .map_err(|error| map_storage_error(&self.index_id, error))?;
        }
        let mut obsolete = Vec::new();
        for (source_id, per_shard) in loaded {
            for shard_id in per_shard.keys() {
                if !current.contains_key(&(source_id.clone(), shard_id.clone())) {
                    obsolete.push(self.shard_path(source_id, shard_id));
                }
            }
        }
        Ok(obsolete)
    }

    /// Commits the root if it changed since it was read.
    pub(crate) async fn store_root(
        &self,
        storage: &dyn Storage,
        index: &mut FileBackedIndex,
        loaded_shard_objects: &ShardObjects,
        previous_root_bytes: &OwnedBytes,
        version: &ObjectVersion,
    ) -> MetastoreResult<()> {
        let splits = index.take_splits();
        // The shard state does not go into the root: it lives in one object per shard, written
        // below. An ordinary publish then leaves the root untouched, which is the point — the
        // root is a single hot object every node used to write.
        let source_shards = index.take_source_shards();
        let root = ManifestRoot {
            format_version: MANIFEST_LAYOUT_FORMAT_VERSION,
            bucket_secs: self.bucket_secs,
            num_stripes: self.num_stripes,
            index: index.clone(),
        };
        let root_bytes = serde_utils::to_json_bytes_pretty(&root);
        index.put_splits(splits);
        index.put_source_shards(source_shards);
        let root_bytes = root_bytes?;
        // The shard objects are written before the root: a crash in between leaves a reader with
        // the new shard state and the old root, which is what the caller's replay expects. The
        // other order would lose the state of a shard the root no longer carries.
        let obsolete_shard_objects = self
            .sync_shard_objects(storage, index, loaded_shard_objects)
            .await?;
        if root_bytes.as_slice() == previous_root_bytes.as_slice() {
            self.delete_shard_objects(storage, obsolete_shard_objects)
                .await?;
            return Ok(());
        }
        #[cfg(test)]
        if test_hooks::take_root_commit_failure(&self.index_id) {
            return Err(MetastoreError::FailedPrecondition {
                entity: quickwit_proto::metastore::EntityKind::Index {
                    index_id: self.index_id.clone(),
                },
                message: "injected index metadata conflict".to_string(),
            });
        }
        storage
            .put_if_version_matches(&self.root_path(), Box::new(root_bytes), version)
            .await
            .map_err(|error| map_storage_error(&self.index_id, error))?;
        self.delete_shard_objects(storage, obsolete_shard_objects)
            .await?;
        Ok(())
    }

    /// Removes the shard objects a mutation made obsolete (a deleted or pruned shard).
    async fn delete_shard_objects(
        &self,
        storage: &dyn Storage,
        paths: Vec<PathBuf>,
    ) -> MetastoreResult<()> {
        for path in paths {
            storage
                .delete(&path)
                .await
                .map_err(|error| map_storage_error(&self.index_id, error))?;
        }
        Ok(())
    }

    /// Publishes operations on the stripes they belong to.
    ///
    /// Each stripe commits on its own; if one fails the caller replays the whole mutation, and
    /// re-applying a mutation is idempotent (it sets states, it does not increment anything).
    pub(crate) async fn publish_ops(
        &self,
        storage: &dyn Storage,
        ops: Vec<SplitOp>,
    ) -> MetastoreResult<()> {
        let mut per_stripe: BTreeMap<usize, Vec<SplitOp>> = BTreeMap::new();
        for op in ops {
            per_stripe
                .entry(self.stripe_of(op.split_id.as_str()))
                .or_default()
                .push(op);
        }
        for (stripe, ops) in per_stripe {
            self.publish(storage, stripe, ops).await?;
        }
        Ok(())
    }

    /// Every split of the index, read from the segments and the WAL tail.
    pub(crate) async fn load_split_map(
        &self,
        storage: &dyn Storage,
    ) -> MetastoreResult<Vec<Split>> {
        self.list_splits(storage, i64::MIN, i64::MAX).await
    }

    /// Reads the splits with these ids, and only those.
    ///
    /// This is what lets a mutation look at the splits it is about to change instead of at the
    /// whole index: the stripe of an id is a pure function of the id, and the segment id ranges
    /// in the manifest tell which segments could hold it. Segments are fetched once per call,
    /// so a mutation touching several splits of one bucket pays for that segment once.
    pub(crate) async fn get_splits_by_id(
        &self,
        storage: &dyn Storage,
        split_ids: &[SplitId],
    ) -> MetastoreResult<std::collections::HashMap<SplitId, Split>> {
        let mut per_stripe: BTreeMap<usize, Vec<SplitId>> = BTreeMap::new();
        for split_id in split_ids {
            per_stripe
                .entry(self.stripe_of(split_id.as_str()))
                .or_default()
                .push(split_id.clone());
        }
        let mut found: std::collections::HashMap<SplitId, Split> = std::collections::HashMap::new();
        for (stripe, wanted) in per_stripe {
            let (manifest, _) = self.read_manifest(storage, stripe).await?;
            let mut fetched: BTreeMap<String, SplitSegment> = BTreeMap::new();
            for split_id in &wanted {
                for segment_ref in manifest.segments.values() {
                    if split_id < &segment_ref.min_split_id || split_id > &segment_ref.max_split_id
                    {
                        continue;
                    }
                    if !fetched.contains_key(&segment_ref.key) {
                        let segment = self.read_segment(storage, &segment_ref.key).await?;
                        fetched.insert(segment_ref.key.clone(), segment);
                    }
                    if let Some(split) = fetched[&segment_ref.key]
                        .splits
                        .iter()
                        .find(|split| split.split_id() == split_id)
                    {
                        found.insert(split_id.clone(), split.clone());
                    }
                }
            }
            // The WAL tail wins: it holds everything published since the last fold.
            for wal_key in &manifest.wal {
                let bytes = storage
                    .get_all(Path::new(wal_key))
                    .await
                    .map_err(|error| map_storage_error(&self.index_id, error))?;
                let wal: WalBatch = serde_utils::from_json_bytes(&bytes)?;
                for op in wal.ops {
                    if !wanted.contains(&op.split_id) {
                        continue;
                    }
                    match op.split {
                        Some(split) => {
                            found.insert(op.split_id, split);
                        }
                        None => {
                            found.remove(&op.split_id);
                        }
                    }
                }
            }
        }
        Ok(found)
    }

    /// Deletes every object of the index.
    pub(crate) async fn delete(&self, storage: &dyn Storage) -> MetastoreResult<()> {
        let mut pages = storage.list(&self.prefix());
        let mut paths = Vec::new();
        loop {
            let page = pages.try_next().await.map_err(|error| {
                internal("failed to list the objects of the index", error.to_string())
            })?;
            let Some(page) = page else {
                break;
            };
            for metadata in page {
                paths.push(metadata.path);
            }
        }
        for path in paths {
            storage
                .delete(&path)
                .await
                .map_err(|error| map_storage_error(&self.index_id, error))?;
        }
        Ok(())
    }

    async fn read_manifest(
        &self,
        storage: &dyn Storage,
        stripe: usize,
    ) -> MetastoreResult<(StripeManifest, ObjectVersion)> {
        let path = self.manifest_path(stripe);
        let (bytes, version_opt) = storage
            .get_all_with_version(&path)
            .await
            .map_err(|error| map_storage_error(&self.index_id, error))?;
        let manifest: StripeManifest = serde_utils::from_json_bytes(&bytes)?;
        if manifest.format_version != MANIFEST_LAYOUT_FORMAT_VERSION {
            return Err(internal(
                "index uses an unknown split layout version",
                format!(
                    "manuscript {} says `{}`, this node understands `{}`",
                    path.display(),
                    manifest.format_version,
                    MANIFEST_LAYOUT_FORMAT_VERSION
                ),
            ));
        }
        let version = version_opt.ok_or_else(|| {
            internal(
                "this layout needs a storage that versions objects",
                format!("no version for `{}`", path.display()),
            )
        })?;
        Ok((manifest, version))
    }

    /// Publishes a batch of operations on one stripe: one WAL object, one manifest commit.
    ///
    /// Returns `FailedPrecondition` when another writer committed first; the caller re-reads and
    /// replays, exactly like the shared metastore's other write paths.
    pub(crate) async fn publish(
        &self,
        storage: &dyn Storage,
        stripe: usize,
        ops: Vec<SplitOp>,
    ) -> MetastoreResult<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let (mut manifest, version) = self.read_manifest(storage, stripe).await?;
        let object_id = Uuid::new_v4().to_string();
        // The object is named after the fold generation that will take it over, which is what lets
        // the collection of a later fold tell an old WAL object from a live one. It is *not* the
        // commit counter: the grace a reader gets is counted in folds, and a stripe commits once
        // per publish but folds once per 32 of them, so commit-named objects never reach a
        // fold-counted watermark and are never collected.
        let wal_generation = manifest.fold_generation + 1;
        let num_ops = ops.len();
        let wal = WalBatch {
            format_version: MANIFEST_LAYOUT_FORMAT_VERSION,
            ops,
        };
        storage
            .put(
                &self.wal_path(stripe, wal_generation, &object_id),
                Box::new(serde_utils::to_json_bytes(&wal)?),
            )
            .await
            .map_err(|error| map_storage_error(&self.index_id, error))?;
        manifest.wal.push(
            self.wal_path(stripe, wal_generation, &object_id)
                .to_string_lossy()
                .to_string(),
        );
        manifest.wal_ops += num_ops;
        manifest.epoch += 1;
        #[cfg(test)]
        if test_hooks::take_failure_for(stripe) {
            return Err(MetastoreError::FailedPrecondition {
                entity: quickwit_proto::metastore::EntityKind::Index {
                    index_id: self.index_id.clone(),
                },
                message: "injected manifest commit conflict".to_string(),
            });
        }
        storage
            .put_if_version_matches(
                &self.manifest_path(stripe),
                Box::new(serde_utils::to_json_bytes(&manifest)?),
                &version,
            )
            .await
            .map_err(|error| map_storage_error(&self.index_id, error))?;
        if (manifest.wal.len() >= FOLD_WAL_THRESHOLD || manifest.wal_ops >= FOLD_WAL_THRESHOLD_OPS)
            && let Err(error) = self.fold(storage, stripe).await
        {
            // The publish is durable; folding is maintenance and the next publish retries it.
            super::metrics::MANIFEST_FOLD_FAILURES_TOTAL.inc();
            warn!(
                index_id = %self.index_id,
                stripe,
                %error,
                "failed to fold a stripe, the next write to it will retry"
            );
        }
        Ok(())
    }

    /// Folds the WAL tail of one stripe into one segment per time bucket, then commits once.
    pub(crate) async fn fold(&self, storage: &dyn Storage, stripe: usize) -> MetastoreResult<bool> {
        let (mut manifest, version) = self.read_manifest(storage, stripe).await?;
        if manifest.wal.is_empty() {
            return Ok(false);
        }
        let mut per_bucket: BTreeMap<i64, Vec<SplitOp>> = BTreeMap::new();
        // Segments already read while placing removals, so a fold does not read the same one twice.
        let mut read_segments: BTreeMap<i64, Vec<Split>> = BTreeMap::new();
        for wal_key in manifest.wal.clone() {
            let bytes = storage
                .get_all(Path::new(&wal_key))
                .await
                .map_err(|error| map_storage_error(&self.index_id, error))?;
            let wal: WalBatch = serde_utils::from_json_bytes(&bytes)?;
            for op in wal.ops {
                let bucket = match &op.split {
                    Some(split) => self.bucket_of(split),
                    // A removal goes to the bucket holding the split it removes. That split is
                    // usually older than this fold's WAL tail — deleting a split a merge replaced,
                    // after it was folded into a segment, is the routine path — so the tail is only
                    // the first place to look; the segments of this stripe are the second. Both
                    // places are on the split's own stripe, because the stripe is a function of the
                    // id. A removal that is in neither is a corrupt state.
                    None => {
                        let mut bucket = self.bucket_of_split_id(&per_bucket, &op.split_id);
                        if bucket.is_none() {
                            bucket = self
                                .bucket_of_folded_split(
                                    storage,
                                    &manifest,
                                    &op.split_id,
                                    &mut read_segments,
                                )
                                .await?;
                        }
                        match bucket {
                            Some(bucket) => bucket,
                            None => {
                                return Err(internal(
                                    "a removal refers to a split neither the tail nor a segment \
                                     of its stripe holds",
                                    format!("split `{}`", op.split_id),
                                ));
                            }
                        }
                    }
                };
                per_bucket.entry(bucket).or_default().push(op);
            }
        }
        for (bucket, ops) in per_bucket {
            let mut splits = match read_segments.remove(&bucket) {
                Some(splits) => splits,
                None => match manifest.segments.get(&bucket) {
                    Some(segment_ref) => self.read_segment(storage, &segment_ref.key).await?.splits,
                    None => Vec::new(),
                },
            };
            apply_ops(&mut splits, ops);
            splits.sort_unstable_by(|left, right| left.split_id().cmp(right.split_id()));
            let segment_ref = SegmentRef {
                key: String::new(),
                min_split_id: splits
                    .first()
                    .map(|split| split.split_id().clone())
                    .unwrap_or_else(|| SplitId::from("")),
                max_split_id: splits
                    .last()
                    .map(|split| split.split_id().clone())
                    .unwrap_or_else(|| SplitId::from("")),
                min_time_range_start: splits
                    .iter()
                    .filter_map(|split| split.split_metadata.time_range.as_ref())
                    .map(|range| *range.start())
                    .min(),
                max_time_range_end: splits
                    .iter()
                    .filter_map(|split| split.split_metadata.time_range.as_ref())
                    .map(|range| *range.end())
                    .max(),
                has_untimed_splits: splits
                    .iter()
                    .any(|split| split.split_metadata.time_range.is_none()),
                num_splits: splits.len(),
            };
            let object_id = Uuid::new_v4().to_string();
            let segment_key =
                self.segment_path(stripe, bucket, manifest.fold_generation + 1, &object_id);
            let segment = SplitSegment {
                format_version: MANIFEST_LAYOUT_FORMAT_VERSION,
                bucket,
                splits,
            };
            storage
                .put(
                    &segment_key,
                    Box::new(serde_utils::to_json_bytes(&segment)?),
                )
                .await
                .map_err(|error| map_storage_error(&self.index_id, error))?;
            manifest.segments.insert(
                bucket,
                SegmentRef {
                    key: segment_key.to_string_lossy().to_string(),
                    ..segment_ref
                },
            );
        }
        manifest.wal.clear();
        manifest.wal_ops = 0;
        manifest.epoch += 1;
        manifest.fold_generation += 1;
        let commit_epoch = manifest.fold_generation;
        storage
            .put_if_version_matches(
                &self.manifest_path(stripe),
                Box::new(serde_utils::to_json_bytes(&manifest)?),
                &version,
            )
            .await
            .map_err(|error| map_storage_error(&self.index_id, error))?;
        super::metrics::MANIFEST_FOLDS_TOTAL.inc();
        self.garbage_collect(storage, stripe, commit_epoch).await;
        self.garbage_collect_wal(storage, stripe, commit_epoch)
            .await;
        Ok(true)
    }

    /// Deletes segments no reader can still need: those the manifests no longer name and whose
    /// generation is older than the grace period.
    ///
    /// One stripe at a time, and against that stripe's own generation: its segments are in its own
    /// directory, its manifest is the only thing that names them, and the reader the grace protects
    /// is the one holding an older manifest *of this stripe*.
    async fn garbage_collect(&self, storage: &dyn Storage, stripe: usize, epoch: u64) {
        let Some(delete_before) = epoch.checked_sub(SEGMENT_GRACE_GENERATIONS) else {
            return;
        };
        let live = match self.live_segments(storage, stripe).await {
            Ok(live) => live,
            Err(error) => {
                warn!(index_id = %self.index_id, %error, "failed to list live segments for gc");
                return;
            }
        };
        let mut pages = storage.list(&self.segment_stripe_prefix(stripe));
        loop {
            let page = match pages.try_next().await {
                Ok(Some(page)) => page,
                Ok(None) => break,
                Err(error) => {
                    warn!(index_id = %self.index_id, %error, "failed to list segments for gc");
                    return;
                }
            };
            for metadata in page {
                let path = metadata.path.to_string_lossy().to_string();
                if live.contains(&path) {
                    continue;
                }
                let Some(generation) = Path::new(&path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.split_once('-'))
                    .and_then(|(generation, _)| generation.parse::<u64>().ok())
                else {
                    continue;
                };
                if generation > delete_before {
                    continue;
                }
                if let Err(error) = storage.delete(Path::new(&path)).await {
                    warn!(index_id = %self.index_id, path = %path, %error, "failed to delete a superseded segment");
                }
            }
        }
    }

    /// Deletes the WAL objects a fold has taken over, once no manifest that could still be read
    /// names them.
    ///
    /// A fold removes those references in the same compare-and-swap that advances the epoch, so an
    /// object is collectible as soon as a manifest two generations later exists — the same grace
    /// the segments get, and for the same reason: a reader holding an older manifest may still
    /// be fetching what it names.
    async fn garbage_collect_wal(&self, storage: &dyn Storage, stripe: usize, epoch: u64) {
        // The WAL objects of a stripe are in that stripe's own directory and are named by its own
        // fold generation, so they are collected against it, exactly like its segments.
        let Some(delete_before) = epoch.checked_sub(SEGMENT_GRACE_GENERATIONS) else {
            return;
        };
        let prefix = self.wal_prefix(stripe);
        let mut pages = storage.list(&prefix);
        loop {
            let page = match pages.try_next().await {
                Ok(Some(page)) => page,
                Ok(None) => break,
                Err(error) => {
                    warn!(index_id = %self.index_id, stripe, %error, "failed to list wal objects for gc");
                    return;
                }
            };
            for metadata in page {
                let path = metadata.path.to_string_lossy().to_string();
                let Some(generation) = Path::new(&path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.split_once('-'))
                    .and_then(|(generation, _)| generation.parse::<u64>().ok())
                else {
                    continue;
                };
                if generation > delete_before {
                    continue;
                }
                if let Err(error) = storage.delete(Path::new(&path)).await {
                    warn!(index_id = %self.index_id, stripe, path = %path, %error, "failed to delete a folded wal object");
                }
            }
        }
    }

    /// Segments the manifest of this stripe names.
    async fn live_segments(
        &self,
        storage: &dyn Storage,
        stripe: usize,
    ) -> MetastoreResult<BTreeSet<String>> {
        let mut live = BTreeSet::new();
        let (manifest, _) = self.read_manifest(storage, stripe).await?;
        live.extend(
            manifest
                .segments
                .values()
                .map(|segment| segment.key.clone()),
        );
        // A fold that has written its segments but not yet committed them leaves them
        // unreferenced; they are younger than the grace period and survive.
        Ok(live)
    }

    async fn read_segment(
        &self,
        storage: &dyn Storage,
        segment_key: &str,
    ) -> MetastoreResult<SplitSegment> {
        let bytes = storage
            .get_all(Path::new(segment_key))
            .await
            .map_err(|error| map_storage_error(&self.index_id, error))?;
        let segment: SplitSegment = serde_utils::from_json_bytes(&bytes)?;
        if segment.format_version != MANIFEST_LAYOUT_FORMAT_VERSION {
            return Err(internal(
                "unknown split segment version",
                format!("`{segment_key}` says `{}`", segment.format_version),
            ));
        }
        Ok(segment)
    }

    /// Splits whose time range overlaps `[from, to)`, read from the segments that can contain them
    /// plus the WAL tail. Buckets outside the window are never fetched.
    ///
    /// Retries when a stripe's manifest moves under it, so a caller never sees a mixed epoch.
    pub(crate) async fn list_splits(
        &self,
        storage: &dyn Storage,
        from: i64,
        to: i64,
    ) -> MetastoreResult<Vec<Split>> {
        if to <= from {
            // An empty window: `to - 1` below would underflow, and there is nothing to return.
            return Ok(Vec::new());
        }
        let mut splits = BTreeMap::new();
        // One round trip per stripe is the floor of this layout, so they are fetched together:
        // eight manifests in sequence turn a windowed read into eight times the bucket's
        // round trip.
        let manifests = futures::future::try_join_all(
            (0..self.num_stripes).map(|stripe| self.read_manifest(storage, stripe)),
        )
        .await?;
        // Everything the manifests name is fetched in parallel too: sequentially, a windowed read
        // would cost one bucket round trip per object it touches.
        let mut segment_keys: Vec<String> = Vec::new();
        let mut wal_keys: Vec<String> = Vec::new();
        for (manifest, _) in &manifests {
            for segment_ref in manifest.segments.values() {
                if segment_overlaps_window(segment_ref, from, to)
                    && !segment_keys.contains(&segment_ref.key)
                {
                    segment_keys.push(segment_ref.key.clone());
                }
            }
            wal_keys.extend(manifest.wal.iter().cloned());
        }
        let segments = futures::future::try_join_all(
            segment_keys
                .iter()
                .map(|segment_key| self.read_segment(storage, segment_key)),
        )
        .await?;
        for segment in segments {
            for split in segment.splits {
                splits.insert(split.split_id().clone(), split);
            }
        }
        // The WAL tail is applied in the order the manifests name it: it holds the newest version
        // of everything published since the last fold, and a removal has to win over an
        // older add.
        let wal_batches =
            futures::future::try_join_all(wal_keys.iter().map(|wal_key| async move {
                let bytes = storage
                    .get_all(Path::new(wal_key))
                    .await
                    .map_err(|error| map_storage_error(&self.index_id, error))?;
                let wal: WalBatch = serde_utils::from_json_bytes(&bytes)?;
                Ok::<_, MetastoreError>(wal)
            }))
            .await?;
        for wal in wal_batches {
            for op in wal.ops {
                apply_op(&mut splits, op);
            }
        }
        Ok(splits
            .into_values()
            .filter(|split| split_overlaps(split, from, to))
            .collect())
    }

    /// Bucket of a split id already seen in this fold, used to place a removal.
    fn bucket_of_split_id(
        &self,
        per_bucket: &BTreeMap<i64, Vec<SplitOp>>,
        split_id: &SplitId,
    ) -> Option<i64> {
        for (bucket, ops) in per_bucket {
            if ops
                .iter()
                .any(|op| op.split_id == *split_id && op.split.is_some())
            {
                return Some(*bucket);
            }
        }
        None
    }

    /// Bucket of a split that a *previous* fold put into a segment.
    ///
    /// The segment references carry the split-id range they cover, so this reads only the segments
    /// that can hold the id, and only when the segment confirms it does it answer with the bucket.
    async fn bucket_of_folded_split(
        &self,
        storage: &dyn Storage,
        manifest: &StripeManifest,
        split_id: &SplitId,
        read_segments: &mut BTreeMap<i64, Vec<Split>>,
    ) -> MetastoreResult<Option<i64>> {
        for (bucket, segment_ref) in &manifest.segments {
            if split_id < &segment_ref.min_split_id || split_id > &segment_ref.max_split_id {
                continue;
            }
            if !read_segments.contains_key(bucket) {
                let segment = self.read_segment(storage, &segment_ref.key).await?;
                read_segments.insert(*bucket, segment.splits);
            }
            if read_segments[bucket]
                .iter()
                .any(|split| split.split_id() == split_id)
            {
                return Ok(Some(*bucket));
            }
        }
        Ok(None)
    }
}

fn apply_ops(splits: &mut Vec<Split>, ops: Vec<SplitOp>) {
    let mut current: BTreeMap<SplitId, Split> = splits
        .drain(..)
        .map(|split| (split.split_id().clone(), split))
        .collect();
    for op in ops {
        match op.split {
            Some(split) => {
                current.insert(op.split_id, split);
            }
            None => {
                current.remove(&op.split_id);
            }
        }
    }
    splits.extend(current.into_values());
}

fn apply_op(splits: &mut BTreeMap<SplitId, Split>, op: SplitOp) {
    match op.split {
        Some(split) => {
            splits.insert(op.split_id, split);
        }
        None => {
            splits.remove(&op.split_id);
        }
    }
}

/// Whether a segment can hold a split the window `[from, to)` keeps.
///
/// A segment with no time range at all (every split untimed) always qualifies, because the
/// metastore's predicate keeps untimed splits for every window.
fn segment_overlaps_window(segment: &SegmentRef, from: i64, to: i64) -> bool {
    if segment.has_untimed_splits {
        return true;
    }
    let (Some(min_start), Some(max_end)) =
        (segment.min_time_range_start, segment.max_time_range_end)
    else {
        // A segment with neither a span nor untimed splits holds nothing.
        return false;
    };
    max_end >= from && min_start < to
}

fn split_overlaps(split: &Split, from: i64, to: i64) -> bool {
    let Some(time_range) = &split.split_metadata.time_range else {
        return true;
    };
    *time_range.end() >= from && *time_range.start() < to
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_storage::RamStorage;

    use super::*;
    use crate::{IndexMetadata, SplitMetadata, SplitState};

    const BUCKET_SECS: i64 = 3_600;

    fn layout() -> ManifestLayout {
        ManifestLayout::new("test-index", BUCKET_SECS, 2)
    }

    fn split(split_id: &str, time_range: Option<std::ops::RangeInclusive<i64>>) -> Split {
        Split {
            split_state: SplitState::Published,
            update_timestamp: 0,
            publish_timestamp: None,
            split_metadata: SplitMetadata {
                time_range,
                ..SplitMetadata::for_test(SplitId::from(split_id))
            },
        }
    }

    async fn publish_and_fold(layout: &ManifestLayout, storage: &dyn Storage, splits: Vec<Split>) {
        let ops: Vec<SplitOp> = splits
            .into_iter()
            .map(|split| SplitOp {
                split_id: split.split_id().clone(),
                split: Some(split),
            })
            .collect();
        layout.publish_ops(storage, ops).await.unwrap();
        for stripe in 0..layout.num_stripes {
            layout.fold(storage, stripe).await.unwrap();
        }
    }

    /// An index with one ingest-v2 source and one open shard in it.
    fn index_with_one_shard() -> (FileBackedIndex, SourceId, ShardId) {
        use quickwit_config::{SourceConfig, SourceParams};
        use quickwit_proto::ingest::ShardState;
        use quickwit_proto::types::Position;

        const SOURCE_ID: &str = "ingest-v2";
        let mut index_metadata = IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
        index_metadata.sources.insert(
            SOURCE_ID.to_string(),
            SourceConfig::for_test(SOURCE_ID, SourceParams::Ingest),
        );
        let mut index = FileBackedIndex::from(index_metadata);
        let source_id = SOURCE_ID.to_string();
        let shard_id = ShardId::from("01J0SHARD");
        let shard = Shard {
            index_uid: index.index_uid().clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            shard_state: ShardState::Open as i32,
            publish_position_inclusive: Some(Position::Beginning),
            ..Default::default()
        };
        index.put_shard_objects(&BTreeMap::from([(
            source_id.clone(),
            BTreeMap::from([(shard_id.clone(), shard)]),
        )]));
        (index, source_id, shard_id)
    }

    /// The state of a shard lives in its own object: changing it writes that object and leaves the
    /// root's bytes alone. That is what stops the nodes publishing different shards of a source
    /// from contending on one object.
    #[tokio::test]
    async fn test_changing_a_shard_writes_its_object_and_not_the_root() {
        use quickwit_proto::types::Position;

        let layout = layout();
        let storage = RamStorage::default();
        let (index, source_id, shard_id) = index_with_one_shard();
        layout.create_index(&storage, &index).await.unwrap();

        let root_bytes = storage.get_all(&layout.root_path()).await.unwrap();
        let root = String::from_utf8(root_bytes.to_vec()).unwrap();
        assert!(
            !root.contains("\"shards\""),
            "the root must not carry the shard state: {root}"
        );
        assert!(
            storage
                .exists(&layout.shard_path(&source_id, &shard_id))
                .await
                .unwrap(),
            "the shard has an object of its own"
        );

        // Move the shard's publish position, the change a publish makes.
        let (root_info, version, previous_root_bytes) = layout.load_root(&storage).await.unwrap();
        let loaded = root_info.shard_objects.clone();
        let mut index = root_info.index;
        let mut shard = index.shard_states()[0].2.clone();
        shard.publish_position_inclusive = Some(Position::offset(7u64));
        index.put_shard_objects(&BTreeMap::from([(
            source_id.clone(),
            BTreeMap::from([(shard_id.clone(), shard)]),
        )]));
        layout
            .store_root(
                &storage,
                &mut index,
                &loaded,
                &previous_root_bytes,
                &version,
            )
            .await
            .unwrap();

        let root_after = storage.get_all(&layout.root_path()).await.unwrap();
        assert_eq!(
            root_after.as_slice(),
            previous_root_bytes.as_slice(),
            "a publish must not rewrite the root"
        );
        let (reloaded, _, _) = layout.load_root(&storage).await.unwrap();
        assert_eq!(
            reloaded.index.shard_states()[0]
                .2
                .publish_position_inclusive,
            Some(Position::offset(7u64)),
            "the object is what carries the new state"
        );
    }

    /// A root written before the change still carries its shards inline. Reading takes them as the
    /// base, and the next write moves every shard it did not touch into an object of its own, so
    /// nothing is lost when the root stops carrying them.
    #[tokio::test]
    async fn test_a_root_with_inline_shards_is_migrated_by_the_next_write() {
        use quickwit_proto::types::Position;

        let layout = layout();
        let storage = RamStorage::default();
        let (index, source_id, shard_id) = index_with_one_shard();
        // Write the root the way a node running the old code did: the shards are inside it.
        let index_without_splits = index.clone();
        let root = ManifestRoot {
            format_version: MANIFEST_LAYOUT_FORMAT_VERSION,
            bucket_secs: layout.bucket_secs,
            num_stripes: layout.num_stripes,
            index: index_without_splits,
        };
        let root_bytes = serde_utils::to_json_bytes_pretty(&root).unwrap();
        storage
            .put(&layout.root_path(), Box::new(root_bytes))
            .await
            .unwrap();
        let (root_info, version, previous_root_bytes) = layout.load_root(&storage).await.unwrap();
        assert_eq!(
            root_info.shard_objects.len(),
            0,
            "the old root has no shard objects yet"
        );
        assert_eq!(
            root_info.index.shard_states().len(),
            1,
            "the inline copy is the base"
        );
        let loaded = root_info.shard_objects.clone();
        let mut index = root_info.index;
        // Touch nothing: even so, the write has to materialise the inline shard, or the root would
        // drop a state that no object carries.
        layout
            .store_root(
                &storage,
                &mut index,
                &loaded,
                &previous_root_bytes,
                &version,
            )
            .await
            .unwrap();
        assert!(
            storage
                .exists(&layout.shard_path(&source_id, &shard_id))
                .await
                .unwrap(),
            "the inline shard moved into its own object"
        );
        let root_after = storage.get_all(&layout.root_path()).await.unwrap();
        let root_after = String::from_utf8(root_after.to_vec()).unwrap();
        assert!(
            !root_after.contains("\"shards\""),
            "the migrated root no longer carries the shard state: {root_after}"
        );
        let (reloaded, _, _) = layout.load_root(&storage).await.unwrap();
        assert_eq!(reloaded.index.shard_states().len(), 1);
        assert_eq!(
            reloaded.index.shard_states()[0]
                .2
                .publish_position_inclusive,
            Some(Position::Beginning)
        );
    }

    /// A split whose range starts in one bucket and reaches into the next must be returned by a
    /// query that only covers the next one. This is the case a bucket-based pruning got wrong: it
    /// dropped a split that the metastore's own predicate keeps.
    #[tokio::test]
    async fn test_a_split_reaching_into_the_next_window_is_not_pruned_away() {
        let layout = layout();
        let storage = RamStorage::default();
        layout.create(&storage).await.unwrap();
        // Start at 1800 (bucket 0), end at 5400 (bucket 1).
        publish_and_fold(
            &layout,
            &storage,
            vec![split("split-a", Some(1_800..=5_400))],
        )
        .await;

        let next_window = layout.list_splits(&storage, 3_600, 7_200).await.unwrap();
        assert_eq!(
            next_window.len(),
            1,
            "a split overlapping the window was pruned away"
        );
        let previous_window = layout.list_splits(&storage, 0, 3_600).await.unwrap();
        assert_eq!(previous_window.len(), 1);
        let far_window = layout
            .list_splits(&storage, 100_000, 103_600)
            .await
            .unwrap();
        assert!(
            far_window.is_empty(),
            "a window that cannot overlap must prune"
        );
    }

    #[tokio::test]
    async fn test_a_split_without_a_time_range_is_never_pruned() {
        let layout = layout();
        let storage = RamStorage::default();
        layout.create(&storage).await.unwrap();
        publish_and_fold(&layout, &storage, vec![split("split-untimed", None)]).await;

        for window in [(0, 3_600), (1_700_000_000, 1_700_003_600)] {
            let splits = layout
                .list_splits(&storage, window.0, window.1)
                .await
                .unwrap();
            assert_eq!(
                splits.len(),
                1,
                "window {window:?} dropped an untimed split"
            );
        }
    }

    /// Folding has to collect the WAL objects it took over, or the layout grows with the number of
    /// writes instead of with the number of splits.
    #[tokio::test]
    async fn test_folding_collects_the_wal_objects_it_took_over() {
        let layout = layout();
        let storage = RamStorage::default();
        layout.create(&storage).await.unwrap();
        let mut num_wal_objects = 0;
        for round in 0..4 {
            let splits: Vec<Split> = (0..3)
                .map(|index| {
                    split(
                        &format!("split-{round}-{index}"),
                        Some(1_700_000_000..=1_700_000_060),
                    )
                })
                .collect();
            publish_and_fold(&layout, &storage, splits).await;
            num_wal_objects += count_objects(&storage, "wal-000").await;
        }
        // Four folds, each collecting what the fold two generations back took over: what is left is
        // the most recent generation and the one before it, never everything ever written. The
        // bound is exact rather than generous on purpose — four objects are written, so an
        // assertion like `remaining <= 4` would hold with the collection switched off altogether.
        let remaining = count_objects(&storage, "wal-000").await;
        assert_eq!(
            remaining, 2,
            "wal objects are piling up: {remaining} left after {num_wal_objects} published"
        );
        let splits = layout
            .list_splits(&storage, 1_699_999_000, 1_700_010_000)
            .await
            .unwrap();
        assert_eq!(
            splits.len(),
            12,
            "the splits themselves must survive the collection"
        );
    }

    /// A fold collects the WAL objects it took over, and the grace that decides when is a number of
    /// *folds*.
    ///
    /// The WAL objects of a stripe have to be named in the same unit as the collection compares
    /// them against. Named after the commit counter instead — a stripe commits once per publish and
    /// folds once per 32 of them — they never reach a watermark counted in folds, so every fold
    /// clears the manifest's list while leaving all the objects behind and the layout grows with
    /// its writes again.
    #[tokio::test]
    async fn test_folding_collects_wal_objects_when_publishes_outnumber_folds() {
        let layout = ManifestLayout::new("test-index", BUCKET_SECS, 1);
        let storage = RamStorage::default();
        layout.create(&storage).await.unwrap();
        // 40 publishes per round: the 32nd folds the stripe, so the rounds are folds that each took
        // over far more objects than they left.
        const ROUNDS: usize = 4;
        const BATCHES_PER_ROUND: usize = 40;
        for round in 0..ROUNDS {
            for batch in 0..BATCHES_PER_ROUND {
                let split = split(
                    &format!("split-{round}-{batch}"),
                    Some(1_700_000_000..=1_700_000_060),
                );
                layout
                    .publish_ops(
                        &storage,
                        vec![SplitOp {
                            split_id: split.split_id().clone(),
                            split: Some(split),
                        }],
                    )
                    .await
                    .unwrap();
            }
        }
        let remaining = count_objects(&storage, "wal-000").await;
        assert!(
            remaining <= 3 * BATCHES_PER_ROUND,
            "wal objects are piling up: {remaining} left after {} publishes and {ROUNDS} folds",
            ROUNDS * BATCHES_PER_ROUND
        );
        let splits = layout
            .list_splits(&storage, 1_699_999_000, 1_700_010_000)
            .await
            .unwrap();
        assert_eq!(
            splits.len(),
            ROUNDS * BATCHES_PER_ROUND,
            "the splits themselves must survive the collection"
        );
    }

    /// A stripe that folds often must not collect what a stripe that folds rarely may still have
    /// readers fetching.
    ///
    /// A stripe's segments are written, named and collected by that stripe: they are in the
    /// stripe's own directory, and its own generation is what the grace counts. A watermark taken
    /// across stripes, or the folding stripe's generation applied to every stripe's directory,
    /// deletes a slow stripe's segments — which its readers, holding a manifest that names them,
    /// are still fetching.
    #[tokio::test]
    async fn test_a_fast_stripe_does_not_collect_a_slow_stripe() {
        let layout = layout();
        let storage = RamStorage::default();
        layout.create(&storage).await.unwrap();
        // One split per stripe, found by hashing candidates, so both stripes have something to
        // fold.
        let split_for_stripe = |stripe: usize| {
            (0..)
                .map(|candidate| {
                    split(
                        &format!("split-{candidate}"),
                        Some(1_700_000_000..=1_700_000_060),
                    )
                })
                .find(|split| layout.stripe_of(split.split_id().as_str()) == stripe)
                .unwrap()
        };
        let publish_and_fold = |split: Split| {
            let layout = layout.clone();
            let storage = storage.clone();
            async move {
                let stripe = layout.stripe_of(split.split_id().as_str());
                let ops = vec![SplitOp {
                    split_id: split.split_id().clone(),
                    split: Some(split),
                }];
                layout.publish_ops(&storage, ops).await.unwrap();
                layout.fold(&storage, stripe).await.unwrap();
            }
        };
        // The slow stripe folds twice; the fast one six times.
        for _ in 0..2 {
            publish_and_fold(split_for_stripe(1)).await;
        }
        for _ in 0..6 {
            publish_and_fold(split_for_stripe(0)).await;
        }
        let segments = count_objects(&storage, "segments").await;
        assert_eq!(
            segments, 4,
            "each stripe collects its own superseded segments and keeps two generations"
        );
        // The state that actually matters, and the one a shared watermark breaks: what a manifest
        // names has to be there.
        for stripe in 0..layout.num_stripes {
            let (manifest, _) = layout.read_manifest(&storage, stripe).await.unwrap();
            for segment in manifest.segments.values() {
                if let Err(error) = storage.get_all(Path::new(&segment.key)).await {
                    panic!(
                        "stripe {stripe} names a segment that was collected: {} ({error})",
                        segment.key
                    );
                }
            }
        }
    }

    /// Collection must not depend on every stripe folding.
    ///
    /// `num_stripes` is 32 by default and a small index publishes into a handful of them: the rest
    /// never fold and keep generation 0. A watermark taken across stripes is then 0, the collection
    /// never runs, and every fold of a busy stripe leaves a segment behind that nothing will ever
    /// delete — the index grows with its writes again, which is what this layout exists to stop.
    #[tokio::test]
    async fn test_a_stripe_that_never_folds_does_not_stop_the_collection() {
        let layout = layout();
        let storage = RamStorage::default();
        layout.create(&storage).await.unwrap();
        // Only stripe 0 is written to; stripe 1 stays at its initial generation.
        for round in 0..6 {
            let split = (0..)
                .map(|candidate| {
                    split(
                        &format!("split-{round}-{candidate}"),
                        Some(1_700_000_000..=1_700_000_060),
                    )
                })
                .find(|split| layout.stripe_of(split.split_id().as_str()) == 0)
                .unwrap();
            let ops = vec![SplitOp {
                split_id: split.split_id().clone(),
                split: Some(split),
            }];
            layout.publish_ops(&storage, ops).await.unwrap();
            layout.fold(&storage, 0).await.unwrap();
        }
        assert_eq!(
            count_objects(&storage, "segments").await,
            2,
            "six folds of one stripe must leave two generations, not six segments"
        );
        let splits = layout
            .list_splits(&storage, 1_699_999_000, 1_700_010_000)
            .await
            .unwrap();
        assert_eq!(
            splits.len(),
            6,
            "the splits themselves must survive the collection"
        );
    }

    /// Objects under a prefix of this test's storage, for the tests that count them.
    ///
    /// The prefix has to name a directory of the layout exactly: `RamStorage::list` matches whole
    /// path components, so `"wal"` counts nothing of `wal-000/` — which is how a test that meant to
    /// assert the WAL was collected passed without looking at anything.
    async fn count_objects(storage: &RamStorage, prefix: &str) -> usize {
        let mut pages = storage.list(Path::new(&format!("test-index/v3/{prefix}")));
        let mut count = 0;
        while let Some(page) = pages.try_next().await.unwrap() {
            count += page.len();
        }
        count
    }

    /// A read of a window must not fetch the segments of other windows.
    #[tokio::test]
    async fn test_a_windowed_read_does_not_read_other_segments() {
        let layout = layout();
        let (storage, counters) =
            quickwit_storage::CountingStorage::instrument_storage(Arc::new(RamStorage::default()));
        layout.create(&*storage).await.unwrap();
        for hour in 0..24 {
            let start = 1_700_000_000 + hour * BUCKET_SECS;
            publish_and_fold(
                &layout,
                &*storage,
                vec![split(&format!("split-{hour}"), Some(start..=start + 60))],
            )
            .await;
        }
        let (bytes_before, _) = counters.snapshot();
        let splits = layout
            .list_splits(&*storage, 1_700_000_000, 1_700_000_000 + BUCKET_SECS)
            .await
            .unwrap();
        assert_eq!(splits.len(), 1);
        let (bytes_read, _) = counters.snapshot();
        let bytes_read = bytes_read - bytes_before;
        assert!(
            bytes_read < 4_000,
            "a one-hour window read {bytes_read} bytes out of 24 hours of segments"
        );
    }

    /// Removing a split a fold has already taken into a segment is the routine path after a merge:
    /// the split is in a segment, its removal is in the tail, and the fold that follows has to
    /// place that removal. Getting this wrong is not a lost delete, it is a stripe whose every
    /// later fold fails on the same op, with its tail growing forever.
    #[tokio::test]
    async fn test_a_removal_of_a_folded_split_folds() {
        let layout = layout();
        let storage = RamStorage::default();
        layout.create(&storage).await.unwrap();
        let split_a = split("split-a", Some(1_700_000_000..=1_700_000_060));
        let split_b = split("split-b", Some(1_700_000_000..=1_700_000_060));
        publish_and_fold(&layout, &storage, vec![split_a.clone(), split_b.clone()]).await;
        // Both are in a segment now; publish the removal of one of them, as a merge would.
        layout
            .publish_ops(
                &storage,
                vec![SplitOp {
                    split_id: split_a.split_id().clone(),
                    split: None,
                }],
            )
            .await
            .unwrap();
        let stripe = layout.stripe_of(split_a.split_id().as_str());
        assert!(
            layout.fold(&storage, stripe).await.unwrap(),
            "the fold that places the removal has to succeed"
        );
        // And again, with nothing in the tail: the stripe must not be stuck on that op.
        assert!(!layout.fold(&storage, stripe).await.unwrap());
        let splits = layout
            .list_splits(&storage, 1_699_999_000, 1_700_010_000)
            .await
            .unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].split_id().as_str(), "split-b");
    }

    /// The negative control: a removal for a split that was never published is a corrupt state, and
    /// saying so is what keeps the routine case above from being a silent no-op.
    #[tokio::test]
    async fn test_a_removal_of_an_unknown_split_is_an_error() {
        let layout = layout();
        let storage = RamStorage::default();
        layout.create(&storage).await.unwrap();
        publish_and_fold(&layout, &storage, vec![split("split-a", None)]).await;
        layout
            .publish_ops(
                &storage,
                vec![SplitOp {
                    split_id: SplitId::from("split-never-existed"),
                    split: None,
                }],
            )
            .await
            .unwrap();
        let stripe = layout.stripe_of("split-never-existed");
        let error = layout.fold(&storage, stripe).await.unwrap_err();
        assert!(
            error.to_string().contains("neither the tail nor a segment"),
            "unexpected error: {error}"
        );
    }
}
