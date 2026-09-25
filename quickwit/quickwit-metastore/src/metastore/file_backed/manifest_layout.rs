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
//! <index_id>/v3/wal-<stripe:03>/<uuid>.json    immutable, one object per published batch
//! <index_id>/v3/segments/<bucket:012>/<epoch:020>-<uuid>.json   immutable, one per time bucket
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
//! 2. A segment is immutable and is named after the epoch that introduced it, so garbage collection
//!    is a pure function of the name and the manifests (never of a wall clock).
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
use quickwit_proto::metastore::{MetastoreError, MetastoreResult, serde_utils};
use quickwit_proto::types::SplitId;
use quickwit_storage::{ObjectVersion, OwnedBytes, Storage};
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

use super::file_backed_index::FileBackedIndex;
use crate::Split;

/// Version of the objects written by this layout. A reader refuses what it does not know.
pub(crate) const MANIFEST_LAYOUT_FORMAT_VERSION: u32 = 1;

/// Generations of segments kept after a fold, so a reader that lost a race can still finish.
const SEGMENT_GRACE_GENERATIONS: u64 = 2;

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
}

/// What the root says about the index: its non-split state and the layout parameters.
#[derive(Clone, Debug)]
pub(crate) struct ManifestRootInfo {
    pub index: FileBackedIndex,
    pub bucket_secs: i64,
    pub num_stripes: usize,
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

    fn wal_path(&self, stripe: usize, epoch: u64, object_id: &str) -> PathBuf {
        self.wal_prefix(stripe)
            .join(format!("{epoch:020}-{object_id}.json"))
    }

    fn wal_prefix(&self, stripe: usize) -> PathBuf {
        self.prefix().join(format!("wal-{stripe:03}"))
    }

    fn segment_path(&self, bucket: i64, epoch: u64, object_id: &str) -> PathBuf {
        self.prefix()
            .join(format!("segments/{bucket:012}"))
            .join(format!("{epoch:020}-{object_id}.json"))
    }

    fn segments_prefix(&self) -> PathBuf {
        self.prefix().join("segments")
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
    pub(crate) async fn create(&self, storage: &dyn Storage) -> MetastoreResult<()> {
        for stripe in 0..self.num_stripes {
            let body = serde_utils::to_json_bytes(&StripeManifest::new(self.bucket_secs))?;
            storage
                .put_if_absent(&self.manifest_path(stripe), Box::new(body))
                .await
                .map_err(|error| map_storage_error(&self.index_id, error))?;
        }
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
        let splits = index_without_splits.take_splits();
        let root = ManifestRoot {
            format_version: MANIFEST_LAYOUT_FORMAT_VERSION,
            bucket_secs: self.bucket_secs,
            num_stripes: self.num_stripes,
            index: index_without_splits,
        };
        let root_bytes = serde_utils::to_json_bytes_pretty(&root)?;
        let create_result = storage
            .put_if_absent(&self.root_path(), Box::new(root_bytes))
            .await;
        let mut restore_splits = root.index;
        restore_splits.put_splits(splits);
        create_result.map_err(|error| map_storage_error(&self.index_id, error))?;
        self.create(storage).await
    }

    /// Reads the root: the index without its splits, with the version to compare-and-swap against.
    pub(crate) async fn load_root(
        &self,
        storage: &dyn Storage,
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
        let info = ManifestRootInfo {
            index: root.index,
            bucket_secs: root.bucket_secs,
            num_stripes: root.num_stripes,
        };
        Ok((info, version, bytes))
    }

    /// Commits the root if it changed since it was read.
    pub(crate) async fn store_root(
        &self,
        storage: &dyn Storage,
        index: &mut FileBackedIndex,
        previous_root_bytes: &OwnedBytes,
        version: &ObjectVersion,
    ) -> MetastoreResult<()> {
        let splits = index.take_splits();
        let root = ManifestRoot {
            format_version: MANIFEST_LAYOUT_FORMAT_VERSION,
            bucket_secs: self.bucket_secs,
            num_stripes: self.num_stripes,
            index: index.clone(),
        };
        let root_bytes = serde_utils::to_json_bytes_pretty(&root);
        index.put_splits(splits);
        let root_bytes = root_bytes?;
        if root_bytes.as_slice() == previous_root_bytes.as_slice() {
            return Ok(());
        }
        storage
            .put_if_version_matches(&self.root_path(), Box::new(root_bytes), version)
            .await
            .map_err(|error| map_storage_error(&self.index_id, error))?;
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
        // The object is named after the epoch that will reference it, which is what lets the
        // garbage collection of a later fold tell an old WAL object from a live one.
        let wal_epoch = manifest.epoch + 1;
        let num_ops = ops.len();
        let wal = WalBatch {
            format_version: MANIFEST_LAYOUT_FORMAT_VERSION,
            ops,
        };
        storage
            .put(
                &self.wal_path(stripe, wal_epoch, &object_id),
                Box::new(serde_utils::to_json_bytes(&wal)?),
            )
            .await
            .map_err(|error| map_storage_error(&self.index_id, error))?;
        manifest.wal.push(
            self.wal_path(stripe, wal_epoch, &object_id)
                .to_string_lossy()
                .to_string(),
        );
        manifest.wal_ops += num_ops;
        manifest.epoch += 1;
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
        for wal_key in manifest.wal.clone() {
            let bytes = storage
                .get_all(Path::new(&wal_key))
                .await
                .map_err(|error| map_storage_error(&self.index_id, error))?;
            let wal: WalBatch = serde_utils::from_json_bytes(&bytes)?;
            for op in wal.ops {
                let bucket = match &op.split {
                    Some(split) => self.bucket_of(split),
                    // A removal has to go to the bucket of the split it removes; find it below by
                    // looking the id up in the buckets this fold already knows about. Splits whose
                    // bucket is unknown are dropped here and can only come from a segment written
                    // by another stripe, which the id cannot be in (the stripe is a function of the
                    // id), so this is a corrupt-state error rather than a routine case.
                    None => match self.bucket_of_split_id(&per_bucket, &op.split_id) {
                        Some(bucket) => bucket,
                        None => {
                            return Err(internal(
                                "a removal refers to a split this fold cannot place",
                                format!("split `{}`", op.split_id),
                            ));
                        }
                    },
                };
                per_bucket.entry(bucket).or_default().push(op);
            }
        }
        for (bucket, ops) in per_bucket {
            let mut splits = match manifest.segments.get(&bucket) {
                Some(segment_ref) => self.read_segment(storage, &segment_ref.key).await?.splits,
                None => Vec::new(),
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
            let segment_key = self.segment_path(bucket, manifest.epoch + 1, &object_id);
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
        let commit_epoch = manifest.epoch;
        storage
            .put_if_version_matches(
                &self.manifest_path(stripe),
                Box::new(serde_utils::to_json_bytes(&manifest)?),
                &version,
            )
            .await
            .map_err(|error| map_storage_error(&self.index_id, error))?;
        super::metrics::MANIFEST_FOLDS_TOTAL.inc();
        self.garbage_collect(storage, commit_epoch).await;
        self.garbage_collect_wal(storage, stripe, commit_epoch)
            .await;
        Ok(true)
    }

    /// Deletes segments no reader can still need: those the manifests no longer name and whose
    /// generation is older than the grace period.
    async fn garbage_collect(&self, storage: &dyn Storage, epoch: u64) {
        let Some(delete_before) = epoch.checked_sub(SEGMENT_GRACE_GENERATIONS) else {
            return;
        };
        let live: BTreeSet<String> = match self.live_segments(storage).await {
            Ok(live) => live,
            Err(error) => {
                warn!(index_id = %self.index_id, %error, "failed to list live segments for gc");
                return;
            }
        };
        let mut pages = storage.list(&self.segments_prefix());
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

    async fn live_segments(&self, storage: &dyn Storage) -> MetastoreResult<BTreeSet<String>> {
        let mut live = BTreeSet::new();
        for stripe in 0..self.num_stripes {
            let (manifest, _) = self.read_manifest(storage, stripe).await?;
            live.extend(
                manifest
                    .segments
                    .values()
                    .map(|segment| segment.key.clone()),
            );
            // A fold that has written its segments but not yet committed them leaves them
            // unreferenced; they are younger than the grace period and survive.
        }
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
        let mut splits = BTreeMap::new();
        for stripe in 0..self.num_stripes {
            let (manifest, _) = self.read_manifest(storage, stripe).await?;
            for segment_ref in manifest.segments.values() {
                if !segment_overlaps_window(segment_ref, from, to) {
                    continue;
                }
                let segment = self.read_segment(storage, &segment_ref.key).await?;
                for split in segment.splits {
                    splits.insert(split.split_id().clone(), split);
                }
            }
            for wal_key in &manifest.wal {
                let bytes = storage
                    .get_all(Path::new(wal_key))
                    .await
                    .map_err(|error| map_storage_error(&self.index_id, error))?;
                let wal: WalBatch = serde_utils::from_json_bytes(&bytes)?;
                for op in wal.ops {
                    apply_op(&mut splits, op);
                }
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
    use crate::{SplitMetadata, SplitState};

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
            num_wal_objects += count_objects(&storage, "wal").await;
        }
        // Four folds, each collecting the WAL of the fold before it: what is left is the most
        // recent generation (and the one before it, inside the grace), never everything
        // ever written.
        let remaining = count_objects(&storage, "wal").await;
        assert!(
            remaining <= 4,
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
}
