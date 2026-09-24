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

//! Sharded layout for the split metadata of an index.
//!
//! The single-object layout rewrites *every split of the index* on every metadata write. That is
//! acceptable for a small index and impossible for a large one: at 5·10^12 documents/day (500 000
//! splits a day at the default split size) a 30-day index holds ~15 M splits (~12 GB of metadata),
//! and every publish would rewrite all of it. See `docs/internals/metastore-sharded-layout.md` for
//! the arithmetic and the alternatives.
//!
//! This module splits that object along `hash(split_id) % num_slots`:
//!
//! ```text
//! <index_id>/v2/root.json                             # the rest of the index, own CAS
//! <index_id>/v2/splits/view.json                      # per-slot bookmark (folded seq + segment)
//! <index_id>/v2/splits/slots/<slot>.json              # entries written since the last fold
//! <index_id>/v2/splits/segments/<slot>/<gen-id>.json  # folded snapshot of one slot
//! ```
//!
//! Invariants, each of which the code below relies on:
//!
//! 1. A writer only rewrites the slots its mutation touched, plus the root when the non-split part
//!    of the index changed. The bytes it writes are bounded by how many entries a slot accumulates
//!    between two folds, not by the number of splits in the index.
//! 2. Writers racing on different slots both win. Racing on the same slot is settled by an ETag
//!    compare-and-swap on that slot file alone, so contention falls with the number of slots.
//! 3. A segment is a complete snapshot of one slot as of the moment it was folded, so applying the
//!    entries of the slot file that come after the bookmark reconstructs the slot.
//! 4. A fold announces, in the same view write, the highest sequence it folded *and* the version of
//!    the slot file it folded. A reader that lists that same version knows the file holds nothing
//!    but folded entries and skips it without downloading it.
//! 5. A reader that finds a slot file written against a *newer* view generation than the view it
//!    holds must restart: its bookmark could be missing entries that the newer view folded away.
//!    Segments are kept for one extra generation so a reader that loses this race can finish.
//! 6. `slot(split_id)` depends only on the id, so two slots never hold entries for the same split
//!    and reconstruction does not depend on the order in which slots are applied.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use futures::TryStreamExt;
use quickwit_proto::metastore::{EntityKind, MetastoreError, MetastoreResult, serde_utils};
use quickwit_proto::types::SplitId;
use quickwit_storage::{ObjectMetadata, ObjectVersion, OwnedBytes, Storage, StorageErrorKind};
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

use super::file_backed_index::FileBackedIndex;
use super::store_operations::convert_error;
use crate::Split;

/// Layout version written into every object of this layout. A node that does not know the version
/// refuses to interpret the object rather than guess.
pub(super) const SHARDED_FORMAT_VERSION: u32 = 1;

/// Number of slots an index is created with.
///
/// Sizing rule (design note): at least ten slots per concurrent writer, and a slot file in the
/// hundreds of kilobytes between two folds. 256 slots leave a slot file well under a megabyte while
/// an index is written at the rate of the 5·10^12 docs/day scenario, and keep the view object small
/// (one entry per slot that holds data).
pub(super) const DEFAULT_NUM_SLOTS: u32 = 256;

/// A slot is folded into a segment once its file holds this many entries.
///
/// This is what bounds both the bytes a writer rewrites (the point of the layout) and the work of a
/// fold, since entries are only ever appended by writes to that one slot.
pub(super) const SLOT_FOLD_THRESHOLD: usize = 512;

fn default_fold_threshold() -> usize {
    SLOT_FOLD_THRESHOLD
}

/// Generations of segments kept after a fold, so a reader that lost a race can still finish.
const SEGMENT_GRACE_GENERATIONS: u64 = 2;

/// Attempts made by a load that keeps catching a view move mid-read.
const LOAD_MAX_ATTEMPTS: usize = 5;

const ROOT_FILE_NAME: &str = "root.json";
const VIEW_FILE_NAME: &str = "view.json";

pub(super) fn layout_prefix(index_id: &str) -> PathBuf {
    Path::new(index_id).join("v2")
}

pub(super) fn root_filepath(index_id: &str) -> PathBuf {
    layout_prefix(index_id).join(ROOT_FILE_NAME)
}

fn splits_prefix(index_id: &str) -> PathBuf {
    layout_prefix(index_id).join("splits")
}

fn view_filepath(index_id: &str) -> PathBuf {
    splits_prefix(index_id).join(VIEW_FILE_NAME)
}

fn slots_prefix(index_id: &str) -> PathBuf {
    splits_prefix(index_id).join("slots")
}

fn slot_filepath(index_id: &str, slot: u32) -> PathBuf {
    slots_prefix(index_id).join(format!("{slot:05}.json"))
}

fn segments_prefix(index_id: &str, slot: u32) -> PathBuf {
    splits_prefix(index_id)
        .join("segments")
        .join(format!("{slot:05}"))
}

/// Path of a segment. The file name carries the generation it was written for, which is what makes
/// garbage collection a pure function of the name.
fn segment_filepath(index_id: &str, slot: u32, segment_file_name: &str) -> PathBuf {
    segments_prefix(index_id, slot).join(segment_file_name)
}

/// Slot a split belongs to.
///
/// FNV-1a over the split id, spelled out here instead of using `DefaultHasher`: the slot of a split
/// is part of the on-disk format and must not change with the toolchain.
pub(super) fn slot_of(split_id: &str, num_slots: u32) -> u32 {
    debug_assert!(num_slots > 0);
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in split_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    (hash % u64::from(num_slots)) as u32
}

fn internal_error(message: impl Into<String>, cause: impl Into<String>) -> MetastoreError {
    MetastoreError::Internal {
        message: message.into(),
        cause: cause.into(),
    }
}

fn stale_view_error(index_id: &str) -> MetastoreError {
    MetastoreError::FailedPrecondition {
        entity: EntityKind::Index {
            index_id: index_id.to_string(),
        },
        message: "the split view moved ahead while the index was being read".to_string(),
    }
}

fn no_version_error(index_id: &str, path: &Path) -> MetastoreError {
    internal_error(
        "the sharded metastore layout requires a storage that versions objects",
        format!(
            "storage returned no version for `{}` of index `{index_id}`",
            path.display()
        ),
    )
}

/// The index minus its splits, plus the layout parameters needed to find them again.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ShardedRoot {
    format_version: u32,
    num_slots: u32,
    /// The rest of the index, serialized with an empty split map.
    index: FileBackedIndex,
}

/// What is already folded into a segment for one slot.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct SlotBookmark {
    /// Highest slot entry sequence folded into the segment.
    folded_seq: u64,
    /// Version of the slot file at fold time; a reader that lists this version skips the file.
    folded_version: Option<String>,
    /// Segment holding the folded snapshot of this slot.
    segment: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SplitView {
    format_version: u32,
    generation: u64,
    slots: BTreeMap<u32, SlotBookmark>,
    /// Number of entries a slot file may hold before it is folded into a segment.
    ///
    /// Part of the layout, recorded when the index is created: it is the knob that trades the
    /// bytes a write rewrites against the number of objects a read has to fetch.
    #[serde(default = "default_fold_threshold")]
    fold_threshold: usize,
}

impl SplitView {
    fn new(fold_threshold: usize) -> Self {
        Self {
            format_version: SHARDED_FORMAT_VERSION,
            generation: 0,
            slots: BTreeMap::new(),
            fold_threshold,
        }
    }

    fn bookmark(&self, slot: u32) -> SlotBookmark {
        self.slots.get(&slot).cloned().unwrap_or_default()
    }
}

/// One operation on one split: its new value, or its removal.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SlotEntry {
    seq: u64,
    split_id: SplitId,
    /// `None` records that the split is gone.
    split: Option<Split>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct SlotFile {
    /// View generation this file was written against. A file written against a newer generation
    /// than the view a reader holds means the reader's view is stale.
    base_generation: u64,
    next_seq: u64,
    entries: Vec<SlotEntry>,
}

impl SlotFile {
    fn push(&mut self, split_id: SplitId, split: Option<Split>) {
        self.next_seq += 1;
        // Sequences start at 1: 0 is the sequence "before the first entry", which is what a slot
        // that has never been folded is bookmarked with.
        let seq = self.next_seq;
        self.entries.push(SlotEntry {
            seq,
            split_id,
            split,
        });
    }

    /// Sequence of the last entry, or `None` when the file holds no entry.
    fn last_seq(&self) -> Option<u64> {
        self.entries.last().map(|entry| entry.seq)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SplitSegment {
    format_version: u32,
    generation: u64,
    splits: Vec<Split>,
}

/// Everything the caller needs to write an index back in this layout.
///
/// Deliberately small: it must not grow with the number of splits.
#[derive(Clone, Debug)]
pub(super) struct ShardedWriteContext {
    num_slots: u32,
    /// Version of `root.json` when it was read.
    root_version: ObjectVersion,
    /// Bytes of `root.json` as read, used to tell whether the non-split part changed.
    root_bytes: OwnedBytes,
    /// Version of `view.json`, `None` while the view does not exist yet.
    view_version_opt: Option<ObjectVersion>,
    view: SplitView,
    /// Version of every slot file that existed when the index was read, keyed by slot.
    ///
    /// Hidden contract: a write compares-and-swaps against *this* version, not against one read
    /// just before writing. Re-reading would let a mutation computed from a stale snapshot land on
    /// top of a newer one, which is what the compare-and-swap exists to prevent.
    slot_versions: BTreeMap<u32, ObjectVersion>,
    /// Entries of the slot files that were downloaded, that is the ones that changed since the
    /// last fold. A slot that is absent from this map holds nothing but entries a fold has
    /// taken over.
    slot_entries: BTreeMap<u32, Vec<SlotEntry>>,
}

/// Serializes the index without its splits.
///
/// The caller takes the splits out of `index` first, so that this does not have to clone them.
fn serialize_root(index: &FileBackedIndex, num_slots: u32) -> MetastoreResult<Vec<u8>> {
    debug_assert!(index.splits_is_empty());
    let root = ShardedRoot {
        format_version: SHARDED_FORMAT_VERSION,
        num_slots,
        index: index.clone(),
    };
    serde_utils::to_json_bytes_pretty(&root).map_err(|error| {
        internal_error("failed to serialize the index metadata", error.to_string())
    })
}

/// Creates an index in the sharded layout.
///
/// The root is written with `put_if_absent`: two nodes racing to create the same index must not be
/// able to overwrite each other's metadata.
pub(super) async fn create_sharded_index(
    storage: &dyn Storage,
    index: &FileBackedIndex,
    num_slots: u32,
    fold_threshold: usize,
) -> MetastoreResult<()> {
    let index_id = index.index_id();
    if num_slots == 0 {
        return Err(internal_error(
            "the sharded metastore layout needs at least one slot",
            format!("index `{index_id}` was created with `num_slots = 0`"),
        ));
    }
    if fold_threshold == 0 {
        return Err(internal_error(
            "the sharded metastore layout needs a non-zero fold threshold",
            format!("index `{index_id}` was created with `fold_threshold = 0`"),
        ));
    }
    let mut index = index.clone();
    let splits = index.take_splits();
    let root_bytes = serialize_root(&index, num_slots)?;
    index.put_splits(splits);
    storage
        .put_if_absent(&root_filepath(index_id), Box::new(root_bytes))
        .await
        .map_err(|error| convert_error(index_id, error))?;
    let view_bytes = serde_utils::to_json_bytes_pretty(&SplitView::new(fold_threshold))
        .map_err(|error| internal_error("failed to serialize the split view", error.to_string()))?;
    storage
        .put_if_absent(&view_filepath(index_id), Box::new(view_bytes))
        .await
        .map_err(|error| convert_error(index_id, error))?;
    Ok(())
}

/// Loads an index stored in the sharded layout, with the context needed to write it back.
///
/// Retries internally when it catches a view move mid-read: that is a race, not an error the caller
/// can do anything about.
pub(super) async fn load_sharded_index(
    storage: &dyn Storage,
    index_id: &str,
) -> MetastoreResult<(FileBackedIndex, ShardedWriteContext)> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match load_sharded_index_once(storage, index_id).await {
            Err(MetastoreError::FailedPrecondition { .. }) if attempt < LOAD_MAX_ATTEMPTS => {
                continue;
            }
            result => return result,
        }
    }
}

async fn load_sharded_index_once(
    storage: &dyn Storage,
    index_id: &str,
) -> MetastoreResult<(FileBackedIndex, ShardedWriteContext)> {
    let root_path = root_filepath(index_id);
    let (root_bytes, root_version_opt) = storage
        .get_all_with_version(&root_path)
        .await
        .map_err(|error| convert_error(index_id, error))?;
    let Some(root_version) = root_version_opt else {
        return Err(no_version_error(index_id, &root_path));
    };
    let root: ShardedRoot = serde_utils::from_json_bytes(&root_bytes)?;
    if root.format_version != SHARDED_FORMAT_VERSION {
        return Err(internal_error(
            format!("index `{index_id}` uses an unknown metastore layout version"),
            format!(
                "found `{}`, this node understands `{SHARDED_FORMAT_VERSION}`",
                root.format_version
            ),
        ));
    }
    let (view, view_version_opt) = load_view(storage, index_id).await?;
    let mut splits: HashMap<SplitId, Split> = HashMap::new();
    let mut slot_versions: BTreeMap<u32, ObjectVersion> = BTreeMap::new();
    let mut slot_entries: BTreeMap<u32, Vec<SlotEntry>> = BTreeMap::new();
    for (slot, bookmark) in &view.slots {
        let Some(segment_id) = &bookmark.segment else {
            continue;
        };
        let segment = load_segment(storage, index_id, *slot, segment_id).await?;
        for split in segment.splits {
            splits.insert(split.split_id().clone(), split);
        }
    }
    for (slot, metadata) in list_slot_files(storage, index_id).await? {
        let bookmark = view.bookmark(slot);
        let Some(listed_version) = metadata.object_version else {
            return Err(no_version_error(index_id, &metadata.path));
        };
        slot_versions.insert(slot, listed_version.clone());
        if bookmark.folded_version.as_deref() == Some(listed_version.as_str()) {
            // Nothing was written to this slot since it was folded.
            continue;
        }
        let (slot_file, slot_version) = load_slot_file(storage, index_id, slot).await?;
        if slot_file.base_generation > view.generation {
            return Err(stale_view_error(index_id));
        }
        apply_entries(&mut splits, &slot_file, bookmark.folded_seq);
        slot_versions.insert(slot, slot_version);
        slot_entries.insert(slot, slot_file.entries);
    }
    let mut index = root.index;
    index.put_splits(splits);
    let context = ShardedWriteContext {
        num_slots: root.num_slots,
        root_version,
        root_bytes,
        view_version_opt,
        view,
        slot_versions,
        slot_entries,
    };
    Ok((index, context))
}

async fn load_view(
    storage: &dyn Storage,
    index_id: &str,
) -> MetastoreResult<(SplitView, Option<ObjectVersion>)> {
    let view_path = view_filepath(index_id);
    match storage.get_all_with_version(&view_path).await {
        Ok((view_bytes, version_opt)) => {
            let view: SplitView = serde_utils::from_json_bytes(&view_bytes)?;
            if view.format_version != SHARDED_FORMAT_VERSION {
                return Err(internal_error(
                    format!("index `{index_id}` uses an unknown split view version"),
                    format!("found `{}`", view.format_version),
                ));
            }
            Ok((view, version_opt))
        }
        Err(error) if error.kind() == StorageErrorKind::NotFound => {
            Ok((SplitView::new(SLOT_FOLD_THRESHOLD), None))
        }
        Err(error) => Err(convert_error(index_id, error)),
    }
}

async fn load_segment(
    storage: &dyn Storage,
    index_id: &str,
    slot: u32,
    segment_file_name: &str,
) -> MetastoreResult<SplitSegment> {
    let segment_path = segment_filepath(index_id, slot, segment_file_name);
    let segment_bytes = storage
        .get_all(&segment_path)
        .await
        .map_err(|error| convert_error(index_id, error))?;
    let segment: SplitSegment = serde_utils::from_json_bytes(&segment_bytes)?;
    if segment.format_version != SHARDED_FORMAT_VERSION {
        return Err(internal_error(
            "unknown split segment version",
            format!("found `{}`", segment.format_version),
        ));
    }
    Ok(segment)
}

/// Lists the slot files of an index, with the object version of each when the backend reports one.
async fn list_slot_files(
    storage: &dyn Storage,
    index_id: &str,
) -> MetastoreResult<Vec<(u32, ObjectMetadata)>> {
    let mut pages = storage.list(&slots_prefix(index_id));
    let mut slot_files = Vec::new();
    while let Some(page) = pages.try_next().await.map_err(|error| {
        internal_error(
            "failed to list the split slots of the index",
            error.to_string(),
        )
    })? {
        for metadata in page {
            let Some(slot) = metadata
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_suffix(".json"))
                .and_then(|name| name.parse::<u32>().ok())
            else {
                continue;
            };
            slot_files.push((slot, metadata));
        }
    }
    Ok(slot_files)
}

async fn load_slot_file(
    storage: &dyn Storage,
    index_id: &str,
    slot: u32,
) -> MetastoreResult<(SlotFile, ObjectVersion)> {
    let path = slot_filepath(index_id, slot);
    let (slot_bytes, version_opt) = storage
        .get_all_with_version(&path)
        .await
        .map_err(|error| convert_error(index_id, error))?;
    let Some(version) = version_opt else {
        return Err(no_version_error(index_id, &path));
    };
    let slot_file: SlotFile = serde_utils::from_json_bytes(&slot_bytes)?;
    Ok((slot_file, version))
}

/// Applies every entry of a slot file that comes after `folded_seq`.
fn apply_entries(splits: &mut HashMap<SplitId, Split>, slot_file: &SlotFile, folded_seq: u64) {
    for entry in &slot_file.entries {
        if entry.seq <= folded_seq {
            continue;
        }
        match &entry.split {
            Some(split) => {
                splits.insert(entry.split_id.clone(), split.clone());
            }
            None => {
                splits.remove(&entry.split_id);
            }
        }
    }
}

/// Writes an index back in the sharded layout.
///
/// Only the slots the mutation touched are rewritten, plus the root when the non-split part of the
/// index changed. Returns [`MetastoreError::FailedPrecondition`] when the root or a slot file lost
/// a compare-and-swap race, which is the caller's signal to reload and replay the mutation.
pub(super) async fn store_sharded_index(
    storage: &dyn Storage,
    index: &mut FileBackedIndex,
    context: &ShardedWriteContext,
) -> MetastoreResult<()> {
    let index_id = index.index_id().to_string();
    let num_slots = context.num_slots;
    let touched_split_ids = index.take_touched_split_ids();
    let splits = index.take_splits();

    // The root is only rewritten when its own content changed: a split publish must not turn into a
    // write of the one object every writer also touches.
    let root_bytes = serialize_root(index, num_slots)?;
    if root_bytes.as_slice() != context.root_bytes.as_slice() {
        let write_result = storage
            .put_if_version_matches(
                &root_filepath(&index_id),
                Box::new(root_bytes),
                &context.root_version,
            )
            .await
            .map_err(|error| convert_error(&index_id, error));
        if let Err(error) = write_result {
            index.put_splits(splits);
            index.put_touched_split_ids(touched_split_ids);
            return Err(error);
        }
    }

    let mut touched_slots: BTreeMap<u32, BTreeSet<SplitId>> = BTreeMap::new();
    for split_id in touched_split_ids {
        touched_slots
            .entry(slot_of(split_id.as_str(), num_slots))
            .or_default()
            .insert(split_id);
    }

    for (slot, split_ids) in touched_slots {
        let bookmark = context.view.bookmark(slot);
        // The file as the read that produced this context saw it, not as it is now: the
        // compare-and-swap below is what tells us whether it moved in the meantime.
        let version_opt = context.slot_versions.get(&slot).cloned();
        let mut slot_file = match context.slot_entries.get(&slot) {
            Some(entries) => SlotFile {
                base_generation: context.view.generation,
                next_seq: entries.last().map(|entry| entry.seq).unwrap_or(0),
                entries: entries
                    .iter()
                    .filter(|entry| entry.seq > bookmark.folded_seq)
                    .cloned()
                    .collect(),
            },
            // The slot was folded and nothing was written to it since: everything it holds is
            // already in the segment.
            None => SlotFile {
                base_generation: context.view.generation,
                next_seq: bookmark.folded_seq,
                entries: Vec::new(),
            },
        };
        for split_id in &split_ids {
            slot_file.push(split_id.clone(), splits.get(split_id).cloned());
        }
        let slot_bytes = serde_utils::to_json_bytes(&slot_file).map_err(|error| {
            internal_error("failed to serialize a split slot", error.to_string())
        })?;
        let path = slot_filepath(&index_id, slot);
        let slot_version_opt = match &version_opt {
            Some(version) => {
                storage
                    .put_if_version_matches(&path, Box::new(slot_bytes), version)
                    .await
            }
            None => storage.put_if_absent(&path, Box::new(slot_bytes)).await,
        }
        .map_err(|error| convert_error(&index_id, error))?;
        let Some(slot_version) = slot_version_opt else {
            index.put_splits(splits);
            return Err(no_version_error(&index_id, &path));
        };
        if slot_file.entries.len() >= context.view.fold_threshold
            && let Err(error) =
                fold_slot(storage, &index_id, slot, &slot_file, &slot_version, context).await
        {
            // The write itself is durable. Folding is maintenance: failing the metastore operation
            // here would make the caller replay a mutation that is already applied.
            warn!(
                index_id = %index_id,
                slot,
                %error,
                "failed to fold a split slot, the next write to it will retry"
            );
        }
    }
    index.put_splits(splits);
    Ok(())
}

/// Folds one slot into a segment and announces it in the view.
///
/// The segment is `previous segment + every entry of the slot file`, which is why a fold needs only
/// that slot file and the previous segment of the same slot, never the whole index.
async fn fold_slot(
    storage: &dyn Storage,
    index_id: &str,
    slot: u32,
    slot_file: &SlotFile,
    slot_file_version: &ObjectVersion,
    context: &ShardedWriteContext,
) -> MetastoreResult<()> {
    let bookmark = context.view.bookmark(slot);
    let mut splits: HashMap<SplitId, Split> = HashMap::new();
    if let Some(segment_id) = &bookmark.segment {
        let segment = load_segment(storage, index_id, slot, segment_id).await?;
        for split in segment.splits {
            splits.insert(split.split_id().clone(), split);
        }
    }
    for entry in &slot_file.entries {
        match &entry.split {
            Some(split) => {
                splits.insert(entry.split_id.clone(), split.clone());
            }
            None => {
                splits.remove(&entry.split_id);
            }
        }
    }
    let new_generation = context.view.generation + 1;
    let segment_file_name = format!("{new_generation:016}-{}.json", Uuid::new_v4());
    let segment = SplitSegment {
        format_version: SHARDED_FORMAT_VERSION,
        generation: new_generation,
        splits: splits.into_values().collect(),
    };
    let segment_bytes = serde_utils::to_json_bytes(&segment).map_err(|error| {
        internal_error("failed to serialize a split segment", error.to_string())
    })?;
    let segment_path = segment_filepath(index_id, slot, &segment_file_name);
    storage
        .put(&segment_path, Box::new(segment_bytes))
        .await
        .map_err(|error| convert_error(index_id, error))?;

    let mut new_view = context.view.clone();
    new_view.generation = new_generation;
    new_view.slots.insert(
        slot,
        SlotBookmark {
            folded_seq: slot_file.last_seq().unwrap_or(bookmark.folded_seq),
            folded_version: Some(slot_file_version.as_str().to_string()),
            segment: Some(segment_file_name.clone()),
        },
    );
    let view_bytes = serde_utils::to_json_bytes_pretty(&new_view)
        .map_err(|error| internal_error("failed to serialize the split view", error.to_string()))?;
    let view_write_result = match &context.view_version_opt {
        Some(version) => storage
            .put_if_version_matches(&view_filepath(index_id), Box::new(view_bytes), version)
            .await
            .map(|_| ()),
        None => storage
            .put_if_absent(&view_filepath(index_id), Box::new(view_bytes))
            .await
            .map(|_| ()),
    };
    if let Err(error) = view_write_result {
        // Another writer folded first. Our segment is unreferenced: drop it instead of leaving it
        // behind.
        let _ = storage.delete(&segment_path).await;
        return Err(convert_error(index_id, error));
    }
    garbage_collect_segments(storage, index_id, slot, new_generation).await;
    Ok(())
}

/// Deletes segments no reader can still need.
///
/// A reader only ever uses the segments named by the view it read; those stay alive for as long as
/// some reader holds that view. Keeping one extra generation of history covers a reader that lost
/// the race against a fold, which is the only way it can be behind.
async fn garbage_collect_segments(
    storage: &dyn Storage,
    index_id: &str,
    slot: u32,
    new_generation: u64,
) {
    let Some(delete_before) = new_generation.checked_sub(SEGMENT_GRACE_GENERATIONS) else {
        return;
    };
    let prefix = segments_prefix(index_id, slot);
    let mut pages = storage.list(&prefix);
    loop {
        let page = match pages.try_next().await {
            Ok(Some(page)) => page,
            Ok(None) => break,
            Err(error) => {
                warn!(
                    index_id,
                    slot,
                    %error,
                    "failed to list split segments for garbage collection"
                );
                return;
            }
        };
        for metadata in page {
            let Some(file_name) = metadata.path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(generation) = file_name
                .split_once('-')
                .and_then(|(generation, _)| generation.parse::<u64>().ok())
            else {
                continue;
            };
            if generation > delete_before {
                continue;
            }
            if let Err(error) = storage.delete(&prefix.join(file_name)).await {
                warn!(
                    index_id,
                    slot,
                    %error,
                    "failed to delete a superseded split segment"
                );
            }
        }
    }
}

/// Deletes every object of an index stored in the sharded layout.
pub(super) async fn delete_sharded_index(
    storage: &dyn Storage,
    index_id: &str,
) -> MetastoreResult<()> {
    let mut pages = storage.list(&layout_prefix(index_id));
    let mut paths = Vec::new();
    loop {
        let page = pages.try_next().await.map_err(|error| {
            internal_error("failed to list the objects of the index", error.to_string())
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
            .map_err(|error| convert_error(index_id, error))?;
    }
    Ok(())
}

/// Whether the index exists in the sharded layout.
pub(super) async fn sharded_index_exists(
    storage: &dyn Storage,
    index_id: &str,
) -> MetastoreResult<bool> {
    storage
        .exists(&root_filepath(index_id))
        .await
        .map_err(|error| convert_error(index_id, error))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_proto::types::{IndexUid, SplitId};
    use quickwit_storage::RamStorage;

    use super::*;
    use crate::{IndexMetadata, SplitMetadata, SplitState};

    const INDEX_ID: &str = "test-index";

    fn test_storage() -> Arc<dyn Storage> {
        Arc::new(RamStorage::default())
    }

    fn stage_splits(index: &mut FileBackedIndex, range: std::ops::Range<usize>) {
        for i in range {
            let split_id = SplitId::from(format!("split-{i:06}"));
            index
                .stage_split(SplitMetadata::for_test(split_id))
                .unwrap();
        }
    }

    async fn create_index(storage: &dyn Storage, num_slots: u32) -> FileBackedIndex {
        let index = FileBackedIndex::new(
            IndexMetadata::for_test(INDEX_ID, "file:///test-index"),
            Vec::new(),
            HashMap::new(),
            Vec::new(),
        );
        create_sharded_index(storage, &index, num_slots, SLOT_FOLD_THRESHOLD)
            .await
            .unwrap();
        index
    }

    async fn write(
        storage: &dyn Storage,
        range: std::ops::Range<usize>,
        context: &ShardedWriteContext,
    ) -> MetastoreResult<()> {
        let (mut index, _) = load_sharded_index(storage, INDEX_ID).await.unwrap();
        stage_splits(&mut index, range);
        store_sharded_index(storage, &mut index, context).await
    }

    async fn list_split_ids(storage: &dyn Storage) -> Vec<String> {
        let (index, _) = load_sharded_index(storage, INDEX_ID).await.unwrap();
        let mut split_ids: Vec<String> = index
            .list_splits(&crate::ListSplitsQuery::for_index(IndexUid::for_test(
                INDEX_ID, 0,
            )))
            .unwrap()
            .iter()
            .map(|split| split.split_id().to_string())
            .collect();
        split_ids.sort();
        split_ids
    }

    #[test]
    fn test_slot_of_is_stable_and_spreads_splits() {
        // The slot of a split is written down in the layout, so it must not drift between
        // toolchains or runs.
        assert_eq!(slot_of("01HK153X00", 16), slot_of("01HK153X00", 16));
        let mut occupied_slots = std::collections::HashSet::new();
        for i in 0..1_000 {
            occupied_slots.insert(slot_of(&format!("split-{i:06}"), 16));
        }
        assert_eq!(occupied_slots.len(), 16);
    }

    #[tokio::test]
    async fn test_sharded_layout_round_trip() {
        let storage = test_storage();
        create_index(&*storage, 8).await;
        let (_, context) = load_sharded_index(&*storage, INDEX_ID).await.unwrap();
        write(&*storage, 0..10, &context).await.unwrap();
        assert_eq!(list_split_ids(&*storage).await.len(), 10);
    }

    // The fold is the part that keeps a slot file bounded: without it the file would grow with the
    // number of splits ever written to the index, which is the problem the layout exists to solve.
    #[tokio::test]
    async fn test_sharded_layout_folds_and_forgets() {
        let storage = test_storage();
        create_index(&*storage, 1).await;
        let (_, context) = load_sharded_index(&*storage, INDEX_ID).await.unwrap();
        let num_splits = SLOT_FOLD_THRESHOLD + 100;
        write(&*storage, 0..num_splits, &context).await.unwrap();

        // Every split of a single-slot index lives in one file, so this write crossed the threshold
        // and the writer folded the slot.
        let (_, context) = load_sharded_index(&*storage, INDEX_ID).await.unwrap();
        let (view, _) = load_view(&*storage, INDEX_ID).await.unwrap();
        assert_eq!(view.slots.len(), 1);
        assert!(
            view.slots
                .values()
                .all(|bookmark| bookmark.segment.is_some()),
            "the slot should have been folded into a segment"
        );
        assert_eq!(list_split_ids(&*storage).await.len(), num_splits);

        // A split deleted after the fold must not come back through the segment.
        let deleted_split_id = SplitId::from("split-000005".to_string());
        let (mut index, _) = load_sharded_index(&*storage, INDEX_ID).await.unwrap();
        index
            .mark_splits_for_deletion([deleted_split_id.as_str()], &[SplitState::Staged], true)
            .unwrap();
        index.delete_splits([deleted_split_id.as_str()]).unwrap();
        store_sharded_index(&*storage, &mut index, &context)
            .await
            .unwrap();
        let split_ids = list_split_ids(&*storage).await;
        assert_eq!(split_ids.len(), num_splits - 1);
        assert!(!split_ids.contains(&deleted_split_id.to_string()));
    }

    #[tokio::test]
    async fn test_sharded_layout_conflicts_only_on_the_same_slot() {
        let storage = test_storage();
        create_index(&*storage, 2).await;

        // Two writers that touch different slots both win.
        let mut split_ids_in_slot: [Option<SplitId>; 2] = [None, None];
        for i in 0.. {
            let split_id = SplitId::from(format!("split-{i:06}"));
            let slot = slot_of(split_id.as_str(), 2) as usize;
            if split_ids_in_slot[slot].is_none() {
                split_ids_in_slot[slot] = Some(split_id);
            }
            if split_ids_in_slot.iter().all(Option::is_some) {
                break;
            }
        }
        let split_id_slot_0 = split_ids_in_slot[0].clone().unwrap();
        let split_id_slot_1 = split_ids_in_slot[1].clone().unwrap();
        assert_ne!(
            slot_of(split_id_slot_0.as_str(), 2),
            slot_of(split_id_slot_1.as_str(), 2)
        );
        let (_, context_0) = load_sharded_index(&*storage, INDEX_ID).await.unwrap();
        let (_, context_1) = load_sharded_index(&*storage, INDEX_ID).await.unwrap();
        write_split(&*storage, &split_id_slot_0, &context_0)
            .await
            .unwrap();
        write_split(&*storage, &split_id_slot_1, &context_1)
            .await
            .unwrap();

        // Two writers that touch the same slot: the second one has to replay.
        let (_, context_a2) = load_sharded_index(&*storage, INDEX_ID).await.unwrap();
        let (_, context_b2) = load_sharded_index(&*storage, INDEX_ID).await.unwrap();
        write_split(&*storage, &split_id_slot_0, &context_a2)
            .await
            .unwrap();
        let error = write_split(&*storage, &split_id_slot_0, &context_b2)
            .await
            .unwrap_err();
        assert!(
            matches!(error, MetastoreError::FailedPrecondition { .. }),
            "expected the losing writer to be told to replay, got {error:?}"
        );
        assert_eq!(list_split_ids(&*storage).await.len(), 2);
    }

    async fn write_split(
        storage: &dyn Storage,
        split_id: &SplitId,
        context: &ShardedWriteContext,
    ) -> MetastoreResult<()> {
        let (mut index, _) = load_sharded_index(storage, INDEX_ID).await.unwrap();
        index
            .stage_split(SplitMetadata::for_test(split_id.clone()))
            .unwrap();
        store_sharded_index(storage, &mut index, context).await
    }

    // A split publish must not rewrite the root: that object is shared by every writer of the
    // index, so rewriting it would put back exactly the contention the layout removes.
    #[tokio::test]
    async fn test_sharded_layout_does_not_rewrite_the_root_for_split_writes() {
        let storage = test_storage();
        create_index(&*storage, 8).await;
        let root_version_before = storage
            .get_all_with_version(&root_filepath(INDEX_ID))
            .await
            .unwrap()
            .1
            .unwrap();
        let (_, context) = load_sharded_index(&*storage, INDEX_ID).await.unwrap();
        write(&*storage, 0..10, &context).await.unwrap();
        let root_version_after = storage
            .get_all_with_version(&root_filepath(INDEX_ID))
            .await
            .unwrap()
            .1
            .unwrap();
        assert_eq!(root_version_before, root_version_after);
    }

    #[tokio::test]
    async fn test_delete_sharded_index_removes_every_object() {
        let storage = test_storage();
        create_index(&*storage, 4).await;
        let (_, context) = load_sharded_index(&*storage, INDEX_ID).await.unwrap();
        write(&*storage, 0..20, &context).await.unwrap();
        assert!(
            !storage
                .list(&layout_prefix(INDEX_ID))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
        delete_sharded_index(&*storage, INDEX_ID).await.unwrap();
        assert!(
            !sharded_index_exists(&*storage, INDEX_ID).await.unwrap(),
            "the root should be gone"
        );
        let remaining: Vec<PathBuf> = storage
            .list(&layout_prefix(INDEX_ID))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .map(|metadata| metadata.path)
            .collect();
        assert!(remaining.is_empty(), "left behind: {remaining:?}");
    }

    /// Size of every object under `prefix`, keyed by path, plus the object version.
    ///
    /// The version is what turns a listing into a change feed: an object whose version is unchanged
    /// was not rewritten.
    async fn object_snapshot(
        storage: &dyn Storage,
        prefix: &Path,
    ) -> HashMap<PathBuf, (Option<ObjectVersion>, u64)> {
        let mut snapshot = HashMap::new();
        let mut pages = storage.list(prefix);
        while let Some(page) = pages.try_next().await.unwrap() {
            for metadata in page {
                snapshot.insert(
                    metadata.path,
                    (metadata.object_version, metadata.size.as_u64()),
                );
            }
        }
        snapshot
    }

    /// Bytes rewritten by one publish that stages and publishes `num_splits` splits at once.
    ///
    /// This is the measurement the layout exists for, and it is endpoint-independent: every byte
    /// counted here is a byte of a request body that goes to the object store. The single-object
    /// layout rewrites every split of the index on every publish, so its cost grows with the index;
    /// the sharded layout rewrites one object per touched slot, so its cost follows the batch.
    async fn bytes_rewritten_by_one_publish(
        storage: &dyn Storage,
        index_id: &str,
        split_ids: &[SplitId],
        layout: LayoutUnderTest,
    ) -> u64 {
        let before = object_snapshot(storage, Path::new("")).await;
        match layout {
            LayoutUnderTest::SingleObject => {
                let (mut index, version) =
                    super::super::store_operations::load_index_with_version(storage, index_id)
                        .await
                        .unwrap();
                let version = version.unwrap();
                for split_id in split_ids {
                    index
                        .stage_split(SplitMetadata::for_test(split_id.clone()))
                        .unwrap();
                }
                super::super::store_operations::put_index_if_version_matches(
                    storage, &mut index, &version,
                )
                .await
                .unwrap();
            }
            LayoutUnderTest::Sharded => {
                let (mut index, context) = load_sharded_index(storage, index_id).await.unwrap();
                for split_id in split_ids {
                    index
                        .stage_split(SplitMetadata::for_test(split_id.clone()))
                        .unwrap();
                }
                store_sharded_index(storage, &mut index, &context)
                    .await
                    .unwrap();
            }
        }
        let after = object_snapshot(storage, Path::new("")).await;
        after
            .iter()
            .filter(|(path, (version, _))| {
                // A new object, or one whose version changed, is one the publish rewrote.
                match before.get(*path) {
                    Some((previous_version, _)) => previous_version != version,
                    None => true,
                }
            })
            .map(|(_, (_, size))| size)
            .sum()
    }

    #[derive(Clone, Copy, Debug)]
    enum LayoutUnderTest {
        SingleObject,
        Sharded,
    }

    // The property the layout exists for: what a publish rewrites must stop following the size of
    // the index. Measured as "which objects did this publish rewrite" (a listing carries the object
    // version), summed over the objects that changed.
    #[tokio::test]
    async fn test_sharded_writes_do_not_follow_the_index_size() {
        const NUM_ROUNDS: usize = 60;
        const SPLITS_PER_ROUND: usize = 3;
        const NUM_SLOTS: u32 = 64;
        // A small fold threshold, so that the sawtooth is visible on an index that a unit test can
        // build. The real threshold is `SLOT_FOLD_THRESHOLD`; what matters here is that a publish
        // only rewrites the entries a slot accumulated since its last fold.
        const FOLD_THRESHOLD: usize = 8;

        let mut rewritten_bytes_per_layout = Vec::new();
        for (label, layout) in [
            ("single-object", LayoutUnderTest::SingleObject),
            ("sharded", LayoutUnderTest::Sharded),
        ] {
            let storage = test_storage();
            let index = FileBackedIndex::new(
                IndexMetadata::for_test(INDEX_ID, "file:///test-index"),
                Vec::new(),
                HashMap::new(),
                Vec::new(),
            );
            match layout {
                LayoutUnderTest::SingleObject => {
                    super::super::store_operations::put_index_if_absent(&*storage, &index)
                        .await
                        .unwrap();
                }
                LayoutUnderTest::Sharded => {
                    create_sharded_index(&*storage, &index, NUM_SLOTS, FOLD_THRESHOLD)
                        .await
                        .unwrap();
                }
            }
            let mut rewritten_bytes = Vec::new();
            for round in 0..NUM_ROUNDS {
                let split_ids: Vec<SplitId> = (0..SPLITS_PER_ROUND)
                    .map(|i| SplitId::from(format!("split-{:06}", round * SPLITS_PER_ROUND + i)))
                    .collect();
                rewritten_bytes.push(
                    bytes_rewritten_by_one_publish(&*storage, INDEX_ID, &split_ids, layout).await,
                );
            }
            let index_bytes: u64 = object_snapshot(&*storage, Path::new(""))
                .await
                .values()
                .map(|(_, size)| *size)
                .sum();
            eprintln!(
                "layout `{label}`: {} bytes rewritten by the first publish, {} by the last, {} \
                 bytes of metadata in total",
                rewritten_bytes.first().unwrap(),
                rewritten_bytes.last().unwrap(),
                index_bytes
            );
            rewritten_bytes_per_layout.push(rewritten_bytes);
        }

        let single_object = &rewritten_bytes_per_layout[0];
        let sharded = &rewritten_bytes_per_layout[1];
        assert!(
            single_object.last().unwrap() > &(2 * single_object.first().unwrap()),
            "the single-object layout rewrites the whole index, so its cost has to grow with it: \
             {single_object:?}"
        );
        assert!(
            sharded.last().unwrap() < single_object.last().unwrap(),
            "the sharded layout rewrote {} bytes on its last publish against {} for the \
             single-object layout",
            sharded.last().unwrap(),
            single_object.last().unwrap()
        );
        // The tail between two folds is what a publish rewrites, so the cost has to stay within the
        // threshold instead of tracking the index.
        let sharded_max = sharded.iter().max().unwrap();
        assert!(
            *sharded_max < *single_object.last().unwrap(),
            "the sharded layout's worst publish rewrote {sharded_max} bytes against {} for the \
             single-object layout's last one",
            single_object.last().unwrap()
        );
    }
}
