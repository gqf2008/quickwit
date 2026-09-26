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

use std::path::{Path, PathBuf};

use quickwit_proto::metastore::{EntityKind, MetastoreError, MetastoreResult, serde_utils};
use quickwit_storage::{ObjectVersion, Storage, StorageError, StorageErrorKind};

use crate::metastore::file_backed::file_backed_index::FileBackedIndex;
use crate::metastore::file_backed::manifest_layout::{
    ManifestLayout, ManifestWriteContext, SplitOp,
};
use crate::metastore::file_backed::sharded_layout::{
    SLOT_FOLD_THRESHOLD as DEFAULT_FOLD_THRESHOLD, ShardedWriteContext, create_sharded_index,
    delete_sharded_index, load_sharded_index, sharded_index_exists, store_sharded_index,
};

/// Index metastore file managed by [`FileBackedMetastore`](crate::FileBackedMetastore).
pub(super) const METASTORE_FILE_NAME: &str = "metastore.json";

/// Path to the metadata file from the given index ID.
pub(super) fn metastore_filepath(index_id: &str) -> PathBuf {
    Path::new(index_id).join(METASTORE_FILE_NAME)
}

pub(super) fn convert_error(index_id: &str, storage_error: StorageError) -> MetastoreError {
    match storage_error.kind() {
        StorageErrorKind::NotFound => MetastoreError::NotFound(EntityKind::Index {
            index_id: index_id.to_string(),
        }),
        StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
            message: "the request credentials do not allow for this operation".to_string(),
        },
        // A lost compare-and-swap race is a normal, retryable outcome: somebody else wrote the
        // index metadata between our read and our write. It must not be reported as an internal
        // error, or callers would treat contention as a service failure.
        StorageErrorKind::PreconditionFailed => MetastoreError::FailedPrecondition {
            entity: EntityKind::Index {
                index_id: index_id.to_string(),
            },
            message: "the index metadata was modified concurrently".to_string(),
        },
        // A backend that cannot version objects cannot take part in the compare-and-swap write
        // path. Say so explicitly: "failed to get index files" would send an operator looking for a
        // file problem instead of the capability gap that is actually there.
        StorageErrorKind::Unsupported => MetastoreError::Internal {
            message: format!("index `{index_id}` was written with a conditional write"),
            cause: "the storage backend does not support conditional writes; the metastore needs \
                    a storage that versions objects (S3-compatible)"
                .to_string(),
        },
        _ => MetastoreError::Internal {
            message: "failed to get index files".to_string(),
            cause: storage_error.to_string(),
        },
    }
}

/// Where an index lives on the storage.
///
/// An index either lives in the historical single object (`<index_id>/metastore.json`) or in the
/// sharded layout, which keeps the splits in one object per slot. The layout is recorded by the
/// objects themselves, so a node reads either one; the layout below only decides what a *new* index
/// is created with.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexLayout {
    /// Historical layout: the whole index, splits included, lives in `<index_id>/metastore.json`.
    SingleObject,
    /// Splits are spread over `num_slots` objects, one per hash slot of the split id.
    Sharded {
        /// Number of slots the split map is spread over.
        num_slots: u32,
    },
    /// Splits live in immutable segments, referenced by striped manifests.
    ///
    /// This is the layout that keeps a read's cost proportional to the query's window rather than
    /// to the index: see [`super::manifest_layout`] and
    /// `docs/internals/metastore-v3-manifest-and-segments.md`.
    ManifestSegments {
        /// Width of a time bucket, in seconds; one segment serves each bucket.
        bucket_secs: i64,
        /// Number of manifests, each with its own compare-and-swap point.
        num_stripes: usize,
    },
}

/// Version of an index, in whichever layout it lives.
#[derive(Debug)]
pub(super) enum IndexVersion {
    SingleObject(ObjectVersion),
    /// Carries what the sharded layout needs to write the index back without re-reading it.
    Sharded(Box<ShardedWriteContext>),
    /// Carries what the manifest layout needs to write the index back.
    ManifestSegments(Box<ManifestWriteContext>),
}

pub(super) async fn load_index_with_version(
    storage: &dyn Storage,
    index_id: &str,
) -> MetastoreResult<(FileBackedIndex, Option<IndexVersion>)> {
    load_index_state(storage, index_id).await
}

/// Loads an index and, when the storage versions objects, the version to compare-and-swap against.
pub(super) async fn load_index_state(
    storage: &dyn Storage,
    index_id: &str,
) -> MetastoreResult<(FileBackedIndex, Option<IndexVersion>)> {
    let metastore_filepath = metastore_filepath(index_id);

    let (content, version) = match storage.get_all_with_version(&metastore_filepath).await {
        Ok((content, version)) => (content, version),
        Err(storage_error) if storage_error.kind() == StorageErrorKind::NotFound => {
            // The index may live in the sharded layout instead. Asking the storage is what keeps a
            // node able to read an index another node created in a layout it did not configure.
            if sharded_index_exists(storage, index_id).await? {
                let (index, context) = load_sharded_index(storage, index_id).await?;
                if index.index_id() != index_id {
                    return Err(MetastoreError::Internal {
                        message: "inconsistent manifest: index_id mismatch".to_string(),
                        cause: format!(
                            "expected index_id `{index_id}`, but found `{}`",
                            index.index_id()
                        ),
                    });
                }
                return Ok((index, Some(IndexVersion::Sharded(Box::new(context)))));
            }
            if let Some((index, context)) = load_manifest_index_if_exists(storage, index_id).await?
            {
                return Ok((
                    index,
                    Some(IndexVersion::ManifestSegments(Box::new(context))),
                ));
            }
            return Err(convert_error(index_id, storage_error));
        }
        Err(storage_error) => return Err(convert_error(index_id, storage_error)),
    };

    let index: FileBackedIndex = serde_utils::from_json_bytes(&content)?;

    if index.index_id() != index_id {
        return Err(MetastoreError::Internal {
            message: "inconsistent manifest: index_id mismatch".to_string(),
            cause: format!(
                "expected index_id `{}`, but found `{}`",
                index_id,
                index.index_id()
            ),
        });
    }
    Ok((index, version.map(IndexVersion::SingleObject)))
}

/// Writes the index metadata back, but only if it still has `version` (compare-and-swap).
///
/// Fails with [`MetastoreError::FailedPrecondition`] when another writer got there first; the
/// caller is expected to reload the index and replay its mutation.
pub(super) async fn put_index_if_version_matches(
    storage: &dyn Storage,
    index: &mut FileBackedIndex,
    version: &IndexVersion,
) -> MetastoreResult<Option<ObjectVersion>> {
    if let IndexVersion::Sharded(context) = version {
        store_sharded_index(storage, index, context).await?;
        return Ok(None);
    }
    if let IndexVersion::ManifestSegments(context) = version {
        store_manifest_index(storage, index, context).await?;
        return Ok(None);
    }
    let IndexVersion::SingleObject(version) = version else {
        unreachable!("handled above");
    };
    let index_id = index.index_id();
    let content: Vec<u8> = serde_utils::to_json_bytes_pretty(index)?;
    let metastore_filepath = metastore_filepath(index_id);
    storage
        .put_if_version_matches(&metastore_filepath, Box::new(content), version)
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))
}

/// Creates the index metadata file, failing if it already exists.
///
/// Used by `create_index` in distributed mode: two nodes racing to create the same index must not
/// be able to overwrite each other's metadata, and the loser has to observe the winner's metadata.
pub(super) async fn create_index_file(
    storage: &dyn Storage,
    index: &FileBackedIndex,
    layout: IndexLayout,
) -> MetastoreResult<Option<ObjectVersion>> {
    match layout {
        IndexLayout::SingleObject => put_index_if_absent(storage, index).await,
        IndexLayout::Sharded { num_slots } => {
            create_sharded_index(storage, index, num_slots, DEFAULT_FOLD_THRESHOLD).await?;
            Ok(None)
        }
        IndexLayout::ManifestSegments {
            bucket_secs,
            num_stripes,
        } => {
            let manifest_layout = ManifestLayout::new(index.index_id(), bucket_secs, num_stripes);
            manifest_layout.create_index(storage, index).await?;
            Ok(None)
        }
    }
}

/// Creates the index metadata file, failing if it already exists.
///
/// Used by `create_index` in distributed mode: two nodes racing to create the same index must not
/// be able to overwrite each other's file, and the loser has to observe the winner's metadata.
pub(super) async fn put_index_if_absent(
    storage: &dyn Storage,
    index: &FileBackedIndex,
) -> MetastoreResult<Option<ObjectVersion>> {
    let index_id = index.index_id();
    let content: Vec<u8> = serde_utils::to_json_bytes_pretty(index)?;
    let metastore_filepath = metastore_filepath(index_id);
    storage
        .put_if_absent(&metastore_filepath, Box::new(content))
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))
}

pub(super) async fn load_index(
    storage: &dyn Storage,
    index_id: &str,
) -> MetastoreResult<FileBackedIndex> {
    let metastore_filepath = metastore_filepath(index_id);

    let content = match storage.get_all(&metastore_filepath).await {
        Ok(content) => content,
        Err(storage_error) if storage_error.kind() == StorageErrorKind::NotFound => {
            if sharded_index_exists(storage, index_id).await? {
                let (index, _) = load_sharded_index(storage, index_id).await?;
                return Ok(index);
            }
            if let Some((index, _)) = load_manifest_index_if_exists(storage, index_id).await? {
                return Ok(index);
            }
            return Err(convert_error(index_id, storage_error));
        }
        Err(storage_error) => return Err(convert_error(index_id, storage_error)),
    };
    let index: FileBackedIndex = serde_utils::from_json_bytes(&content)?;

    if index.index_id() != index_id {
        return Err(MetastoreError::Internal {
            message: "inconsistent manifest: index_id mismatch".to_string(),
            cause: format!(
                "expected index_id `{}`, but found `{}`",
                index_id,
                index.index_id()
            ),
        });
    }
    Ok(index)
}

pub(super) async fn index_exists(storage: &dyn Storage, index_id: &str) -> MetastoreResult<bool> {
    let metastore_filepath = metastore_filepath(index_id);
    let exists = storage
        .exists(&metastore_filepath)
        .await
        .map_err(|storage_error| convert_error(index_id, storage_error))?;
    if exists {
        return Ok(true);
    }
    if sharded_index_exists(storage, index_id).await? {
        return Ok(true);
    }
    Ok(load_manifest_index_if_exists(storage, index_id)
        .await?
        .is_some())
}

/// Serializes the `Index` object and stores the data on the storage.
///
/// Do not call this method. Instead, call `put_index`.
/// The point of having two methods here is just to make it usable in a unit test.
pub(super) async fn put_index_given_index_id(
    storage: &dyn Storage,
    index: &FileBackedIndex,
    index_id: &str,
) -> MetastoreResult<()> {
    // Serialize Index.
    let content: Vec<u8> = serde_utils::to_json_bytes_pretty(index)?;
    let metastore_filepath = metastore_filepath(index_id);
    // Put data back into storage.
    storage
        .put(&metastore_filepath, Box::new(content))
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))?;
    Ok(())
}

/// Serializes the `Index` object and stores the data on the storage.
pub(super) async fn put_index(
    storage: &dyn Storage,
    index: &FileBackedIndex,
) -> MetastoreResult<()> {
    put_index_given_index_id(storage, index, index.index_id()).await
}

/// Loads an index stored in the manifest layout, when it exists.
///
/// Returns `None` when there is no root at `<index_id>/v3/root.json`, so a caller can fall through
/// to the layouts that came before.
pub(super) async fn load_manifest_index_if_exists(
    storage: &dyn Storage,
    index_id: &str,
) -> MetastoreResult<Option<(FileBackedIndex, ManifestWriteContext)>> {
    // The root is the one object every manifest-layout index has, so it is what a reader probes.
    let probe = ManifestLayout::new(index_id, DEFAULT_BUCKET_SECS, 1);
    if !probe.exists(storage).await? {
        return Ok(None);
    }
    let (root_info, root_version, root_bytes) = probe.load_root(storage).await?;
    // The layout parameters travel with the index, so a node reads an index another node created
    // with settings it does not share.
    let layout = ManifestLayout::new(index_id, root_info.bucket_secs, root_info.num_stripes);
    // The split map is deliberately *not* loaded here.
    //
    // This is what a node caches for an index, and for a manifest-layout index the split map can be
    // the whole index: measured at 4 000 splits, loading it costs 289 object reads and leaves a
    // copy in memory on every node that reads anything about the index (the control plane and
    // the janitor read its metadata on their own schedules). Every path that needs splits of a
    // manifest-layout index goes through the layout instead — `list_splits`,
    // `get_splits_by_id`, the stats — so the cached view holds the metadata, the sources, the
    // checkpoints, the shards and the delete tasks, which live in `root.json`, and nothing
    // else.
    let index = root_info.index;
    if index.index_id() != index_id {
        return Err(MetastoreError::Internal {
            message: "inconsistent manifest: index_id mismatch".to_string(),
            cause: format!(
                "expected index_id `{index_id}`, but found `{}`",
                index.index_id()
            ),
        });
    }
    let context = ManifestWriteContext {
        layout,
        root_version,
        root_bytes,
        shard_objects: root_info.shard_objects,
    };
    Ok(Some((index, context)))
}

/// Default bucket width for a new index: one segment per hour, so a 30-day index keeps ~720
/// segments and a query for one hour fetches one of them.
const DEFAULT_BUCKET_SECS: i64 = 3_600;

/// Writes an index back in the manifest layout.
///
/// The split map is not rewritten: only the splits the mutation touched are published, as an
/// immutable WAL object per stripe, using the compare-and-swap of that stripe's manifest. The rest
/// of the index (metadata, sources, checkpoints, delete tasks) keeps a small single-object commit.
pub(super) async fn store_manifest_index(
    storage: &dyn Storage,
    index: &mut FileBackedIndex,
    context: &ManifestWriteContext,
) -> MetastoreResult<()> {
    let layout = &context.layout;
    let touched_split_ids = index.take_touched_split_ids();
    let ops: Vec<SplitOp> = touched_split_ids
        .into_iter()
        .map(|split_id| SplitOp {
            split: index.split_opt(&split_id).cloned(),
            split_id,
        })
        .collect();
    if !ops.is_empty() {
        layout.publish_ops(storage, ops).await?;
    }
    layout
        .store_root(
            storage,
            index,
            &context.shard_objects,
            &context.root_bytes,
            &context.root_version,
        )
        .await
}

/// Serializes the Index and stores the data on the storage.
pub(super) async fn delete_index(storage: &dyn Storage, index_id: &str) -> MetastoreResult<()> {
    let metastore_filepath = metastore_filepath(index_id);

    let file_exists = storage
        .exists(&metastore_filepath)
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))?;

    if !file_exists {
        // An index created in the sharded layout has no single metadata file: it is spread over a
        // root, a view, the slot files and the segments.
        if sharded_index_exists(storage, index_id).await? {
            return delete_sharded_index(storage, index_id).await;
        }
        if let Some((_, context)) = load_manifest_index_if_exists(storage, index_id).await? {
            return context.layout.delete(storage).await;
        }
        return Err(MetastoreError::NotFound(EntityKind::Index {
            index_id: index_id.to_string(),
        }));
    }
    // Put data back into storage.
    storage
        .delete(&metastore_filepath)
        .await
        .map_err(|storage_error| match storage_error.kind() {
            StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
                message: "the request credentials do not allow for this operation".to_string(),
            },
            _ => MetastoreError::Internal {
                message: format!(
                    "failed to delete metastore file located at `{}/{}`",
                    storage.uri(),
                    metastore_filepath.display()
                ),
                cause: storage_error.to_string(),
            },
        })?;
    Ok(())
}
