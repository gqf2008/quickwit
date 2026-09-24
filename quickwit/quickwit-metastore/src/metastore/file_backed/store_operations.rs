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
}

/// Version of an index, in whichever layout it lives.
#[derive(Debug)]
pub(super) enum IndexVersion {
    SingleObject(ObjectVersion),
    /// Carries what the sharded layout needs to write the index back without re-reading it.
    Sharded(Box<ShardedWriteContext>),
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
    sharded_index_exists(storage, index_id).await
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
