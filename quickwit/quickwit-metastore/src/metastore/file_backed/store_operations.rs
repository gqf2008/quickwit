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

/// Index metastore file managed by [`FileBackedMetastore`](crate::FileBackedMetastore).
pub(super) const METASTORE_FILE_NAME: &str = "metastore.json";

/// Path to the metadata file from the given index ID.
pub(super) fn metastore_filepath(index_id: &str) -> PathBuf {
    Path::new(index_id).join(METASTORE_FILE_NAME)
}

fn convert_error(index_id: &str, storage_error: StorageError) -> MetastoreError {
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
        _ => MetastoreError::Internal {
            message: "failed to get index files".to_string(),
            cause: storage_error.to_string(),
        },
    }
}

/// Loads an index together with the version a conditional write has to match.
///
/// Hidden contract: the returned version is only meaningful for the exact bytes returned here. It
/// is `None` on backends that cannot version an object, and callers that intend to compare-and-swap
/// must treat that as "unsupported" instead of falling back to an unconditional write.
pub(super) async fn load_index_with_version(
    storage: &dyn Storage,
    index_id: &str,
) -> MetastoreResult<(FileBackedIndex, Option<ObjectVersion>)> {
    let metastore_filepath = metastore_filepath(index_id);

    let (content, version) = storage
        .get_all_with_version(&metastore_filepath)
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))?;

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
    Ok((index, version))
}

/// Writes the index metadata back, but only if it still has `version` (compare-and-swap).
///
/// Fails with [`MetastoreError::FailedPrecondition`] when another writer got there first; the
/// caller is expected to reload the index and replay its mutation.
pub(super) async fn put_index_if_version_matches(
    storage: &dyn Storage,
    index: &FileBackedIndex,
    version: &ObjectVersion,
) -> MetastoreResult<Option<ObjectVersion>> {
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

    let content = storage
        .get_all(&metastore_filepath)
        .await
        .map_err(|storage_err| convert_error(index_id, storage_err))?;

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
    Ok(exists)
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
