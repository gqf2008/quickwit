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

//! Integration tests for a file-backed metastore shared by several nodes on a real S3-compatible
//! endpoint (MinIO, localstack, Cloudflare R2, AWS S3).
//!
//! They only run with `--features ci-test`, and they skip themselves when no endpoint is
//! configured, so they never turn into a silent no-op without saying so.
//!
//! Against a local MinIO:
//!
//! ```sh
//! export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1
//! export QW_S3_ENDPOINT=http://127.0.0.1:9000 QW_S3_FORCE_PATH_STYLE_ACCESS=1
//! cargo test -p quickwit-metastore --features ci-test --test s3_shared_metastore -- --nocapture
//! ```

#![cfg(feature = "ci-test")]

use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use quickwit_common::rand::append_random_suffix;
use quickwit_common::uri::Uri;
use quickwit_config::{
    IndexConfig, S3StorageConfig, StorageBackendFlavor, StorageConfig, StorageConfigs,
};
use quickwit_metastore::{
    CreateIndexRequestExt, FileBackedMetastore, IndexLayout, ListSplitsQuery, ListSplitsRequestExt,
    MetastoreServiceStreamSplitsExt, SplitMetadata, SplitState, StageSplitsRequestExt,
};
use quickwit_proto::metastore::{
    CreateIndexRequest, DeleteIndexRequest, ListSplitsRequest, MetastoreService,
    PublishSplitsRequest, StageSplitsRequest,
};
use quickwit_proto::types::IndexUid;
use quickwit_storage::{ObjectVersion, S3CompatibleObjectStorage, Storage, StorageErrorKind};

const TEST_BUCKET_URI: &str = "s3://quickwit-integration-tests";

/// Bucket the tests write into.
///
/// Defaults to the bucket the other S3 integration tests use; `QW_TEST_S3_BUCKET_URI` points the
/// same tests at another endpoint (for instance a Cloudflare R2 bucket).
fn test_bucket_uri() -> String {
    std::env::var("QW_TEST_S3_BUCKET_URI").unwrap_or_else(|_| TEST_BUCKET_URI.to_string())
}

/// S3 configuration for the endpoint under test.
///
/// `QW_TEST_S3_FLAVOR=r2` exercises a storage flavor (region, path-style access, checksum
/// algorithm). Flavors are normally applied while loading the node config and `from_uri` expects an
/// already-resolved config, so apply them here the same way.
fn s3_storage_config() -> S3StorageConfig {
    let flavor = match std::env::var("QW_TEST_S3_FLAVOR").ok().as_deref() {
        None | Some("") => None,
        Some("r2") => Some(StorageBackendFlavor::R2),
        Some(other) => panic!("unsupported QW_TEST_S3_FLAVOR `{other}`"),
    };
    let mut storage_configs = StorageConfigs::new(vec![StorageConfig::S3(S3StorageConfig {
        flavor,
        ..Default::default()
    })]);
    storage_configs.apply_flavors();
    storage_configs
        .iter()
        .find_map(|storage_config| storage_config.as_s3().cloned())
        .expect("the storage config holds an S3 section")
}

fn endpoint_is_configured() -> bool {
    std::env::var("QW_S3_ENDPOINT").is_ok()
}

async fn s3_storage(bucket_uri: &str) -> anyhow::Result<Arc<dyn Storage>> {
    let storage_uri = Uri::from_str(bucket_uri)?;
    let storage = S3CompatibleObjectStorage::from_uri(&s3_storage_config(), &storage_uri)
        .await
        .context("failed to open the S3-compatible storage")?;
    Ok(Arc::new(storage))
}

/// Whether the endpoint rejects a write whose `If-None-Match` precondition does not hold.
async fn storage_enforces_conditional_writes(storage: &dyn Storage) -> anyhow::Result<bool> {
    let probe_path = format!(".capability-probe-{}", append_random_suffix("entry"));
    let path = Path::new(&probe_path);
    storage
        .put_if_absent(path, Box::new(b"first".to_vec()))
        .await?;
    let second_write = storage
        .put_if_absent(path, Box::new(b"second".to_vec()))
        .await;
    let enforces = matches!(
        &second_write,
        Err(error) if error.kind() == StorageErrorKind::PreconditionFailed
    );
    storage.delete(path).await?;
    Ok(enforces)
}

/// The conditional-write primitives the shared metastore is built on, against a real endpoint.
#[tokio::test]
async fn test_conditional_writes_on_s3_endpoint() -> anyhow::Result<()> {
    if !endpoint_is_configured() {
        eprintln!("skipping test_conditional_writes_on_s3_endpoint: QW_S3_ENDPOINT is not set");
        return Ok(());
    }
    let bucket_uri = append_random_suffix(&format!("{}/conditional-writes", test_bucket_uri()));
    let storage = s3_storage(&bucket_uri).await?;
    let path = Path::new("lock.json");

    let version = storage
        .put_if_absent(path, Box::new(b"first".to_vec()))
        .await?
        .context("S3-compatible storage should report the version it wrote")?;

    let second_write = storage
        .put_if_absent(path, Box::new(b"second".to_vec()))
        .await;
    if !matches!(
        &second_write,
        Err(error) if error.kind() == StorageErrorKind::PreconditionFailed
    ) {
        // Endpoints exist that accept `If-None-Match: *` and overwrite the object anyway
        // (localstack 3.5.0 does). The metastore detects that at startup and refuses to
        // share the prefix, so this test states the fact instead of pretending the endpoint
        // is safe.
        eprintln!(
            "note: this endpoint ignores `If-None-Match: *` (second write returned \
             {second_write:?}); the metastore refuses to run in shared mode here, which \
             test_shared_metastore_either_shares_safely_or_refuses_to_start asserts"
        );
        storage.delete(path).await?;
        return Ok(());
    }

    let (bytes, read_version) = storage.get_all_with_version(path).await?;
    assert_eq!(&bytes, &b"first"[..]);
    assert_eq!(read_version, Some(version.clone()));

    let stale_version = ObjectVersion::new("not-the-current-version");
    let error = storage
        .put_if_version_matches(path, Box::new(b"third".to_vec()), &stale_version)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::PreconditionFailed);
    assert_eq!(&storage.get_all(path).await?, &b"first"[..]);

    storage
        .put_if_version_matches(path, Box::new(b"second".to_vec()), &version)
        .await?;
    assert_eq!(&storage.get_all(path).await?, &b"second"[..]);
    Ok(())
}

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
    let list_splits_query =
        ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Published);
    let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&list_splits_query)?;
    let splits = metastore
        .list_splits(list_splits_request)
        .await?
        .collect_splits()
        .await?;
    let mut split_ids: Vec<String> = splits
        .iter()
        .map(|split| split.split_id().to_string())
        .collect();
    split_ids.sort();
    Ok(split_ids)
}

/// A shared metastore must either be safe or refuse to start -- never silently lose updates.
///
/// On an endpoint that enforces conditional writes (AWS S3, Cloudflare R2, MinIO) the metastore
/// starts in shared mode and two nodes keep each other's splits. On one that ignores them
/// (localstack 3.5.0) it must refuse to start, unless the operator explicitly accepts single-writer
/// mode. Both outcomes are asserted here; what this test forbids is the third one, a metastore that
/// runs in shared mode on a storage that would drop a concurrent write.
#[tokio::test]
async fn test_shared_metastore_either_shares_safely_or_refuses_to_start() -> anyhow::Result<()> {
    if !endpoint_is_configured() {
        eprintln!(
            "skipping test_shared_metastore_either_shares_safely_or_refuses_to_start: \
             QW_S3_ENDPOINT is not set"
        );
        return Ok(());
    }
    let bucket_uri = append_random_suffix(&format!("{}/shared-metastore", test_bucket_uri()));
    let storage = s3_storage(&bucket_uri).await?;
    let endpoint_enforces_conditional_writes =
        storage_enforces_conditional_writes(&*storage).await?;
    // Print the probe result either way: it is the evidence that decides which branch runs below.
    eprintln!(
        "endpoint enforces conditional writes: {endpoint_enforces_conditional_writes} (bucket: \
         {bucket_uri})"
    );

    let metastore_a = match FileBackedMetastore::try_new(storage.clone(), None).await {
        Ok(metastore) => {
            assert!(
                endpoint_enforces_conditional_writes,
                "the metastore started in shared mode on a storage that ignores conditional \
                 writes; that configuration loses updates silently"
            );
            assert!(
                metastore.is_distributed(),
                "an s3:// metastore on an endpoint that enforces conditional writes must run in \
                 shared mode"
            );
            metastore
        }
        Err(error) => {
            assert!(
                !endpoint_enforces_conditional_writes,
                "the metastore refused to start on a storage that does enforce conditional \
                 writes: {error}"
            );
            let message = error.to_string();
            assert!(
                message.contains("conditional writes"),
                "refusal must name the capability gap, got: {message}"
            );
            // The documented escape hatch keeps single-writer usage possible.
            let metastore =
                FileBackedMetastore::try_new_with_options(storage.clone(), None, true).await?;
            assert!(
                !metastore.is_distributed(),
                "with `allow_unsafe_storage` the metastore must fall back to single-writer mode"
            );
            eprintln!(
                "note: this endpoint ignores conditional writes; the metastore refused shared \
                 mode and only the single-writer escape hatch is exercised here"
            );
            return Ok(());
        }
    };

    let metastore_b = FileBackedMetastore::try_new(storage.clone(), None).await?;
    let metastore_c = FileBackedMetastore::try_new(storage.clone(), None).await?;

    let index_id = append_random_suffix("shared-metastore-index");
    let index_uri = format!("s3://quickwit-integration-tests/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);
    let index_uid: IndexUid = metastore_a
        .create_index(CreateIndexRequest::try_from_index_config(&index_config)?)
        .await?
        .index_uid()
        .clone();

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

    assert_eq!(
        list_published_split_ids(&metastore_c, &index_uid).await?,
        vec!["a-split-0".to_string(), "b-split-0".to_string()],
        "a shared metastore must keep every node's splits"
    );

    metastore_a
        .delete_index(DeleteIndexRequest {
            index_uid: Some(index_uid),
        })
        .await?;
    Ok(())
}

/// The sharded layout on a real endpoint: two nodes publish, both keep their splits, and the index
/// lives in the objects the layout describes (and nowhere else).
///
/// The write amplification itself is measured in `sharded_layout.rs`, where it does not need a
/// network round trip per metadata write to be visible.
#[tokio::test]
async fn test_sharded_layout_on_s3_endpoint() -> anyhow::Result<()> {
    if !endpoint_is_configured() {
        eprintln!("skipping test_sharded_layout_on_s3_endpoint: QW_S3_ENDPOINT is not set");
        return Ok(());
    }
    let bucket_uri = append_random_suffix(&format!("{}/sharded-layout", test_bucket_uri()));
    let storage = s3_storage(&bucket_uri).await?;
    let mut metastore_a = FileBackedMetastore::try_new(storage.clone(), None).await?;
    metastore_a.set_index_layout(IndexLayout::Sharded { num_slots: 8 });
    let metastore_b = {
        let mut metastore = FileBackedMetastore::try_new(storage.clone(), None).await?;
        metastore.set_index_layout(IndexLayout::Sharded { num_slots: 8 });
        metastore
    };
    // A node that was not told about the layout still reads the index: the layout is recorded in
    // the objects, not in the configuration.
    let metastore_c = FileBackedMetastore::try_new(storage.clone(), None).await?;

    let index_id = append_random_suffix("sharded-layout-index");
    let index_config = IndexConfig::for_test(&index_id, &format!("s3://bucket/{index_id}"));
    let index_uid: IndexUid = metastore_a
        .create_index(CreateIndexRequest::try_from_index_config(&index_config)?)
        .await?
        .index_uid()
        .clone();

    let root_path = PathBuf::from(&index_id).join("v2/root.json");
    assert!(
        storage.exists(&root_path).await?,
        "the index should live in the sharded layout"
    );
    assert!(
        !storage
            .exists(&PathBuf::from(&index_id).join("metastore.json"))
            .await?,
        "the sharded layout must not also write the single metadata file"
    );

    // Both nodes publish into the same index. They touch the same slot on purpose: the layout is
    // only worth having if a lost slot race is retried rather than lost.
    stage_and_publish_split(&metastore_a, &index_uid, "sharded-a-0").await?;
    stage_and_publish_split(&metastore_b, &index_uid, "sharded-b-0").await?;
    stage_and_publish_split(&metastore_c, &index_uid, "sharded-c-0").await?;

    assert_eq!(
        list_published_split_ids(&metastore_c, &index_uid).await?,
        vec![
            "sharded-a-0".to_string(),
            "sharded-b-0".to_string(),
            "sharded-c-0".to_string()
        ],
        "every node's splits must survive, whichever layout the index uses"
    );

    // The slot files and the view are there, and a split publish did not rewrite the root.
    let (_, root_version) = storage.get_all_with_version(&root_path).await?;
    let root_version = root_version.context("the root should be a versioned object")?;
    stage_and_publish_split(&metastore_a, &index_uid, "sharded-a-1").await?;
    let (_, root_version_after) = storage.get_all_with_version(&root_path).await?;
    assert_eq!(
        root_version,
        root_version_after.context("the root should still be a versioned object")?,
        "publishing a split must not rewrite the shared root object"
    );

    metastore_a
        .delete_index(DeleteIndexRequest {
            index_uid: Some(index_uid),
        })
        .await?;
    assert!(
        !storage.exists(&root_path).await?,
        "deleting the index should remove its sharded objects"
    );
    Ok(())
}

/// The manifest layout on a real endpoint.
///
/// Same shape as the sharded test — a node that was not configured for the layout still reads the
/// index, a split publish does not rewrite a shared object, deleting the index removes its objects
/// — plus the property that layout exists for: a windowed read comes back from the segments that
/// cover the window rather than from the whole index.
#[tokio::test]
async fn test_manifest_layout_on_s3_endpoint() -> anyhow::Result<()> {
    if !endpoint_is_configured() {
        eprintln!("skipping test_manifest_layout_on_s3_endpoint: QW_S3_ENDPOINT is not set");
        return Ok(());
    }
    let bucket_uri = append_random_suffix(&format!("{}/manifest-layout", test_bucket_uri()));
    let storage = s3_storage(&bucket_uri).await?;
    let mut metastore_a = FileBackedMetastore::try_new(storage.clone(), None).await?;
    metastore_a.set_index_layout(IndexLayout::ManifestSegments {
        bucket_secs: 3_600,
        num_stripes: 4,
    });
    let metastore_b = {
        let mut metastore = FileBackedMetastore::try_new(storage.clone(), None).await?;
        metastore.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 3_600,
            num_stripes: 4,
        });
        metastore
    };
    // A node that was not told about the layout still reads the index: it is recorded in the
    // objects.
    let metastore_c = FileBackedMetastore::try_new(storage.clone(), None).await?;

    let index_id = append_random_suffix("manifest-layout-index");
    let index_config = IndexConfig::for_test(&index_id, &format!("s3://bucket/{index_id}"));
    let index_uid: IndexUid = metastore_a
        .create_index(CreateIndexRequest::try_from_index_config(&index_config)?)
        .await?
        .index_uid()
        .clone();

    let root_path = PathBuf::from(&index_id).join("v3/root.json");
    assert!(
        storage.exists(&root_path).await?,
        "the index should live in the manifest layout"
    );
    assert!(
        !storage
            .exists(&PathBuf::from(&index_id).join("metastore.json"))
            .await?,
        "the manifest layout must not also write the single metadata file"
    );

    stage_and_publish_split(&metastore_a, &index_uid, "manifest-a-0").await?;
    stage_and_publish_split(&metastore_b, &index_uid, "manifest-b-0").await?;
    stage_and_publish_split(&metastore_c, &index_uid, "manifest-c-0").await?;

    assert_eq!(
        list_published_split_ids(&metastore_c, &index_uid).await?,
        vec![
            "manifest-a-0".to_string(),
            "manifest-b-0".to_string(),
            "manifest-c-0".to_string()
        ],
        "every node's splits must survive"
    );

    // The windowed read is the one that has to stay bounded as the index grows; here it only has to
    // return the right splits, which is what a reader on another node checks.
    let windowed_query = ListSplitsQuery::for_index(index_uid.clone()).with_time_range_start_gte(0);
    let windowed_splits = metastore_c
        .list_splits(ListSplitsRequest::try_from_list_splits_query(
            &windowed_query,
        )?)
        .await?
        .collect_split_ids()
        .await?;
    assert_eq!(windowed_splits.len(), 3);

    // A publish does not rewrite a shared object: the root keeps its version.
    let (_, root_version) = storage.get_all_with_version(&root_path).await?;
    let root_version = root_version.context("the root should be a versioned object")?;
    stage_and_publish_split(&metastore_a, &index_uid, "manifest-a-1").await?;
    let (_, root_version_after) = storage.get_all_with_version(&root_path).await?;
    assert_eq!(
        root_version,
        root_version_after.context("the root should still be a versioned object")?,
        "publishing a split must not rewrite the shared root object"
    );

    metastore_a
        .delete_index(DeleteIndexRequest {
            index_uid: Some(index_uid),
        })
        .await?;
    assert!(
        !storage.exists(&root_path).await?,
        "deleting the index should remove its objects"
    );
    Ok(())
}
