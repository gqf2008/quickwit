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
    MetastoreServiceExt, MetastoreServiceStreamSplitsExt, SplitMetadata, SplitState,
    StageSplitsRequestExt,
};
use quickwit_proto::metastore::{
    CreateIndexRequest, DeleteIndexRequest, IndexMetadataRequest, LastDeleteOpstampRequest,
    ListDeleteTasksRequest, ListSplitsRequest, MetastoreService, PublishSplitsRequest,
    StageSplitsRequest,
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
    stage_and_publish_split_with_time_range(metastore, index_uid, split_id, 0..=99).await
}

async fn stage_and_publish_split_with_time_range(
    metastore: &FileBackedMetastore,
    index_uid: &IndexUid,
    split_id: &str,
    time_range: RangeInclusive<i64>,
) -> anyhow::Result<()> {
    let split_metadata = SplitMetadata {
        footer_offsets: 0..10,
        split_id: split_id.to_string().into(),
        num_docs: 1,
        time_range: Some(time_range),
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
    // The root alone is not the question: the layout also writes a view, one slot file per touched
    // slot and the segments they name. `exists(root)` passes while any of those stays behind, so
    // this walks the whole listing the way an operator auditing the bucket would.
    // Measured: dropping the view's delete from `delete_sharded_index` leaves the root check
    // green and this one red, naming `v2/splits/view.json`.
    assert_index_left_nothing_behind(&*storage, &index_id, "deleting a sharded index").await?;
    // And nothing the delete missed makes the index look alive to a node that starts afterwards,
    // with no cache to answer from. A node that already holds the index keeps answering until its
    // polling interval elapses — that is the read path's cache, not a leftover, so asserting on a
    // running node would be asserting the interval.
    let mut metastore_d = FileBackedMetastore::try_new(storage.clone(), None).await?;
    assert!(
        !metastore_d.index_exists(&index_id).await?,
        "a node that starts after the delete still sees the index: something was left behind"
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

    // A window that cannot overlap a split's time range must not return it: that pruning is what
    // this layout exists for, and doing it on a real bucket also exercises the segment and WAL
    // filtering.
    stage_and_publish_split_with_time_range(
        &metastore_a,
        &index_uid,
        "manifest-late",
        4_000_000_000..=4_000_000_060,
    )
    .await?;
    let early_window = ListSplitsQuery::for_index(index_uid.clone())
        .with_time_range_start_gte(0)
        .with_time_range_end_lt(3_600);
    let early_split_ids = metastore_c
        .list_splits(ListSplitsRequest::try_from_list_splits_query(
            &early_window,
        )?)
        .await?
        .collect_split_ids()
        .await?;
    assert!(
        !early_split_ids
            .iter()
            .any(|split_id| split_id.as_str() == "manifest-late"),
        "a split outside the query window must be pruned"
    );
    let late_window = ListSplitsQuery::for_index(index_uid.clone())
        .with_time_range_start_gte(4_000_000_000)
        .with_time_range_end_lt(4_000_003_600);
    let late_split_ids = metastore_c
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&late_window)?)
        .await?
        .collect_split_ids()
        .await?;
    assert_eq!(
        late_split_ids
            .iter()
            .filter(|split_id| split_id.as_str() == "manifest-late")
            .count(),
        1,
        "the split inside the window must be returned"
    );

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
    // Same question as the sharded test, with more objects to miss: the stripes, their WAL files,
    // the segments a fold wrote and the per-shard objects.
    assert_index_left_nothing_behind(&*storage, &index_id, "deleting a manifest index").await?;
    let mut metastore_d = FileBackedMetastore::try_new(storage.clone(), None).await?;
    assert!(
        !metastore_d.index_exists(&index_id).await?,
        "a node that starts after the delete still sees the index: something was left behind"
    );
    Ok(())
}

/// The fold path on a real endpoint. Folding is what writes the segments and what collects the ones
/// it supersedes, so it is the part of this layout that has to be seen against a bucket and not
/// only against RAM.
///
/// A stripe folds once its WAL tail holds a thousand operations, so one batch of a thousand splits
/// folds the single stripe of this index: the segment has to land under the stripe's own directory,
/// the splits have to be readable from it, and the folds that follow have to collect the
/// generations outside the grace period.
#[tokio::test]
async fn test_manifest_layout_fold_on_s3_endpoint() -> anyhow::Result<()> {
    if !endpoint_is_configured() {
        eprintln!("skipping test_manifest_layout_fold_on_s3_endpoint: QW_S3_ENDPOINT is not set");
        return Ok(());
    }
    let bucket_uri = append_random_suffix(&format!("{}/manifest-fold", test_bucket_uri()));
    let storage = s3_storage(&bucket_uri).await?;
    let mut metastore = FileBackedMetastore::try_new(storage.clone(), None).await?;
    // One stripe: every split of the batch hashes into it, so the batch itself is what makes the
    // stripe fold.
    metastore.set_index_layout(IndexLayout::ManifestSegments {
        bucket_secs: 3_600,
        num_stripes: 1,
    });
    let index_id = append_random_suffix("manifest-fold-index");
    let index_config = IndexConfig::for_test(&index_id, &format!("s3://bucket/{index_id}"));
    let index_uid = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config)?)
        .await?
        .index_uid()
        .clone();

    const ROUNDS: usize = 3;
    const SPLITS_PER_ROUND: usize = 1_000;
    for round in 0..ROUNDS {
        let splits_metadata: Vec<SplitMetadata> = (0..SPLITS_PER_ROUND)
            .map(|index| SplitMetadata {
                footer_offsets: 0..10,
                split_id: format!("manifest-fold-{round}-{index}").into(),
                num_docs: 1,
                time_range: Some(1_700_000_000..=1_700_000_060),
                ..Default::default()
            })
            .collect();
        let staged_split_ids: Vec<String> = splits_metadata
            .iter()
            .map(|split_metadata| split_metadata.split_id.to_string())
            .collect();
        metastore
            .stage_splits(StageSplitsRequest::try_from_splits_metadata(
                index_uid.clone(),
                splits_metadata,
            )?)
            .await?;
        metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids,
                ..Default::default()
            })
            .await?;
    }

    // The segments of the stripe, and only those: one per fold, and two kept once the collection
    // has run — the generations inside the grace period.
    let segments =
        object_snapshot(&*storage, Path::new(&format!("{index_id}/v3/segments"))).await?;
    assert!(
        segments
            .keys()
            .all(|path| path.starts_with(Path::new(&index_id).join("v3/segments/000"))),
        "a segment must live under the stripe that wrote it: {:?}",
        segments.keys()
    );
    assert_eq!(
        segments.len(),
        2,
        "the folds write one segment each, and the collection keeps the two the grace period \
         covers: {:?}",
        segments.keys()
    );
    // The WAL objects those folds took over are collected the same way.
    let wal_objects =
        object_snapshot(&*storage, Path::new(&format!("{index_id}/v3/wal-000"))).await?;
    assert!(
        wal_objects.len() <= 2,
        "the folded wal objects must be collected: {:?}",
        wal_objects.keys()
    );

    // Every split is still readable, from the segments the folds wrote.
    let window = ListSplitsQuery::for_index(index_uid.clone())
        .with_time_range_start_gte(1_699_999_000)
        .with_time_range_end_lt(1_700_010_000);
    let split_ids = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&window)?)
        .await?
        .collect_split_ids()
        .await?;
    assert_eq!(
        split_ids.len(),
        ROUNDS * SPLITS_PER_ROUND,
        "the splits must survive the folds"
    );
    // A window the splits cannot fall in is pruned from the segment's own time range.
    let other_window = ListSplitsQuery::for_index(index_uid.clone())
        .with_time_range_start_gte(0)
        .with_time_range_end_lt(3_600);
    let other_split_ids = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(
            &other_window,
        )?)
        .await?
        .collect_split_ids()
        .await?;
    assert!(
        other_split_ids.is_empty(),
        "a window outside the segment must be pruned"
    );

    metastore
        .delete_index(DeleteIndexRequest {
            index_uid: Some(index_uid),
        })
        .await?;
    // A fold leaves segments and collected WAL behind as well, so this is the delete to walk the
    // listing on: whatever the folds wrote has to go with the index.
    assert_index_left_nothing_behind(&*storage, &index_id, "deleting a folded manifest index")
        .await?;
    Ok(())
}

/// The sharded layout's fold, against a bucket rather than against RAM.
///
/// A slot folds once its file holds 512 entries, so an index created with a single slot folds on
/// its first batch of 600 splits: the segment has to land under that slot's own directory, the
/// splits have to stay readable from it, and the folds that follow have to collect the generations
/// outside the grace period. Folding is a compare-and-swap on the view and a delete of the
/// superseded segments, which is exactly the part that has to be seen against a bucket.
#[tokio::test]
async fn test_sharded_layout_fold_on_s3_endpoint() -> anyhow::Result<()> {
    if !endpoint_is_configured() {
        eprintln!("skipping test_sharded_layout_fold_on_s3_endpoint: QW_S3_ENDPOINT is not set");
        return Ok(());
    }
    let bucket_uri = append_random_suffix(&format!("{}/sharded-fold", test_bucket_uri()));
    let storage = s3_storage(&bucket_uri).await?;
    let mut metastore = FileBackedMetastore::try_new(storage.clone(), None).await?;
    // One slot: every split of a batch hashes into it, so the batch itself is what makes the slot
    // fold, the way one stripe does in the manifest layout's own fold test.
    metastore.set_index_layout(IndexLayout::Sharded { num_slots: 1 });
    let index_id = append_random_suffix("sharded-fold-index");
    let index_config = IndexConfig::for_test(&index_id, &format!("s3://bucket/{index_id}"));
    let index_uid = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config)?)
        .await?
        .index_uid()
        .clone();

    const ROUNDS: usize = 3;
    const SPLITS_PER_ROUND: usize = 600;
    for round in 0..ROUNDS {
        let splits_metadata: Vec<SplitMetadata> = (0..SPLITS_PER_ROUND)
            .map(|index| SplitMetadata {
                footer_offsets: 0..10,
                split_id: format!("sharded-fold-{round}-{index}").into(),
                num_docs: 1,
                time_range: Some(1_700_000_000..=1_700_000_060),
                ..Default::default()
            })
            .collect();
        let staged_split_ids: Vec<String> = splits_metadata
            .iter()
            .map(|split_metadata| split_metadata.split_id.to_string())
            .collect();
        metastore
            .stage_splits(StageSplitsRequest::try_from_splits_metadata(
                index_uid.clone(),
                splits_metadata,
            )?)
            .await?;
        metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids,
                ..Default::default()
            })
            .await?;
    }

    // The segments of that one slot, and only those: one per fold, two kept once the collection has
    // run — the generations the grace period covers.
    let segments = object_snapshot(
        &*storage,
        Path::new(&format!("{index_id}/v2/splits/segments/00000")),
    )
    .await?;
    assert_eq!(
        segments.len(),
        2,
        "the folds write one segment each, and the collection keeps the two the grace period \
         covers: {:?}",
        segments.keys()
    );
    // The view's bookmark for that slot records what the fold took over: the highest sequence it
    // folded and the version of the slot file it folded. A reader that lists that same version
    // skips the file without downloading it, which is why the file itself still holds the batch.
    let view = storage
        .get_all(Path::new(&format!("{index_id}/v2/splits/view.json")))
        .await?;
    let view: serde_json::Value = serde_json::from_slice(&view)?;
    let view_generation = view["generation"].as_u64().unwrap_or(0);
    assert!(
        view_generation >= 1,
        "the batches crossed the fold threshold, so the view moved on: generation \
         {view_generation}"
    );
    let bookmarks = view["slots"]
        .as_object()
        .expect("the view names the slots it folded");
    assert_eq!(
        bookmarks.len(),
        1,
        "only the slot that folded is in the view"
    );
    let bookmark = bookmarks.get("0").expect("slot 0 is the only slot");
    let folded_seq = bookmark["folded_seq"].as_u64().unwrap_or(0);
    assert!(
        // Staging a split writes one entry and publishing it another, so a batch of splits writes
        // two entries per split; what matters here is that the folds took over at least the
        // publishing of every one of them.
        folded_seq >= (ROUNDS * SPLITS_PER_ROUND) as u64,
        "every split the batches published has to be folded, and the folds reached {folded_seq}"
    );
    assert!(
        bookmark["segment"].as_str().is_some(),
        "the bookmark has to name the segment that holds the folded snapshot"
    );
    let slot_file = storage
        .get_all(Path::new(&format!("{index_id}/v2/splits/slots/00000.json")))
        .await?;
    let slot_file: serde_json::Value = serde_json::from_slice(&slot_file)?;
    let slot_file_generation = slot_file["base_generation"].as_u64().unwrap_or(u64::MAX);
    assert!(
        // The last write to the slot happened before the last fold took it over, so the file is
        // written against that view or an older one — never against a newer one, which is what
        // would make a reader's view stale.
        slot_file_generation <= view_generation,
        "the slot file was written against generation {slot_file_generation}, the view is at \
         {view_generation}"
    );

    // Every split is still readable, from the segments the folds wrote.
    let window = ListSplitsQuery::for_index(index_uid.clone())
        .with_time_range_start_gte(1_699_999_000)
        .with_time_range_end_lt(1_700_010_000);
    let split_ids = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&window)?)
        .await?
        .collect_split_ids()
        .await?;
    assert_eq!(
        split_ids.len(),
        ROUNDS * SPLITS_PER_ROUND,
        "the splits must survive the folds"
    );
    // A window the splits cannot fall in is filtered out by the split's own time range. (This
    // layout has no segment-level time pruning to test: the manifest layout's fold test asserts
    // that, and its segments carry a time range this one does not.)
    let other_window = ListSplitsQuery::for_index(index_uid.clone())
        .with_time_range_start_gte(0)
        .with_time_range_end_lt(3_600);
    let other_split_ids = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(
            &other_window,
        )?)
        .await?
        .collect_split_ids()
        .await?;
    assert!(
        other_split_ids.is_empty(),
        "a window outside the segment must be pruned"
    );

    metastore
        .delete_index(DeleteIndexRequest {
            index_uid: Some(index_uid),
        })
        .await?;
    assert_index_left_nothing_behind(&*storage, &index_id, "deleting a folded sharded index")
        .await?;
    Ok(())
}

/// What the reads that need no split data cost on a real endpoint, cold and warm.
///
/// The meta question behind the manifest layout is how much of an index a read has to touch. A
/// windowed query touches the window, which the other measurement covers; the metadata, the delete
/// tasks, the last delete opstamp and the shards have no window, and today they are served by
/// materialising the index — root, manifests, segments and WAL tail — which is what a node pays
/// once per reload and what a fresh node pays at startup. This measures that, cold (a metastore
/// that has just been built, as at startup) and warm (the same metastore again).
///
/// Opt-in with `QW_TEST_S3_MEASURE=1`, like the cost measurement next to it. `QW_TEST_S3_SPLITS`
/// sets how many splits the index holds (default 4 000, in batches of 1 000).
#[tokio::test]
async fn test_manifest_layout_metadata_read_cost_on_s3_endpoint() -> anyhow::Result<()> {
    if !endpoint_is_configured() || std::env::var("QW_TEST_S3_MEASURE").ok().as_deref() != Some("1")
    {
        eprintln!(
            "skipping test_manifest_layout_metadata_read_cost_on_s3_endpoint: set QW_S3_ENDPOINT \
             and QW_TEST_S3_MEASURE=1"
        );
        return Ok(());
    }
    let bucket_uri = append_random_suffix(&format!("{}/manifest-metadata-read", test_bucket_uri()));
    let storage = s3_storage(&bucket_uri).await?;
    let index_id = append_random_suffix("manifest-metadata-read-index");
    let index_config = IndexConfig::for_test(&index_id, &format!("s3://bucket/{index_id}"));
    let num_splits: usize = std::env::var("QW_TEST_S3_SPLITS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(4_000);
    let splits_per_batch = 1_000;
    let index_uid = {
        let mut metastore = FileBackedMetastore::try_new(storage.clone(), None).await?;
        metastore.set_index_layout(IndexLayout::ManifestSegments {
            bucket_secs: 3_600,
            num_stripes: 32,
        });
        let index_uid = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config)?)
            .await?
            .index_uid()
            .clone();
        // One hour of a day of splits, so a windowed read has something to prune.
        let now = 1_700_000_000i64;
        for batch_start in (0..num_splits).step_by(splits_per_batch) {
            let batch_end = (batch_start + splits_per_batch).min(num_splits);
            let splits_metadata: Vec<SplitMetadata> = (batch_start..batch_end)
                .map(|index| {
                    let start = now + (index as i64 % 24) * 3_600;
                    SplitMetadata {
                        footer_offsets: 0..10,
                        split_id: format!("split-{index}").into(),
                        num_docs: 1,
                        time_range: Some(start..=start + 60),
                        ..Default::default()
                    }
                })
                .collect();
            let staged_split_ids: Vec<String> = splits_metadata
                .iter()
                .map(|split_metadata| split_metadata.split_id.to_string())
                .collect();
            metastore
                .stage_splits(StageSplitsRequest::try_from_splits_metadata(
                    index_uid.clone(),
                    splits_metadata,
                )?)
                .await?;
            metastore
                .publish_splits(PublishSplitsRequest {
                    index_uid: Some(index_uid.clone()),
                    staged_split_ids,
                    ..Default::default()
                })
                .await?;
        }
        index_uid
    };
    let index_prefix = object_snapshot(&*storage, Path::new(&index_id)).await?;
    eprintln!(
        "manifest index with {num_splits} splits holds {} objects",
        index_prefix.len()
    );

    // A metastore that has just been built, as a node starting up has: nothing is cached.
    let metastore = FileBackedMetastore::try_new(storage.clone(), None).await?;
    let window = ListSplitsQuery::for_index(index_uid.clone())
        .with_time_range_start_gte(1_700_000_000)
        .with_time_range_end_lt(1_700_000_000 + 3_600);
    for round in ["cold", "warm"] {
        let started = std::time::Instant::now();
        metastore
            .index_metadata(IndexMetadataRequest {
                index_uid: Some(index_uid.clone()),
                ..Default::default()
            })
            .await?;
        let index_metadata = started.elapsed();
        let started = std::time::Instant::now();
        metastore
            .last_delete_opstamp(LastDeleteOpstampRequest {
                index_uid: Some(index_uid.clone()),
            })
            .await?;
        let last_delete_opstamp = started.elapsed();
        let started = std::time::Instant::now();
        metastore
            .list_delete_tasks(ListDeleteTasksRequest {
                index_uid: Some(index_uid.clone()),
                opstamp_start: 0,
            })
            .await?;
        let list_delete_tasks = started.elapsed();
        let started = std::time::Instant::now();
        let windowed_splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&window)?)
            .await?
            .collect_split_ids()
            .await?;
        let windowed_read = started.elapsed();
        eprintln!(
            "{round} reads on R2 ({num_splits} splits): index_metadata {index_metadata:?}, \
             last_delete_opstamp {last_delete_opstamp:?}, list_delete_tasks \
             {list_delete_tasks:?}, windowed list_splits {windowed_read:?} ({} splits)",
            windowed_splits.len()
        );
    }

    metastore
        .delete_index(DeleteIndexRequest {
            index_uid: Some(index_uid),
        })
        .await?;
    Ok(())
}

/// What the manifest layout costs on a real endpoint, in the units that do not lie about it: the
/// wall-clock of a publish and of a windowed read, and the bytes a publish writes.
///
/// Opt-in with `QW_TEST_S3_MEASURE=1`, and it says what the numbers mean: a publish is three
/// storage calls (read the manifest, write the WAL object, commit the manifest), so on a bucket
/// whose round trip is R a single writer publishes about once per 3R, and the layout wants its
/// nodes next to the bucket for the write rate a 5·10^12 documents/day index needs.
#[tokio::test]
async fn test_manifest_layout_cost_on_s3_endpoint() -> anyhow::Result<()> {
    if !endpoint_is_configured() || std::env::var("QW_TEST_S3_MEASURE").is_err() {
        eprintln!(
            "skipping test_manifest_layout_cost_on_s3_endpoint: QW_S3_ENDPOINT or \
             QW_TEST_S3_MEASURE is not set"
        );
        return Ok(());
    }
    let bucket_uri = append_random_suffix(&format!("{}/manifest-cost", test_bucket_uri()));
    let storage = s3_storage(&bucket_uri).await?;
    let mut metastore = FileBackedMetastore::try_new(storage.clone(), None).await?;
    let num_stripes: usize = std::env::var("QW_TEST_S3_STRIPES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(32);
    metastore.set_index_layout(IndexLayout::ManifestSegments {
        bucket_secs: 3_600,
        num_stripes,
    });
    let index_id = append_random_suffix("manifest-cost-index");
    let index_config = IndexConfig::for_test(&index_id, &format!("s3://bucket/{index_id}"));
    let index_uid: IndexUid = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config)?)
        .await?
        .index_uid()
        .clone();

    // Bytes a single publish writes, from the objects that changed.
    let mut publish_latencies = Vec::new();
    let mut bytes_written_per_publish = Vec::new();
    for round in 0..6 {
        let before = object_snapshot(&*storage, Path::new(&index_id)).await?;
        let split_id = format!("split-{round:03}");
        let start = std::time::Instant::now();
        stage_and_publish_split(&metastore, &index_uid, &split_id).await?;
        publish_latencies.push(start.elapsed());
        let after = object_snapshot(&*storage, Path::new(&index_id)).await?;
        let written: u64 = after
            .iter()
            .filter(|(path, (version, _))| {
                before
                    .get(*path)
                    .map(|(previous_version, _)| previous_version != version)
                    .unwrap_or(true)
            })
            .map(|(_, (_, size))| size)
            .sum();
        bytes_written_per_publish.push(written);
    }
    publish_latencies.sort();
    eprintln!(
        "publish one split on R2: p50 {:?}, p95 {:?}, {} bytes written per publish",
        publish_latencies[publish_latencies.len() / 2],
        publish_latencies[publish_latencies.len() * 95 / 100],
        bytes_written_per_publish.iter().sum::<u64>() / bytes_written_per_publish.len() as u64,
    );

    // A windowed read: what a search asks for.
    let query = ListSplitsQuery::for_index(index_uid.clone()).with_time_range_start_gte(0);
    let mut read_latencies = Vec::new();
    for _ in 0..4 {
        let start = std::time::Instant::now();
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query)?)
            .await?
            .collect_splits()
            .await?;
        read_latencies.push(start.elapsed());
        assert_eq!(splits.len(), 6);
    }
    read_latencies.sort();
    eprintln!(
        "list_splits on R2: p50 {:?}, p95 {:?}",
        read_latencies[read_latencies.len() / 2],
        read_latencies[read_latencies.len() * 95 / 100],
    );

    // Concurrent writers, the only place where contention shows up as something other than latency.
    // `QW_TEST_S3_WRITERS` drives the count so the stripe count can be checked against it: writers
    // that hash to the same stripe contend.
    //
    // Caveat on the reader's side of this number: the split ids here are `writer-N-M`, which do not
    // hash like real ULIDs, so the conflicts below are a lower bound on what a deployment with the
    // same writer count would see. The rule it checks (stripes at or above the writer count) is
    // what matters, and a deployment should recheck it with its own ids.
    let num_writers: usize = std::env::var("QW_TEST_S3_WRITERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(4);
    let publishes_per_writer = 5;
    let conflicts_before = metastore.cas_conflicts_total();
    let start = std::time::Instant::now();
    let mut handles = Vec::new();
    for writer in 0..num_writers {
        let metastore = metastore.clone();
        let index_uid = index_uid.clone();
        handles.push(tokio::spawn(async move {
            for round in 0..publishes_per_writer {
                stage_and_publish_split(
                    &metastore,
                    &index_uid,
                    &format!("writer-{writer}-{round}"),
                )
                .await
                .unwrap();
            }
        }));
    }
    futures::future::try_join_all(handles).await?;
    let elapsed = start.elapsed();
    let conflicts = metastore.cas_conflicts_total() - conflicts_before;
    let num_publishes = num_writers * publishes_per_writer;
    eprintln!(
        "{num_writers} writers x {publishes_per_writer} publishes on R2 ({num_stripes} stripes): \
         {num_publishes} publishes in {elapsed:?} ({:.2}/s), {conflicts} conflicts ({:.2} per \
         publish)",
        num_publishes as f64 / elapsed.as_secs_f64(),
        conflicts as f64 / num_publishes as f64,
    );

    metastore
        .delete_index(DeleteIndexRequest {
            index_uid: Some(index_uid),
        })
        .await?;
    Ok(())
}

/// Size and version of every object under `prefix`, keyed by path: the version tells what a step
/// rewrote, the size how much.
async fn object_snapshot(
    storage: &dyn Storage,
    prefix: &Path,
) -> anyhow::Result<std::collections::HashMap<PathBuf, (Option<String>, u64)>> {
    let mut snapshot = std::collections::HashMap::new();
    let mut pages = storage.list(prefix);
    while let Some(page) = futures::StreamExt::next(&mut pages).await {
        for metadata in page? {
            snapshot.insert(
                metadata.path,
                (
                    metadata.object_version.map(|version| version.to_string()),
                    metadata.size.as_u64(),
                ),
            );
        }
    }
    Ok(snapshot)
}

/// Every object under `prefix`, following the listing's pages. A real endpoint paginates, so an
/// assertion about what an index left behind has to walk the pages rather than look at one key.
async fn objects_under(storage: &dyn Storage, prefix: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let mut pages = storage.list(prefix);
    while let Some(page) = futures::StreamExt::next(&mut pages).await {
        for metadata in page? {
            paths.push(metadata.path);
        }
    }
    Ok(paths)
}

/// Asserts that deleting the index left nothing at all under `<index_id>/`.
///
/// [`Storage::exists`] answers for one key, so a delete that misses the view, a slot file, a
/// segment, a shard object or a WAL still passes an `exists(root)` check while the bucket keeps
/// paying for it. This walks the whole listing instead, the way an operator auditing the prefix
/// would, and names every object that is still there.
async fn assert_index_left_nothing_behind(
    storage: &dyn Storage,
    index_id: &str,
    what: &str,
) -> anyhow::Result<()> {
    let leftovers = objects_under(storage, Path::new(index_id)).await?;
    assert!(
        leftovers.is_empty(),
        "{what} left {} object(s) under `{index_id}/`: {leftovers:?}",
        leftovers.len()
    );
    Ok(())
}
