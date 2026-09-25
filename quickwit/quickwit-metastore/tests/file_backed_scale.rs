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

//! Scale test for the file-backed (object storage) metastore, the counterpart of
//! `postgres_scale.rs`.
//!
//! It answers the same question with the same workload — a windowed `list_splits`, which is what a
//! search asks for, and a publish while the index is that big — so the two backends can be compared
//! at the same size. The storage is RAM: what is measured is the metastore's own work (bytes read,
//! bytes written, time), not the network, which is what makes it comparable across backends.
//!
//! ```sh
//! export QW_TEST_SCALE_SPLITS=50000
//! cargo test -p quickwit-metastore --all-features --test file_backed_scale -- --nocapture
//! ```

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use quickwit_common::rand::append_random_suffix;
use quickwit_config::IndexConfig;
use quickwit_metastore::{
    CreateIndexRequestExt, FileBackedMetastore, IndexLayout, ListSplitsQuery, ListSplitsRequestExt,
    MetastoreServiceStreamSplitsExt, SplitMetadata, StageSplitsRequestExt,
};
use quickwit_proto::metastore::{
    CreateIndexRequest, ListSplitsRequest, MetastoreService, PublishSplitsRequest,
    StageSplitsRequest,
};
use quickwit_proto::types::{IndexUid, SplitId};
use quickwit_storage::{RamStorage, Storage};

const NUM_DAYS: i64 = 30;
const STAGE_BATCH_SIZE: usize = 2_000;

fn num_splits() -> usize {
    std::env::var("QW_TEST_SCALE_SPLITS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(50_000)
}

fn split_metadata(split_index: usize, num_splits: usize) -> SplitMetadata {
    let timestamp = 1_700_000_000 + (split_index as i64 * NUM_DAYS * 86_400) / (num_splits as i64);
    SplitMetadata {
        split_id: SplitId::from(format!("split-{split_index:09}")),
        time_range: Some(timestamp..=timestamp + 60),
        num_docs: 10_000_000,
        footer_offsets: 0..1_000_000,
        uncompressed_docs_size_in_bytes: 1_000_000_000,
        ..Default::default()
    }
}

/// Bytes of the objects under the index prefix: what the layout stores, and what a read has to
/// consider.
async fn index_bytes(storage: &dyn Storage, index_id: &str) -> u64 {
    let mut pages = storage.list(Path::new(index_id));
    let mut num_bytes = 0;
    while let Some(page) = futures::StreamExt::next(&mut pages).await {
        for metadata in page.unwrap() {
            num_bytes += metadata.size.as_u64();
        }
    }
    num_bytes
}

async fn measure_layout(index_layout: IndexLayout, layout_name: &str) {
    let num_splits = num_splits();
    let storage: Arc<dyn Storage> = Arc::new(RamStorage::default());
    let mut metastore = FileBackedMetastore::try_new(storage.clone(), None)
        .await
        .unwrap();
    metastore.set_distributed(true);
    metastore.set_index_layout(index_layout);
    let index_id = append_random_suffix("file-backed-scale");
    let index_config = IndexConfig::for_test(&index_id, "ram:///indexes/file-backed-scale");
    let index_uid: IndexUid = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();

    eprintln!("layout `{layout_name}`: staging {num_splits} splits");
    let start = Instant::now();
    for batch_start in (0..num_splits).step_by(STAGE_BATCH_SIZE) {
        let split_metadata_list: Vec<SplitMetadata> = (batch_start
            ..(batch_start + STAGE_BATCH_SIZE).min(num_splits))
            .map(|split_index| split_metadata(split_index, num_splits))
            .collect();
        let stage_splits_request =
            StageSplitsRequest::try_from_splits_metadata(index_uid.clone(), split_metadata_list)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();
    }
    let stage_elapsed = start.elapsed();
    eprintln!(
        "  staged in {:?} ({:.0} splits/s)",
        stage_elapsed,
        num_splits as f64 / stage_elapsed.as_secs_f64()
    );

    let start = Instant::now();
    for batch_start in (0..num_splits).step_by(STAGE_BATCH_SIZE) {
        let split_ids: Vec<String> = (batch_start
            ..(batch_start + STAGE_BATCH_SIZE).min(num_splits))
            .map(|split_index| format!("split-{split_index:09}"))
            .collect();
        metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: split_ids,
                ..Default::default()
            })
            .await
            .unwrap();
    }
    let publish_elapsed = start.elapsed();
    eprintln!(
        "  published in {:?} ({:.0} splits/s)",
        publish_elapsed,
        num_splits as f64 / publish_elapsed.as_secs_f64()
    );
    eprintln!(
        "  index metadata on storage: {} bytes",
        index_bytes(&*storage, &index_id).await
    );

    // The search-critical read: a query window that contains a fraction of the splits.
    let last_hour_start = 1_700_000_000 + NUM_DAYS * 86_400 - 3_600;
    let windowed_query =
        ListSplitsQuery::for_index(index_uid.clone()).with_time_range_start_gte(last_hour_start);
    let start = Instant::now();
    let windowed_splits = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&windowed_query).unwrap())
        .await
        .unwrap()
        .collect_splits()
        .await
        .unwrap();
    let windowed_elapsed = start.elapsed();
    eprintln!(
        "  list_splits(last hour): {} splits in {:?} ({} bytes of split metadata)",
        windowed_splits.len(),
        windowed_elapsed,
        serde_json::to_vec(&windowed_splits).unwrap().len()
    );

    // A publish while the index is that big.
    let extra_split_metadata = SplitMetadata {
        split_id: SplitId::from("split-extra".to_string()),
        time_range: Some(last_hour_start..=last_hour_start + 60),
        ..Default::default()
    };
    let stage_splits_request =
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &extra_split_metadata)
            .unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();
    let start = Instant::now();
    metastore
        .publish_splits(PublishSplitsRequest {
            index_uid: Some(index_uid),
            staged_split_ids: vec!["split-extra".to_string()],
            ..Default::default()
        })
        .await
        .unwrap();
    eprintln!(
        "  publish of one split at {num_splits} splits: {:?}",
        start.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_file_backed_metastore_scale() {
    // Opt-in: this one measures minutes of work, so it only runs when asked for a size.
    if std::env::var("QW_TEST_SCALE_SPLITS").is_err() {
        eprintln!(
            "skipping test_file_backed_metastore_scale: QW_TEST_SCALE_SPLITS is not set (e.g. \
             50000)"
        );
        return;
    }
    measure_layout(IndexLayout::SingleObject, "single object").await;
    measure_layout(
        IndexLayout::Sharded { num_slots: 256 },
        "sharded (256 slots)",
    )
    .await;
}
