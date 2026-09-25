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

//! Scale test for the PostgreSQL metastore: what the *search* path costs on an index that is big
//! enough for the question "can this hold 5·10^12 documents/day" to be about more than arithmetic.
//!
//! The search path asks the metastore for the splits overlapping the query's time window
//! (`quickwit-search/src/lib.rs`, `query.with_time_range_start_gte(..)`), so that is what this test
//! measures: how long a windowed `list_splits` takes, how many rows and bytes it moves, and how
//! long a publish takes while the index is that big.
//!
//! It only runs when `QW_TEST_POSTGRES_URI` is set, and it skips itself loudly otherwise so it can
//! never become a silent no-op.
//!
//! ```sh
//! export QW_TEST_POSTGRES_URI="postgres://quickwit@127.0.0.1:5433/quickwit_scale"
//! export QW_TEST_POSTGRES_SPLITS=1000000
//! cargo test -p quickwit-metastore --features postgres --test postgres_scale -- --nocapture
//! ```

#![cfg(feature = "postgres")]

use std::str::FromStr;
use std::time::{Duration, Instant};

use futures::TryStreamExt;
use quickwit_common::rand::append_random_suffix;
use quickwit_common::uri::Uri;
use quickwit_config::{IndexConfig, PostgresMetastoreConfig};
use quickwit_metastore::{
    CreateIndexRequestExt, IndexMetadataResponseExt, ListSplitsQuery, ListSplitsRequestExt,
    ListSplitsResponseExt, MetastoreServiceStreamSplitsExt, PostgresqlMetastore, SplitMetadata,
    StageSplitsRequestExt,
};
use quickwit_proto::metastore::{
    CreateIndexRequest, DeleteIndexRequest, IndexMetadataRequest, ListSplitsRequest,
    MetastoreService, PublishSplitsRequest, StageSplitsRequest,
};
use quickwit_proto::types::{IndexUid, SplitId};

const NUM_DAYS: i64 = 30;
const STAGE_BATCH_SIZE: usize = 2_000;
const PUBLISH_BATCH_SIZE: usize = 2_000;

fn postgres_uri() -> Option<Uri> {
    let uri = std::env::var("QW_TEST_POSTGRES_URI").ok()?;
    Some(Uri::from_str(&uri).expect("QW_TEST_POSTGRES_URI should parse as a URI"))
}

fn num_splits() -> usize {
    std::env::var("QW_TEST_POSTGRES_SPLITS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200_000)
}

/// Reruns the measurements against an index a previous run left behind, instead of seeding a new
/// one: seeding a million splits takes ten minutes, measuring them takes seconds.
fn existing_index_id() -> Option<String> {
    std::env::var("QW_TEST_POSTGRES_INDEX_ID").ok()
}

/// Splits spread evenly over `NUM_DAYS`, one per second of that window.
fn split_metadata(split_index: usize) -> SplitMetadata {
    let timestamp =
        1_700_000_000 + (split_index as i64 * NUM_DAYS * 86_400) / (num_splits() as i64);
    SplitMetadata {
        split_id: SplitId::from(format!("split-{split_index:09}")),
        time_range: Some(timestamp..=timestamp + 60),
        num_docs: 10_000_000,
        footer_offsets: 0..1_000_000,
        uncompressed_docs_size_in_bytes: 1_000_000_000,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_postgres_metastore_scale() -> anyhow::Result<()> {
    let Some(postgres_uri) = postgres_uri() else {
        eprintln!("skipping test_postgres_metastore_scale: QW_TEST_POSTGRES_URI is not set");
        return Ok(());
    };
    let metastore = PostgresqlMetastore::new(&PostgresMetastoreConfig::default(), &postgres_uri)
        .await
        .unwrap();
    let (index_id, index_uid, num_splits) = match existing_index_id() {
        Some(index_id) => {
            let index_uid = metastore
                .index_metadata(IndexMetadataRequest::for_index_id(index_id.clone()))
                .await?
                .deserialize_index_metadata()?
                .index_uid
                .clone();
            eprintln!("index `{index_id}`: measuring the splits left by a previous run");
            let num_splits = index_uid_seeded_split_count(&metastore, &index_uid).await?;
            (index_id, index_uid, num_splits)
        }
        None => {
            let index_id = append_random_suffix("scale-test");
            let index_config = IndexConfig::for_test(&index_id, "ram:///indexes/scale-test");
            let index_uid: IndexUid = metastore
                .create_index(CreateIndexRequest::try_from_index_config(&index_config)?)
                .await?
                .index_uid()
                .clone();
            let num_splits = num_splits();
            eprintln!("index `{index_id}`: staging {num_splits} splits");
            (index_id, index_uid, num_splits)
        }
    };
    let seeded_splits = index_uid_seeded_split_count(&metastore, &index_uid).await?;
    if seeded_splits < num_splits {
        stage_and_publish_splits(&metastore, &index_uid, seeded_splits, num_splits).await?;
    }

    // What a search for the last hour of the index costs.
    let last_hour_start = 1_700_000_000 + NUM_DAYS * 86_400 - 3_600;
    let windowed_query =
        ListSplitsQuery::for_index(index_uid.clone()).with_time_range_start_gte(last_hour_start);
    let start = Instant::now();
    let windowed_splits = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(
            &windowed_query,
        )?)
        .await?
        .collect_splits()
        .await?;
    let windowed_elapsed = start.elapsed();
    let windowed_bytes = serde_json::to_vec(&windowed_splits)?.len();
    eprintln!(
        "  list_splits(last hour): {} splits in {:?} ({} bytes of split metadata)",
        windowed_splits.len(),
        windowed_elapsed,
        windowed_bytes
    );

    // What a search over the whole index costs: the streaming path must not have to hold it all.
    let full_query = ListSplitsQuery::for_index(index_uid.clone());
    let start = Instant::now();
    let mut num_full_splits = 0;
    let mut first_chunk_elapsed = Duration::ZERO;
    let stream = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&full_query)?)
        .await?;
    let mut stream = Box::pin(stream);
    loop {
        let chunk_start = Instant::now();
        let Some(list_splits_response) = stream.try_next().await? else {
            break;
        };
        let chunk = list_splits_response.deserialize_splits().await?;
        if num_full_splits == 0 {
            first_chunk_elapsed = chunk_start.elapsed();
        }
        num_full_splits += chunk.len();
    }
    let full_elapsed = start.elapsed();
    eprintln!(
        "  list_splits(whole index): {num_full_splits} splits in {:?}, first chunk in {:?}",
        full_elapsed, first_chunk_elapsed
    );

    // A publish while the index is that big.
    let extra_split_metadata = SplitMetadata {
        split_id: SplitId::from(format!("split-extra-{}", std::process::id())),
        time_range: Some(last_hour_start..=last_hour_start + 60),
        ..Default::default()
    };
    let stage_splits_request =
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &extra_split_metadata)?;
    metastore.stage_splits(stage_splits_request).await?;
    let start = Instant::now();
    metastore
        .publish_splits(PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![extra_split_metadata.split_id.to_string()],
            ..Default::default()
        })
        .await?;
    let extra_publish_elapsed = start.elapsed();
    eprintln!("  publish of one split at {num_splits} splits: {extra_publish_elapsed:?}");

    // The last hour of a window spread evenly over 30 days is 1/720 of the splits; the filter is
    // "splits whose end is at or after the window start", so it also picks up the few that overlap
    // the beginning of the window.
    let expected_windowed = num_splits / (NUM_DAYS as usize * 24);
    assert!(
        windowed_splits.len().abs_diff(expected_windowed) <= 50,
        "expected about {expected_windowed} splits in the last hour, got {}",
        windowed_splits.len()
    );
    assert!(num_full_splits >= num_splits);

    if existing_index_id().is_none() {
        metastore
            .delete_index(DeleteIndexRequest {
                index_uid: Some(index_uid),
            })
            .await?;
    }
    let _ = index_id;
    Ok(())
}

async fn index_uid_seeded_split_count(
    metastore: &PostgresqlMetastore,
    index_uid: &IndexUid,
) -> anyhow::Result<usize> {
    let query = ListSplitsQuery::for_index(index_uid.clone());
    let num_splits = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&query)?)
        .await?
        .collect_splits()
        .await?
        .len();
    Ok(num_splits)
}

async fn stage_and_publish_splits(
    metastore: &PostgresqlMetastore,
    index_uid: &IndexUid,
    _from_split_index: usize,
    num_splits: usize,
) -> anyhow::Result<()> {
    let start = Instant::now();
    for batch_start in (0..num_splits).step_by(STAGE_BATCH_SIZE) {
        let split_metadata_list: Vec<SplitMetadata> = (batch_start
            ..(batch_start + STAGE_BATCH_SIZE).min(num_splits))
            .map(split_metadata)
            .collect();
        let stage_splits_request =
            StageSplitsRequest::try_from_splits_metadata(index_uid.clone(), split_metadata_list)?;
        metastore.stage_splits(stage_splits_request).await?;
    }
    let stage_elapsed = start.elapsed();
    eprintln!(
        "  staged in {:?} ({:.0} splits/s, batches of {STAGE_BATCH_SIZE})",
        stage_elapsed,
        num_splits as f64 / stage_elapsed.as_secs_f64()
    );

    let start = Instant::now();
    for batch_start in (0..num_splits).step_by(PUBLISH_BATCH_SIZE) {
        let split_ids: Vec<String> = (batch_start
            ..(batch_start + PUBLISH_BATCH_SIZE).min(num_splits))
            .map(|split_index| format!("split-{split_index:09}"))
            .collect();
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: split_ids,
            ..Default::default()
        };
        metastore.publish_splits(publish_splits_request).await?;
    }
    let publish_elapsed = start.elapsed();
    eprintln!(
        "  published in {:?} ({:.0} splits/s, batches of {PUBLISH_BATCH_SIZE})",
        publish_elapsed,
        num_splits as f64 / publish_elapsed.as_secs_f64()
    );

    Ok(())
}
