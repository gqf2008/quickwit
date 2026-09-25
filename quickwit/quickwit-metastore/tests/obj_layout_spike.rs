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

//! Does an object-store metastore have to cost `O(splits in the index)`? Measured answer.
//!
//! The file-backed metastore puts every split of an index in one object, so a read materialises the
//! whole index and every mutation reloads it. The neighbouring project `objsearch` solves the same
//! problem on the same kind of storage with a different shape: the mutable object only holds
//! *references* to immutable segments (`src/manifest.rs`), a write appends an immutable WAL object
//! and bumps that manifest, and a read fetches the manifest plus the segments it needs
//! (`src/engine.rs`, SPEC §5.1-5.4, §7.4).
//!
//! This test applies that shape to *split metadata* and measures it at the scale the questions are
//! about: an index of one million splits spread over 30 days, the search path's windowed read
//! (`list_splits` for the last hour, which is what `quickwit-search` asks for), and a publish.
//! Storage is RAM and every call is counted, so the numbers are storage work — round trips and
//! bytes — which is what transfers to a real bucket.
//!
//! ```sh
//! export QW_TEST_OBJ_LAYOUT_SPLITS=1000000
//! cargo test -p quickwit-metastore --all-features --test obj_layout_spike -- --nocapture
//! ```

#![cfg(feature = "testsuite")]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use quickwit_proto::metastore::{MetastoreError, serde_utils};
use quickwit_storage::{RamStorage, Storage};
use serde::{Deserialize, Serialize};

const NUM_DAYS: i64 = 30;
const BUCKET_SECS: i64 = 3_600;
const SPLITS_PER_PUBLISH: usize = 2_000;
const BASE_TIMESTAMP: i64 = 1_700_000_000;

/// Counters for the storage calls the layout makes. The spike counts explicitly instead of
/// wrapping the storage: the `Storage` trait's `copy_to` takes a `SendableAsync` that is not
/// re-exported outside the storage crate, so a wrapper cannot be implemented from here.
#[derive(Debug, Default)]
struct Counter {
    round_trips: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
}

impl Counter {
    fn reset(&self) {
        self.round_trips.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
    }

    fn round_trips(&self) -> u64 {
        self.round_trips.load(Ordering::Relaxed)
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }
}

type SpikeResult<T> = quickwit_proto::metastore::MetastoreResult<T>;

async fn storage_put(
    storage: &RamStorage,
    counter: &Counter,
    path: &str,
    body: Vec<u8>,
) -> SpikeResult<()> {
    counter.round_trips.fetch_add(1, Ordering::Relaxed);
    counter
        .bytes_written
        .fetch_add(body.len() as u64, Ordering::Relaxed);
    storage
        .put(Path::new(path), Box::new(body))
        .await
        .map_err(|error| MetastoreError::Internal {
            message: "put failed".to_string(),
            cause: error.to_string(),
        })
}

async fn storage_put_if_matches(
    storage: &RamStorage,
    counter: &Counter,
    path: &str,
    body: Vec<u8>,
    version: &quickwit_storage::ObjectVersion,
) -> SpikeResult<()> {
    counter.round_trips.fetch_add(1, Ordering::Relaxed);
    counter
        .bytes_written
        .fetch_add(body.len() as u64, Ordering::Relaxed);
    storage
        .put_if_version_matches(Path::new(path), Box::new(body), version)
        .await
        .map_err(|error| MetastoreError::Internal {
            message: "compare-and-swap failed".to_string(),
            cause: error.to_string(),
        })?;
    Ok(())
}

async fn storage_get(
    storage: &RamStorage,
    counter: &Counter,
    path: &str,
) -> SpikeResult<quickwit_storage::OwnedBytes> {
    counter.round_trips.fetch_add(1, Ordering::Relaxed);
    let bytes =
        storage
            .get_all(Path::new(path))
            .await
            .map_err(|error| MetastoreError::Internal {
                message: "get failed".to_string(),
                cause: error.to_string(),
            })?;
    counter
        .bytes_read
        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
    Ok(bytes)
}

async fn storage_get_with_version(
    storage: &RamStorage,
    counter: &Counter,
    path: &str,
) -> SpikeResult<(
    quickwit_storage::OwnedBytes,
    quickwit_storage::ObjectVersion,
)> {
    counter.round_trips.fetch_add(1, Ordering::Relaxed);
    let (bytes, version_opt) = storage
        .get_all_with_version(Path::new(path))
        .await
        .map_err(|error| MetastoreError::Internal {
            message: "versioned get failed".to_string(),
            cause: error.to_string(),
        })?;
    counter
        .bytes_read
        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
    let version = version_opt.ok_or_else(|| MetastoreError::Internal {
        message: "the spike needs versioned objects".to_string(),
        cause: "storage returned no version".to_string(),
    })?;
    Ok((bytes, version))
}

/// One split, in the shape the metastore already has to serialise.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SpikeSplit {
    split_id: String,
    time_range_start: i64,
    time_range_end: i64,
}

/// The mutable commit point: references to segments and to the WAL tail, never the splits.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct SpikeManifest {
    format_version: u32,
    epoch: u64,
    /// One segment per time bucket, rewritten only when that bucket is folded.
    segments: BTreeMap<i64, String>,
    /// WAL objects not yet folded, oldest first.
    wal: Vec<String>,
    next_seq: u64,
}

#[derive(Serialize, Deserialize)]
struct SpikeSegment {
    bucket: i64,
    splits: Vec<SpikeSplit>,
}

fn bucket_of(timestamp: i64) -> i64 {
    timestamp / BUCKET_SECS
}

fn split_metadata(split_index: usize, num_splits: usize) -> SpikeSplit {
    let time_range_start =
        BASE_TIMESTAMP + (split_index as i64 * NUM_DAYS * 86_400) / (num_splits as i64);
    SpikeSplit {
        split_id: format!("split-{split_index:09}"),
        time_range_start,
        time_range_end: time_range_start + 60,
    }
}

fn index_prefix() -> &'static str {
    "spike-index/v3"
}

fn manifest_path() -> String {
    format!("{}/manifest.json", index_prefix())
}

fn wal_path(seq: u64) -> String {
    format!("{}/wal/{seq:020}.json", index_prefix())
}

fn segment_path(bucket: i64) -> String {
    format!("{}/segments/{bucket:012}.json", index_prefix())
}

async fn read_manifest(
    storage: &RamStorage,
    counter: &Counter,
) -> SpikeResult<(SpikeManifest, quickwit_storage::ObjectVersion)> {
    let (bytes, version) = storage_get_with_version(storage, counter, &manifest_path()).await?;
    let manifest: SpikeManifest = serde_utils::from_json_bytes(&bytes)?;
    Ok((manifest, version))
}

/// Publishes a batch of splits: one immutable WAL object, one manifest compare-and-swap.
async fn publish(
    storage: &RamStorage,
    counter: &Counter,
    splits: &[SpikeSplit],
) -> SpikeResult<()> {
    let (mut manifest, version) = read_manifest(storage, counter).await?;
    let seq = manifest.next_seq;
    storage_put(
        storage,
        counter,
        &wal_path(seq),
        serde_utils::to_json_bytes(&splits)?,
    )
    .await?;
    manifest.next_seq += 1;
    manifest.wal.push(wal_path(seq));
    manifest.epoch += 1;
    storage_put_if_matches(
        storage,
        counter,
        &manifest_path(),
        serde_utils::to_json_bytes(&manifest)?,
        &version,
    )
    .await
}

/// Folds the WAL tail into one segment per time bucket, then commits once.
async fn fold(storage: &RamStorage, counter: &Counter) -> SpikeResult<()> {
    let (mut manifest, version) = read_manifest(storage, counter).await?;
    let mut per_bucket: BTreeMap<i64, Vec<SpikeSplit>> = BTreeMap::new();
    for wal_key in manifest.wal.clone() {
        let bytes = storage_get(storage, counter, &wal_key).await?;
        let splits: Vec<SpikeSplit> = serde_utils::from_json_bytes(&bytes)?;
        for split in splits {
            per_bucket
                .entry(bucket_of(split.time_range_start))
                .or_default()
                .push(split);
        }
    }
    for (bucket, new_splits) in per_bucket {
        let mut splits = match manifest.segments.get(&bucket) {
            Some(segment_key) => {
                let bytes = storage_get(storage, counter, segment_key).await?;
                serde_utils::from_json_bytes::<SpikeSegment>(&bytes)?.splits
            }
            None => Vec::new(),
        };
        splits.extend(new_splits);
        let segment = SpikeSegment { bucket, splits };
        storage_put(
            storage,
            counter,
            &segment_path(bucket),
            serde_utils::to_json_bytes(&segment)?,
        )
        .await?;
        manifest.segments.insert(bucket, segment_path(bucket));
    }
    manifest.wal.clear();
    manifest.epoch += 1;
    storage_put_if_matches(
        storage,
        counter,
        &manifest_path(),
        serde_utils::to_json_bytes(&manifest)?,
        &version,
    )
    .await
}

/// The search path's read: everything overlapping `[from, to)`.
async fn list_splits(
    storage: &RamStorage,
    counter: &Counter,
    from: i64,
    to: i64,
) -> SpikeResult<Vec<SpikeSplit>> {
    let (manifest, _) = read_manifest(storage, counter).await?;
    let mut splits = Vec::new();
    let first_bucket = bucket_of(from);
    let last_bucket = bucket_of(to - 1);
    for (bucket, segment_key) in manifest.segments.clone() {
        if bucket < first_bucket || bucket > last_bucket {
            continue;
        }
        let bytes = storage_get(storage, counter, &segment_key).await?;
        let segment: SpikeSegment = serde_utils::from_json_bytes(&bytes)?;
        splits.extend(
            segment
                .splits
                .into_iter()
                .filter(|split| split.time_range_end >= from && split.time_range_start < to),
        );
    }
    for wal_key in manifest.wal.clone() {
        let bytes = storage_get(storage, counter, &wal_key).await?;
        let wal_splits: Vec<SpikeSplit> = serde_utils::from_json_bytes(&bytes)?;
        splits.extend(
            wal_splits
                .into_iter()
                .filter(|split| split.time_range_end >= from && split.time_range_start < to),
        );
    }
    Ok(splits)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_split_metadata_as_manifest_plus_segments() {
    // Opt-in: the shape measurement takes a few seconds and exists to be run on purpose.
    let Some(num_splits) = std::env::var("QW_TEST_OBJ_LAYOUT_SPLITS")
        .ok()
        .and_then(|value| value.parse().ok())
    else {
        eprintln!(
            "skipping test_split_metadata_as_manifest_plus_segments: QW_TEST_OBJ_LAYOUT_SPLITS is \
             not set (e.g. 1000000)"
        );
        return;
    };
    let storage = RamStorage::default();
    let counter = Counter::default();
    // The manifest is created first, empty.
    storage_put(
        &storage,
        &counter,
        &manifest_path(),
        serde_utils::to_json_bytes(&SpikeManifest::default()).unwrap(),
    )
    .await
    .unwrap();

    let publish_start = Instant::now();
    let mut num_published = 0;
    for batch_start in (0..num_splits).step_by(SPLITS_PER_PUBLISH) {
        let splits: Vec<SpikeSplit> = (batch_start
            ..(batch_start + SPLITS_PER_PUBLISH).min(num_splits))
            .map(|split_index| split_metadata(split_index, num_splits))
            .collect();
        publish(&storage, &counter, &splits).await.unwrap();
        num_published += splits.len();
        if num_published % (SPLITS_PER_PUBLISH * 10) == 0 {
            fold(&storage, &counter).await.unwrap();
        }
    }
    fold(&storage, &counter).await.unwrap();
    let publish_elapsed = publish_start.elapsed();
    eprintln!(
        "seeded {num_published} splits in {:?} ({:.0} splits/s)",
        publish_elapsed,
        num_published as f64 / publish_elapsed.as_secs_f64()
    );

    let (manifest, _) = read_manifest(&storage, &counter).await.unwrap();
    let manifest_bytes = storage_get(&storage, &counter, &manifest_path())
        .await
        .unwrap()
        .len();
    eprintln!(
        "manifest: {} segments, {} bytes ({:.1} bytes per segment)",
        manifest.segments.len(),
        manifest_bytes,
        manifest_bytes as f64 / manifest.segments.len() as f64
    );

    // The search path's read: the last hour of a 30-day index.
    let last_hour_start = BASE_TIMESTAMP + NUM_DAYS * 86_400 - 3_600;
    counter.reset();
    let start = Instant::now();
    let windowed_splits = list_splits(&storage, &counter, last_hour_start, last_hour_start + 3_600)
        .await
        .unwrap();
    let windowed_elapsed = start.elapsed();
    let windowed_round_trips = counter.round_trips();
    eprintln!(
        "list_splits(last hour): {} splits, {} round trips, {} bytes read, {:?}",
        windowed_splits.len(),
        counter.round_trips(),
        counter.bytes_read(),
        windowed_elapsed
    );

    // A publish, one split.
    counter.reset();
    let start = Instant::now();
    let extra_split = split_metadata(0, 1);
    publish(&storage, &counter, std::slice::from_ref(&extra_split))
        .await
        .unwrap();
    eprintln!(
        "publish(one split): {} round trips, {} bytes written, {:?}",
        counter.round_trips(),
        counter.bytes_written(),
        start.elapsed()
    );

    // The read still sees the just-published split, from the WAL tail: strong consistency without
    // rewriting anything.
    counter.reset();
    let start = Instant::now();
    let after = list_splits(&storage, &counter, 0, i64::MAX).await.unwrap();
    eprintln!(
        "list_splits(whole index): {} splits, {} round trips (one GET per segment; a client would \
         batch these, like objsearch's get_many), {:?}",
        after.len(),
        counter.round_trips(),
        start.elapsed()
    );
    assert_eq!(after.len(), num_splits + 1);
    // One hour of a thirty-day window is 1/720 of the splits, plus the ones that overlap the
    // beginning of the window.
    let expected_windowed = num_splits / (NUM_DAYS as usize * 24);
    assert!(
        windowed_splits.len().abs_diff(expected_windowed) <= 40,
        "expected about {expected_windowed} splits in the last hour, got {}",
        windowed_splits.len()
    );
    assert!(
        windowed_round_trips <= 4,
        "a windowed read must not depend on the number of segments: {windowed_round_trips} round \
         trips for {} segments",
        manifest.segments.len()
    );
}

/// Can one manifest compare-and-swap carry the write rate a 5·10¹² documents/day index needs?
///
/// An object store has no server to serialise writers, so the only ordering primitive is the
/// compare-and-swap on the manifest. Concurrent writers will conflict; the question is whether the
/// retries stay bounded at the rate the workload actually needs (~11.6 publishes/s at
/// 5·10¹² documents/day), and whether striping the manifest across writers fixes it if they do not.
///
/// `QW_TEST_OBJ_RTT_MS` adds an artificial round trip to every storage call (an in-region bucket is
/// tens of milliseconds; the cross-region one measured earlier is ~810 ms), `QW_TEST_OBJ_RATE` sets
/// the offered publishes/s, and `QW_TEST_OBJ_STRIPES` the number of manifests a writer may choose
/// from (1 = objsearch's single manifest per namespace).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_manifest_compare_and_swap_under_concurrent_writers() {
    if std::env::var("QW_TEST_OBJ_RATE").is_err() {
        eprintln!(
            "skipping test_manifest_compare_and_swap_under_concurrent_writers: QW_TEST_OBJ_RATE \
             is not set (e.g. 12 for the rate 5e12 documents/day needs)"
        );
        return;
    }
    let num_writers: usize = env_usize("QW_TEST_OBJ_WRITERS", 5);
    let rate_per_sec: usize = env_usize("QW_TEST_OBJ_RATE", 12);
    let stripes: usize = env_usize("QW_TEST_OBJ_STRIPES", 1);
    let duration = std::time::Duration::from_secs(env_usize("QW_TEST_OBJ_SECS", 10) as u64);
    let round_trip = std::time::Duration::from_millis(env_usize("QW_TEST_OBJ_RTT_MS", 20) as u64);
    let storage = Arc::new(DelayedRamStorage {
        inner: RamStorage::default(),
        round_trip,
        conflicts: AtomicU64::new(0),
        exhausted: AtomicU64::new(0),
        calls: AtomicU64::new(0),
    });
    for stripe in 0..stripes {
        storage
            .inner
            .put(
                Path::new(&manifest_path_for(stripe)),
                Box::new(serde_utils::to_json_bytes(&SpikeManifest::default()).unwrap()),
            )
            .await
            .unwrap();
    }

    let per_writer_interval =
        std::time::Duration::from_secs_f64(num_writers as f64 / rate_per_sec as f64);
    let start = Instant::now();
    let mut handles = Vec::new();
    for writer in 0..num_writers {
        let storage = storage.clone();
        handles.push(tokio::spawn(async move {
            let stripe = if stripes == 1 { 0 } else { writer % stripes };
            let mut published = 0;
            let mut next_publish = Instant::now();
            while start.elapsed() < duration {
                let split = SpikeSplit {
                    split_id: format!("writer-{writer}-split-{published:08}"),
                    time_range_start: BASE_TIMESTAMP + published as i64,
                    time_range_end: BASE_TIMESTAMP + published as i64 + 60,
                };
                let mut published_this_one = false;
                for attempt in 0..16 {
                    match publish_delayed(&storage, stripe, std::slice::from_ref(&split)).await {
                        Ok(()) => {
                            published_this_one = true;
                            break;
                        }
                        Err(ContentionError::Conflict) if attempt < 15 => {
                            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        }
                        Err(_) => break,
                    }
                }
                if published_this_one {
                    published += 1;
                } else {
                    storage.exhausted.fetch_add(1, Ordering::Relaxed);
                }
                next_publish += per_writer_interval;
                let now = Instant::now();
                if next_publish > now {
                    tokio::time::sleep(next_publish - now).await;
                } else {
                    next_publish = now;
                }
            }
            published
        }));
    }
    let published: Vec<usize> = futures::future::try_join_all(handles).await.unwrap();
    let elapsed = start.elapsed();
    let total_published: u64 = published.iter().map(|count| *count as u64).sum();
    let calls = storage.calls.load(Ordering::Relaxed);
    eprintln!(
        "{num_writers} writers, {stripes} manifest(s), offered {rate_per_sec}/s, {round_trip:?} \
         round trip, {:?}: {total_published} published ({:.1}/s), {} conflicts, {} exhausted, \
         {:.2} storage calls per publish",
        elapsed,
        total_published as f64 / elapsed.as_secs_f64(),
        storage.conflicts.load(Ordering::Relaxed),
        storage.exhausted.load(Ordering::Relaxed),
        calls as f64 / total_published.max(1) as f64,
    );
    assert_eq!(
        storage.exhausted.load(Ordering::Relaxed),
        0,
        "a writer ran out of its replay budget"
    );
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[derive(Debug)]
enum ContentionError {
    Conflict,
    Fatal(String),
}

impl std::fmt::Display for ContentionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContentionError::Conflict => write!(f, "conflict"),
            ContentionError::Fatal(message) => write!(f, "{message}"),
        }
    }
}

/// The real storage with an artificial round trip and conflict counters, so the contention
/// experiment can run at a realistic latency without waiting for a real bucket.
struct DelayedRamStorage {
    inner: RamStorage,
    round_trip: std::time::Duration,
    conflicts: AtomicU64,
    exhausted: AtomicU64,
    calls: AtomicU64,
}

async fn publish_delayed(
    storage: &DelayedRamStorage,
    stripe: usize,
    splits: &[SpikeSplit],
) -> Result<(), ContentionError> {
    storage.calls.fetch_add(1, Ordering::Relaxed);
    tokio::time::sleep(storage.round_trip).await;
    let (bytes, version_opt) = storage
        .inner
        .get_all_with_version(Path::new(&manifest_path_for(stripe)))
        .await
        .map_err(|error| ContentionError::Fatal(error.to_string()))?;
    let version = version_opt
        .ok_or_else(|| ContentionError::Fatal("the spike needs versioned objects".to_string()))?;
    let mut manifest: SpikeManifest = serde_utils::from_json_bytes(&bytes)
        .map_err(|error| ContentionError::Fatal(error.to_string()))?;
    let seq = manifest.next_seq;
    storage.calls.fetch_add(1, Ordering::Relaxed);
    tokio::time::sleep(storage.round_trip).await;
    storage
        .inner
        .put(
            Path::new(&wal_path_for(stripe, seq)),
            Box::new(
                serde_utils::to_json_bytes(&splits)
                    .map_err(|error| ContentionError::Fatal(error.to_string()))?,
            ),
        )
        .await
        .map_err(|error| ContentionError::Fatal(error.to_string()))?;
    manifest.next_seq += 1;
    manifest.wal.push(wal_path_for(stripe, seq));
    manifest.epoch += 1;
    storage.calls.fetch_add(1, Ordering::Relaxed);
    tokio::time::sleep(storage.round_trip).await;
    match storage
        .inner
        .put_if_version_matches(
            Path::new(&manifest_path_for(stripe)),
            Box::new(
                serde_utils::to_json_bytes(&manifest)
                    .map_err(|error| ContentionError::Fatal(error.to_string()))?,
            ),
            &version,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(_) => {
            storage.conflicts.fetch_add(1, Ordering::Relaxed);
            Err(ContentionError::Conflict)
        }
    }
}

fn manifest_path_for(stripe: usize) -> String {
    if stripe == 0 {
        manifest_path()
    } else {
        format!("{}/manifest-{stripe:03}.json", index_prefix())
    }
}

fn wal_path_for(stripe: usize, seq: u64) -> String {
    if stripe == 0 {
        wal_path(seq)
    } else {
        format!("{}/wal-{stripe:03}/{seq:020}.json", index_prefix())
    }
}
