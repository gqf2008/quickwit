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

use std::collections::{BTreeMap, HashMap};

use itertools::Itertools;
use quickwit_proto::ingest::Shard;
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::{DocMappingUid, SourceId};
use serde::{Deserialize, Serialize};

use super::StoredParquetSplit;
use super::shards::Shards;
use crate::file_backed::file_backed_index::FileBackedIndex;
use crate::metastore::DeleteTask;
use crate::{IndexMetadata, Split};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
pub(crate) enum VersionedFileBackedIndex {
    #[serde(rename = "0.9")]
    V0_9(FileBackedIndexV0_8),
    // Retro compatibility.
    #[serde(alias = "0.8")]
    #[serde(alias = "0.7")]
    V0_8(FileBackedIndexV0_8),
}

impl From<FileBackedIndex> for VersionedFileBackedIndex {
    fn from(index: FileBackedIndex) -> Self {
        VersionedFileBackedIndex::V0_9(index.into())
    }
}

impl From<VersionedFileBackedIndex> for FileBackedIndex {
    fn from(index: VersionedFileBackedIndex) -> Self {
        match index {
            VersionedFileBackedIndex::V0_8(mut v0_8) => {
                for shards in v0_8.shards.values_mut() {
                    for shard in shards {
                        shard.doc_mapping_uid = Some(DocMappingUid::default());
                    }
                }
                v0_8.into()
            }
            VersionedFileBackedIndex::V0_9(v0_8) => v0_8.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FileBackedIndexV0_8 {
    #[serde(rename = "index")]
    metadata: IndexMetadata,
    splits: Vec<Split>,
    // TODO: Remove `skip_serializing_if` when we release ingest v2.
    // A `BTreeMap` rather than a `HashMap`: its keys are serialized in order, and this object is
    // compared byte for byte by the writers that skip a compare-and-swap when nothing changed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    shards: BTreeMap<SourceId, Vec<Shard>>,
    #[serde(default)]
    delete_tasks: Vec<DeleteTask>,
    /// Metrics splits (for metrics pipeline).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    metrics_splits: Vec<StoredParquetSplit>,
    /// Sketch splits (for DDSketch pipeline).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    sketch_splits: Vec<StoredParquetSplit>,
}

impl From<FileBackedIndex> for FileBackedIndexV0_8 {
    fn from(index: FileBackedIndex) -> Self {
        // Every list is sorted *deterministically*, down to the last field of the key. The byte
        // comparison in `store_root` (and in the sharded layout's bookmark) is what lets a write
        // that changes nothing skip its compare-and-swap, and hash-map order differs
        // between processes: the same state from two nodes used to serialize differently,
        // so the write happened anyway.
        let splits = index
            .splits
            .into_values()
            .sorted_by_key(|split| (split.update_timestamp, split.split_id().clone()))
            .collect();
        let shards = index
            .per_source_shards
            .into_iter()
            .filter_map(|(source_id, shards)| {
                // TODO: Remove this filter when we release ingest v2.
                // Skip serializing empty shards since the feature is hidden and disabled by
                // default. This way, we can still modify the serialization format without worrying
                // about backward compatibility post `0.7`.
                if !shards.is_empty() {
                    Some((
                        source_id,
                        shards
                            .into_shards_vec()
                            .into_iter()
                            .sorted_by_key(|shard| shard.shard_id.clone())
                            .collect(),
                    ))
                } else {
                    None
                }
            })
            .collect();
        let delete_tasks = index
            .delete_tasks
            .into_iter()
            .sorted_by_key(|delete_task| (delete_task.opstamp, delete_task.create_timestamp))
            .collect();
        let metrics_splits = index
            .metrics_splits
            .into_values()
            .sorted_by_key(|split| (split.update_timestamp, split.metadata.split_id.to_string()))
            .collect();
        let sketch_splits = index
            .sketch_splits
            .into_values()
            .sorted_by_key(|split| (split.update_timestamp, split.metadata.split_id.to_string()))
            .collect();
        Self {
            metadata: index.metadata,
            splits,
            shards,
            delete_tasks,
            metrics_splits,
            sketch_splits,
        }
    }
}

impl From<FileBackedIndexV0_8> for FileBackedIndex {
    fn from(index: FileBackedIndexV0_8) -> Self {
        let mut per_source_shards: HashMap<SourceId, Shards> = index
            .shards
            .into_iter()
            .map(|(source_id, shards_vec)| {
                let index_uid = index.metadata.index_uid.clone();
                (
                    source_id.clone(),
                    Shards::from_shards_vec(index_uid, source_id, shards_vec),
                )
            })
            .collect();
        // TODO: Remove this when we release ingest v2.
        for source in index.metadata.sources.values() {
            if source.source_type() == SourceType::IngestV2
                && !per_source_shards.contains_key(&source.source_id)
            {
                let index_uid = index.metadata.index_uid.clone();
                let source_id = source.source_id.clone();
                per_source_shards.insert(source_id.clone(), Shards::empty(index_uid, source_id));
            }
        }
        Self::new_with_metrics_splits(
            index.metadata,
            index.splits,
            per_source_shards,
            index.delete_tasks,
            index.metrics_splits,
            index.sketch_splits,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::time::{Duration, SystemTime};

    use quickwit_config::{IndexConfig, SourceConfig, SourceParams};
    use quickwit_parquet_engine::split::{
        ParquetSplitId, ParquetSplitMetadata, ParquetSplitMetadataBuilder, TimeRange,
    };
    use quickwit_proto::ingest::Shard;
    use quickwit_proto::types::ShardId;
    use serde_json::Value;

    use super::*;
    use crate::{SplitMetadata, SplitState};

    /// The ids of every list the fixture fills. The delete tasks are built in descending opstamp
    /// order, so a serializer that stops sorting them keeps that order and the assertions below
    /// see it; the other lists come out of hash maps, whose order is random, so for them the
    /// assertion "the serialized list is sorted" is what has to hold.
    const SOURCE_IDS: [&str; 6] = [
        "source-1", "source-2", "source-3", "source-4", "source-5", "source-6",
    ];
    const SHARD_IDS: [&str; 4] = ["shard-a", "shard-b", "shard-c", "shard-d"];
    const SPLIT_IDS: [&str; 6] = [
        "split-1", "split-2", "split-3", "split-4", "split-5", "split-6",
    ];
    const DELETE_OPSTAMPS: [u64; 6] = [1, 2, 3, 4, 5, 6];
    const METRICS_SPLIT_IDS: [&str; 4] = ["metrics-1", "metrics-2", "metrics-3", "metrics-4"];
    const SKETCH_SPLIT_IDS: [&str; 4] = ["sketch-1", "sketch-2", "sketch-3", "sketch-4"];
    /// Only used to fill the sets the parquet split metadata carries: the metrics pipeline fills
    /// them in production, and their serialized order has to be stable too.
    const METRIC_NAMES: [&str; 6] = [
        "cpu.usage",
        "disk.io",
        "load.1m",
        "mem.used",
        "net.rx",
        "net.tx",
    ];

    /// Returns the first string found under `key`, at any depth.
    fn find_str<'a>(value: &'a Value, key: &str) -> &'a str {
        match value {
            Value::Object(map) => {
                if let Some(Value::String(found)) = map.get(key) {
                    return found;
                }
                map.values()
                    .find_map(|value| try_find_str(value, key))
                    .unwrap_or_else(|| panic!("no {key} in {value}"))
            }
            _ => panic!("no {key} in {value}"),
        }
    }

    fn try_find_str<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
        match value {
            Value::Object(map) => {
                if let Some(Value::String(found)) = map.get(key) {
                    return Some(found);
                }
                map.values().find_map(|value| try_find_str(value, key))
            }
            Value::Array(entries) => entries.iter().find_map(|value| try_find_str(value, key)),
            _ => None,
        }
    }

    /// Asserts that the serialized list is in ascending order and returns the keys.
    ///
    /// A list that is not sorted is the signature of a hash map leaking its random order into the
    /// bytes, which is exactly what makes two nodes write different bytes for the same state.
    fn assert_ascending<T, K: Ord + Clone + Debug>(
        values: &[T],
        key: impl Fn(&T) -> K,
        what: &str,
    ) -> Vec<K> {
        let keys: Vec<K> = values.iter().map(&key).collect();
        let mut expected = keys.clone();
        expected.sort();
        assert_eq!(
            keys, expected,
            "{what} is not serialized in a deterministic order"
        );
        keys
    }

    fn expected_ids(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|id| id.to_string()).collect()
    }

    /// An index whose contents are the same has to serialize to the same bytes.
    ///
    /// The metastore compares them to decide whether a write changed anything and can skip its
    /// compare-and-swap, and the order of the map the index keeps its splits and shards in differs
    /// between processes: the same state from two nodes used to serialize differently, so the write
    /// happened anyway.
    ///
    /// The assertions are constructive rather than a repeated coin toss: every list is checked to
    /// come out sorted by its key, and it is checked to hold the whole fixture. A revision that
    /// drops one of the sorts is red on the very next run, with no dependence on two hash maps
    /// happening to disagree.
    #[test]
    fn test_serializing_an_index_is_deterministic() {
        let index_config =
            IndexConfig::for_test("test-deterministic", "ram:///indexes/test-deterministic");
        let mut metadata = IndexMetadata::new(index_config);
        // Two sources are enough to make the index metadata's source list and the outer shard map
        // order-dependent; six make a lucky agreement of two random orders unlikely.
        for source_id in SOURCE_IDS {
            metadata
                .add_source(SourceConfig::for_test(source_id, SourceParams::Ingest))
                .unwrap();
        }
        let index_uid = metadata.index_uid.clone();
        let split = |split_id: &str| Split {
            split_state: SplitState::Published,
            // The same timestamp on purpose: the id is what has to break the tie.
            update_timestamp: 7,
            publish_timestamp: None,
            split_metadata: SplitMetadata::for_test(quickwit_proto::types::SplitId::from(split_id)),
        };
        let shard = |shard_id: &str| Shard {
            shard_id: Some(ShardId::from(shard_id)),
            // The conversion asserts on the fields ingest v2 requires.
            publish_position_inclusive: Some(quickwit_proto::types::Position::Beginning),
            doc_mapping_uid: Some(quickwit_proto::types::DocMappingUid::default()),
            ..Default::default()
        };
        let parquet_split = |split_id: &str, mut builder: ParquetSplitMetadataBuilder| {
            builder = builder
                .split_id(ParquetSplitId::new(split_id))
                .index_uid(index_uid.to_string())
                .time_range(TimeRange::new(1000, 2000))
                .num_rows(100)
                .size_bytes(4096)
                .parquet_file(format!("{split_id}.parquet"));
            for metric_name in METRIC_NAMES {
                builder = builder.add_metric_name(metric_name);
            }
            for (key, value) in [
                ("env", "prod"),
                ("env", "staging"),
                ("region", "eu-west-1"),
                ("region", "us-east-1"),
                ("service", "api"),
                ("service", "web"),
            ] {
                builder = builder.add_low_cardinality_tag(key, value);
            }
            for tag_key in ["host.name", "pod_name", "span_id", "trace_id"] {
                builder = builder.add_high_cardinality_tag_key(tag_key);
            }
            for (column, regex) in [
                ("host", "^host-.*$"),
                ("metric_name", "^cpu\\..*$"),
                ("pod", ".*-prod-.*"),
                ("zone", "^(a|b|c)$"),
            ] {
                builder = builder.add_zonemap_regex(column, regex);
            }
            let mut metadata = builder.build();
            // The builder stamps "now" into the metadata; two builds of the same state have to
            // carry the same timestamp for the byte comparison below to be about the ordering.
            metadata.created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
            StoredParquetSplit {
                metadata,
                state: SplitState::Published,
                update_timestamp: 7,
                create_timestamp: 0,
                node_id: String::new(),
                delete_opstamp: 0,
                maturity_timestamp: 0,
            }
        };
        let build = |reverse: bool| {
            let mut splits: Vec<Split> = SPLIT_IDS.iter().map(|split_id| split(split_id)).collect();
            let mut shards: BTreeMap<SourceId, Vec<Shard>> = SOURCE_IDS
                .iter()
                .map(|source_id| {
                    let shards = SHARD_IDS.iter().map(|shard_id| shard(shard_id)).collect();
                    (source_id.to_string(), shards)
                })
                .collect();
            // Descending: the serializer has to put them back in opstamp order.
            let mut delete_tasks: Vec<DeleteTask> = DELETE_OPSTAMPS
                .iter()
                .rev()
                .map(|opstamp| DeleteTask {
                    opstamp: *opstamp,
                    create_timestamp: 1_700_000_000 + *opstamp as i64,
                    delete_query: None,
                })
                .collect();
            let mut metrics_splits: Vec<StoredParquetSplit> = METRICS_SPLIT_IDS
                .iter()
                .map(|split_id| parquet_split(split_id, ParquetSplitMetadata::metrics_builder()))
                .collect();
            let mut sketch_splits: Vec<StoredParquetSplit> = SKETCH_SPLIT_IDS
                .iter()
                .map(|split_id| parquet_split(split_id, ParquetSplitMetadata::sketches_builder()))
                .collect();
            if reverse {
                splits.reverse();
                shards = shards.into_iter().rev().collect();
                delete_tasks.reverse();
                metrics_splits.reverse();
                sketch_splits.reverse();
            }
            let index = FileBackedIndexV0_8 {
                metadata: metadata.clone(),
                splits,
                shards,
                delete_tasks,
                metrics_splits,
                sketch_splits,
            };
            FileBackedIndex::from(VersionedFileBackedIndex::V0_9(index))
        };
        let serialized = |index: FileBackedIndex| {
            quickwit_proto::metastore::serde_utils::to_json_bytes_pretty(&index).unwrap()
        };
        let forward = serialized(build(false));
        let reversed = serialized(build(true));
        assert_eq!(
            forward, reversed,
            "the same state in a different order must serialize to the same bytes"
        );

        let text = String::from_utf8(forward).unwrap();
        let parsed: Value = serde_json::from_str(&text).unwrap();

        let sources = parsed["index"]["sources"]
            .as_array()
            .expect("index sources");
        assert_eq!(
            assert_ascending(
                sources,
                |source| find_str(source, "source_id").to_string(),
                "index.sources"
            ),
            expected_ids(&SOURCE_IDS)
        );

        let splits = parsed["splits"].as_array().expect("splits");
        assert_eq!(
            assert_ascending(
                splits,
                |split| find_str(split, "split_id").to_string(),
                "splits"
            ),
            expected_ids(&SPLIT_IDS)
        );

        let delete_tasks = parsed["delete_tasks"].as_array().expect("delete_tasks");
        assert_eq!(
            assert_ascending(
                delete_tasks,
                |delete_task| delete_task["opstamp"].as_u64().unwrap(),
                "delete_tasks"
            ),
            DELETE_OPSTAMPS.to_vec()
        );

        let metrics_splits = parsed["metrics_splits"].as_array().expect("metrics_splits");
        assert_eq!(
            assert_ascending(
                metrics_splits,
                |split| find_str(split, "split_id").to_string(),
                "metrics_splits"
            ),
            expected_ids(&METRICS_SPLIT_IDS)
        );

        let sketch_splits = parsed["sketch_splits"].as_array().expect("sketch_splits");
        assert_eq!(
            assert_ascending(
                sketch_splits,
                |split| find_str(split, "split_id").to_string(),
                "sketch_splits"
            ),
            expected_ids(&SKETCH_SPLIT_IDS)
        );

        // The sets a parquet split metadata carries are serialized as lists too.
        let metadata = &metrics_splits[0]["metadata"];
        assert_ascending(
            metadata["metric_names"].as_array().unwrap(),
            |metric_name| metric_name.as_str().unwrap().to_string(),
            "metric_names",
        );
        assert_ascending(
            metadata["high_cardinality_tag_keys"].as_array().unwrap(),
            |tag_key| tag_key.as_str().unwrap().to_string(),
            "high_cardinality_tag_keys",
        );
        let low_cardinality_tags = metadata["low_cardinality_tags"].as_object().unwrap();
        for (tag_key, tag_values) in low_cardinality_tags {
            assert_ascending(
                tag_values.as_array().unwrap(),
                |tag_value| tag_value.as_str().unwrap().to_string(),
                tag_key,
            );
        }

        let shards_by_source = parsed["shards"].as_object().expect("shards");
        assert_eq!(shards_by_source.len(), SOURCE_IDS.len());
        for (source_id, shards) in shards_by_source {
            assert!(SOURCE_IDS.contains(&source_id.as_str()));
            assert_eq!(
                assert_ascending(
                    shards.as_array().unwrap(),
                    |shard| find_str(shard, "shard_id").to_string(),
                    "the shards of a source"
                ),
                expected_ids(&SHARD_IDS)
            );
        }

        // `serde_json`'s `Value` does not keep the key order of a JSON object, so the order of the
        // sources in the `shards` object is checked on the raw text: it is the order the bytes
        // have, and it is the one another node compares.
        let shards_section = &text[text.find("\"shards\":").expect("a shards section")..];
        let mut cursor = 0;
        for source_id in SOURCE_IDS {
            let needle = format!("\"{source_id}\":");
            let offset = shards_section[cursor..]
                .find(&needle)
                .unwrap_or_else(|| panic!("{source_id} is not in shard order in {shards_section}"));
            cursor += offset + needle.len();
        }
    }
}
