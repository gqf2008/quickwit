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

use std::collections::HashMap;

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
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    shards: HashMap<SourceId, Vec<Shard>>,
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
    use quickwit_config::IndexConfig;
    use quickwit_proto::ingest::Shard;
    use quickwit_proto::types::ShardId;

    use super::*;
    use crate::{IndexMetadata, SplitMetadata, SplitState};

    /// An index whose contents are the same has to serialize to the same bytes.
    ///
    /// The metastore compares them to decide whether a write changed anything and can skip its
    /// compare-and-swap, and the order of the map the index keeps its splits and shards in differs
    /// between processes: the same state from two nodes used to serialize differently, so the write
    /// happened anyway.
    #[test]
    fn test_serializing_an_index_is_deterministic() {
        let index_config =
            IndexConfig::for_test("test-deterministic", "ram:///indexes/test-deterministic");
        let metadata = IndexMetadata::new(index_config);
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
        let build = |reverse: bool| {
            let mut splits = vec![split("split-a"), split("split-b"), split("split-c")];
            let mut shards = vec![shard("shard-a"), shard("shard-b")];
            if reverse {
                splits.reverse();
                shards.reverse();
            }
            let index = FileBackedIndexV0_8 {
                metadata: metadata.clone(),
                splits,
                shards: HashMap::from([("source".to_string(), shards)]),
                delete_tasks: Vec::new(),
                metrics_splits: Vec::new(),
                sketch_splits: Vec::new(),
            };
            FileBackedIndex::from(VersionedFileBackedIndex::V0_9(index))
        };
        let serialized = |index: FileBackedIndex| {
            quickwit_proto::metastore::serde_utils::to_json_bytes_pretty(&index).unwrap()
        };
        assert_eq!(
            serialized(build(false)),
            serialized(build(true)),
            "the same state in a different order must serialize to the same bytes"
        );
    }
}
