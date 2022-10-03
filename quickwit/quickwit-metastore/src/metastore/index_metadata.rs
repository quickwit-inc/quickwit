// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use itertools::Itertools;
use quickwit_common::uri::Uri;
use quickwit_config::{
    DocMapping, IndexingSettings, RetentionPolicy, SearchSettings, SourceConfig,
};
use serde::{Deserialize, Serialize};

use crate::checkpoint::IndexCheckpoint;
use crate::split_metadata::utc_now_timestamp;
use crate::{MetastoreError, MetastoreResult};

/// An index metadata carries all meta data about an index.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(into = "VersionedIndexMetadata")]
#[serde(from = "VersionedIndexMetadata")]
pub struct IndexMetadata {
    /// Index ID, uniquely identifies an index when querying the metastore.
    pub index_id: String,
    /// Index URI, defines the location of the storage that holds the split files.
    pub index_uri: Uri,
    /// Checkpoint relative to a source or a set of sources. It expresses up to which point
    /// documents have been indexed.
    pub checkpoint: IndexCheckpoint,
    /// Describes how ingested JSON documents are indexed.
    pub doc_mapping: DocMapping,
    /// Configures various indexing settings such as commit timeout, max split size, indexing
    /// resources.
    pub indexing_settings: IndexingSettings,
    /// Configures various search settings such as default search fields.
    pub search_settings: SearchSettings,
    /// Data sources keyed by their `source_id`.
    pub sources: HashMap<String, SourceConfig>,
    /// An optional retention policy which will be applied to the splits of the index.
    pub retention_policy: Option<RetentionPolicy>,
    /// Time at which the index was created.
    pub create_timestamp: i64,
    /// Time at which the index was last updated.
    pub update_timestamp: i64,
}

impl IndexMetadata {
    /// Returns an [`IndexMetadata`] object with multiple hard coded values for tests.
    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(index_id: &str, index_uri: &str) -> Self {
        use quickwit_config::IndexingResources;
        use quickwit_doc_mapper::SortOrder;

        let index_uri = Uri::new(index_uri.to_string());
        let doc_mapping_json = r#"{
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "i64",
                    "fast": true
                },
                {
                    "name": "body",
                    "type": "text",
                    "stored": true
                },
                {
                    "name": "response_date",
                    "type": "datetime",
                    "fast": true
                },
                {
                    "name": "response_time",
                    "type": "f64",
                    "fast": true
                },
                {
                    "name": "response_payload",
                    "type": "bytes",
                    "fast": true
                },
                {
                    "name": "owner",
                    "type": "text",
                    "tokenizer": "raw"
                },
                {
                    "name": "attributes",
                    "type": "object",
                    "field_mappings": [
                        {
                            "name": "tags",
                            "type": "array<i64>"
                        },
                        {
                            "name": "server",
                            "type": "text"
                        },
                        {
                            "name": "server.status",
                            "type": "array<text>"
                        },
                        {
                            "name": "server.payload",
                            "type": "array<bytes>"
                        }
                    ]
                }
            ],
            "tag_fields": ["owner"],
            "store_source": true
        }"#;
        let doc_mapping = serde_json::from_str(doc_mapping_json).unwrap();
        let indexing_settings = IndexingSettings {
            timestamp_field: Some("timestamp".to_string()),
            sort_field: Some("timestamp".to_string()),
            sort_order: Some(SortOrder::Desc),
            resources: IndexingResources::for_test(),
            ..Default::default()
        };
        let search_settings = SearchSettings {
            default_search_fields: vec![
                "body".to_string(),
                r#"attributes.server"#.to_string(),
                r#"attributes.server\.status"#.to_string(),
            ],
        };
        let now_timestamp = utc_now_timestamp();
        Self {
            index_id: index_id.to_string(),
            index_uri,
            checkpoint: Default::default(),
            doc_mapping,
            indexing_settings,
            search_settings,
            sources: Default::default(),
            retention_policy: None, // TODO
            create_timestamp: now_timestamp,
            update_timestamp: now_timestamp,
        }
    }

    pub(crate) fn add_source(&mut self, source: SourceConfig) -> MetastoreResult<()> {
        let entry = self.sources.entry(source.source_id.clone());
        let source_id = source.source_id.clone();
        if let Entry::Occupied(_) = entry {
            return Err(MetastoreError::SourceAlreadyExists {
                source_id: source_id.clone(),
                source_type: source.source_type().to_string(),
            });
        }
        entry.or_insert(source);
        self.checkpoint.add_source(&source_id);
        Ok(())
    }

    pub(crate) fn delete_source(&mut self, source_id: &str) -> MetastoreResult<()> {
        self.sources
            .remove(source_id)
            .ok_or_else(|| MetastoreError::SourceDoesNotExist {
                source_id: source_id.to_string(),
            })?;
        self.checkpoint.remove_source(source_id);
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
pub(crate) enum VersionedIndexMetadata {
    #[serde(rename = "1")]
    V1(IndexMetadataV1),
}

impl From<IndexMetadata> for VersionedIndexMetadata {
    fn from(index_metadata: IndexMetadata) -> Self {
        VersionedIndexMetadata::V1(index_metadata.into())
    }
}

impl From<VersionedIndexMetadata> for IndexMetadata {
    fn from(index_metadata: VersionedIndexMetadata) -> Self {
        match index_metadata {
            VersionedIndexMetadata::V1(v1) => v1.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IndexMetadataV1 {
    pub index_id: String,
    pub index_uri: String,
    pub checkpoint: IndexCheckpoint,
    pub doc_mapping: DocMapping,
    #[serde(default)]
    pub indexing_settings: IndexingSettings,
    pub search_settings: SearchSettings,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<SourceConfig>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_policy: Option<RetentionPolicy>,
    #[serde(default = "utc_now_timestamp")]
    pub create_timestamp: i64,
    #[serde(default = "utc_now_timestamp")]
    pub update_timestamp: i64,
}

impl From<IndexMetadata> for IndexMetadataV1 {
    fn from(index_metadata: IndexMetadata) -> Self {
        let sources = index_metadata
            .sources
            .into_values()
            .sorted_by(|left, right| left.source_id.cmp(&right.source_id))
            .collect();
        Self {
            index_id: index_metadata.index_id,
            index_uri: index_metadata.index_uri.into_string(),
            checkpoint: index_metadata.checkpoint,
            doc_mapping: index_metadata.doc_mapping,
            indexing_settings: index_metadata.indexing_settings,
            search_settings: index_metadata.search_settings,
            sources,
            retention_policy: index_metadata.retention_policy,
            create_timestamp: index_metadata.create_timestamp,
            update_timestamp: index_metadata.update_timestamp,
        }
    }
}

impl From<IndexMetadataV1> for IndexMetadata {
    fn from(v1: IndexMetadataV1) -> Self {
        let sources = v1
            .sources
            .into_iter()
            .map(|source| (source.source_id.clone(), source))
            .collect();
        Self {
            index_id: v1.index_id,
            index_uri: Uri::new(v1.index_uri),
            checkpoint: v1.checkpoint,
            doc_mapping: v1.doc_mapping,
            indexing_settings: v1.indexing_settings,
            search_settings: v1.search_settings,
            sources,
            retention_policy: v1.retention_policy,
            create_timestamp: v1.create_timestamp,
            update_timestamp: v1.update_timestamp,
        }
    }
}
