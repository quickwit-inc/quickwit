// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use futures::future::try_join_all;
use itertools::Itertools;
use quickwit_common::shared_consts::SPLIT_FIELDS_FILE_NAME;
use quickwit_common::uri::Uri;
use quickwit_config::build_doc_mapper;
use quickwit_doc_mapper::DocMapper;
use quickwit_indexing::models::read_split_fields;
use quickwit_metastore::{ListIndexesMetadataResponseExt, SplitMetadata};
use quickwit_proto::metastore::{
    ListIndexesMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::search::{
    LeafListFieldsRequest, ListFieldsEntryResponse, ListFieldsRequest, ListFieldsResponse,
    SplitIdAndFooterOffsets,
};
use quickwit_proto::types::IndexUid;
use quickwit_storage::Storage;
use tantivy::FieldMetadata;

use crate::leaf::open_split_bundle;
use crate::service::SearcherContext;
use crate::{list_relevant_splits, ClusterClient, SearchError, SearchJob};

/// Get the list of splits for the request which we need to scan.
pub async fn get_fields_from_split<'a>(
    searcher_context: &SearcherContext,
    index_id: String,
    split_and_footer_offsets: &'a SplitIdAndFooterOffsets,
    index_storage: Arc<dyn Storage>,
) -> anyhow::Result<Box<dyn Iterator<Item = io::Result<ListFieldsEntryResponse>> + Send>> {
    // TODO: Add fancy caching
    let (_, split_bundle) =
        open_split_bundle(searcher_context, index_storage, split_and_footer_offsets).await?;

    let serialized_split_fields = split_bundle
        .get_all(Path::new(SPLIT_FIELDS_FILE_NAME))
        .await?;
    let serialized_split_fields_len = serialized_split_fields.len();
    let list_fields_iter = read_split_fields(serialized_split_fields).with_context(|| {
        format!(
            "could not read split fields (serialized len: {})",
            serialized_split_fields_len,
        )
    })?;
    Ok(Box::new(list_fields_iter.map(move |metadata| {
        metadata.map(|metadata| field_metadata_to_fields_entry_response(metadata, &index_id))
    })))
}

/// Get the list of splits for the request which we need to scan.
pub fn get_fields_from_schema(
    index_id: String,
    doc_mapper: Arc<dyn DocMapper>,
) -> Box<dyn Iterator<Item = io::Result<ListFieldsEntryResponse>> + Send> {
    let schema = doc_mapper.schema();
    let mut list_fields = schema
        .fields()
        .map(|(_field, entry)| FieldMetadata {
            field_name: entry.name().to_string(),
            typ: entry.field_type().value_type(),
            indexed: entry.is_indexed(),
            fast: entry.is_fast(),
            stored: entry.is_stored(),
        })
        .collect_vec();
    list_fields.sort();
    Box::new(
        list_fields
            .into_iter()
            .map(move |metadata| Ok(field_metadata_to_fields_entry_response(metadata, &index_id))),
    )
}

fn field_metadata_to_fields_entry_response(
    metadata: FieldMetadata,
    index_id: &str,
) -> ListFieldsEntryResponse {
    ListFieldsEntryResponse {
        field_name: metadata.field_name,
        field_type: metadata.typ.to_code() as u32,
        index_ids: vec![index_id.to_string()],
        searchable: metadata.indexed,
        aggregatable: metadata.fast,
        non_searchable_index_ids: Vec::new(),
        non_aggregatable_index_ids: Vec::new(),
    }
}

/// Since we want to kmerge the results, we simplify by always using `FieldMetadata`, to enforce
/// the same ordering
fn field_metadata_from_list_field_response(resp: &ListFieldsEntryResponse) -> FieldMetadata {
    FieldMetadata {
        field_name: resp.field_name.to_string(),
        typ: tantivy::schema::Type::from_code(resp.field_type as u8).expect("invalid field type"),
        indexed: resp.aggregatable,
        fast: resp.searchable,
        stored: true,
    }
}

/// `current_group` needs to contain at least one element.
/// The group needs to be of the same field name and type.
fn merge_same_field_group(
    current_group: &mut Vec<ListFieldsEntryResponse>,
) -> ListFieldsEntryResponse {
    // Make sure all fields have the same name and type in current_group
    assert!(!current_group.is_empty());
    assert!(current_group
        .windows(2)
        .all(|window| window[0].field_name == window[1].field_name
            && window[0].field_type == window[1].field_type));

    if current_group.len() == 1 {
        return current_group.pop().unwrap();
    }
    let metadata = &current_group.last().unwrap();
    let searchable = current_group.iter().any(|entry| entry.searchable);
    let aggregatable = current_group.iter().any(|entry| entry.aggregatable);
    let field_name = metadata.field_name.to_string();
    let field_type = metadata.field_type;
    let mut non_searchable_index_ids = if searchable {
        // We need to combine the non_searchable_index_ids + index_ids where searchable is set to
        // false (as they are all non_searchable)
        current_group
            .iter()
            .flat_map(|entry| {
                if !entry.searchable {
                    entry.index_ids.iter().cloned()
                } else {
                    entry.non_searchable_index_ids.iter().cloned()
                }
            })
            .collect()
    } else {
        // Not searchable => no need to list all the indices
        Vec::new()
    };
    non_searchable_index_ids.sort();
    non_searchable_index_ids.dedup();

    let mut non_aggregatable_index_ids = if aggregatable {
        // We need to combine the non_aggregatable_index_ids + index_ids where aggregatable is set
        // to false (as they are all non_aggregatable)
        current_group
            .iter()
            .flat_map(|entry| {
                if !entry.aggregatable {
                    entry.index_ids.iter().cloned()
                } else {
                    entry.non_aggregatable_index_ids.iter().cloned()
                }
            })
            .collect()
    } else {
        // Not aggregatable => no need to list all the indices
        Vec::new()
    };
    non_aggregatable_index_ids.sort();
    non_aggregatable_index_ids.dedup();
    let mut index_ids: Vec<String> = current_group
        .drain(..)
        .flat_map(|entry| entry.index_ids.into_iter())
        .collect();
    index_ids.sort();
    index_ids.dedup();
    ListFieldsEntryResponse {
        field_name,
        field_type,
        searchable,
        aggregatable,
        non_searchable_index_ids,
        non_aggregatable_index_ids,
        index_ids,
    }
}

/// Merge iterators of sorted (FieldMetadata, index_id) into a Vec<ListFieldsEntryResponse>.
fn merge_leaf_list_fields(
    iterators: Vec<impl Iterator<Item = io::Result<ListFieldsEntryResponse>>>,
) -> crate::Result<Vec<ListFieldsEntryResponse>> {
    let merged = iterators.into_iter().kmerge_by(|a, b| {
        match (a, b) {
            (Ok(ref a_field), Ok(ref b_field)) => {
                field_metadata_from_list_field_response(a_field)
                    <= field_metadata_from_list_field_response(b_field)
            }
            _ => true, // Prioritize error results to halt early on errors
        }
    });
    let mut responses = Vec::new();

    let mut current_group: Vec<ListFieldsEntryResponse> = Vec::new();
    // Build ListFieldsEntryResponse from current group
    let flush_group = |responses: &mut Vec<_>, current_group: &mut Vec<ListFieldsEntryResponse>| {
        let entry = merge_same_field_group(current_group);
        responses.push(entry);
        current_group.clear();
    };

    for entry in merged {
        let entry =
            entry.map_err(|err| crate::error::SearchError::Internal(format!("{:?}", err)))?; // TODO: No early return on error

        if let Some(last) = current_group.last() {
            if last.field_name != entry.field_name || last.field_type != entry.field_type {
                flush_group(&mut responses, &mut current_group);
            }
        }
        current_group.push(entry);
    }
    if !current_group.is_empty() {
        flush_group(&mut responses, &mut current_group);
    }

    Ok(responses)
}

fn matches_any_pattern(field_name: &str, field_patterns: &[String]) -> bool {
    if field_patterns.is_empty() {
        return true;
    }
    field_patterns
        .iter()
        .any(|pattern| matches_pattern(pattern, field_name))
}

/// Supports up to 1 wildcard.
fn matches_pattern(field_pattern: &str, field_name: &str) -> bool {
    match field_pattern.find('*') {
        None => field_pattern == field_name,
        Some(index) => {
            if index == 0 {
                // "*field"
                field_name.ends_with(&field_pattern[1..])
            } else if index == field_pattern.len() - 1 {
                // "field*"
                field_name.starts_with(&field_pattern[..index])
            } else {
                // "fi*eld"
                field_name.starts_with(&field_pattern[..index])
                    && field_name.ends_with(&field_pattern[index + 1..])
            }
        }
    }
}
///
pub async fn leaf_list_fields(
    index_id: String,
    index_storage: Arc<dyn Storage>,
    searcher_context: &SearcherContext,
    split_ids: &[SplitIdAndFooterOffsets],
    doc_mapper: Arc<dyn DocMapper>,
    field_patterns: &[String],
) -> crate::Result<ListFieldsResponse> {
    let mut iter_per_split = Vec::new();
    // This only works well, if the field data is in a local cache.
    for split_id in split_ids.iter() {
        let fields = get_fields_from_split(
            searcher_context,
            index_id.to_string(),
            split_id,
            index_storage.clone(),
        )
        .await;
        let list_fields_iter = match fields {
            Ok(fields) => fields,
            Err(_err) => {
                // Schema fallback
                get_fields_from_schema(index_id.to_string(), doc_mapper.clone())
            }
        };
        let list_fields_iter = list_fields_iter.filter(|field| {
            if let Ok(field) = field {
                // We don't want to leak the _dynamic hack to the user API.
                if field.field_name.starts_with("_dynamic.") {
                    return matches_any_pattern(&field.field_name, field_patterns)
                        || matches_any_pattern(
                            &field.field_name["_dynamic.".len()..],
                            field_patterns,
                        );
                } else {
                    return matches_any_pattern(&field.field_name, field_patterns);
                };
            }
            true
        });
        iter_per_split.push(list_fields_iter);
    }
    let fields = merge_leaf_list_fields(iter_per_split)?;
    Ok(ListFieldsResponse { fields })
}

/// Index metas needed for executing a leaf search request.
#[derive(Clone, Debug)]
pub struct IndexMetasForLeafSearch {
    /// Index id.
    pub index_id: String,
    /// Index URI.
    pub index_uri: Uri,
    /// Doc mapper json string.
    pub doc_mapper_str: String,
}

/// Performs a distributed list fields request.
/// 1. Sends leaf request over gRPC to multiple leaf nodes.
/// 2. Merges the search results.
/// 3. Builds the response and returns.
pub async fn root_list_fields(
    list_fields_req: ListFieldsRequest,
    cluster_client: &ClusterClient,
    mut metastore: MetastoreServiceClient,
) -> crate::Result<ListFieldsResponse> {
    let list_indexes_metadata_request = if list_fields_req.index_ids.is_empty() {
        ListIndexesMetadataRequest::all()
    } else {
        ListIndexesMetadataRequest {
            // TODO: Check index id pattern
            index_id_patterns: list_fields_req.index_ids.clone(),
        }
    };

    // Get the index ids from the request
    let indexes_metadatas = metastore
        .clone()
        .list_indexes_metadata(list_indexes_metadata_request)
        .await?
        .deserialize_indexes_metadata()?;
    let index_uid_to_index_meta: HashMap<IndexUid, IndexMetasForLeafSearch> = indexes_metadatas
        .iter()
        .map(|index_metadata| {
            let doc_mapper = build_doc_mapper(
                &index_metadata.index_config.doc_mapping,
                &index_metadata.index_config.search_settings,
            )
            .map_err(|err| {
                SearchError::Internal(format!("failed to build doc mapper. cause: {err}"))
            })
            .unwrap();

            let index_metadata_for_leaf_search = IndexMetasForLeafSearch {
                index_uri: index_metadata.index_uri().clone(),
                index_id: index_metadata.index_config.index_id.to_string(),
                doc_mapper_str: serde_json::to_string(&doc_mapper)
                    .map_err(|err| {
                        SearchError::Internal(format!(
                            "failed to serialize doc mapper. cause: {err}"
                        ))
                    })
                    .unwrap(),
            };

            (
                index_metadata.index_uid.clone(),
                index_metadata_for_leaf_search,
            )
        })
        .collect();
    let index_uids: Vec<IndexUid> = indexes_metadatas
        .into_iter()
        .map(|index_metadata| index_metadata.index_uid)
        .collect();

    // TODO if search after is set, we sort by timestamp and we don't want to count all results,
    // we can refine more here. Same if we sort by _shard_doc
    let split_metadatas: Vec<SplitMetadata> =
        list_relevant_splits(index_uids, None, None, None, &mut metastore).await?;

    // Build requests for each index id
    let jobs: Vec<SearchJob> = split_metadatas.iter().map(SearchJob::from).collect();
    let assigned_leaf_search_jobs = cluster_client
        .search_job_placer
        .assign_jobs(jobs, &HashSet::default())
        .await?;
    let mut leaf_request_tasks = Vec::new();
    for (client, client_jobs) in assigned_leaf_search_jobs {
        let leaf_requests =
            jobs_to_leaf_requests(&list_fields_req, &index_uid_to_index_meta, client_jobs)?;
        for leaf_request in leaf_requests {
            leaf_request_tasks.push(cluster_client.leaf_list_fields(leaf_request, client.clone()));
        }
    }
    let leaf_search_responses: Vec<ListFieldsResponse> = try_join_all(leaf_request_tasks).await?;
    let fields = merge_leaf_list_fields(
        leaf_search_responses
            .into_iter()
            .map(|resp| resp.fields.into_iter().map(Result::Ok))
            .collect_vec(),
    )?;
    Ok(ListFieldsResponse { fields })

    // Extract the list of index ids from the splits.
    // For each node, forward to a node with an affinity for that index id.
}
/// Builds a list of [`LeafListFieldsRequest`], one per index, from a list of [`SearchJob`].
pub fn jobs_to_leaf_requests(
    request: &ListFieldsRequest,
    index_uid_to_id: &HashMap<IndexUid, IndexMetasForLeafSearch>,
    jobs: Vec<SearchJob>,
) -> crate::Result<Vec<LeafListFieldsRequest>> {
    let search_request_for_leaf = request.clone();
    let mut leaf_search_requests = Vec::new();
    // Group jobs by index uid.
    for (index_uid, job_group) in &jobs.into_iter().group_by(|job| job.index_uid.clone()) {
        let index_meta = index_uid_to_id.get(&index_uid).ok_or_else(|| {
            SearchError::Internal(format!(
                "received list fields job for an unknown index {index_uid}. it should never happen"
            ))
        })?;
        let leaf_search_request = LeafListFieldsRequest {
            index_id: index_meta.index_id.to_string(),
            index_uri: index_meta.index_uri.to_string(),
            doc_mapper: index_meta.doc_mapper_str.to_string(),
            fields: search_request_for_leaf.fields.clone(),
            split_offsets: job_group.into_iter().map(|job| job.offsets).collect(),
        };
        leaf_search_requests.push(leaf_search_request);
    }
    Ok(leaf_search_requests)
}

#[cfg(test)]
mod tests {
    use quickwit_proto::search::ListFieldsEntryResponse;
    use tantivy::schema::Type;

    use super::*;

    #[test]
    fn test_pattern() {
        assert!(matches_any_pattern("field", &["field".to_string()]));
        assert!(matches_any_pattern("field", &["fi*eld".to_string()]));
        assert!(matches_any_pattern("field", &["*field".to_string()]));
        assert!(matches_any_pattern("field", &["field*".to_string()]));

        assert!(matches_any_pattern("field1", &["field*".to_string()]));
        assert!(!matches_any_pattern("field1", &["*field".to_string()]));
        assert!(!matches_any_pattern("field1", &["fi*eld".to_string()]));
        assert!(!matches_any_pattern("field1", &["field".to_string()]));

        // 2.nd pattern matches
        assert!(matches_any_pattern(
            "field",
            &["a".to_string(), "field".to_string()]
        ));
        assert!(matches_any_pattern(
            "field",
            &["a".to_string(), "fi*eld".to_string()]
        ));
        assert!(matches_any_pattern(
            "field",
            &["a".to_string(), "*field".to_string()]
        ));
        assert!(matches_any_pattern(
            "field",
            &["a".to_string(), "field*".to_string()]
        ));
    }

    #[test]
    fn merge_leaf_list_fields_identical_test() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone()].into_iter().map(Result::Ok),
            vec![entry2.clone()].into_iter().map(Result::Ok),
        ])
        .unwrap();
        assert_eq!(resp, vec![entry1]);
    }
    #[test]
    fn merge_leaf_list_fields_different_test() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field2".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone()].into_iter().map(Result::Ok),
            vec![entry2.clone()].into_iter().map(Result::Ok),
        ])
        .unwrap();
        assert_eq!(resp, vec![entry1, entry2]);
    }
    #[test]
    fn merge_leaf_list_fields_non_searchable_test() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: false,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index2".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone()].into_iter().map(Result::Ok),
            vec![entry2.clone()].into_iter().map(Result::Ok),
        ])
        .unwrap();
        let expected = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: vec!["index2".to_string()],
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string(), "index2".to_string()],
        };
        assert_eq!(resp, vec![expected]);
    }
    #[test]
    fn merge_leaf_list_fields_non_aggregatable_test() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: false,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index2".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone()].into_iter().map(Result::Ok),
            vec![entry2.clone()].into_iter().map(Result::Ok),
        ])
        .unwrap();
        let expected = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: vec!["index2".to_string()],
            index_ids: vec!["index1".to_string(), "index2".to_string()],
        };
        assert_eq!(resp, vec![expected]);
    }
    #[test]
    fn merge_leaf_list_fields_mixed_types1() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry3 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::U64.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone(), entry2.clone()]
                .into_iter()
                .map(Result::Ok),
            vec![entry3.clone()].into_iter().map(Result::Ok),
        ])
        .unwrap();
        assert_eq!(resp, vec![entry1.clone(), entry3.clone()]);
    }
    #[test]
    fn merge_leaf_list_fields_mixed_types2() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry3 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::U64.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone(), entry3.clone()]
                .into_iter()
                .map(Result::Ok),
            vec![entry2.clone()].into_iter().map(Result::Ok),
        ])
        .unwrap();
        assert_eq!(resp, vec![entry1.clone(), entry3.clone()]);
    }
    #[test]
    fn merge_leaf_list_fields_multiple_field_names() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let entry3 = ListFieldsEntryResponse {
            field_name: "field2".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index1".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone(), entry3.clone()]
                .into_iter()
                .map(Result::Ok),
            vec![entry2.clone()].into_iter().map(Result::Ok),
        ])
        .unwrap();
        assert_eq!(resp, vec![entry1.clone(), entry3.clone()]);
    }
    #[test]
    fn merge_leaf_list_fields_non_aggregatable_list_test() {
        let entry1 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: vec!["index1".to_string()],
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec![
                "index1".to_string(),
                "index2".to_string(),
                "index3".to_string(),
            ],
        };
        let entry2 = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: false,
            aggregatable: true,
            non_searchable_index_ids: Vec::new(),
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec!["index4".to_string()],
        };
        let resp = merge_leaf_list_fields(vec![
            vec![entry1.clone()].into_iter().map(Result::Ok),
            vec![entry2.clone()].into_iter().map(Result::Ok),
        ])
        .unwrap();
        let expected = ListFieldsEntryResponse {
            field_name: "field1".to_string(),
            field_type: Type::Str.to_code() as u32,
            searchable: true,
            aggregatable: true,
            non_searchable_index_ids: vec!["index1".to_string(), "index4".to_string()],
            non_aggregatable_index_ids: Vec::new(),
            index_ids: vec![
                "index1".to_string(),
                "index2".to_string(),
                "index3".to_string(),
                "index4".to_string(),
            ],
        };
        assert_eq!(resp, vec![expected]);
    }
}
