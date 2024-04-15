// Copyright (C) 2024 Quickwit, Inc.
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

// Index API tests
//
//  - create_index
//  - index_exists
//  - index_metadata
//  - list_indexes
//  - delete_index

use std::vec;

use quickwit_common::rand::append_random_suffix;
use quickwit_config::{
    IndexConfig, RetentionPolicy, SearchSettings, SourceConfig, CLI_SOURCE_ID, INGEST_V2_SOURCE_ID,
};
use quickwit_proto::metastore::{
    CreateIndexRequest, DeleteIndexRequest, EntityKind, IndexMetadataRequest,
    ListIndexesMetadataRequest, MetastoreError, MetastoreService, StageSplitsRequest,
    UpdateIndexRequest,
};
use quickwit_proto::types::IndexUid;

use super::DefaultForTest;
use crate::tests::cleanup_index;
use crate::{
    CreateIndexRequestExt, IndexMetadataResponseExt, ListIndexesMetadataResponseExt,
    MetastoreServiceExt, SplitMetadata, StageSplitsRequestExt, UpdateIndexRequestExt,
};

pub async fn test_metastore_create_index<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-create-index");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid = metastore
        .create_index(create_index_request.clone())
        .await
        .unwrap()
        .index_uid()
        .clone();

    assert!(metastore.index_exists(&index_id).await.unwrap());

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();

    assert_eq!(index_metadata.index_id(), index_id);
    assert_eq!(index_metadata.index_uri(), &index_uri);

    let error = metastore
        .create_index(create_index_request)
        .await
        .unwrap_err();
    assert!(matches!(error, MetastoreError::AlreadyExists { .. }));

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_update_index<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-update-index");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid = metastore
        .create_index(create_index_request.clone())
        .await
        .unwrap()
        .index_uid()
        .clone();

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();

    let new_search_setting = SearchSettings {
        default_search_fields: vec!["body".to_string(), "owner".to_string()],
    };
    assert_ne!(
        index_metadata.index_config.search_settings, new_search_setting,
        "original and updated value are the same, test became inefficient"
    );

    let new_retention_policy_opt = Some(RetentionPolicy {
        retention_period: String::from("3 days"),
        evaluation_schedule: String::from("daily"),
    });
    assert_ne!(
        index_metadata.index_config.retention_policy_opt, new_retention_policy_opt,
        "original and updated value are the same, test became inefficient"
    );

    // run same update twice to check indempotence, then None as a corner case check
    for loop_retention_policy_opt in [
        new_retention_policy_opt.clone(),
        new_retention_policy_opt.clone(),
        None,
    ] {
        let index_update = UpdateIndexRequest::try_from_updates(
            index_uid.clone(),
            &new_search_setting,
            &loop_retention_policy_opt,
        )
        .unwrap();
        let response_metadata = metastore
            .update_index(index_update)
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert_eq!(response_metadata.index_uid, index_uid);
        assert_eq!(
            response_metadata.index_config.search_settings,
            new_search_setting
        );
        assert_eq!(
            response_metadata.index_config.retention_policy_opt,
            loop_retention_policy_opt
        );
        let updated_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert_eq!(response_metadata, updated_metadata);
    }

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_create_index_with_sources<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-create-index-with-sources");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);
    let index_config_json = serde_json::to_string(&index_config).unwrap();

    let source_configs_json = vec![
        serde_json::to_string(&SourceConfig::cli()).unwrap(),
        serde_json::to_string(&SourceConfig::ingest_v2()).unwrap(),
    ];
    let create_index_request = CreateIndexRequest {
        index_config_json,
        source_configs_json,
    };
    let index_uid: IndexUid = metastore
        .create_index(create_index_request.clone())
        .await
        .unwrap()
        .index_uid()
        .clone();

    assert!(metastore.index_exists(&index_id).await.unwrap());

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();

    assert_eq!(index_metadata.index_id(), index_id);
    assert_eq!(index_metadata.index_uri(), &index_uri);

    assert_eq!(index_metadata.sources.len(), 2);
    assert!(index_metadata.sources.contains_key(CLI_SOURCE_ID));
    assert!(index_metadata.sources.contains_key(INGEST_V2_SOURCE_ID));

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_create_index_enforces_index_id_maximum_length<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix(format!("very-long-index-{}", "a".repeat(233)).as_str());
    assert_eq!(index_id.len(), 255);
    let index_uri = format!("ram:///indexes/{index_id}");

    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    assert!(metastore.index_exists(&index_id).await.unwrap());

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_index_exists<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-index-exists");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);
    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();

    assert!(!metastore.index_exists(&index_id).await.unwrap());

    let index_uid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    assert!(metastore.index_exists(&index_id).await.unwrap());

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_index_metadata<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-index-metadata");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let error = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();

    assert_eq!(index_metadata.index_id(), index_id);
    assert_eq!(index_metadata.index_uri(), &index_uri);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_list_all_indexes<
    MetastoreToTest: MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id_prefix = append_random_suffix("test-list-all-indexes");
    let index_id_1 = format!("{index_id_prefix}-1");
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);

    let index_id_2 = format!("{index_id_prefix}-2");
    let index_uri_2 = format!("ram:///indexes/{index_id_2}");
    let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);
    let indexes_count = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest::all())
        .await
        .unwrap()
        .deserialize_indexes_metadata()
        .await
        .unwrap()
        .into_iter()
        .filter(|index| index.index_id().starts_with(&index_id_prefix))
        .count();
    assert_eq!(indexes_count, 0);

    let index_uid_1 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_1).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();
    let index_uid_2 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_2).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();

    let indexes_count = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest::all())
        .await
        .unwrap()
        .deserialize_indexes_metadata()
        .await
        .unwrap()
        .into_iter()
        .filter(|index| index.index_id().starts_with(&index_id_prefix))
        .count();
    assert_eq!(indexes_count, 2);

    cleanup_index(&mut metastore, index_uid_1).await;
    cleanup_index(&mut metastore, index_uid_2).await;
}

pub async fn test_metastore_list_indexes<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id_fragment = append_random_suffix("test-list-indexes");
    let index_id_1 = format!("prefix-1-{index_id_fragment}-suffix-1");
    let index_uri_1 = format!("ram:///indexes/{index_id_1}");
    let index_config_1 = IndexConfig::for_test(&index_id_1, &index_uri_1);

    let index_id_2 = format!("prefix-2-{index_id_fragment}-suffix-2");
    let index_uri_2 = format!("ram:///indexes/{index_id_2}");
    let index_config_2 = IndexConfig::for_test(&index_id_2, &index_uri_2);

    let index_id_3 = format!("prefix.3.{index_id_fragment}.3");
    let index_uri_3 = format!("ram:///indexes/{index_id_3}");
    let index_config_3 = IndexConfig::for_test(&index_id_3, &index_uri_3);

    let index_id_4 = format!("p-4-{index_id_fragment}-suffix-4");
    let index_uri_4 = format!("ram:///indexes/{index_id_4}");
    let index_config_4 = IndexConfig::for_test(&index_id_4, &index_uri_4);

    let index_id_patterns = vec![
        format!("prefix-*-{index_id_fragment}-suffix-*"),
        format!("prefix*{index_id_fragment}*suffix-*"),
    ];
    let indexes_count = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest { index_id_patterns })
        .await
        .unwrap()
        .deserialize_indexes_metadata()
        .await
        .unwrap()
        .len();
    assert_eq!(indexes_count, 0);

    let index_uid_1 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_1).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();
    let index_uid_2 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_2).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();
    let index_uid_3 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_3).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();
    let index_uid_4 = metastore
        .create_index(CreateIndexRequest::try_from_index_config(&index_config_4).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone();

    let index_id_patterns = vec![format!("prefix-*-{index_id_fragment}-suffix-*")];
    let indexes_count = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest { index_id_patterns })
        .await
        .unwrap()
        .deserialize_indexes_metadata()
        .await
        .unwrap()
        .len();
    assert_eq!(indexes_count, 2);

    cleanup_index(&mut metastore, index_uid_1).await;
    cleanup_index(&mut metastore, index_uid_2).await;
    cleanup_index(&mut metastore, index_uid_3).await;
    cleanup_index(&mut metastore, index_uid_4).await;
}

pub async fn test_metastore_delete_index<
    MetastoreToTest: MetastoreService + MetastoreServiceExt + DefaultForTest,
>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-delete-index");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let index_uid_not_existing = IndexUid::new_with_random_ulid("index-not-found");
    let error = metastore
        .delete_index(DeleteIndexRequest {
            index_uid: Some(index_uid_not_existing.clone()),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    let error = metastore
        .delete_index(DeleteIndexRequest {
            index_uid: Some(index_uid_not_existing),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    metastore
        .delete_index(DeleteIndexRequest {
            index_uid: index_uid.clone().into(),
        })
        .await
        .unwrap();

    assert!(!metastore.index_exists(&index_id).await.unwrap());

    let split_id = format!("{index_id}--split");
    let split_metadata = SplitMetadata {
        split_id: split_id.clone(),
        index_uid: index_uid.clone(),
        ..Default::default()
    };

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid
        .unwrap();

    let stage_splits_request =
        StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata).unwrap();
    metastore.stage_splits(stage_splits_request).await.unwrap();

    // TODO: We should not be able to delete an index that has remaining splits, at least not as
    // a default behavior. Let's implement the logic that allows this test to pass.
    // let error = metastore.delete_index(index_uid).await.unwrap_err();
    // assert!(matches!(error, MetastoreError::IndexNotEmpty { .. }));
    // let splits = metastore.list_all_splits(index_uid.clone()).await.unwrap();
    // assert_eq!(splits.len(), 1)

    cleanup_index(&mut metastore, index_uid).await;
}
