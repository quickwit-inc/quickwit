/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use std::collections::HashMap;
use std::ops::{Range, RangeInclusive};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::RwLock;

use quickwit_storage::{PutPayload, Storage, StorageErrorKind};

use crate::{
    IndexMetadata, MetadataSet, Metastore, MetastoreError, MetastoreResult, SplitMetadata,
    SplitState,
};

/// Metadata file managed by [`SingleFileMetastore`].
const META_FILENAME: &str = "quickwit.json";

/// Creates a path to the metadata file from the given index ID.
fn meta_path(index_id: &str) -> PathBuf {
    Path::new(index_id).join(Path::new(META_FILENAME))
}

/// Takes 2 intervals and returns true iff their intersection is empty
fn is_disjoint(left: &Range<i64>, right: &RangeInclusive<i64>) -> bool {
    left.end <= *right.start() || *right.end() < left.start
}

/// Single file meta store implementation.
pub struct SingleFileMetastore {
    storage: Arc<dyn Storage>,
    cache: Arc<RwLock<HashMap<String, MetadataSet>>>,
}

#[allow(dead_code)]
impl SingleFileMetastore {
    #[cfg(test)]
    pub fn for_test() -> Self {
        use quickwit_storage::RamStorage;
        SingleFileMetastore::new(Arc::new(RamStorage::default()))
    }

    /// Creates a meta store given a storage.
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        SingleFileMetastore {
            storage,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Checks whether the index exists in storage.
    async fn index_exists(&self, index_id: &str) -> MetastoreResult<bool> {
        let index_path = meta_path(index_id);

        let exists =
            self.storage
                .exists(&index_path)
                .await
                .map_err(|storage_err| match storage_err.kind() {
                    StorageErrorKind::DoesNotExist => MetastoreError::IndexDoesNotExist {
                        index_id: index_id.to_string(),
                    },
                    StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
                        message: "The request credentials do not allow this operation.".to_string(),
                    },
                    _ => MetastoreError::InternalError {
                        message: "Failed to check index file existence.".to_string(),
                        cause: anyhow::anyhow!(storage_err),
                    },
                })?;

        Ok(exists)
    }

    /// Returns all of the data associated with the given index.
    ///
    /// If the value is already in cache, then the call returns right away.
    /// If not, it is fetched from the storage.
    async fn get_index(&self, index_id: &str) -> MetastoreResult<MetadataSet> {
        // We first check if the index is in the cache...
        {
            let cache = self.cache.read().await;
            if let Some(index_metadata) = cache.get(index_id) {
                return Ok(index_metadata.clone());
            }
        }

        // It is not in the cache yet, let's fetch it from the storage...
        let index_path = meta_path(index_id);
        let content = self
            .storage
            .get_all(&index_path)
            .await
            .map_err(|storage_err| match storage_err.kind() {
                StorageErrorKind::DoesNotExist => MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                },
                StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
                    message: "The request credentials do not allow for this operation.".to_string(),
                },
                _ => MetastoreError::InternalError {
                    message: "Failed to get index files.".to_string(),
                    cause: anyhow::anyhow!(storage_err),
                },
            })?;

        let metadata_set = serde_json::from_slice::<MetadataSet>(&content[..])
            .map_err(|serde_err| MetastoreError::InvalidManifest { cause: serde_err })?;

        // Finally, update the cache accordingly
        let mut cache = self.cache.write().await;
        cache.insert(index_id.to_string(), metadata_set.clone());

        Ok(metadata_set)
    }

    /// Serializes the metadata set and stores the data on the storage.
    async fn put_index(&self, metadata_set: MetadataSet) -> MetastoreResult<()> {
        // Serialize metadata set.
        let content: Vec<u8> = serde_json::to_vec(&metadata_set).map_err(|serde_err| {
            MetastoreError::InternalError {
                message: "Failed to serialize Metadata set".to_string(),
                cause: anyhow::anyhow!(serde_err),
            }
        })?;

        let index_id = metadata_set.index.index_id.clone();
        let index_path = meta_path(&index_id);

        // Put data back into storage.
        self.storage
            .put(&index_path, PutPayload::from(content))
            .await
            .map_err(|storage_err| match storage_err.kind() {
                StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
                    message: "The request credentials do not allow for this operation.".to_string(),
                },
                _ => MetastoreError::InternalError {
                    message: "Failed to put metadata set back into storage.".to_string(),
                    cause: anyhow::anyhow!(storage_err),
                },
            })?;

        // Update the internal data if the storage is successfully updated.
        let mut cache = self.cache.write().await;
        cache.insert(index_id, metadata_set);

        Ok(())
    }
}

#[async_trait]
impl Metastore for SingleFileMetastore {
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()> {
        // Check for the existence of index.
        let exists = self.index_exists(&index_metadata.index_id).await?;

        if exists {
            return Err(MetastoreError::IndexAlreadyExists {
                index_id: index_metadata.index_id.clone(),
            });
        }

        let metadata_set = MetadataSet {
            index: index_metadata,
            splits: HashMap::new(),
        };
        self.put_index(metadata_set).await?;

        Ok(())
    }

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        // Check whether the index exists.
        let exists = self.index_exists(index_id).await?;

        if !exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        let index_path = meta_path(index_id);

        // Delete metadata set from storage.
        self.storage
            .delete(&index_path)
            .await
            .map_err(|storage_err| match storage_err.kind() {
                StorageErrorKind::DoesNotExist => MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                },
                StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
                    message: "The request credentials do not allow for this operation.".to_string(),
                },
                _ => MetastoreError::InternalError {
                    message: "Failed to delete metadata set from storage.".to_string(),
                    cause: anyhow::anyhow!(storage_err),
                },
            })?;

        // Update the internal data if the storage is successfully updated.
        let mut cache = self.cache.write().await;
        cache.remove(index_id);

        Ok(())
    }

    async fn stage_split(
        &self,
        index_id: &str,
        mut split_metadata: SplitMetadata,
    ) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        // Check whether the split exists.
        // If the split exists, return an error to prevent the split from being registered.
        if metadata_set.splits.contains_key(&split_metadata.split_id) {
            return Err(MetastoreError::InternalError {
                message: format!(
                    "Try to stage split that already exists ({})",
                    split_metadata.split_id
                ),
                cause: anyhow::anyhow!(""),
            });
        }

        // Insert a new split metadata as `Staged` state.
        split_metadata.split_state = SplitState::Staged;
        split_metadata.update_timestamp = Utc::now().timestamp();
        metadata_set
            .splits
            .insert(split_metadata.split_id.to_string(), split_metadata);

        self.put_index(metadata_set).await?;

        Ok(())
    }

    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        for &split_id in split_ids {
            // Check for the existence of split.
            let mut split_metadata = metadata_set.splits.get_mut(split_id).ok_or_else(|| {
                MetastoreError::SplitDoesNotExist {
                    split_id: split_id.to_string(),
                }
            })?;

            match split_metadata.split_state {
                SplitState::Published => {
                    // Split is already published. This is fine, we just skip it.
                    continue;
                }
                SplitState::Staged => {
                    // The split state needs to be updated.
                    split_metadata.split_state = SplitState::Published;
                    split_metadata.update_timestamp = Utc::now().timestamp();
                }
                _ => {
                    return Err(MetastoreError::SplitIsNotStaged {
                        split_id: split_id.to_string(),
                    })
                }
            }
        }
        self.put_index(metadata_set).await?;
        Ok(())
    }

    async fn list_splits(
        &self,
        index_id: &str,
        state: SplitState,
        time_range_opt: Option<Range<i64>>,
        tag_opt: Option<String>,
    ) -> MetastoreResult<Vec<SplitMetadata>> {
        let time_range_filter = |split_metadata: &SplitMetadata| match (
            time_range_opt.as_ref(),
            split_metadata.time_range.as_ref(),
        ) {
            (Some(filter_time_range), Some(split_time_range)) => {
                !is_disjoint(filter_time_range, split_time_range)
            }
            _ => true, // Return `true` if `time_range` is omitted or the split has no time range.
        };

        let tag_filter = |split_metadata: &SplitMetadata| match tag_opt.as_ref() {
            Some(tag) => split_metadata.tags.contains(tag),
            _ => true,
        };

        let metadata_set = self.get_index(index_id).await?;
        let splits = metadata_set
            .splits
            .into_values()
            .filter(|split_metadata| {
                split_metadata.split_state == state
                    && time_range_filter(split_metadata)
                    && tag_filter(split_metadata)
            })
            .collect();
        Ok(splits)
    }

    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<SplitMetadata>> {
        let metadata_set = self.get_index(index_id).await?;
        let splits = metadata_set.splits.into_values().collect();
        Ok(splits)
    }

    async fn mark_splits_as_deleted<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        let mut is_modified = false;
        for &split_id in split_ids {
            // Check for the existence of split.
            let split_metadata = metadata_set.splits.get_mut(split_id).ok_or_else(|| {
                MetastoreError::SplitDoesNotExist {
                    split_id: split_id.to_string(),
                }
            })?;

            if split_metadata.split_state == SplitState::ScheduledForDeletion {
                // If the split is already scheduled for deletion, this API call returns success.
                continue;
            }

            split_metadata.split_state = SplitState::ScheduledForDeletion;
            split_metadata.update_timestamp = Utc::now().timestamp();
            is_modified = true;
        }

        if is_modified {
            self.put_index(metadata_set).await?;
        }

        Ok(())
    }

    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        for &split_id in split_ids {
            // Check for the existence of split.
            let split_metadata = metadata_set.splits.get_mut(split_id).ok_or_else(|| {
                MetastoreError::SplitDoesNotExist {
                    split_id: split_id.to_string(),
                }
            })?;

            match split_metadata.split_state {
                SplitState::ScheduledForDeletion | SplitState::Staged => {
                    // Only `ScheduledForDeletion` and `Staged` can be deleted
                    metadata_set.splits.remove(split_id);
                }
                _ => {
                    let message: String = format!(
                        "This split is not in a deletable state: {:?}:{:?}",
                        split_id, &split_metadata.split_state
                    );
                    return Err(MetastoreError::Forbidden { message });
                }
            }
        }

        self.put_index(metadata_set).await?;

        Ok(())
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        let index_metadata = self.get_index(index_id).await?;
        Ok(index_metadata.index)
    }

    fn uri(&self) -> String {
        self.storage.uri()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use quickwit_index_config::AllFlattenIndexConfig;
    use quickwit_storage::{MockStorage, StorageErrorKind};

    use crate::tests::*;
    use crate::{IndexMetadata, Metastore, MetastoreError, SingleFileMetastore};

    #[tokio::test]
    async fn test_single_file_metastore_index_exists() {
        let metastore = SingleFileMetastore::for_test();
        let index_id = "my-index";

        {
            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = false;
            assert_eq!(result, expected);

            let index_metadata = IndexMetadata {
                index_id: index_id.to_string(),
                index_uri: "ram://indexes/my-index".to_string(),
                index_config: Arc::new(AllFlattenIndexConfig::default()),
            };

            // Create index
            metastore.create_index(index_metadata).await.unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_get_index() {
        let metastore = SingleFileMetastore::for_test();
        let index_id = "my-index";

        {
            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = false;
            assert_eq!(result, expected);

            let index_metadata = IndexMetadata {
                index_id: index_id.to_string(),
                index_uri: "ram://indexes/my-index".to_string(),
                index_config: Arc::new(AllFlattenIndexConfig::default()),
            };

            // Create index
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);

            // Open index and check its metadata
            let created_index = metastore.get_index(index_id).await.unwrap();
            assert_eq!(created_index.index.index_id, index_metadata.index_id);
            assert_eq!(
                created_index.index.index_uri.clone(),
                index_metadata.index_uri
            );

            assert_eq!(
                format!("{:?}", created_index.index.index_config),
                "AllFlattenIndexConfig".to_string()
            );

            // Open a non-existent index.
            let metastore_error = metastore.get_index("non-existent-index").await.unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::IndexDoesNotExist { .. }
            ));
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_create_index() {
        let metastore = SingleFileMetastore::for_test();
        test_metastore_create_index(&metastore).await;
    }

    #[tokio::test]
    async fn test_single_file_metastore_delete_index() {
        let metastore = SingleFileMetastore::for_test();
        test_metastore_delete_index(&metastore).await;
    }

    #[tokio::test]
    async fn test_single_file_metastore_index_metadata() {
        let metastore = SingleFileMetastore::for_test();
        test_metastore_index_metadata(&metastore).await;
    }

    #[tokio::test]
    async fn test_single_file_metastore_stage_split() {
        let metastore = SingleFileMetastore::for_test();
        test_metastore_stage_split(&metastore).await;
    }

    #[tokio::test]
    async fn test_single_file_metastore_publish_splits() {
        let metastore = SingleFileMetastore::for_test();
        test_metastore_publish_splits(&metastore).await;
    }

    #[tokio::test]
    async fn test_single_file_metastore_mark_splits_as_deleted() {
        let metastore = SingleFileMetastore::for_test();
        test_metastore_mark_splits_as_deleted(&metastore).await;
    }

    #[tokio::test]
    async fn test_single_file_metastore_delete_splits() {
        let metastore = SingleFileMetastore::for_test();
        test_metastore_delete_splits(&metastore).await;
    }

    #[tokio::test]
    async fn test_single_file_metastore_list_all_splits() {
        let metastore = SingleFileMetastore::for_test();
        test_metastore_list_all_splits(&metastore).await;
    }

    #[tokio::test]
    async fn test_single_file_metastore_list_splits() {
        let metastore = SingleFileMetastore::for_test();
        test_metastore_list_splits(&metastore).await;
    }

    #[tokio::test]
    async fn test_single_file_metastore_split_update_timestamp() {
        let metastore = SingleFileMetastore::for_test();
        test_metastore_split_update_timestamp(&metastore).await;
    }

    #[tokio::test]
    async fn test_single_file_metastore_storage_failing() {
        // The single file metastore should not update its internal state if the storage fails.
        let mut mock_storage = MockStorage::default();

        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(false));
        mock_storage.expect_put().times(2).returning(|uri, _| {
            assert_eq!(uri, Path::new("my-index/quickwit.json"));
            Ok(())
        });
        mock_storage.expect_put().times(1).returning(|_uri, _| {
            Err(StorageErrorKind::Io
                .with_error(anyhow::anyhow!("Oops. Some network problem maybe?")))
        });

        let metastore = SingleFileMetastore::new(Arc::new(mock_storage));
        test_metastore_storage_failing(&metastore).await;
    }
}
