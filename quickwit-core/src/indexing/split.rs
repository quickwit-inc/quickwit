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

use std::{fmt, path::Path};

use crate::indexing::{manifest::Manifest, statistics::StatisticEvent};
use anyhow::{self, Context};
use quickwit_directories::write_hotcache;
use quickwit_metastore::{MetastoreUriResolver, SplitMetadata};
use quickwit_storage::{PutPayload, Storage, StorageUriResolver};
use std::sync::Arc;
use std::time::Instant;
use std::{path::PathBuf, usize};
use tantivy::Directory;
use tantivy::{directory::MmapDirectory, merge_policy::NoMergePolicy, schema::Schema, Document};
use tokio::{fs, sync::mpsc::Sender};
use tracing::{info, warn};
use uuid::Uuid;

use super::IndexDataParams;

const MAX_DOC_PER_SPLIT: usize = if cfg!(test) { 100 } else { 5_000_000 };

/// Struct that represents an instance of split
pub struct Split {
    /// Id of the split.
    pub id: Uuid,
    /// uri of the index this split belongs to.
    pub index_uri: String,
    /// A combination of index_uri & split_id.
    pub split_uri: String,
    /// The split metadata.
    pub metadata: SplitMetadata,
    /// The local directory hosting this split artifacts.
    pub local_directory: PathBuf,
    /// The tantivy index for this split.
    pub index: tantivy::Index,
    /// The configured index writer for this split.
    pub index_writer: Option<tantivy::IndexWriter>,
    /// The number of parsing errors occurred during this split construction
    pub num_parsing_errors: usize,
    /// The storage instance for this split.
    pub storage: Arc<dyn Storage>,
}

impl fmt::Debug for Split {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Split")
            .field("id", &self.id)
            .field("metadata", &self.metadata)
            .field("local_directory", &self.local_directory)
            .field("index_uri", &self.index_uri)
            .field("num_parsing_errors", &self.num_parsing_errors)
            .finish()
    }
}

impl Split {
    /// Create a new instance of an index split.
    pub async fn create(
        params: &IndexDataParams,
        storage_resolver: Arc<StorageUriResolver>,
        schema: Schema,
    ) -> anyhow::Result<Self> {
        let id = Uuid::new_v4();
        let local_directory = params.temp_dir.join(format!("{}", id));
        fs::create_dir(local_directory.as_path()).await?;
        let index = tantivy::Index::create_in_dir(local_directory.as_path(), schema)?;
        let index_writer =
            index.writer_with_num_threads(params.num_threads, params.heap_size as usize)?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        let index_uri = params.index_uri.to_string_lossy().to_string();
        let metadata = SplitMetadata::new(id.to_string());

        let split_uri = format!("{}/{}", index_uri, id);
        let storage = storage_resolver.resolve(&split_uri)?;
        Ok(Self {
            id,
            index_uri,
            split_uri,
            metadata,
            local_directory,
            index,
            index_writer: Some(index_writer),
            num_parsing_errors: 0,
            storage,
        })
    }

    /// Add document to the index split.
    pub fn add_document(&mut self, doc: Document) -> anyhow::Result<()> {
        //TODO: handle time range when docMapper is available
        self.metadata.num_records += 1;
        self.index_writer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing index writer."))?
            .add_document(doc);
        Ok(())
    }

    /// Checks to see if the split has enough documents.
    pub fn has_enough_docs(&self) -> bool {
        self.metadata.num_records >= MAX_DOC_PER_SPLIT
    }

    /// Commits the split.
    pub async fn commit(&mut self) -> anyhow::Result<u64> {
        let directory_path = self.local_directory.to_path_buf();
        let mut index_writer = self.index_writer.take().unwrap();

        let (moved_index_writer, commit_opstamp) = tokio::task::spawn_blocking(move || {
            let opstamp = index_writer.commit()?;
            let hotcache_path = directory_path.join("hotcache");
            let mut hotcache_file = std::fs::File::create(&hotcache_path)?;
            let mmap_directory = MmapDirectory::open(directory_path)?;
            write_hotcache(mmap_directory, &mut hotcache_file)?;
            anyhow::Result::<(tantivy::IndexWriter, u64)>::Ok((index_writer, opstamp))
        })
        .await
        .map_err(|error| anyhow::anyhow!(error))??;

        self.index_writer = Some(moved_index_writer);
        Ok(commit_opstamp)
    }

    /// Merge all segments of the split into one.
    pub async fn merge_all_segments(&mut self) -> anyhow::Result<tantivy::SegmentMeta> {
        let segment_ids = self.index.searchable_segment_ids()?;
        self.index_writer
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Missing index writer."))?
            .merge(&segment_ids)
            .await
            .map_err(|e| e.into())
    }

    /// Stage a split in the metastore.
    pub async fn stage(&self, statistic_sender: Sender<StatisticEvent>) -> anyhow::Result<String> {
        let metastore = MetastoreUriResolver::default().resolve(&self.index_uri)?;
        let stage_result = metastore
            .stage_split(
                self.index_uri.clone(),
                self.id.to_string(),
                self.metadata.clone(),
            )
            .await;

        statistic_sender
            .send(StatisticEvent::SplitStage {
                id: self.id.to_string(),
                error: stage_result.is_err(),
            })
            .await?;
        stage_result.map_err(|err| err.into())
    }

    /// Upload all split artifacts using the storage.
    pub async fn upload(
        &self,
        statistic_sender: Sender<StatisticEvent>,
    ) -> anyhow::Result<Manifest> {
        let upload_result = put_to_storage(&*self.storage, self).await;
        match upload_result {
            Ok(manifest) => {
                statistic_sender
                    .send(StatisticEvent::SplitUpload {
                        uri: self.id.to_string(),
                        upload_size: manifest.split_size_in_bytes as usize,
                        error: false,
                    })
                    .await?;
                Ok(manifest)
            }
            Err(err) => {
                statistic_sender
                    .send(StatisticEvent::SplitUpload {
                        uri: self.id.to_string(),
                        upload_size: 0,
                        error: true,
                    })
                    .await?;
                Err(err)
            }
        }
    }

    /// Publish the split in the metastore.
    pub async fn publish(&self, statistic_sender: Sender<StatisticEvent>) -> anyhow::Result<()> {
        let metastore = MetastoreUriResolver::default().resolve(&self.index_uri)?;
        let publish_result = metastore
            .publish_split(self.index_uri.clone(), self.id.to_string())
            .await;
        statistic_sender
            .send(StatisticEvent::SplitPublish {
                uri: self.id.to_string(),
                error: publish_result.is_err(),
            })
            .await?;
        publish_result.map_err(|err| err.into())
    }
}

async fn put_to_storage(storage: &dyn Storage, split: &Split) -> anyhow::Result<Manifest> {
    info!("upload-split");
    let start = Instant::now();

    let mut manifest = Manifest::new(split.metadata.clone());
    manifest.segments = split.index.searchable_segment_ids()?;

    let mut files_to_upload: Vec<PathBuf> = split
        .index
        .searchable_segment_metas()?
        .into_iter()
        .flat_map(|segment_meta| segment_meta.list_files())
        .filter(|filepath| {
            // the list given by segment_meta.list_files() can contain false positives.
            // Some of those files actually do not exists.
            // Lets' filter them out.
            // TODO modify tantivy to make this list
            split.index.directory().exists(filepath).unwrap_or(true) //< true might look like a very odd choice here.
                                                                     // It has the benefit of triggering an error when we will effectively try to upload the files.
        })
        .map(|relative_filepath| split.local_directory.join(relative_filepath))
        .collect();
    files_to_upload.push(split.local_directory.join("meta.json"));
    files_to_upload.push(split.local_directory.join("hotcache"));

    let mut upload_res_futures = vec![];

    for path in files_to_upload {
        let file: tokio::fs::File = tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("Failed to get metadata for {:?}", &path))?;
        let metadata = file.metadata().await?;
        let file_name = match path.file_name() {
            Some(fname) => fname.to_string_lossy().to_string(),
            _ => {
                warn!(path = %path.display(), "Could not extract path as string");
                continue;
            }
        };

        manifest.push(&file_name, metadata.len());
        let key = Path::new(&split.split_uri).join(&file_name);
        let payload = quickwit_storage::PutPayload::from(path.clone());
        let upload_res_future = async move {
            storage.put(&key, payload).await.with_context(|| {
                format!(
                    "Failed uploading key {} in bucket {}",
                    key.display(),
                    split.index_uri
                )
            })?;
            Result::<(), anyhow::Error>::Ok(())
        };

        upload_res_futures.push(upload_res_future);
    }

    futures::future::try_join_all(upload_res_futures).await?;

    let manifest_body = manifest.to_json()?.into_bytes();
    let manifest_path = Path::new(split.index_uri.as_str()).join(".manifest");
    storage
        .put(&manifest_path, PutPayload::from(manifest_body))
        .await?;

    let elapsed_secs = start.elapsed().as_secs();
    let file_statistics = manifest.file_statistics();
    info!(
        min_file_size_in_bytes = %file_statistics.min_file_size_in_bytes,
        max_file_size_in_bytes = %file_statistics.max_file_size_in_bytes,
        avg_file_size_in_bytes = %file_statistics.avg_file_size_in_bytes,
        split_size_in_megabytes = %manifest.split_size_in_bytes / 1000,
        elapsed_secs = %elapsed_secs,
        throughput_mb_s = %manifest.split_size_in_bytes / 1000 / elapsed_secs.max(1),
        "Uploaded split to storage"
    );

    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_split() -> anyhow::Result<()> {
        let split_dir = tempfile::tempdir()?;
        let params = &IndexDataParams {
            index_uri: PathBuf::from_str("file://test")?,
            input_uri: None,
            temp_dir: split_dir.into_path(),
            num_threads: 1,
            heap_size: 3000000,
            overwrite: false,
        };
        let schema = Schema::builder().build();
        let storage_resolver = Arc::new(StorageUriResolver::default());

        let split_result = Split::create(params, storage_resolver, schema).await;
        assert_eq!(split_result.is_ok(), true);

        let mut split = split_result?;

        for _ in 0..10 {
            split.add_document(Document::default())?;
        }

        assert_eq!(split.metadata.num_records, 10);
        assert_eq!(split.num_parsing_errors, 0);

        Ok(())
    }
}
