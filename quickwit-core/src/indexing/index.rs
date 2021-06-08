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

use std::path::PathBuf;
use std::sync::Arc;

use futures::try_join;
use quickwit_metastore::{MetastoreUriResolver, SplitState};
use quickwit_storage::StorageUriResolver;
use tokio::sync::mpsc::channel;
use tracing::warn;

use crate::index::garbage_collect;
use crate::indexing::document_retriever::DocumentSource;
use crate::indexing::split_finalizer::finalize_split;
use crate::indexing::{document_indexer::index_documents, split::Split};

use super::IndexingStatistics;

const SPLIT_CHANNEL_SIZE: usize = 30;

/// A struct to bundle index cli args
/// TODO: remove when there is a better structure
#[derive(Debug, Clone)]
pub struct IndexDataParams {
    /// Index uri.
    pub index_uri: PathBuf,
    /// Input path from where to read new-line delimited json documents
    pub input_uri: Option<PathBuf>,
    /// Tempory directory to use for indexing.
    pub temp_dir: PathBuf,
    /// Number of thread to use for indexing.
    pub num_threads: usize,
    /// Amount of memory shared among indexing threads.
    pub heap_size: u64,
    /// Clear existing indexed data before indexing.
    pub overwrite: bool,
}

/// Indexes a Newline Delimited JSON (NDJSON) dataset located at `params.index_uri` or read from stdin.
/// The data is appended to the target index specified by `params.index_uri`.
/// When `params.overwrite` is specified, the previously indexed data is cleared before proceeding.
/// The indexing also takes an [`IndexingStatistics`] object for collecting various indexing metrics.
///
/// * `metastore_uri` - The metastore uri.
/// * `index_id` - The target index Id.
/// * `params` - The indexing parameters; see [`IndexDataParams`].
/// * `statistics` - The statistic counter object; see [`IndexingStatistics`].
///
pub async fn index_data(
    metastore_uri: &str,
    index_id: &str,
    params: IndexDataParams,
    statistics: Arc<IndexingStatistics>,
) -> anyhow::Result<()> {
    let metastore = MetastoreUriResolver::default()
        .resolve(&metastore_uri)
        .await?;
    let storage_resolver = Arc::new(StorageUriResolver::default());

    if params.overwrite {
        reset_index(metastore_uri, index_id, storage_resolver.clone()).await?;
    }

    if params.input_uri.is_none() {
        println!("Please enter your new line delimited json documents.");
    }

    let document_retriever = Box::new(DocumentSource::create(&params.input_uri).await?);
    let (split_sender, split_receiver) = channel::<Split>(SPLIT_CHANNEL_SIZE);
    try_join!(
        index_documents(
            index_id.to_owned(),
            &params,
            metastore.clone(),
            storage_resolver,
            document_retriever,
            split_sender,
            statistics.clone(),
        ),
        finalize_split(split_receiver, metastore, statistics.clone()),
    )?;

    Ok(())
}

/// Clears the index by applying the following actions:
/// - mark all split as deleted.
/// - delete the files of all splits marked as deleted using garbage collection.
/// - delete the splits from the metastore.
///
/// * `index_uri` - The target index Uri.
/// * `index_id` - The target index Id.
/// * `storage_resolver` - A storage resolver object to access the storage.
/// * `metastore` - A emtastore object for interracting with the metastore.
///
async fn reset_index(
    metastore_uri: &str,
    index_id: &str,
    storage_resolver: Arc<StorageUriResolver>,
) -> anyhow::Result<()> {
    let metastore = MetastoreUriResolver::default()
        .resolve(&metastore_uri)
        .await?;

    let splits = metastore
        .list_splits(index_id, SplitState::Published, None)
        .await?;
    let split_ids = splits
        .iter()
        .map(|split_meta| split_meta.split_id.as_str())
        .collect::<Vec<_>>();
    metastore
        .mark_splits_as_deleted(index_id, split_ids)
        .await?;

    let garbage_collection_result =
        garbage_collect(metastore_uri, index_id, storage_resolver).await;
    if garbage_collection_result.is_err() {
        warn!(metastore_uri =% metastore_uri, "All split files could not be removed during garbage collection.");
    }

    let delete_tasks = splits
        .iter()
        .map(|split| metastore.delete_split(index_id, &split.split_id))
        .collect::<Vec<_>>();
    futures::future::try_join_all(delete_tasks).await?;

    Ok(())
}
