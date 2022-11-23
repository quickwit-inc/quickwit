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

use std::path::PathBuf;

use quickwit_metastore_utils::{GrpcCall, GrpcRequest};
use quickwit_proto::metastore_api::metastore_api_service_client::MetastoreApiServiceClient;
use quickwit_proto::tonic::transport::Channel;
use structopt::StructOpt;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;

async fn replay_grpc_request(
    client: &mut MetastoreApiServiceClient<Channel>,
    req: GrpcRequest,
) -> anyhow::Result<()> {
    match req {
        GrpcRequest::CreateIndexRequest(req) => {
            client.create_index(req).await?;
        }
        GrpcRequest::IndexMetadataRequest(req) => {
            client.index_metadata(req).await?;
        }
        GrpcRequest::ListIndexesMetadatasRequest(req) => {
            client.list_indexes_metadatas(req).await?;
        }
        GrpcRequest::DeleteIndexRequest(req) => {
            client.delete_index(req).await?;
        }
        GrpcRequest::ListAllSplitsRequest(req) => {
            client.list_all_splits(req).await?;
        }
        GrpcRequest::ListSplitsRequest(req) => {
            client.list_splits(req).await?;
        }
        GrpcRequest::StageSplitRequest(req) => {
            client.stage_split(req).await?;
        }
        GrpcRequest::StageSplitsRequest(req) => {
            client.stage_splits(req).await?;
        }
        GrpcRequest::PublishSplitsRequest(req) => {
            client.publish_splits(req).await?;
        }
        GrpcRequest::MarkSplitsForDeletionRequest(req) => {
            client.mark_splits_for_deletion(req).await?;
        }
        GrpcRequest::DeleteSplitsRequest(req) => {
            client.delete_splits(req).await?;
        }
        GrpcRequest::AddSourceRequest(req) => {
            client.add_source(req).await?;
        }
        GrpcRequest::ToggleSourceRequest(req) => {
            client.toggle_source(req).await?;
        }
        GrpcRequest::DeleteSourceRequest(req) => {
            client.delete_source(req).await?;
        }
        GrpcRequest::LastDeleteOpstampRequest(req) => {
            client.last_delete_opstamp(req).await?;
        }
        GrpcRequest::ResetSourceCheckpointRequest(req) => {
            client.reset_source_checkpoint(req).await?;
        }
        GrpcRequest::DeleteQuery(req) => {
            client.create_delete_task(req).await?;
        }
        GrpcRequest::UpdateSplitsDeleteOpstampRequest(req) => {
            client.update_splits_delete_opstamp(req).await?;
        }
        GrpcRequest::ListDeleteTasksRequest(req) => {
            client.list_delete_tasks(req).await?;
        }
        GrpcRequest::ListStaleSplitsRequest(req) => {
            client.list_stale_splits(req).await?;
        }
    }
    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "replay",
    about = "A quickwit-metastore program to replay request log generated by proxy"
)]
struct Opt {
    #[structopt(
        long,
        default_value = "./replay-data/requests-partition-wikitenant.ndjson"
    )]
    file: PathBuf,
    #[structopt(long, default_value = "http://127.0.0.1:7281")]
    forward_to: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    let file = File::open(&opt.file).await?;
    let buffered = tokio::io::BufReader::new(file);
    let mut lines = buffered.lines();
    let mut client = MetastoreApiServiceClient::connect(opt.forward_to.clone()).await?;
    let mut i = 0;
    while let Some(line) = lines.next_line().await? {
        println!("line {i} = {line}");
        let grpc_call: GrpcCall = serde_json::from_str(&line)?;
        replay_grpc_request(&mut client, grpc_call.grpc_request).await?;
        i += 1;
    }
    Ok(())
}
