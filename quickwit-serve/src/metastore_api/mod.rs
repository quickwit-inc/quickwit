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

mod grpc_adapter;
mod rest_handler;

pub use grpc_adapter::GrpcMetastoreServiceAdapter;

pub use self::rest_handler::metastore_api_handlers;

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use futures::StreamExt;
    use quickwit_cluster::{Member, QuickwitService};
    use quickwit_control_plane::MetastoreService;
    use quickwit_metastore::checkpoint::IndexCheckpointDelta;
    use quickwit_metastore::{IndexMetadata, Metastore, MetastoreError, MockMetastore};
    use quickwit_proto::metastore_api::metastore_api_service_server::MetastoreApiServiceServer;
    use quickwit_proto::metastore_api::{
        CreateIndexRequest, IndexMetadataRequest, PublishSplitsRequest,
    };
    use quickwit_proto::tonic::transport::Server;
    use tokio::sync::watch;
    use tokio_stream::wrappers::WatchStream;

    use super::GrpcMetastoreServiceAdapter;

    async fn start_test_server(
        address: SocketAddr,
        metastore_service: MetastoreService,
    ) -> anyhow::Result<()> {
        let grpc_adpater = GrpcMetastoreServiceAdapter::from(metastore_service);
        tokio::spawn(async move {
            Server::builder()
                .add_service(MetastoreApiServiceServer::new(grpc_adpater))
                .serve(address)
                .await?;
            Result::<_, anyhow::Error>::Ok(())
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_metastore_service_grpc_with_one_control_plan_service() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        let metastore_service = MetastoreService::from_metastore(Arc::new(metastore));

        let grpc_port = quickwit_common::net::find_available_tcp_port()?;
        let grpc_addr: SocketAddr = ([127, 0, 0, 1], grpc_port).into();
        start_test_server(grpc_addr, metastore_service).await?;

        let control_plane_member = Member::new(
            "1".to_string(),
            0,
            grpc_addr,
            HashSet::from([QuickwitService::ControlPlane, QuickwitService::Indexer]),
            grpc_addr,
        );
        let searcher_member = Member::new(
            "1".to_string(),
            0,
            grpc_addr,
            HashSet::from([QuickwitService::Searcher]),
            grpc_addr,
        );
        let (members_tx, members_rx) = watch::channel::<Vec<Member>>(Vec::new());
        let mut watch_members = WatchStream::new(members_rx);
        watch_members.next().await; // Consumes the first value in channel which is empty vec.
        let mut grpc_service = MetastoreService::create_and_update_grpc_service_from_members(
            &vec![control_plane_member.clone()],
            watch_members,
        )
        .await
        .unwrap();

        let request = IndexMetadataRequest {
            index_id: "test-index".to_string(),
        };

        // gRPC service should send request on the running server.
        let result = grpc_service.index_metadata(request.clone()).await;
        assert!(result.is_ok());

        // Send empty vec to signal that there is no more control plane in the cluster.
        let _ = members_tx.send(Vec::new());
        let err = grpc_service
            .index_metadata(request.clone())
            .await
            .unwrap_err();
        println!("metastore error {:?}", err);
        assert!(
            matches!(err, MetastoreError::ConnectionError { message } if message.starts_with("gRPC request timeout triggered by the channel timeout"))
        );

        // Send control plan member and check it's working again.
        let _ = members_tx.send(vec![control_plane_member.clone()]);
        let result = grpc_service.index_metadata(request.clone()).await;
        assert!(result.is_ok());

        // Send searcher member only.
        let _ = members_tx.send(vec![searcher_member.clone()]);
        let result = grpc_service.index_metadata(request.clone()).await;
        assert!(result.is_err());

        // Send control plane + searcher members.
        let _ = members_tx.send(vec![control_plane_member.clone(), searcher_member.clone()]);
        let result = grpc_service.index_metadata(request.clone()).await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_metastore_service_grpc_with_multiple_control_plan_services() -> anyhow::Result<()>
    {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        let metastore_service = MetastoreService::from_metastore(Arc::new(metastore));

        let grpc_port_1 = quickwit_common::net::find_available_tcp_port()?;
        let grpc_addr_1: SocketAddr = ([127, 0, 0, 1], grpc_port_1).into();
        let grpc_addr_2: SocketAddr = ([127, 0, 0, 1], grpc_port_1 + 1).into();
        let grpc_addr_3: SocketAddr = ([127, 0, 0, 1], grpc_port_1 - 1).into();
        // Only one grpc socket will work.
        start_test_server(grpc_addr_1, metastore_service).await?;

        let control_plane_member_1 = Member::new(
            "1".to_string(),
            0,
            grpc_addr_1,
            HashSet::from([QuickwitService::ControlPlane]),
            grpc_addr_1,
        );
        let control_plane_member_2 = Member::new(
            "2".to_string(),
            0,
            grpc_addr_2,
            HashSet::from([QuickwitService::ControlPlane]),
            grpc_addr_2,
        );
        let control_plane_member_3 = Member::new(
            "3".to_string(),
            0,
            grpc_addr_3,
            HashSet::from([QuickwitService::ControlPlane]),
            grpc_addr_3,
        );
        let (members_tx, members_rx) = watch::channel::<Vec<Member>>(Vec::new());
        let mut watch_members = WatchStream::new(members_rx);
        watch_members.next().await; // <- Consumes the first value in channel which is empty vec.
        let mut grpc_service = MetastoreService::create_and_update_grpc_service_from_members(
            &vec![
                control_plane_member_1.clone(),
                control_plane_member_2.clone(),
            ],
            watch_members,
        )
        .await
        .unwrap();

        let request = IndexMetadataRequest {
            index_id: "test-index".to_string(),
        };

        // gRPC service should send request to the first grpc_addr (`SocketAddress` order is used).
        let result = grpc_service.index_metadata(request.clone()).await;
        assert!(result.is_ok());

        // Send 3 control plane members, this time the first address (grpc_addr_3) will not respond
        // as we did not start any gRPC server on this address.
        let _ = members_tx.send(vec![
            control_plane_member_1.clone(),
            control_plane_member_3.clone(),
            control_plane_member_2.clone(),
        ]);
        let err = grpc_service
            .index_metadata(request.clone())
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("tcp connect error: Connection refused"));

        // Send the running control plan member.
        let _ = members_tx.send(vec![control_plane_member_1.clone()]);
        let result = grpc_service.index_metadata(request.clone()).await;
        assert!(result.is_ok());

        Ok(())
    }

    // Creates an [`MetastoreService`] and use a gRPC server with the adapter so
    // that it sends requests to the [`MetastoreService`].
    async fn create_metastore_grpc_service_with_duplex_stream(
        mock_metastore: Arc<dyn Metastore>,
    ) -> anyhow::Result<MetastoreService> {
        let (client, server) = tokio::io::duplex(1024);
        let metastore_service_local = MetastoreService::from_metastore(mock_metastore);
        let grpc_adapter = GrpcMetastoreServiceAdapter::from(metastore_service_local);
        tokio::spawn(async move {
            Server::builder()
                .add_service(MetastoreApiServiceServer::new(grpc_adapter))
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });
        let metastore_service_client = MetastoreService::from_duplex_stream(client).await?;
        Ok(metastore_service_client)
    }

    #[tokio::test]
    async fn test_grpc_metastore_service_index_metadata() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .return_once(|index_id: &str| {
                assert_eq!(index_id, "test-index");
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        let mut grpc_service =
            create_metastore_grpc_service_with_duplex_stream(Arc::new(metastore))
                .await
                .unwrap();

        let request = IndexMetadataRequest {
            index_id: "test-index".to_string(),
        };
        let result = grpc_service.index_metadata(request).await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_grpc_metastore_service_create_index() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        let index_to_create = IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
        let index_metadata_serialized_json = serde_json::to_string(&index_to_create).unwrap();
        let index_metadata_serialized_json_clone = index_metadata_serialized_json.clone();
        metastore
            .expect_create_index()
            .return_once(move |index_metadata| {
                assert_eq!(
                    serde_json::to_string(&index_metadata).unwrap(),
                    index_metadata_serialized_json_clone
                );
                Ok(())
            });
        let mut grpc_service =
            create_metastore_grpc_service_with_duplex_stream(Arc::new(metastore))
                .await
                .unwrap();

        let request = CreateIndexRequest {
            index_metadata_serialized_json,
        };
        let result = grpc_service.create_index(request).await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_grpc_metastore_service_publish_splits() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        let requested_index_id = "test-index".to_string();
        let requested_split_ids = vec!["new-split-1".to_string()];
        let requested_replaced_split_ids = vec!["replaced-split-1".to_string()];
        let requested_index_checkpoint_delta =
            IndexCheckpointDelta::for_test("source_id", 10..20).into();
        let publish_request = PublishSplitsRequest {
            index_id: requested_index_id.clone(),
            split_ids: requested_split_ids.clone(),
            replaced_split_ids: requested_replaced_split_ids.clone(),
            index_checkpoint_delta_serialized_json: Some(
                serde_json::to_string(&requested_index_checkpoint_delta).unwrap(),
            ),
        };
        metastore.expect_publish_splits().return_once(
            move |index_id, split_ids, replaced_split_ids, checkpoint_delta_opt| {
                assert_eq!(requested_index_id, index_id);
                assert_eq!(requested_split_ids, split_ids);
                assert_eq!(requested_replaced_split_ids, replaced_split_ids);
                assert_eq!(Some(requested_index_checkpoint_delta), checkpoint_delta_opt);
                Ok(())
            },
        );
        let mut grpc_service =
            create_metastore_grpc_service_with_duplex_stream(Arc::new(metastore))
                .await
                .unwrap();
        let result = grpc_service.publish_splits(publish_request).await;
        assert!(result.is_ok());

        Ok(())
    }
}
