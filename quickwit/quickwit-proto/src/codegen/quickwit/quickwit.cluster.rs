#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChitchatId {
    #[prost(string, tag = "1")]
    pub node_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub generation_id: u64,
    #[prost(string, tag = "3")]
    pub gossip_advertise_addr: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VersionedKeyValue {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub version: u64,
    #[prost(enumeration = "DeletionStatus", tag = "4")]
    pub status: i32,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeState {
    #[prost(message, optional, tag = "1")]
    pub chitchat_id: ::core::option::Option<ChitchatId>,
    #[prost(message, repeated, tag = "2")]
    pub key_values: ::prost::alloc::vec::Vec<VersionedKeyValue>,
    #[prost(uint64, tag = "3")]
    pub max_version: u64,
    #[prost(uint64, tag = "4")]
    pub last_gc_version: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchClusterStateRequest {
    #[prost(string, tag = "1")]
    pub cluster_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchClusterStateResponse {
    #[prost(string, tag = "1")]
    pub cluster_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub node_states: ::prost::alloc::vec::Vec<NodeState>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DeletionStatus {
    Set = 0,
    Deleted = 1,
    DeleteAfterTtl = 2,
}
impl DeletionStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            DeletionStatus::Set => "Set",
            DeletionStatus::Deleted => "Deleted",
            DeletionStatus::DeleteAfterTtl => "DeleteAfterTtl",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Set" => Some(Self::Set),
            "Deleted" => Some(Self::Deleted),
            "DeleteAfterTtl" => Some(Self::DeleteAfterTtl),
            _ => None,
        }
    }
}
/// BEGIN quickwit-codegen
#[allow(unused_imports)]
use std::str::FromStr;
use tower::{Layer, Service, ServiceExt};
use quickwit_common::tower::RpcName;
impl RpcName for FetchClusterStateRequest {
    fn rpc_name() -> &'static str {
        "fetch_cluster_state"
    }
}
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait ClusterService: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    async fn fetch_cluster_state(
        &mut self,
        request: FetchClusterStateRequest,
    ) -> crate::cluster::ClusterResult<FetchClusterStateResponse>;
}
dyn_clone::clone_trait_object!(ClusterService);
#[cfg(any(test, feature = "testsuite"))]
impl Clone for MockClusterService {
    fn clone(&self) -> Self {
        MockClusterService::new()
    }
}
#[derive(Debug, Clone)]
pub struct ClusterServiceClient {
    inner: Box<dyn ClusterService>,
}
impl ClusterServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: ClusterService,
    {
        #[cfg(any(test, feature = "testsuite"))]
        assert!(
            std::any::TypeId::of:: < T > () != std::any::TypeId::of:: <
            MockClusterService > (),
            "`MockClusterService` must be wrapped in a `MockClusterServiceWrapper`. Use `MockClusterService::from(mock)` to instantiate the client."
        );
        Self { inner: Box::new(instance) }
    }
    pub fn as_grpc_service(
        &self,
        max_message_size: bytesize::ByteSize,
    ) -> cluster_service_grpc_server::ClusterServiceGrpcServer<
        ClusterServiceGrpcServerAdapter,
    > {
        let adapter = ClusterServiceGrpcServerAdapter::new(self.clone());
        cluster_service_grpc_server::ClusterServiceGrpcServer::new(adapter)
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize)
    }
    pub fn from_channel(
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
        max_message_size: bytesize::ByteSize,
    ) -> Self {
        let (_, connection_keys_watcher) = tokio::sync::watch::channel(
            std::collections::HashSet::from_iter([addr]),
        );
        let client = cluster_service_grpc_client::ClusterServiceGrpcClient::new(channel)
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        let adapter = ClusterServiceGrpcClientAdapter::new(
            client,
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_balance_channel(
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
        max_message_size: bytesize::ByteSize,
    ) -> ClusterServiceClient {
        let connection_keys_watcher = balance_channel.connection_keys_watcher();
        let client = cluster_service_grpc_client::ClusterServiceGrpcClient::new(
                balance_channel,
            )
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        let adapter = ClusterServiceGrpcClientAdapter::new(
            client,
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        ClusterServiceMailbox<A>: ClusterService,
    {
        ClusterServiceClient::new(ClusterServiceMailbox::new(mailbox))
    }
    pub fn tower() -> ClusterServiceTowerLayerStack {
        ClusterServiceTowerLayerStack::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mock() -> MockClusterService {
        MockClusterService::new()
    }
}
#[async_trait::async_trait]
impl ClusterService for ClusterServiceClient {
    async fn fetch_cluster_state(
        &mut self,
        request: FetchClusterStateRequest,
    ) -> crate::cluster::ClusterResult<FetchClusterStateResponse> {
        self.inner.fetch_cluster_state(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod cluster_service_mock {
    use super::*;
    #[derive(Debug, Clone)]
    struct MockClusterServiceWrapper {
        inner: std::sync::Arc<tokio::sync::Mutex<MockClusterService>>,
    }
    #[async_trait::async_trait]
    impl ClusterService for MockClusterServiceWrapper {
        async fn fetch_cluster_state(
            &mut self,
            request: super::FetchClusterStateRequest,
        ) -> crate::cluster::ClusterResult<super::FetchClusterStateResponse> {
            self.inner.lock().await.fetch_cluster_state(request).await
        }
    }
    impl From<MockClusterService> for ClusterServiceClient {
        fn from(mock: MockClusterService) -> Self {
            let mock_wrapper = MockClusterServiceWrapper {
                inner: std::sync::Arc::new(tokio::sync::Mutex::new(mock)),
            };
            ClusterServiceClient::new(mock_wrapper)
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<FetchClusterStateRequest> for Box<dyn ClusterService> {
    type Response = FetchClusterStateResponse;
    type Error = crate::cluster::ClusterError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: FetchClusterStateRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.fetch_cluster_state(request).await };
        Box::pin(fut)
    }
}
/// A tower service stack is a set of tower services.
#[derive(Debug)]
struct ClusterServiceTowerServiceStack {
    inner: Box<dyn ClusterService>,
    fetch_cluster_state_svc: quickwit_common::tower::BoxService<
        FetchClusterStateRequest,
        FetchClusterStateResponse,
        crate::cluster::ClusterError,
    >,
}
impl Clone for ClusterServiceTowerServiceStack {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            fetch_cluster_state_svc: self.fetch_cluster_state_svc.clone(),
        }
    }
}
#[async_trait::async_trait]
impl ClusterService for ClusterServiceTowerServiceStack {
    async fn fetch_cluster_state(
        &mut self,
        request: FetchClusterStateRequest,
    ) -> crate::cluster::ClusterResult<FetchClusterStateResponse> {
        self.fetch_cluster_state_svc.ready().await?.call(request).await
    }
}
type FetchClusterStateLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        FetchClusterStateRequest,
        FetchClusterStateResponse,
        crate::cluster::ClusterError,
    >,
    FetchClusterStateRequest,
    FetchClusterStateResponse,
    crate::cluster::ClusterError,
>;
#[derive(Debug, Default)]
pub struct ClusterServiceTowerLayerStack {
    fetch_cluster_state_layers: Vec<FetchClusterStateLayer>,
}
impl ClusterServiceTowerLayerStack {
    pub fn stack_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    FetchClusterStateRequest,
                    FetchClusterStateResponse,
                    crate::cluster::ClusterError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                FetchClusterStateRequest,
                FetchClusterStateResponse,
                crate::cluster::ClusterError,
            >,
        >>::Service: tower::Service<
                FetchClusterStateRequest,
                Response = FetchClusterStateResponse,
                Error = crate::cluster::ClusterError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                FetchClusterStateRequest,
                FetchClusterStateResponse,
                crate::cluster::ClusterError,
            >,
        >>::Service as tower::Service<FetchClusterStateRequest>>::Future: Send + 'static,
    {
        self.fetch_cluster_state_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self
    }
    pub fn stack_fetch_cluster_state_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    FetchClusterStateRequest,
                    FetchClusterStateResponse,
                    crate::cluster::ClusterError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                FetchClusterStateRequest,
                Response = FetchClusterStateResponse,
                Error = crate::cluster::ClusterError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<FetchClusterStateRequest>>::Future: Send + 'static,
    {
        self.fetch_cluster_state_layers
            .push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn build<T>(self, instance: T) -> ClusterServiceClient
    where
        T: ClusterService,
    {
        self.build_from_boxed(Box::new(instance))
    }
    pub fn build_from_channel(
        self,
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
        max_message_size: bytesize::ByteSize,
    ) -> ClusterServiceClient {
        self.build_from_boxed(
            Box::new(ClusterServiceClient::from_channel(addr, channel, max_message_size)),
        )
    }
    pub fn build_from_balance_channel(
        self,
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
        max_message_size: bytesize::ByteSize,
    ) -> ClusterServiceClient {
        self.build_from_boxed(
            Box::new(
                ClusterServiceClient::from_balance_channel(
                    balance_channel,
                    max_message_size,
                ),
            ),
        )
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> ClusterServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        ClusterServiceMailbox<A>: ClusterService,
    {
        self.build_from_boxed(Box::new(ClusterServiceMailbox::new(mailbox)))
    }
    fn build_from_boxed(
        self,
        boxed_instance: Box<dyn ClusterService>,
    ) -> ClusterServiceClient {
        let fetch_cluster_state_svc = self
            .fetch_cluster_state_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(boxed_instance.clone()),
                |svc, layer| layer.layer(svc),
            );
        let tower_svc_stack = ClusterServiceTowerServiceStack {
            inner: boxed_instance.clone(),
            fetch_cluster_state_svc,
        };
        ClusterServiceClient::new(tower_svc_stack)
    }
}
#[derive(Debug, Clone)]
struct MailboxAdapter<A: quickwit_actors::Actor, E> {
    inner: quickwit_actors::Mailbox<A>,
    phantom: std::marker::PhantomData<E>,
}
impl<A, E> std::ops::Deref for MailboxAdapter<A, E>
where
    A: quickwit_actors::Actor,
{
    type Target = quickwit_actors::Mailbox<A>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
#[derive(Debug)]
pub struct ClusterServiceMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::cluster::ClusterError>,
}
impl<A: quickwit_actors::Actor> ClusterServiceMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for ClusterServiceMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A, M, T, E> tower::Service<M> for ClusterServiceMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send
        + 'static,
    M: std::fmt::Debug + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Debug + Send + 'static,
    crate::cluster::ClusterError: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::cluster::ClusterError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        //! This does not work with balance middlewares such as `tower::balance::pool::Pool` because
        //! this always returns `Poll::Ready`. The fix is to acquire a permit from the
        //! mailbox in `poll_ready` and consume it in `call`.
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, message: M) -> Self::Future {
        let mailbox = self.inner.clone();
        let fut = async move {
            mailbox.ask_for_res(message).await.map_err(|error| error.into())
        };
        Box::pin(fut)
    }
}
#[async_trait::async_trait]
impl<A> ClusterService for ClusterServiceMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug,
    ClusterServiceMailbox<
        A,
    >: tower::Service<
        FetchClusterStateRequest,
        Response = FetchClusterStateResponse,
        Error = crate::cluster::ClusterError,
        Future = BoxFuture<FetchClusterStateResponse, crate::cluster::ClusterError>,
    >,
{
    async fn fetch_cluster_state(
        &mut self,
        request: FetchClusterStateRequest,
    ) -> crate::cluster::ClusterResult<FetchClusterStateResponse> {
        self.call(request).await
    }
}
#[derive(Debug, Clone)]
pub struct ClusterServiceGrpcClientAdapter<T> {
    inner: T,
    #[allow(dead_code)]
    connection_addrs_rx: tokio::sync::watch::Receiver<
        std::collections::HashSet<std::net::SocketAddr>,
    >,
}
impl<T> ClusterServiceGrpcClientAdapter<T> {
    pub fn new(
        instance: T,
        connection_addrs_rx: tokio::sync::watch::Receiver<
            std::collections::HashSet<std::net::SocketAddr>,
        >,
    ) -> Self {
        Self {
            inner: instance,
            connection_addrs_rx,
        }
    }
}
#[async_trait::async_trait]
impl<T> ClusterService
for ClusterServiceGrpcClientAdapter<
    cluster_service_grpc_client::ClusterServiceGrpcClient<T>,
>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn fetch_cluster_state(
        &mut self,
        request: FetchClusterStateRequest,
    ) -> crate::cluster::ClusterResult<FetchClusterStateResponse> {
        self.inner
            .fetch_cluster_state(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                FetchClusterStateRequest::rpc_name(),
            ))
    }
}
#[derive(Debug)]
pub struct ClusterServiceGrpcServerAdapter {
    inner: Box<dyn ClusterService>,
}
impl ClusterServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: ClusterService,
    {
        Self { inner: Box::new(instance) }
    }
}
#[async_trait::async_trait]
impl cluster_service_grpc_server::ClusterServiceGrpc
for ClusterServiceGrpcServerAdapter {
    async fn fetch_cluster_state(
        &self,
        request: tonic::Request<FetchClusterStateRequest>,
    ) -> Result<tonic::Response<FetchClusterStateResponse>, tonic::Status> {
        self.inner
            .clone()
            .fetch_cluster_state(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
}
/// Generated client implementations.
pub mod cluster_service_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct ClusterServiceGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ClusterServiceGrpcClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ClusterServiceGrpcClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ClusterServiceGrpcClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            ClusterServiceGrpcClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn fetch_cluster_state(
            &mut self,
            request: impl tonic::IntoRequest<super::FetchClusterStateRequest>,
        ) -> std::result::Result<
            tonic::Response<super::FetchClusterStateResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.cluster.ClusterService/FetchClusterState",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.cluster.ClusterService",
                        "FetchClusterState",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod cluster_service_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ClusterServiceGrpcServer.
    #[async_trait]
    pub trait ClusterServiceGrpc: Send + Sync + 'static {
        async fn fetch_cluster_state(
            &self,
            request: tonic::Request<super::FetchClusterStateRequest>,
        ) -> std::result::Result<
            tonic::Response<super::FetchClusterStateResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct ClusterServiceGrpcServer<T: ClusterServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ClusterServiceGrpc> ClusterServiceGrpcServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ClusterServiceGrpcServer<T>
    where
        T: ClusterServiceGrpc,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/quickwit.cluster.ClusterService/FetchClusterState" => {
                    #[allow(non_camel_case_types)]
                    struct FetchClusterStateSvc<T: ClusterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: ClusterServiceGrpc,
                    > tonic::server::UnaryService<super::FetchClusterStateRequest>
                    for FetchClusterStateSvc<T> {
                        type Response = super::FetchClusterStateResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FetchClusterStateRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).fetch_cluster_state(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = FetchClusterStateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: ClusterServiceGrpc> Clone for ClusterServiceGrpcServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: ClusterServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ClusterServiceGrpc> tonic::server::NamedService
    for ClusterServiceGrpcServer<T> {
        const NAME: &'static str = "quickwit.cluster.ClusterService";
    }
}
