#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDebugInfoRequest {
    /// Restricts the debug info to the given roles.
    #[prost(string, repeated, tag = "1")]
    pub roles: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetDebugInfoResponse {
    #[prost(bytes = "bytes", tag = "1")]
    pub debug_info_json: ::prost::bytes::Bytes,
}
/// BEGIN quickwit-codegen
#[allow(unused_imports)]
use std::str::FromStr;
use tower::{Layer, Service, ServiceExt};
use quickwit_common::tower::RpcName;
impl RpcName for GetDebugInfoRequest {
    fn rpc_name() -> &'static str {
        "get_debug_info"
    }
}
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait DeveloperService: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    async fn get_debug_info(
        &mut self,
        request: GetDebugInfoRequest,
    ) -> crate::developer::DeveloperResult<GetDebugInfoResponse>;
}
dyn_clone::clone_trait_object!(DeveloperService);
#[cfg(any(test, feature = "testsuite"))]
impl Clone for MockDeveloperService {
    fn clone(&self) -> Self {
        MockDeveloperService::new()
    }
}
#[derive(Debug, Clone)]
pub struct DeveloperServiceClient {
    inner: Box<dyn DeveloperService>,
}
impl DeveloperServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: DeveloperService,
    {
        #[cfg(any(test, feature = "testsuite"))]
        assert!(
            std::any::TypeId::of:: < T > () != std::any::TypeId::of:: <
            MockDeveloperService > (),
            "`MockDeveloperService` must be wrapped in a `MockDeveloperServiceWrapper`: use `DeveloperServiceClient::from_mock(mock)` to instantiate the client"
        );
        Self { inner: Box::new(instance) }
    }
    pub fn as_grpc_service(
        &self,
        max_message_size: bytesize::ByteSize,
    ) -> developer_service_grpc_server::DeveloperServiceGrpcServer<
        DeveloperServiceGrpcServerAdapter,
    > {
        let adapter = DeveloperServiceGrpcServerAdapter::new(self.clone());
        developer_service_grpc_server::DeveloperServiceGrpcServer::new(adapter)
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
        let client = developer_service_grpc_client::DeveloperServiceGrpcClient::new(
                channel,
            )
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        let adapter = DeveloperServiceGrpcClientAdapter::new(
            client,
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_balance_channel(
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
        max_message_size: bytesize::ByteSize,
    ) -> DeveloperServiceClient {
        let connection_keys_watcher = balance_channel.connection_keys_watcher();
        let client = developer_service_grpc_client::DeveloperServiceGrpcClient::new(
                balance_channel,
            )
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        let adapter = DeveloperServiceGrpcClientAdapter::new(
            client,
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        DeveloperServiceMailbox<A>: DeveloperService,
    {
        DeveloperServiceClient::new(DeveloperServiceMailbox::new(mailbox))
    }
    pub fn tower() -> DeveloperServiceTowerLayerStack {
        DeveloperServiceTowerLayerStack::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn from_mock(mock: MockDeveloperService) -> Self {
        let mock_wrapper = mock_developer_service::MockDeveloperServiceWrapper {
            inner: std::sync::Arc::new(tokio::sync::Mutex::new(mock)),
        };
        Self::new(mock_wrapper)
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mocked() -> Self {
        Self::from_mock(MockDeveloperService::new())
    }
}
#[async_trait::async_trait]
impl DeveloperService for DeveloperServiceClient {
    async fn get_debug_info(
        &mut self,
        request: GetDebugInfoRequest,
    ) -> crate::developer::DeveloperResult<GetDebugInfoResponse> {
        self.inner.get_debug_info(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod mock_developer_service {
    use super::*;
    #[derive(Debug, Clone)]
    pub struct MockDeveloperServiceWrapper {
        pub(super) inner: std::sync::Arc<tokio::sync::Mutex<MockDeveloperService>>,
    }
    #[async_trait::async_trait]
    impl DeveloperService for MockDeveloperServiceWrapper {
        async fn get_debug_info(
            &mut self,
            request: super::GetDebugInfoRequest,
        ) -> crate::developer::DeveloperResult<super::GetDebugInfoResponse> {
            self.inner.lock().await.get_debug_info(request).await
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<GetDebugInfoRequest> for Box<dyn DeveloperService> {
    type Response = GetDebugInfoResponse;
    type Error = crate::developer::DeveloperError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: GetDebugInfoRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.get_debug_info(request).await };
        Box::pin(fut)
    }
}
/// A tower service stack is a set of tower services.
#[derive(Debug)]
struct DeveloperServiceTowerServiceStack {
    inner: Box<dyn DeveloperService>,
    get_debug_info_svc: quickwit_common::tower::BoxService<
        GetDebugInfoRequest,
        GetDebugInfoResponse,
        crate::developer::DeveloperError,
    >,
}
impl Clone for DeveloperServiceTowerServiceStack {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            get_debug_info_svc: self.get_debug_info_svc.clone(),
        }
    }
}
#[async_trait::async_trait]
impl DeveloperService for DeveloperServiceTowerServiceStack {
    async fn get_debug_info(
        &mut self,
        request: GetDebugInfoRequest,
    ) -> crate::developer::DeveloperResult<GetDebugInfoResponse> {
        self.get_debug_info_svc.ready().await?.call(request).await
    }
}
type GetDebugInfoLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        GetDebugInfoRequest,
        GetDebugInfoResponse,
        crate::developer::DeveloperError,
    >,
    GetDebugInfoRequest,
    GetDebugInfoResponse,
    crate::developer::DeveloperError,
>;
#[derive(Debug, Default)]
pub struct DeveloperServiceTowerLayerStack {
    get_debug_info_layers: Vec<GetDebugInfoLayer>,
}
impl DeveloperServiceTowerLayerStack {
    pub fn stack_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    GetDebugInfoRequest,
                    GetDebugInfoResponse,
                    crate::developer::DeveloperError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                GetDebugInfoRequest,
                GetDebugInfoResponse,
                crate::developer::DeveloperError,
            >,
        >>::Service: tower::Service<
                GetDebugInfoRequest,
                Response = GetDebugInfoResponse,
                Error = crate::developer::DeveloperError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                GetDebugInfoRequest,
                GetDebugInfoResponse,
                crate::developer::DeveloperError,
            >,
        >>::Service as tower::Service<GetDebugInfoRequest>>::Future: Send + 'static,
    {
        self.get_debug_info_layers
            .push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self
    }
    pub fn stack_get_debug_info_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    GetDebugInfoRequest,
                    GetDebugInfoResponse,
                    crate::developer::DeveloperError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                GetDebugInfoRequest,
                Response = GetDebugInfoResponse,
                Error = crate::developer::DeveloperError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<GetDebugInfoRequest>>::Future: Send + 'static,
    {
        self.get_debug_info_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn build<T>(self, instance: T) -> DeveloperServiceClient
    where
        T: DeveloperService,
    {
        self.build_from_boxed(Box::new(instance))
    }
    pub fn build_from_channel(
        self,
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
        max_message_size: bytesize::ByteSize,
    ) -> DeveloperServiceClient {
        self.build_from_boxed(
            Box::new(
                DeveloperServiceClient::from_channel(addr, channel, max_message_size),
            ),
        )
    }
    pub fn build_from_balance_channel(
        self,
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
        max_message_size: bytesize::ByteSize,
    ) -> DeveloperServiceClient {
        self.build_from_boxed(
            Box::new(
                DeveloperServiceClient::from_balance_channel(
                    balance_channel,
                    max_message_size,
                ),
            ),
        )
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> DeveloperServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        DeveloperServiceMailbox<A>: DeveloperService,
    {
        self.build_from_boxed(Box::new(DeveloperServiceMailbox::new(mailbox)))
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn build_from_mock(self, mock: MockDeveloperService) -> DeveloperServiceClient {
        self.build_from_boxed(Box::new(DeveloperServiceClient::from_mock(mock)))
    }
    fn build_from_boxed(
        self,
        boxed_instance: Box<dyn DeveloperService>,
    ) -> DeveloperServiceClient {
        let get_debug_info_svc = self
            .get_debug_info_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(boxed_instance.clone()),
                |svc, layer| layer.layer(svc),
            );
        let tower_svc_stack = DeveloperServiceTowerServiceStack {
            inner: boxed_instance.clone(),
            get_debug_info_svc,
        };
        DeveloperServiceClient::new(tower_svc_stack)
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
pub struct DeveloperServiceMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::developer::DeveloperError>,
}
impl<A: quickwit_actors::Actor> DeveloperServiceMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for DeveloperServiceMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A, M, T, E> tower::Service<M> for DeveloperServiceMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send
        + 'static,
    M: std::fmt::Debug + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Debug + Send + 'static,
    crate::developer::DeveloperError: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::developer::DeveloperError;
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
impl<A> DeveloperService for DeveloperServiceMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug,
    DeveloperServiceMailbox<
        A,
    >: tower::Service<
        GetDebugInfoRequest,
        Response = GetDebugInfoResponse,
        Error = crate::developer::DeveloperError,
        Future = BoxFuture<GetDebugInfoResponse, crate::developer::DeveloperError>,
    >,
{
    async fn get_debug_info(
        &mut self,
        request: GetDebugInfoRequest,
    ) -> crate::developer::DeveloperResult<GetDebugInfoResponse> {
        self.call(request).await
    }
}
#[derive(Debug, Clone)]
pub struct DeveloperServiceGrpcClientAdapter<T> {
    inner: T,
    #[allow(dead_code)]
    connection_addrs_rx: tokio::sync::watch::Receiver<
        std::collections::HashSet<std::net::SocketAddr>,
    >,
}
impl<T> DeveloperServiceGrpcClientAdapter<T> {
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
impl<T> DeveloperService
for DeveloperServiceGrpcClientAdapter<
    developer_service_grpc_client::DeveloperServiceGrpcClient<T>,
>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn get_debug_info(
        &mut self,
        request: GetDebugInfoRequest,
    ) -> crate::developer::DeveloperResult<GetDebugInfoResponse> {
        self.inner
            .get_debug_info(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                GetDebugInfoRequest::rpc_name(),
            ))
    }
}
#[derive(Debug)]
pub struct DeveloperServiceGrpcServerAdapter {
    inner: Box<dyn DeveloperService>,
}
impl DeveloperServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: DeveloperService,
    {
        Self { inner: Box::new(instance) }
    }
}
#[async_trait::async_trait]
impl developer_service_grpc_server::DeveloperServiceGrpc
for DeveloperServiceGrpcServerAdapter {
    async fn get_debug_info(
        &self,
        request: tonic::Request<GetDebugInfoRequest>,
    ) -> Result<tonic::Response<GetDebugInfoResponse>, tonic::Status> {
        self.inner
            .clone()
            .get_debug_info(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
}
/// Generated client implementations.
pub mod developer_service_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct DeveloperServiceGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl DeveloperServiceGrpcClient<tonic::transport::Channel> {
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
    impl<T> DeveloperServiceGrpcClient<T>
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
        ) -> DeveloperServiceGrpcClient<InterceptedService<T, F>>
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
            DeveloperServiceGrpcClient::new(InterceptedService::new(inner, interceptor))
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
        pub async fn get_debug_info(
            &mut self,
            request: impl tonic::IntoRequest<super::GetDebugInfoRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetDebugInfoResponse>,
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
                "/quickwit.developer.DeveloperService/GetDebugInfo",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.developer.DeveloperService",
                        "GetDebugInfo",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod developer_service_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with DeveloperServiceGrpcServer.
    #[async_trait]
    pub trait DeveloperServiceGrpc: Send + Sync + 'static {
        async fn get_debug_info(
            &self,
            request: tonic::Request<super::GetDebugInfoRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetDebugInfoResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct DeveloperServiceGrpcServer<T: DeveloperServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: DeveloperServiceGrpc> DeveloperServiceGrpcServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>>
    for DeveloperServiceGrpcServer<T>
    where
        T: DeveloperServiceGrpc,
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
                "/quickwit.developer.DeveloperService/GetDebugInfo" => {
                    #[allow(non_camel_case_types)]
                    struct GetDebugInfoSvc<T: DeveloperServiceGrpc>(pub Arc<T>);
                    impl<
                        T: DeveloperServiceGrpc,
                    > tonic::server::UnaryService<super::GetDebugInfoRequest>
                    for GetDebugInfoSvc<T> {
                        type Response = super::GetDebugInfoResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetDebugInfoRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_debug_info(request).await
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
                        let method = GetDebugInfoSvc(inner);
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
    impl<T: DeveloperServiceGrpc> Clone for DeveloperServiceGrpcServer<T> {
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
    impl<T: DeveloperServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: DeveloperServiceGrpc> tonic::server::NamedService
    for DeveloperServiceGrpcServer<T> {
        const NAME: &'static str = "quickwit.developer.DeveloperService";
    }
}
