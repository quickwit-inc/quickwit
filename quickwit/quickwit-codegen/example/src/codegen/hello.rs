#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HelloRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HelloResponse {
    #[prost(string, tag = "1")]
    pub message: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GoodbyeRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GoodbyeResponse {
    #[prost(string, tag = "1")]
    pub message: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PingRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PingResponse {
    #[prost(string, tag = "1")]
    pub message: ::prost::alloc::string::String,
}
/// BEGIN quickwit-codegen
#[allow(unused_imports)]
use std::str::FromStr;
use tower::{Layer, Service, ServiceExt};
use quickwit_common::tower::RpcName;
impl RpcName for HelloRequest {
    fn rpc_name() -> &'static str {
        "hello"
    }
}
impl RpcName for GoodbyeRequest {
    fn rpc_name() -> &'static str {
        "goodbye"
    }
}
impl RpcName for PingRequest {
    fn rpc_name() -> &'static str {
        "ping"
    }
}
pub type HelloStream<T> = quickwit_common::ServiceStream<crate::HelloResult<T>>;
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait Hello: std::fmt::Debug + Send + Sync + 'static {
    /// Says hello.
    async fn hello(&self, request: HelloRequest) -> crate::HelloResult<HelloResponse>;
    /// Says goodbye.
    async fn goodbye(
        &self,
        request: GoodbyeRequest,
    ) -> crate::HelloResult<GoodbyeResponse>;
    /// Ping pong.
    async fn ping(
        &self,
        request: quickwit_common::ServiceStream<PingRequest>,
    ) -> crate::HelloResult<HelloStream<PingResponse>>;
    async fn check_connectivity(&self) -> anyhow::Result<()>;
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri>;
}
#[derive(Debug, Clone)]
pub struct HelloClient {
    inner: InnerHelloClient,
}
#[derive(Debug, Clone)]
struct InnerHelloClient(std::sync::Arc<dyn Hello>);
impl HelloClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: Hello,
    {
        #[cfg(any(test, feature = "testsuite"))]
        assert!(
            std::any::TypeId::of:: < T > () != std::any::TypeId::of:: < MockHello > (),
            "`MockHello` must be wrapped in a `MockHelloWrapper`: use `HelloClient::from_mock(mock)` to instantiate the client"
        );
        Self {
            inner: InnerHelloClient(std::sync::Arc::new(instance)),
        }
    }
    pub fn as_grpc_service(
        &self,
        max_message_size: bytesize::ByteSize,
    ) -> hello_grpc_server::HelloGrpcServer<HelloGrpcServerAdapter> {
        let adapter = HelloGrpcServerAdapter::new(self.clone());
        hello_grpc_server::HelloGrpcServer::new(adapter)
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
        let client = hello_grpc_client::HelloGrpcClient::new(channel)
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        let adapter = HelloGrpcClientAdapter::new(client, connection_keys_watcher);
        Self::new(adapter)
    }
    pub fn from_balance_channel(
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
        max_message_size: bytesize::ByteSize,
    ) -> HelloClient {
        let connection_keys_watcher = balance_channel.connection_keys_watcher();
        let client = hello_grpc_client::HelloGrpcClient::new(balance_channel)
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        let adapter = HelloGrpcClientAdapter::new(client, connection_keys_watcher);
        Self::new(adapter)
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        HelloMailbox<A>: Hello,
    {
        HelloClient::new(HelloMailbox::new(mailbox))
    }
    pub fn tower() -> HelloTowerLayerStack {
        HelloTowerLayerStack::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn from_mock(mock: MockHello) -> Self {
        let mock_wrapper = mock_hello::MockHelloWrapper {
            inner: tokio::sync::Mutex::new(mock),
        };
        Self::new(mock_wrapper)
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mocked() -> Self {
        Self::from_mock(MockHello::new())
    }
}
#[async_trait::async_trait]
impl Hello for HelloClient {
    async fn hello(&self, request: HelloRequest) -> crate::HelloResult<HelloResponse> {
        self.inner.0.hello(request).await
    }
    async fn goodbye(
        &self,
        request: GoodbyeRequest,
    ) -> crate::HelloResult<GoodbyeResponse> {
        self.inner.0.goodbye(request).await
    }
    async fn ping(
        &self,
        request: quickwit_common::ServiceStream<PingRequest>,
    ) -> crate::HelloResult<HelloStream<PingResponse>> {
        self.inner.0.ping(request).await
    }
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.inner.0.check_connectivity().await
    }
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        self.inner.0.endpoints()
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod mock_hello {
    use super::*;
    #[derive(Debug)]
    pub struct MockHelloWrapper {
        pub(super) inner: tokio::sync::Mutex<MockHello>,
    }
    #[async_trait::async_trait]
    impl Hello for MockHelloWrapper {
        async fn hello(
            &self,
            request: super::HelloRequest,
        ) -> crate::HelloResult<super::HelloResponse> {
            self.inner.lock().await.hello(request).await
        }
        async fn goodbye(
            &self,
            request: super::GoodbyeRequest,
        ) -> crate::HelloResult<super::GoodbyeResponse> {
            self.inner.lock().await.goodbye(request).await
        }
        async fn ping(
            &self,
            request: quickwit_common::ServiceStream<super::PingRequest>,
        ) -> crate::HelloResult<HelloStream<super::PingResponse>> {
            self.inner.lock().await.ping(request).await
        }
        async fn check_connectivity(&self) -> anyhow::Result<()> {
            self.inner.lock().await.check_connectivity().await
        }
        fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
            futures::executor::block_on(self.inner.lock()).endpoints()
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<HelloRequest> for InnerHelloClient {
    type Response = HelloResponse;
    type Error = crate::HelloError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: HelloRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.hello(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<GoodbyeRequest> for InnerHelloClient {
    type Response = GoodbyeResponse;
    type Error = crate::HelloError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: GoodbyeRequest) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.goodbye(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<quickwit_common::ServiceStream<PingRequest>> for InnerHelloClient {
    type Response = HelloStream<PingResponse>;
    type Error = crate::HelloError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(
        &mut self,
        request: quickwit_common::ServiceStream<PingRequest>,
    ) -> Self::Future {
        let svc = self.clone();
        let fut = async move { svc.0.ping(request).await };
        Box::pin(fut)
    }
}
/// A tower service stack is a set of tower services.
#[derive(Debug)]
struct HelloTowerServiceStack {
    #[allow(dead_code)]
    inner: InnerHelloClient,
    hello_svc: quickwit_common::tower::BoxService<
        HelloRequest,
        HelloResponse,
        crate::HelloError,
    >,
    goodbye_svc: quickwit_common::tower::BoxService<
        GoodbyeRequest,
        GoodbyeResponse,
        crate::HelloError,
    >,
    ping_svc: quickwit_common::tower::BoxService<
        quickwit_common::ServiceStream<PingRequest>,
        HelloStream<PingResponse>,
        crate::HelloError,
    >,
}
#[async_trait::async_trait]
impl Hello for HelloTowerServiceStack {
    async fn hello(&self, request: HelloRequest) -> crate::HelloResult<HelloResponse> {
        self.hello_svc.clone().ready().await?.call(request).await
    }
    async fn goodbye(
        &self,
        request: GoodbyeRequest,
    ) -> crate::HelloResult<GoodbyeResponse> {
        self.goodbye_svc.clone().ready().await?.call(request).await
    }
    async fn ping(
        &self,
        request: quickwit_common::ServiceStream<PingRequest>,
    ) -> crate::HelloResult<HelloStream<PingResponse>> {
        self.ping_svc.clone().ready().await?.call(request).await
    }
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.inner.0.check_connectivity().await
    }
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        self.inner.0.endpoints()
    }
}
type HelloLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<HelloRequest, HelloResponse, crate::HelloError>,
    HelloRequest,
    HelloResponse,
    crate::HelloError,
>;
type GoodbyeLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        GoodbyeRequest,
        GoodbyeResponse,
        crate::HelloError,
    >,
    GoodbyeRequest,
    GoodbyeResponse,
    crate::HelloError,
>;
type PingLayer = quickwit_common::tower::BoxLayer<
    quickwit_common::tower::BoxService<
        quickwit_common::ServiceStream<PingRequest>,
        HelloStream<PingResponse>,
        crate::HelloError,
    >,
    quickwit_common::ServiceStream<PingRequest>,
    HelloStream<PingResponse>,
    crate::HelloError,
>;
#[derive(Debug, Default)]
pub struct HelloTowerLayerStack {
    hello_layers: Vec<HelloLayer>,
    goodbye_layers: Vec<GoodbyeLayer>,
    ping_layers: Vec<PingLayer>,
}
impl HelloTowerLayerStack {
    pub fn stack_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    HelloRequest,
                    HelloResponse,
                    crate::HelloError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                HelloRequest,
                HelloResponse,
                crate::HelloError,
            >,
        >>::Service: tower::Service<
                HelloRequest,
                Response = HelloResponse,
                Error = crate::HelloError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                HelloRequest,
                HelloResponse,
                crate::HelloError,
            >,
        >>::Service as tower::Service<HelloRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    GoodbyeRequest,
                    GoodbyeResponse,
                    crate::HelloError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                GoodbyeRequest,
                GoodbyeResponse,
                crate::HelloError,
            >,
        >>::Service: tower::Service<
                GoodbyeRequest,
                Response = GoodbyeResponse,
                Error = crate::HelloError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                GoodbyeRequest,
                GoodbyeResponse,
                crate::HelloError,
            >,
        >>::Service as tower::Service<GoodbyeRequest>>::Future: Send + 'static,
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    quickwit_common::ServiceStream<PingRequest>,
                    HelloStream<PingResponse>,
                    crate::HelloError,
                >,
            > + Clone + Send + Sync + 'static,
        <L as tower::Layer<
            quickwit_common::tower::BoxService<
                quickwit_common::ServiceStream<PingRequest>,
                HelloStream<PingResponse>,
                crate::HelloError,
            >,
        >>::Service: tower::Service<
                quickwit_common::ServiceStream<PingRequest>,
                Response = HelloStream<PingResponse>,
                Error = crate::HelloError,
            > + Clone + Send + Sync + 'static,
        <<L as tower::Layer<
            quickwit_common::tower::BoxService<
                quickwit_common::ServiceStream<PingRequest>,
                HelloStream<PingResponse>,
                crate::HelloError,
            >,
        >>::Service as tower::Service<
            quickwit_common::ServiceStream<PingRequest>,
        >>::Future: Send + 'static,
    {
        self.hello_layers.push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.goodbye_layers.push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.ping_layers.push(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self
    }
    pub fn stack_hello_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    HelloRequest,
                    HelloResponse,
                    crate::HelloError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                HelloRequest,
                Response = HelloResponse,
                Error = crate::HelloError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<HelloRequest>>::Future: Send + 'static,
    {
        self.hello_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_goodbye_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    GoodbyeRequest,
                    GoodbyeResponse,
                    crate::HelloError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                GoodbyeRequest,
                Response = GoodbyeResponse,
                Error = crate::HelloError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<GoodbyeRequest>>::Future: Send + 'static,
    {
        self.goodbye_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stack_ping_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<
                quickwit_common::tower::BoxService<
                    quickwit_common::ServiceStream<PingRequest>,
                    HelloStream<PingResponse>,
                    crate::HelloError,
                >,
            > + Send + Sync + 'static,
        L::Service: tower::Service<
                quickwit_common::ServiceStream<PingRequest>,
                Response = HelloStream<PingResponse>,
                Error = crate::HelloError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            quickwit_common::ServiceStream<PingRequest>,
        >>::Future: Send + 'static,
    {
        self.ping_layers.push(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn build<T>(self, instance: T) -> HelloClient
    where
        T: Hello,
    {
        let inner_client = InnerHelloClient(std::sync::Arc::new(instance));
        self.build_from_inner_client(inner_client)
    }
    pub fn build_from_channel(
        self,
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
        max_message_size: bytesize::ByteSize,
    ) -> HelloClient {
        let client = HelloClient::from_channel(addr, channel, max_message_size);
        let inner_client = client.inner;
        self.build_from_inner_client(inner_client)
    }
    pub fn build_from_balance_channel(
        self,
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
        max_message_size: bytesize::ByteSize,
    ) -> HelloClient {
        let client = HelloClient::from_balance_channel(
            balance_channel,
            max_message_size,
        );
        let inner_client = client.inner;
        self.build_from_inner_client(inner_client)
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> HelloClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        HelloMailbox<A>: Hello,
    {
        let inner_client = InnerHelloClient(
            std::sync::Arc::new(HelloMailbox::new(mailbox)),
        );
        self.build_from_inner_client(inner_client)
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn build_from_mock(self, mock: MockHello) -> HelloClient {
        let client = HelloClient::from_mock(mock);
        let inner_client = client.inner;
        self.build_from_inner_client(inner_client)
    }
    fn build_from_inner_client(self, inner_client: InnerHelloClient) -> HelloClient {
        let hello_svc = self
            .hello_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let goodbye_svc = self
            .goodbye_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let ping_svc = self
            .ping_layers
            .into_iter()
            .rev()
            .fold(
                quickwit_common::tower::BoxService::new(inner_client.clone()),
                |svc, layer| layer.layer(svc),
            );
        let tower_svc_stack = HelloTowerServiceStack {
            inner: inner_client,
            hello_svc,
            goodbye_svc,
            ping_svc,
        };
        HelloClient::new(tower_svc_stack)
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
pub struct HelloMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::HelloError>,
}
impl<A: quickwit_actors::Actor> HelloMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for HelloMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A, M, T, E> tower::Service<M> for HelloMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send
        + 'static,
    M: std::fmt::Debug + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Debug + Send + 'static,
    crate::HelloError: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::HelloError;
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
impl<A> Hello for HelloMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug,
    HelloMailbox<
        A,
    >: tower::Service<
            HelloRequest,
            Response = HelloResponse,
            Error = crate::HelloError,
            Future = BoxFuture<HelloResponse, crate::HelloError>,
        >
        + tower::Service<
            GoodbyeRequest,
            Response = GoodbyeResponse,
            Error = crate::HelloError,
            Future = BoxFuture<GoodbyeResponse, crate::HelloError>,
        >
        + tower::Service<
            quickwit_common::ServiceStream<PingRequest>,
            Response = HelloStream<PingResponse>,
            Error = crate::HelloError,
            Future = BoxFuture<HelloStream<PingResponse>, crate::HelloError>,
        >,
{
    async fn hello(&self, request: HelloRequest) -> crate::HelloResult<HelloResponse> {
        self.clone().call(request).await
    }
    async fn goodbye(
        &self,
        request: GoodbyeRequest,
    ) -> crate::HelloResult<GoodbyeResponse> {
        self.clone().call(request).await
    }
    async fn ping(
        &self,
        request: quickwit_common::ServiceStream<PingRequest>,
    ) -> crate::HelloResult<HelloStream<PingResponse>> {
        self.clone().call(request).await
    }
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        if self.inner.is_disconnected() {
            anyhow::bail!("actor `{}` is disconnected", self.inner.actor_instance_id())
        }
        Ok(())
    }
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        vec![
            quickwit_common::uri::Uri::from_str(& format!("actor://localhost/{}", self
            .inner.actor_instance_id())).expect("URI should be valid")
        ]
    }
}
#[derive(Debug, Clone)]
pub struct HelloGrpcClientAdapter<T> {
    inner: T,
    #[allow(dead_code)]
    connection_addrs_rx: tokio::sync::watch::Receiver<
        std::collections::HashSet<std::net::SocketAddr>,
    >,
}
impl<T> HelloGrpcClientAdapter<T> {
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
impl<T> Hello for HelloGrpcClientAdapter<hello_grpc_client::HelloGrpcClient<T>>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn hello(&self, request: HelloRequest) -> crate::HelloResult<HelloResponse> {
        self.inner
            .clone()
            .hello(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                <HelloRequest as quickwit_common::tower::RpcName>::rpc_name(),
            ))
    }
    async fn goodbye(
        &self,
        request: GoodbyeRequest,
    ) -> crate::HelloResult<GoodbyeResponse> {
        self.inner
            .clone()
            .goodbye(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                <GoodbyeRequest as quickwit_common::tower::RpcName>::rpc_name(),
            ))
    }
    async fn ping(
        &self,
        request: quickwit_common::ServiceStream<PingRequest>,
    ) -> crate::HelloResult<HelloStream<PingResponse>> {
        self.inner
            .clone()
            .ping(request)
            .await
            .map(|response| {
                let streaming: tonic::Streaming<_> = response.into_inner();
                let stream = quickwit_common::ServiceStream::from(streaming);
                stream
                    .map_err(|status| crate::error::grpc_status_to_service_error(
                        status,
                        <PingRequest as quickwit_common::tower::RpcName>::rpc_name(),
                    ))
            })
            .map_err(|status| crate::error::grpc_status_to_service_error(
                status,
                <PingRequest as quickwit_common::tower::RpcName>::rpc_name(),
            ))
    }
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        if self.connection_addrs_rx.borrow().len() == 0 {
            anyhow::bail!("no server currently available")
        }
        Ok(())
    }
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        self.connection_addrs_rx
            .borrow()
            .iter()
            .flat_map(|addr| quickwit_common::uri::Uri::from_str(
                &format!("grpc://{addr}/{}.{}", "hello", "Hello"),
            ))
            .collect()
    }
}
#[derive(Debug)]
pub struct HelloGrpcServerAdapter {
    inner: InnerHelloClient,
}
impl HelloGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: Hello,
    {
        Self {
            inner: InnerHelloClient(std::sync::Arc::new(instance)),
        }
    }
}
#[async_trait::async_trait]
impl hello_grpc_server::HelloGrpc for HelloGrpcServerAdapter {
    async fn hello(
        &self,
        request: tonic::Request<HelloRequest>,
    ) -> Result<tonic::Response<HelloResponse>, tonic::Status> {
        self.inner
            .0
            .hello(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    async fn goodbye(
        &self,
        request: tonic::Request<GoodbyeRequest>,
    ) -> Result<tonic::Response<GoodbyeResponse>, tonic::Status> {
        self.inner
            .0
            .goodbye(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
    type PingStream = quickwit_common::ServiceStream<tonic::Result<PingResponse>>;
    async fn ping(
        &self,
        request: tonic::Request<tonic::Streaming<PingRequest>>,
    ) -> Result<tonic::Response<Self::PingStream>, tonic::Status> {
        self.inner
            .0
            .ping({
                let streaming: tonic::Streaming<_> = request.into_inner();
                quickwit_common::ServiceStream::from(streaming)
            })
            .await
            .map(|stream| tonic::Response::new(
                stream.map_err(crate::error::grpc_error_to_grpc_status),
            ))
            .map_err(crate::error::grpc_error_to_grpc_status)
    }
}
/// Generated client implementations.
pub mod hello_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct HelloGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl HelloGrpcClient<tonic::transport::Channel> {
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
    impl<T> HelloGrpcClient<T>
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
        ) -> HelloGrpcClient<InterceptedService<T, F>>
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
            HelloGrpcClient::new(InterceptedService::new(inner, interceptor))
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
        /// Says hello.
        pub async fn hello(
            &mut self,
            request: impl tonic::IntoRequest<super::HelloRequest>,
        ) -> std::result::Result<tonic::Response<super::HelloResponse>, tonic::Status> {
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
            let path = http::uri::PathAndQuery::from_static("/hello.Hello/Hello");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("hello.Hello", "Hello"));
            self.inner.unary(req, path, codec).await
        }
        /// Says goodbye.
        pub async fn goodbye(
            &mut self,
            request: impl tonic::IntoRequest<super::GoodbyeRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GoodbyeResponse>,
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
            let path = http::uri::PathAndQuery::from_static("/hello.Hello/Goodbye");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("hello.Hello", "Goodbye"));
            self.inner.unary(req, path, codec).await
        }
        /// Ping pong.
        pub async fn ping(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::PingRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::PingResponse>>,
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
            let path = http::uri::PathAndQuery::from_static("/hello.Hello/Ping");
            let mut req = request.into_streaming_request();
            req.extensions_mut().insert(GrpcMethod::new("hello.Hello", "Ping"));
            self.inner.streaming(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod hello_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with HelloGrpcServer.
    #[async_trait]
    pub trait HelloGrpc: Send + Sync + 'static {
        /// Says hello.
        async fn hello(
            &self,
            request: tonic::Request<super::HelloRequest>,
        ) -> std::result::Result<tonic::Response<super::HelloResponse>, tonic::Status>;
        /// Says goodbye.
        async fn goodbye(
            &self,
            request: tonic::Request<super::GoodbyeRequest>,
        ) -> std::result::Result<tonic::Response<super::GoodbyeResponse>, tonic::Status>;
        /// Server streaming response type for the Ping method.
        type PingStream: futures_core::Stream<
                Item = std::result::Result<super::PingResponse, tonic::Status>,
            >
            + Send
            + 'static;
        /// Ping pong.
        async fn ping(
            &self,
            request: tonic::Request<tonic::Streaming<super::PingRequest>>,
        ) -> std::result::Result<tonic::Response<Self::PingStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct HelloGrpcServer<T: HelloGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: HelloGrpc> HelloGrpcServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for HelloGrpcServer<T>
    where
        T: HelloGrpc,
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
                "/hello.Hello/Hello" => {
                    #[allow(non_camel_case_types)]
                    struct HelloSvc<T: HelloGrpc>(pub Arc<T>);
                    impl<T: HelloGrpc> tonic::server::UnaryService<super::HelloRequest>
                    for HelloSvc<T> {
                        type Response = super::HelloResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::HelloRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).hello(request).await };
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
                        let method = HelloSvc(inner);
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
                "/hello.Hello/Goodbye" => {
                    #[allow(non_camel_case_types)]
                    struct GoodbyeSvc<T: HelloGrpc>(pub Arc<T>);
                    impl<T: HelloGrpc> tonic::server::UnaryService<super::GoodbyeRequest>
                    for GoodbyeSvc<T> {
                        type Response = super::GoodbyeResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GoodbyeRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).goodbye(request).await };
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
                        let method = GoodbyeSvc(inner);
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
                "/hello.Hello/Ping" => {
                    #[allow(non_camel_case_types)]
                    struct PingSvc<T: HelloGrpc>(pub Arc<T>);
                    impl<
                        T: HelloGrpc,
                    > tonic::server::StreamingService<super::PingRequest>
                    for PingSvc<T> {
                        type Response = super::PingResponse;
                        type ResponseStream = T::PingStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::PingRequest>>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).ping(request).await };
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
                        let method = PingSvc(inner);
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
                        let res = grpc.streaming(method, req).await;
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
    impl<T: HelloGrpc> Clone for HelloGrpcServer<T> {
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
    impl<T: HelloGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: HelloGrpc> tonic::server::NamedService for HelloGrpcServer<T> {
        const NAME: &'static str = "hello.Hello";
    }
}
