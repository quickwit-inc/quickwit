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

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project::pin_project;
use tokio::task::{JoinError, JoinHandle};
use tower::{Layer, Service};
use tracing::error;

use crate::tower::RpcName;

/// This layer spawns a new task for each call to the inner service.
///
/// This is useful for service where the handle is not cancel-safe:
/// On a connection drop for instance, tonic can cancel the Future associated
/// to a request execution.
///
/// By executing it on a dedicated task, we ensure the future is run to
/// completion.
///
/// Disclaimer: This layer should be used with caution, as it means that timeout
/// are not possible anymore.
///
/// It also can behave in an unexpected way when combined with layers like the
/// `GlobalConcurrencyLimitLayer`.
pub struct OneTaskPerCallLayer;

impl<S: Clone> Layer<S> for OneTaskPerCallLayer {
    type Service = OneTaskPerCallService<S>;

    fn layer(&self, service: S) -> Self::Service {
        OneTaskPerCallService { service }
    }
}

#[derive(Clone)]
pub struct OneTaskPerCallService<S: Clone> {
    service: S,
}

impl<S, Request> Service<Request> for OneTaskPerCallService<S>
where
    S: Service<Request> + Send + Clone + 'static,
    S::Future: Send,
    S::Response: Send,
    S::Error: From<TaskCancelled> + Send,
    Request: fmt::Debug + Send + RpcName + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = UnwrapOrElseFuture<S::Response, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let service_clone = self.service.clone();
        // See https://docs.rs/tower/latest/tower/trait.Service.html##be-careful-when-cloning-inner-services
        let mut service = std::mem::replace(&mut self.service, service_clone);
        let request_name: &'static str = Request::rpc_name();
        UnwrapOrElseFuture {
            request_name,
            join_handle: tokio::spawn(async move {
                let fut = service.call(request);
                fut.await
            }),
        }
    }
}

#[pin_project]
pub struct UnwrapOrElseFuture<T, E> {
    request_name: &'static str,
    #[pin]
    join_handle: JoinHandle<Result<T, E>>,
}

impl<T, E> Future for UnwrapOrElseFuture<T, E>
where E: From<TaskCancelled>
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let request_name = self.request_name;
        let pinned_join_handle: Pin<&mut JoinHandle<Result<T, E>>> = self.project().join_handle;
        match pinned_join_handle.poll(cx) {
            Poll::Ready(Ok(Ok(t))) => Poll::Ready(Ok(t)),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(Err(join_error)) => {
                error!(
                    "task running the request `{}` was cancelled or panicked. please report! \
                     JoinError: {:?}",
                    request_name, join_error
                );
                let task_cancelled = TaskCancelled {
                    request_name,
                    join_error,
                };
                Poll::Ready(Err(E::from(task_cancelled)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct TaskCancelled {
    pub request_name: &'static str,
    pub join_error: JoinError,
}

impl std::fmt::Display for TaskCancelled {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let TaskCancelled {
            request_name,
            join_error,
        } = self;
        write!(
            f,
            "task running `{request_name}` was cancelled or panicked. JoinError: {join_error:?})"
        )
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::Mutex;
    use tower::ServiceExt;

    use super::*;
    use crate::tower::RpcName;

    #[derive(Debug)]
    struct Request;

    impl RpcName for Request {
        fn rpc_name() -> &'static str {
            "dummy_request"
        }
    }

    #[derive(Debug)]
    struct DummyError;

    impl From<TaskCancelled> for DummyError {
        fn from(_task_cancelled: TaskCancelled) -> DummyError {
            DummyError
        }
    }

    // In this toy example, we want to make sure, upon all observation
    // left == right.
    //
    // In reality, OneTaskPerCallLayer is meant to protect more complicated
    // invariants.
    #[derive(Default)]
    struct State {
        left: usize,
        right: usize,
    }

    #[tokio::test]
    async fn test_task_cancelled() {
        let state: Arc<Mutex<State>> = Default::default();
        let state_clone: Arc<Mutex<State>> = state.clone();
        let service = tower::service_fn(move |_request: Request| {
            let state_clone = state.clone();
            async move {
                let mut lock = state_clone.lock().await;
                assert_eq!(lock.left, lock.right);
                lock.left += 1;
                // If the task was cancelled at this point, it would leave us with
                // a broken invariant.
                tokio::time::sleep(Duration::from_millis(100)).await;
                lock.right += 1;
                Result::Ok::<(), DummyError>(())
            }
        });
        let mut one_task_per_call_service = OneTaskPerCallService { service };
        tokio::select!(
            _ = async { one_task_per_call_service.ready().await.unwrap().call(Request).await } => {
                panic!("this sould have timed out");
            },
            _ = tokio::time::sleep(Duration::from_millis(10)) => (),
        );
        let state_guard = state_clone.lock().await;
        assert_eq!(state_guard.left, state_guard.right);
        assert_eq!(state_guard.left, 1);
    }
}
