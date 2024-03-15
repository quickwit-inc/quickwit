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

use std::convert::Infallible;
use std::error::Error;
use std::fmt::Debug;

use anyhow::Context;
use quickwit_actors::AskError;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tonic::metadata::BinaryMetadataValue;
use tracing::{error, warn};

const QW_ERROR_HEADER_NAME: &str = "qw-error-bin";

/// This enum maps our internal error codes to
/// gRPC and HTTP status codes.
///
/// It is voluntarily a restricted subset of gRPC status codes. Please introduce new variants
/// thoughtfully.
#[derive(Clone, Copy)]
pub enum ServiceErrorCode {
    AlreadyExists,
    BadRequest,
    // Use `Unauthenticated` if the caller cannot be identified.
    Forbidden,
    Internal,
    NotFound,
    Timeout,
    TooManyRequests,
    Unauthenticated,
    Unavailable,
}

impl ServiceErrorCode {
    fn grpc_status_code(&self) -> tonic::Code {
        match self {
            Self::AlreadyExists => tonic::Code::AlreadyExists,
            Self::BadRequest => tonic::Code::InvalidArgument,
            Self::Forbidden => tonic::Code::PermissionDenied,
            Self::Internal => tonic::Code::Internal,
            Self::NotFound => tonic::Code::NotFound,
            Self::Timeout => tonic::Code::DeadlineExceeded,
            Self::TooManyRequests => tonic::Code::ResourceExhausted,
            Self::Unauthenticated => tonic::Code::Unauthenticated,
            Self::Unavailable => tonic::Code::Unavailable,
        }
    }

    pub fn http_status_code(&self) -> http::StatusCode {
        match self {
            Self::AlreadyExists => http::StatusCode::BAD_REQUEST,
            Self::BadRequest => http::StatusCode::BAD_REQUEST,
            Self::Forbidden => http::StatusCode::FORBIDDEN,
            Self::Internal => http::StatusCode::INTERNAL_SERVER_ERROR,
            Self::NotFound => http::StatusCode::NOT_FOUND,
            Self::Timeout => http::StatusCode::REQUEST_TIMEOUT,
            Self::TooManyRequests => http::StatusCode::TOO_MANY_REQUESTS,
            Self::Unauthenticated => http::StatusCode::UNAUTHORIZED,
            Self::Unavailable => http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

pub trait ServiceError: Error + Debug + 'static {
    fn error_code(&self) -> ServiceErrorCode;
}

impl ServiceError for Infallible {
    fn error_code(&self) -> ServiceErrorCode {
        unreachable!()
    }
}

impl<E> ServiceError for AskError<E>
where E: ServiceError
{
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            AskError::ErrorReply(error) => error.error_code(),
            AskError::MessageNotDelivered => ServiceErrorCode::Unavailable,
            AskError::ProcessMessageError => ServiceErrorCode::Internal,
        }
    }
}

/// A trait for encoding/decoding service errors to/from gRPC statuses. Errors are stored in JSON
/// in the gRPC header [`QW_ERROR_HEADER_NAME`]. This allows for propagating them transparently
/// between clients and servers over the network without being semantically limited to a status code
/// and a message. However, it also means that modifying the serialization format of existing errors
/// or introducing new ones is not backward compatible.
pub trait GrpcServiceError: ServiceError + Serialize + DeserializeOwned + Send + Sync {
    fn into_grpc_status(self) -> tonic::Status {
        grpc_error_to_grpc_status(self)
    }

    fn new_internal(message: String) -> Self;

    fn new_timeout(message: String) -> Self;

    fn new_unavailable(message: String) -> Self;
}

/// Converts a service error into a gRPC status.
pub fn grpc_error_to_grpc_status<E>(service_error: E) -> tonic::Status
where E: GrpcServiceError {
    let code = service_error.error_code().grpc_status_code();
    let message = service_error.to_string();
    let mut status = tonic::Status::new(code, message);

    match encode_error(&service_error) {
        Ok(header_value) => {
            status
                .metadata_mut()
                .insert_bin(QW_ERROR_HEADER_NAME, header_value);
        }
        Err(error) => {
            warn!(%error, "failed to encode error `{service_error:?}`");
        }
    }
    status
}

/// Converts a gRPC status into a service error.
pub fn grpc_status_to_service_error<E>(status: tonic::Status, rpc_name: &'static str) -> E
where E: GrpcServiceError {
    if let Some(header_value) = status.metadata().get_bin(QW_ERROR_HEADER_NAME) {
        let service_error = match decode_error(header_value) {
            Ok(service_error) => service_error,
            Err(error) => {
                let message = format!(
                    "failed to deserialize error returned from server (this can happen during \
                     rolling upgrades): {error}"
                );
                E::new_internal(message)
            }
        };
        return service_error;
    }
    let message = status.message().to_string();
    error!(code = ?status.code(), rpc = rpc_name, "gRPC transport error: {message}");

    match status.code() {
        // `Cancelled` is a client timeout whereas `DeadlineExceeded` is a server timeout. At this
        // stage, we don't distinguish them.
        tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => E::new_timeout(message),
        tonic::Code::Unavailable => E::new_unavailable(message),
        _ => E::new_internal(message),
    }
}

/// Encodes a service error into a gRPC header value.
fn encode_error<E: Serialize>(service_error: &E) -> anyhow::Result<BinaryMetadataValue> {
    let service_error_json = serde_json::to_vec(&service_error)?;
    let header_value = BinaryMetadataValue::from_bytes(&service_error_json);
    Ok(header_value)
}

/// Decodes a service error from a gRPC header value.
fn decode_error<E: DeserializeOwned>(header_value: &BinaryMetadataValue) -> anyhow::Result<E> {
    let service_error_json = header_value.to_bytes().context("invalid header value")?;
    let service_error = serde_json::from_slice(&service_error_json).with_context(|| {
        if let Ok(service_error_json_str) = std::str::from_utf8(&service_error_json) {
            format!("invalid JSON `{service_error_json_str}`")
        } else {
            "invalid JSON".to_string()
        }
    })?;
    Ok(service_error)
}

pub fn convert_to_grpc_result<T, E: GrpcServiceError>(
    result: Result<T, E>,
) -> tonic::Result<tonic::Response<T>> {
    result
        .map(tonic::Response::new)
        .map_err(|error| error.into_grpc_status())
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[test]
    fn test_grpc_service_error_roundtrip() {
        #[derive(Clone, Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum MyError {
            #[error("internal error: {0}")]
            Internal(String),
            #[error("request timed out: {0}")]
            Timeout(String),
            #[error("service unavailable: {0}")]
            Unavailable(String),
        }

        impl ServiceError for MyError {
            fn error_code(&self) -> ServiceErrorCode {
                match self {
                    MyError::Internal(_) => ServiceErrorCode::Internal,
                    MyError::Timeout(_) => ServiceErrorCode::Timeout,
                    MyError::Unavailable(_) => ServiceErrorCode::Unavailable,
                }
            }
        }

        impl GrpcServiceError for MyError {
            fn new_internal(message: String) -> Self {
                MyError::Internal(message)
            }

            fn new_timeout(message: String) -> Self {
                MyError::Timeout(message)
            }

            fn new_unavailable(message: String) -> Self {
                MyError::Unavailable(message)
            }
        }

        let service_error = MyError::new_internal("test".to_string());
        let status = grpc_error_to_grpc_status(service_error.clone());
        let expected_error: MyError = grpc_status_to_service_error(status, "rpc_name");
        assert_eq!(service_error, expected_error);
    }
}
