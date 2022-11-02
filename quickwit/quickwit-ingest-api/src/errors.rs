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

use std::io;

use mrecordlog::error::*;
use quickwit_proto::{tonic, ServiceError, ServiceErrorCode};
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Debug, Serialize)]
pub enum IngestApiError {
    #[error("Data corruption: {msg}.")]
    Corruption { msg: String },
    #[error("Index `{index_id}` does not exist.")]
    IndexDoesNotExist { index_id: String },
    #[error("Index `{index_id}` already exists.")]
    IndexAlreadyExists { index_id: String },
    #[error("Ingest API service is down")]
    IngestAPIServiceDown,
    #[error("Io Error")]
    IoError(String),
    #[error("Invalid position: {0}.")]
    InvalidPosition(String),
}

impl ServiceError for IngestApiError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            IngestApiError::Corruption { .. } => ServiceErrorCode::Internal,
            IngestApiError::IndexDoesNotExist { .. } => ServiceErrorCode::NotFound,
            IngestApiError::IndexAlreadyExists { .. } => ServiceErrorCode::BadRequest,
            IngestApiError::IngestAPIServiceDown => ServiceErrorCode::Internal,
            IngestApiError::IoError(_) => ServiceErrorCode::Internal,
            IngestApiError::InvalidPosition(_) => ServiceErrorCode::BadRequest,
        }
    }
}

#[derive(Error, Debug)]
#[error("Key should contain 16 bytes. It contained {0} bytes.")]
pub struct CorruptedKey(pub usize);

impl From<CorruptedKey> for IngestApiError {
    fn from(err: CorruptedKey) -> Self {
        IngestApiError::Corruption {
            msg: format!("CorruptedKey: {err:?}"),
        }
    }
}

impl From<IngestApiError> for tonic::Status {
    fn from(error: IngestApiError) -> tonic::Status {
        let code = match &error {
            IngestApiError::Corruption { .. } => tonic::Code::Internal,
            IngestApiError::IndexDoesNotExist { .. } => tonic::Code::NotFound,
            IngestApiError::IndexAlreadyExists { .. } => tonic::Code::AlreadyExists,
            IngestApiError::IngestAPIServiceDown => tonic::Code::Internal,
            IngestApiError::IoError(_) => tonic::Code::Internal,
            IngestApiError::InvalidPosition(_) => tonic::Code::InvalidArgument,
        };
        let message = error.to_string();
        tonic::Status::new(code, message)
    }
}

impl From<io::Error> for IngestApiError {
    fn from(io_err: io::Error) -> Self {
        IngestApiError::IoError(format!("{io_err:?}"))
    }
}

impl From<ReadRecordError> for IngestApiError {
    fn from(e: ReadRecordError) -> IngestApiError {
        match e {
            ReadRecordError::IoError(ioe) => ioe.into(),
            ReadRecordError::Corruption => IngestApiError::Corruption {
                msg: "failed to read record".to_owned(),
            },
        }
    }
}

impl From<AppendError> for IngestApiError {
    fn from(e: AppendError) -> IngestApiError {
        match e {
            AppendError::IoError(ioe) => ioe.into(),
            AppendError::MissingQueue(index_id) => IngestApiError::IndexDoesNotExist { index_id },
            // these errors can't be reached right now
            AppendError::Past => {
                IngestApiError::InvalidPosition("appending record in the past".to_owned())
            }
            AppendError::Future => {
                IngestApiError::InvalidPosition("appending record in the future".to_owned())
            }
        }
    }
}

impl From<DeleteQueueError> for IngestApiError {
    fn from(e: DeleteQueueError) -> IngestApiError {
        match e {
            DeleteQueueError::IoError(ioe) => ioe.into(),
            DeleteQueueError::MissingQueue(index_id) => {
                IngestApiError::IndexDoesNotExist { index_id }
            }
        }
    }
}

impl From<TruncateError> for IngestApiError {
    fn from(e: TruncateError) -> IngestApiError {
        match e {
            TruncateError::IoError(ioe) => ioe.into(),
            TruncateError::MissingQueue(index_id) => IngestApiError::IndexDoesNotExist { index_id },
            // this error shouldn't happen (except due to a bug in MRecordLog?)
            TruncateError::TouchError(_) => todo!(),
            // this error can happen now, it used to happily trunk everything
            TruncateError::Future => IngestApiError::InvalidPosition(
                "trying to truncate past last ingested record".to_owned(),
            ),
        }
    }
}

pub type Result<T> = std::result::Result<T, IngestApiError>;
