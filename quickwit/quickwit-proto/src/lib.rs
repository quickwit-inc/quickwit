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

#![allow(clippy::derive_partial_eq_without_eq)]

mod quickwit;
mod quickwit_common;
mod quickwit_ingest_api;
mod quickwit_metastore_api;

pub mod ingest_api {
    pub use crate::quickwit_ingest_api::*;
}

pub mod metastore_api {
    pub use crate::quickwit_metastore_api::*;
    pub use crate::quickwit_common::*;
}

pub mod jaeger {
    pub mod api_v2 {
            include!("jaeger.api_v2.rs");
    }
    pub mod storage {
        pub mod v1 {
            include!("jaeger.storage.v1.rs");
        }
    }
}

pub mod opentelemetry {
    #[cfg(not(doctest))]
    pub mod proto {

        pub mod collector {
            pub mod logs {
                pub mod v1 {
                    include!("opentelemetry.proto.collector.logs.v1.rs");
                }
            }
            // pub mod metrics {
            //     pub mod v1 {
            //         include!("opentelemetry.proto.collector.metrics.v1.rs");
            //     }
            // }
            pub mod trace {
                pub mod v1 {
                    include!("opentelemetry.proto.collector.trace.v1.rs");
                }
            }
        }
        pub mod common {
            pub mod v1 {
                include!("opentelemetry.proto.common.v1.rs");
            }
        }
        pub mod logs {
            pub mod v1 {
                include!("opentelemetry.proto.logs.v1.rs");
            }
        }
        // pub mod metrics {
        //     pub mod experimental {
        //         include!("opentelemetry.proto.metrics.experimental.rs");
        //     }
        //     pub mod v1 {
        //         tonic::include_proto!("opentelemetry.proto.metrics.v1");
        //     }
        // }
        pub mod resource {
            pub mod v1 {
                include!("opentelemetry.proto.resource.v1.rs");
            }
        }
        pub mod trace {
            pub mod v1 {
                include!("opentelemetry.proto.trace.v1.rs");
            }
        }
    }
}

#[macro_use]
extern crate serde;

use std::convert::Infallible;
use std::fmt;

pub use quickwit::*;
use quickwit_metastore_api::DeleteQuery;
pub use tonic;
use tonic::codegen::http;

/// This enum serves as a Rosetta stone of
/// gRPC and Http status code.
///
/// It is voluntarily a restricted subset.
#[derive(Clone, Copy)]
pub enum ServiceErrorCode {
    NotFound,
    Internal,
    MethodNotAllowed,
    UnsupportedMediaType,
    BadRequest,
}

impl ServiceErrorCode {
    pub fn to_grpc_status_code(self) -> tonic::Code {
        match self {
            ServiceErrorCode::NotFound => tonic::Code::NotFound,
            ServiceErrorCode::Internal => tonic::Code::Internal,
            ServiceErrorCode::BadRequest => tonic::Code::InvalidArgument,
            ServiceErrorCode::MethodNotAllowed => tonic::Code::InvalidArgument,
            ServiceErrorCode::UnsupportedMediaType => tonic::Code::InvalidArgument,
        }
    }
    pub fn to_http_status_code(self) -> http::StatusCode {
        match self {
            ServiceErrorCode::NotFound => http::StatusCode::NOT_FOUND,
            ServiceErrorCode::Internal => http::StatusCode::INTERNAL_SERVER_ERROR,
            ServiceErrorCode::BadRequest => http::StatusCode::BAD_REQUEST,
            ServiceErrorCode::MethodNotAllowed => http::StatusCode::METHOD_NOT_ALLOWED,
            ServiceErrorCode::UnsupportedMediaType => http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
        }
    }
}

impl ServiceError for Infallible {
    fn status_code(&self) -> ServiceErrorCode {
        unreachable!()
    }
}

pub trait ServiceError: ToString {
    fn grpc_error(&self) -> tonic::Status {
        let grpc_code = self.status_code().to_grpc_status_code();
        let error_msg = self.to_string();
        tonic::Status::new(grpc_code, error_msg)
    }

    fn status_code(&self) -> ServiceErrorCode;
}

pub fn convert_to_grpc_result<T, E: ServiceError>(
    res: Result<T, E>,
) -> Result<tonic::Response<T>, tonic::Status> {
    res.map(|outcome| tonic::Response::new(outcome))
        .map_err(|err| err.grpc_error())
}

impl From<SearchStreamRequest> for SearchRequest {
    fn from(item: SearchStreamRequest) -> Self {
        use crate::search_request::Query;
        let query = Some(Query::Text(item.query));
        Self {
            index_id: item.index_id,
            query,
            search_fields: item.search_fields,
            snippet_fields: item.snippet_fields,
            start_timestamp: item.start_timestamp,
            end_timestamp: item.end_timestamp,
            max_hits: 0,
            start_offset: 0,
            sort_by_field: None,
            sort_order: None,
            aggregation_request: None,
        }
    }
}

impl From<DeleteQuery> for SearchRequest {
    fn from(delete_query: DeleteQuery) -> Self {
        Self {
            index_id: delete_query.index_id,
            query: delete_query.query.map(Into::into),
            start_timestamp: delete_query.start_timestamp,
            end_timestamp: delete_query.end_timestamp,
            search_fields: delete_query.search_fields,
            ..Default::default()
        }
    }
}

impl fmt::Display for SplitSearchError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, split_id: {})", self.error, self.split_id)
    }
}

impl From<search_request::Query> for metastore_api::delete_query::Query {
    fn from(query: search_request::Query) -> metastore_api::delete_query::Query {
        use search_request::Query as Src;
        use metastore_api::delete_query::Query as Dst;
        match query {
            Src::Text(text) => Dst::Text(text),
            Src::SetQuery(query) => Dst::SetQuery(query),
        }
    }
}

impl From<metastore_api::delete_query::Query> for search_request::Query {
    fn from(query: metastore_api::delete_query::Query) -> search_request::Query {
        use metastore_api::delete_query::Query as Src;
        use search_request::Query as Dst;
        match query {
            Src::Text(text) => Dst::Text(text),
            Src::SetQuery(query) => Dst::SetQuery(query),
        }
    }
}

impl From<String> for metastore_api::delete_query::Query {
    fn from(query: String) -> metastore_api::delete_query::Query {
        metastore_api::delete_query::Query::Text(query)
    }
}

impl From<String> for search_request::Query {
    fn from(query: String) -> search_request::Query {
        search_request::Query::Text(query)
    }
}

impl From<metastore_api::Term> for serde_json::Value {
    fn from(term: metastore_api::Term) -> serde_json::Value {
        use metastore_api::term::Term as InnerTerm;
        match term.term {
            Some(InnerTerm::Text(s)) => s.into(),
            Some(InnerTerm::Unsigned(u)) => u.into(),
            Some(InnerTerm::Signed(i)) => i.into(),
            Some(InnerTerm::Fp64(f)) => f.into(),
            Some(InnerTerm::Boolean(b)) => b.into(),
            None => serde_json::Value::Null,
        }
    }
}

impl TryFrom<serde_json::Value> for metastore_api::Term {
    type Error = serde_json::Value;
    fn try_from(term: serde_json::Value) -> Result<metastore_api::Term, serde_json::Value> {
        use metastore_api::term::Term as InnerTerm;
        use serde_json::Value as JsonValue;
        let inner = match term {
            JsonValue::Null | JsonValue::Array(_) | JsonValue::Object(_) => return Err(term),
            JsonValue::String(s) => InnerTerm::Text(s),
            JsonValue::Number(ref n) => {
                if let Some(u) = n.as_u64() {
                    InnerTerm::Unsigned(u)
                } else if let Some(i) = n.as_i64() {
                    InnerTerm::Signed(i)
                } else if let Some(f) = n.as_f64() {
                    InnerTerm::Fp64(f)
                } else {
                    // unreachable without arbitrary_precision flag on serde
                    return Err(term);
                }
            },
            JsonValue::Bool(b) => InnerTerm::Boolean(b),
        };
        Ok(metastore_api::Term {
            term: Some(inner),
        })
    }
}

pub(crate) mod serde_helpers {
    use serde::de::{Deserialize, Deserializer};
    use serde::Serialize;
    use crate::metastore_api::delete_query::Query;
    use crate::quickwit_common::SetQuery;

    pub fn required_option<'de, D, T: Deserialize<'de> + std::fmt::Debug>(deserializer: D) -> Result<Option<T>, D::Error>
    where D: Deserializer<'de> {
            dbg!(T::deserialize(deserializer).map(Some))
    }

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    pub enum SerializedQuery {
        Text {
            query: String
        },
        SetQuery(SetQuery),
    }

    impl From<Query> for SerializedQuery {
        fn from(query: Query) -> SerializedQuery {
            match query {
                Query::Text(query) => SerializedQuery::Text {query},
                Query::SetQuery(sq) => SerializedQuery::SetQuery (sq),
            }
        }
    }

    impl From<SerializedQuery> for Query {
        fn from(query: SerializedQuery) -> Query {
            match query {
                SerializedQuery::Text {query} => Query::Text(query),
                SerializedQuery::SetQuery (sq) => Query::SetQuery(sq),
            }
        }
    }
}
