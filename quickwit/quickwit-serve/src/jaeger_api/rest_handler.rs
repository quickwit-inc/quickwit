// Copyright (C) 2023 Quickwit, Inc.
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

use hyper::StatusCode;
use quickwit_jaeger::JaegerService;
use quickwit_opentelemetry::otlp::TraceId;
use quickwit_proto::jaeger::storage::v1::span_reader_plugin_server::SpanReaderPlugin;
use quickwit_proto::jaeger::storage::v1::{
    FindTraceIDsRequest, GetOperationsRequest, GetServicesRequest, GetTraceRequest,
    SpansResponseChunk, TraceQueryParameters,
};
use quickwit_proto::tonic;
use quickwit_proto::tonic::Request;
use tokio_stream::StreamExt;
use warp::{Filter, Rejection};

use crate::jaeger_api::model::{
    JaegerError, JaegerResponseBody, JaegerSearchBody, TracesSearchQueryParams,
};
use crate::json_api_response::JsonApiResponse;
use crate::{require, with_arg, BodyFormat};

/// Setup Jaeger API handlers
///
/// This is where all Jaeger handlers
/// should be registered.
/// Request are executed on the `otel traces v0_6` index.
pub(crate) fn jaeger_api_handlers(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    jaeger_services_handler(jaeger_service_opt.clone())
        .or(jaeger_service_operations_handler(
            jaeger_service_opt.clone(),
        ))
        .or(jaeger_traces_search_handler(jaeger_service_opt.clone()))
        .or(jaeger_traces_handler(jaeger_service_opt.clone()))
    // Register newly created handlers here.
}

/// GET otel-traces-v0_6/services
pub fn jaeger_services_handler(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    jaeger_services_filter()
        .and(require(jaeger_service_opt))
        .then(jaeger_services)
        .map(|result| make_jaeger_api_response(result, BodyFormat::default()))
}

/// GET otel-traces-v0_6/services/{service}/operations
pub fn jaeger_service_operations_handler(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    jaeger_service_operations_filter(jaeger_service_opt)
        .then(jaeger_service_operations)
        .map(|result| make_jaeger_api_response(result, BodyFormat::default()))
}

/// GET otel-traces-v0_6/traces?service={service}&start={start_in_ns}&end={end_in_ns}&lookback=custom
pub fn jaeger_traces_search_handler(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    jaeger_traces_search_filter(jaeger_service_opt)
        .then(jaeger_traces_search)
        .map(|result| make_jaeger_api_response(result, BodyFormat::default()))
}

/// GET otel-traces-v0_6/traces/{trace-id-base-64}
pub fn jaeger_traces_handler(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    jaeger_traces_filter(jaeger_service_opt)
        .then(jaeger_get_trace)
        .map(|result| make_jaeger_api_response(result, BodyFormat::default()))
}

#[utoipa::path(
    get,
    tag = "Jaeger Services",
    path = "/services",
    responses(
        (status = 200, description = "Successfully fetched services information.", body = TODO )
    )
)]
pub(crate) fn jaeger_services_filter() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path!("otel-traces-v0_6" / "services").and(warp::get())
}

#[utoipa::path(
    get,
    tag = "Operations",
    path = "api/services/{service}/operations",
    responses(
        (status = 200, description = "Successfully fetched operations information for the specified services.", body = TODO )
    )
)]
pub(crate) fn jaeger_service_operations_filter(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (String, Option<JaegerService>), Error = Rejection> + Clone {
    warp::path!("otel-traces-v0_6" / "services" / String / "operations")
        .and(warp::get())
        .and(with_arg(jaeger_service_opt))
}

#[utoipa::path(
    get,
    tag = "Traces",
    path = "api/traces?service={service}&start={start_in_ns}&end={end_in_ns}&lookback=custom",
    responses(
        (status = 200, description = "Successfully fetched traces information.", body = TODO )
    )
)]
pub(crate) fn jaeger_traces_search_filter(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (Option<JaegerService>, TracesSearchQueryParams), Error = Rejection> + Clone
{
    warp::path!("otel-traces-v0_6" / "traces")
        .and(warp::get())
        .and(with_arg(jaeger_service_opt))
        .and(serde_qs::warp::query(serde_qs::Config::default()))
}

#[utoipa::path(
    get,
    tag = "Traces",
    path = "api/traces/{trace-id-base-64}",
    responses(
        (status = 200, description = "Successfully fetched traces information for the specified id.", body = TODO )
    )
)]
pub(crate) fn jaeger_traces_filter(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (String, Option<JaegerService>), Error = Rejection> + Clone {
    warp::path!("otel-traces-v0_6" / "traces" / String)
        .and(warp::get())
        .and(with_arg(jaeger_service_opt))
}

async fn jaeger_services(
    jaeger_service: JaegerService,
) -> Result<JaegerResponseBody<Vec<String>>, JaegerError> {
    let get_services_response = jaeger_service
        .get_services(with_tonic(GetServicesRequest {}))
        .await
        .unwrap()
        .into_inner();
    Ok(JaegerResponseBody::<Vec<String>> {
        data: get_services_response.services,
    })
}

async fn jaeger_service_operations(
    service_name: String,
    jaeger_service_opt: Option<JaegerService>,
) -> Result<JaegerSearchBody, JaegerError> {
    match jaeger_service_opt {
        Some(jaeger_service) => {
            let get_operations_request = GetOperationsRequest {
                service: service_name,
                span_kind: "".to_string(),
            };
            let get_operations_response = jaeger_service
                .get_operations(with_tonic(get_operations_request))
                .await
                .unwrap()
                .into_inner();

            Ok(JaegerSearchBody {
                data: Some(get_operations_response.operation_names),
            })
        }
        None => Err(JaegerError::internal_jaeger_error()),
    }
}

async fn jaeger_traces_search(
    jaeger_service_opt: Option<JaegerService>,
    search_params: TracesSearchQueryParams,
) -> Result<JaegerSearchBody, JaegerError> {
    match jaeger_service_opt {
        Some(jaeger_service) => {
            let query = TraceQueryParameters {
                service_name: search_params.service.unwrap_or_default(),
                operation_name: "stage_splits".to_string(),
                tags: Default::default(),
                start_time_min: None,
                start_time_max: None,
                duration_min: None,
                duration_max: None,
                num_traces: 10,
            };
            let find_trace_ids_request = FindTraceIDsRequest { query: Some(query) };

            let find_trace_ids_response = jaeger_service
                .find_trace_i_ds(with_tonic(find_trace_ids_request))
                .await
                .unwrap()
                .into_inner();

            let result = find_trace_ids_response
                .trace_ids
                .iter()
                .map(|v| String::from_utf8(v.to_vec()).unwrap())
                .collect::<Vec<String>>();

            Ok(JaegerSearchBody { data: Some(result) })
        }
        None => Err(JaegerError::internal_jaeger_error()),
    }
}

async fn jaeger_get_trace(
    trace_id_json: String,
    jaeger_service_opt: Option<JaegerService>,
) -> Result<JaegerSearchBody, JaegerError> {
    match jaeger_service_opt {
        Some(jaeger_service) => {
            let trace_id = serde_json::from_str::<TraceId>(&trace_id_json).unwrap();
            let get_trace_request = GetTraceRequest {
                trace_id: trace_id.to_vec(),
            };
            let mut span_stream = jaeger_service
                .get_trace(with_tonic(get_trace_request))
                .await
                .unwrap()
                .into_inner();
            let SpansResponseChunk { spans } = span_stream.next().await.unwrap().unwrap();
            let result = spans
                .iter()
                .map(|span| span.operation_name.clone())
                .collect::<Vec<String>>();
            Ok(JaegerSearchBody { data: Some(result) })
        }
        None => Err(JaegerError::internal_jaeger_error()),
    }
}

fn make_jaeger_api_response<T: serde::Serialize>(
    jaeger_result: Result<T, JaegerError>,
    format: BodyFormat,
) -> JsonApiResponse {
    let status_code = match &jaeger_result {
        Ok(_) => StatusCode::OK,
        Err(err) => err.status,
    };
    JsonApiResponse::new(&jaeger_result, status_code, &format)
}

fn with_tonic<T>(message: T) -> Request<T> {
    tonic::Request::new(message)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use assert_json_diff::assert_json_include;
    use quickwit_config::JaegerConfig;
    use quickwit_opentelemetry::otlp::OTEL_TRACES_INDEX_ID;
    use quickwit_search::{encode_term_for_test, MockSearchService};
    use serde_json::Value as JsonValue;

    use super::*;
    use crate::recover_fn;

    #[tokio::test]
    async fn test_when_jaeger_not_found() {
        let jaeger_api_handler = jaeger_api_handlers(None).recover(recover_fn);
        let resp = warp::test::request()
            .path("/otel-traces-v0_6/services")
            .reply(&jaeger_api_handler)
            .await;
        let error_body = serde_json::from_slice::<HashMap<String, String>>(resp.body()).unwrap();
        assert_eq!(resp.status(), 404);
        assert!(error_body.contains_key("message"));
        assert_eq!(error_body.get("message").unwrap(), "Route not found");
    }

    #[tokio::test]
    async fn test_jaeger_services() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_list_terms()
            .withf(|req| {
                req.index_id == OTEL_TRACES_INDEX_ID
                    && req.field == "service_name"
                    && req.start_timestamp.is_some()
            })
            .return_once(|_| {
                Ok(quickwit_proto::search::ListTermsResponse {
                    num_hits: 3,
                    terms: vec![
                        encode_term_for_test!("service1"),
                        encode_term_for_test!("service2"),
                        encode_term_for_test!("service3"),
                    ],
                    elapsed_time_micros: 0,
                    errors: Vec::new(),
                })
            });
        let mock_search_service = Arc::new(mock_search_service);
        let jaeger = JaegerService::new(JaegerConfig::default(), mock_search_service);

        let jaeger_api_handler = jaeger_api_handlers(Some(jaeger)).recover(recover_fn);
        let resp = warp::test::request()
            .path("/otel-traces-v0_6/services")
            .reply(&jaeger_api_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let actual_response_json: JsonValue = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!(["service1", "service2", "service3"]);
        assert_json_include!(
            actual: actual_response_json.get("data").unwrap(),
            expected: expected_response_json
        );
        Ok(())
    }
}
