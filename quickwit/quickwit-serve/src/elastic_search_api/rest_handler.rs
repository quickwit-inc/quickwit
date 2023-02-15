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

use std::sync::Arc;

use bytes::Bytes;
use elasticsearch_dsl::{Query, Search};
use quickwit_common::simple_list::SimpleList;
use quickwit_query::elastic_search_input_to_search_ast;
use quickwit_search::{SearchError, SearchResponseRest, SearchService};
use tracing::info;
use warp::{Filter, Rejection};

use super::api_specs::{
    elastic_get_index_search_filter, elastic_get_search_filter, elastic_post_index_search_filter,
    elastic_post_search_filter, SearchQueryParams,
};
use crate::elastic_search_api::extract_sort_by;
use crate::format::Format;
use crate::with_arg;

/// GET _elastic/_search
pub fn elastic_get_search_handler(
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_get_search_filter().then(|params: SearchQueryParams| async move {
        // TODO: implement
        let resp = serde_json::json!({
            "index": "all indexes",
            "params": params,
        });
        warp::reply::json(&resp)
    })
}

/// POST _elastic/_search
pub fn elastic_post_search_handler(
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_post_search_filter().then(|params: SearchQueryParams| async move {
        // TODO: implement
        let resp = serde_json::json!({
            "index": "all indexes",
            "params": params,
        });
        warp::reply::json(&resp)
    })
}

/// GET _elastic/{index}/_search
pub fn elastic_get_index_search_handler(
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_get_index_search_filter().then(
        |index: SimpleList, params: SearchQueryParams| async move {
            // TODO: implement
            let resp = serde_json::json!({
                "index": index.0,
                "params": params,
            });
            warp::reply::json(&resp)
        },
    )
}

/// POST api/_elastic/{index}/_search
pub fn elastic_post_index_search_handler(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_post_index_search_filter()
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::filters::body::bytes())
        .and(with_arg(search_service))
        .then(
            |index: SimpleList,
             params: SearchQueryParams,
             body: Bytes,
             search_service: Arc<dyn SearchService>| async move {
                info!(index_ids = ?index, params =? params, "elastic-search");
                Format::Json.make_rest_reply(
                    elastic_search_endpoint(index.0, params, body, &*search_service).await,
                )
            },
        )
}

async fn elastic_search_endpoint(
    index_ids: Vec<String>,
    params: SearchQueryParams,
    body: Bytes,
    search_service: &dyn SearchService,
) -> Result<SearchResponseRest, SearchError> {
    let index_id = index_ids.get(0).unwrap().clone();
    let elastic_search_input: Search = if let Some(query_str) = &params.q {
        let query: Query = serde_json::from_str(query_str)?;
        Search::new().query(query)
    } else {
        serde_json::from_slice(&body)?
    };
    let search_input_ast = elastic_search_input_to_search_ast(&elastic_search_input)?;
    let search_fields: Option<Vec<String>> = if params.q.is_some() {
        params.df.map(|default_field| vec![default_field])
    } else {
        None
    };

    let aggregation_request = if !elastic_search_input.aggs.is_empty() {
        Some(
            serde_json::to_string(&elastic_search_input.aggs)
                .expect("could not serialize Aggregation"),
        )
    } else {
        None
    };

    let (sort_order, sort_by_field) =
        extract_sort_by(&params.sort, elastic_search_input.sort.clone())
            .map_err(SearchError::InvalidArgument)?;

    let query = serde_json::to_string(&search_input_ast)
        .expect("could not serialize SearchInputAst");

    let search_request = quickwit_proto::SearchRequest {
        index_id,
        query,
        search_fields: search_fields.unwrap_or_default(),
        snippet_fields: elastic_search_input
            .highlight
            .map(|highlight| {
                highlight
                    .fields
                    .iter()
                    .map(|kv| kv.key.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        start_timestamp: None,
        end_timestamp: None,
        max_hits: elastic_search_input.size.unwrap_or_default(),
        start_offset: elastic_search_input.from.unwrap_or_default(),
        aggregation_request,
        sort_order,
        sort_by_field,
        ..Default::default()
    };
    let search_response = search_service.root_search(search_request).await?;
    let search_response_rest = SearchResponseRest::try_from(search_response)?;
    Ok(search_response_rest)
}
