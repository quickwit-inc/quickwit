use std::collections::HashMap;
use std::sync::Mutex;

use quickwit_proto::{LeafSearchResponse, SearchRequest, SplitIdAndFooterOffsets};

#[derive(Debug)]
pub struct LeafSearchCache {
    content: Mutex<HashMap<CacheKey, LeafSearchResponse>>,
}

impl LeafSearchCache {
    pub fn new(_capacity: usize) -> LeafSearchCache {
        LeafSearchCache {
            content: Mutex::new(HashMap::new()),
        }
    }
    pub fn get(
        &self,
        split_info: SplitIdAndFooterOffsets,
        search_request: SearchRequest,
    ) -> Option<LeafSearchResponse> {
        let key = CacheKey::from_split_meta_and_request(split_info, search_request);
        self.content.lock().unwrap().get(&key).cloned()
    }

    pub fn put(
        &self,
        split_info: SplitIdAndFooterOffsets,
        search_request: SearchRequest,
        result: LeafSearchResponse,
    ) {
        let key = CacheKey::from_split_meta_and_request(split_info, search_request);

        self.content.lock().unwrap().insert(key, result);
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct CacheKey {
    split_id: String,
    request: SearchRequest,
    request_time_range: Range,
}

impl CacheKey {
    fn from_split_meta_and_request(
        split_info: SplitIdAndFooterOffsets,
        mut search_request: SearchRequest,
    ) -> Self {
        let split_time_range = Range {
            start: split_info.timestamp_start,
            end: split_info.timestamp_end,
        };
        let request_time_range = Range {
            start: search_request.start_timestamp,
            end: search_request.end_timestamp,
        }
        .crop(&split_time_range);

        search_request.start_timestamp = None;
        search_request.end_timestamp = None;

        CacheKey {
            split_id: split_info.split_id,
            request: search_request,
            request_time_range,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Range {
    start: Option<i64>,
    end: Option<i64>,
}

impl Range {
    fn crop(&self, other: &Range) -> Range {
        let start = match (self.start, other.start) {
            (Some(this), Some(other)) => Some(this.max(other)),
            (Some(this), None) => Some(this),
            (None, other) => other,
        };

        let end = match (self.end, other.end) {
            (Some(this), Some(other)) => Some(this.max(other)),
            (Some(this), None) => Some(this),
            (None, other) => other,
        };
        Range { start, end }
    }
}
