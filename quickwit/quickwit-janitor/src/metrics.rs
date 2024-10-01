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

use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    new_counter, new_counter_vec, new_gauge_vec, IntCounter, IntCounterVec, IntGaugeVec
};
use quickwit_index_management::RecordGcMetrics;


pub struct GcMetrics {
    pub deleted_splits: IntCounter,
    pub deleted_bytes: IntCounter,
    pub failed_splits: IntCounter,
}

impl RecordGcMetrics for GcMetrics {
    fn record(&self, num_deleted_splits: usize, num_deleted_bytes: u64, num_failed_splits: usize) {
        self.deleted_splits.inc_by(num_deleted_splits as u64);
        self.deleted_bytes.inc_by(num_deleted_bytes);
        self.failed_splits.inc_by(num_failed_splits as u64);
    }
}

impl Default for GcMetrics {
    fn default() -> GcMetrics {
        let deleted_splits =  new_counter_vec(
            "gc_deleted_splits_count",
            "Total number of splits deleted by the garbage collector.",
            "quickwit_janitor",
            &[],
            ["result"],
        );
        let deleted_bytes = new_counter(
            "gc_deleted_bytes_total",
            "Total number of bytes deleted by the garbage collector.",
            "quickwit_janitor",
            &[],
        );
        GcMetrics {
            deleted_splits: deleted_splits.with_label_values(["success"]),
            deleted_bytes,
            failed_splits: deleted_splits.with_label_values(["failure"])
        }
    }

}

pub struct JanitorMetrics {
    pub ongoing_num_delete_operations_total: IntGaugeVec<1>,
    pub gc_run_count: IntCounterVec<1>,
    pub gc_metrics: GcMetrics,
    pub gc_seconds_total: IntCounter,
    // TODO having a current run duration which is 0|undefined out of run, and returns `now -
    // start_time` during a run would be nice
}

impl Default for JanitorMetrics {
    fn default() -> Self {
        JanitorMetrics {
            ongoing_num_delete_operations_total: new_gauge_vec(
                "ongoing_num_delete_operations_total",
                "Num of ongoing delete operations (per index).",
                "quickwit_janitor",
                &[],
                ["index"],
            ),
            gc_run_count: new_counter_vec(
                     "gc_run_total",
                     "Total number of garbage collector execition.",
                     "quickwit_janitor",
                     &[],
                     ["result"],
                 ),
            gc_seconds_total: new_counter(
                "gc_seconds_total",
                     "Total time spent running the garbage collector",
                     "quickwit_janitor",
                     &[],
            ),
            gc_metrics: GcMetrics::default(),
        }
    }
}

/// `JANITOR_METRICS` exposes a bunch of related metrics through a prometheus
/// endpoint.
pub static JANITOR_METRICS: Lazy<JanitorMetrics> = Lazy::new(JanitorMetrics::default);
