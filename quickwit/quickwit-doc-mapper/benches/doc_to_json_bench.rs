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

use binggan::plugins::*;
use binggan::{black_box, BenchRunner, PeakMemAlloc, INSTRUMENTED_SYSTEM};
use quickwit_doc_mapper::DocMapper;
use tantivy::TantivyDocument;

const SIMPLE_JSON_TEST_DATA: &str = include_str!("data/simple-parse-bench.json");
const ROUTING_TEST_DATA: &str = include_str!("data/simple-routing-expression-bench.json");

const DOC_MAPPER_CONF_SIMPLE_JSON: &str = r#"{
    "type": "default",
    "default_search_fields": [],
    "tag_fields": [],
    "field_mappings": [
        {"name": "id", "type": "u64", "fast": false },
        {"name": "first_name", "type": "text" },
        {"name": "last_name", "type": "text" },
        {"name": "email", "type": "text" }
    ]
}"#;

/// Note that {"name": "date", "type": "datetime", "input_formats": ["%Y-%m-%d"], "output_format":
/// "%Y-%m-%d"}, is removed since tantivy parsing only supports RFC3339
const ROUTING_DOC_MAPPER_CONF: &str = r#"{
    "type": "default",
    "default_search_fields": [],
    "tag_fields": [],
    "field_mappings": [
        {"name": "timestamp", "type": "datetime", "input_formats": ["unix_timestamp"], "output_format": "%Y-%m-%d %H:%M:%S", "output_format": "%Y-%m-%d %H:%M:%S", "fast": true },
        {"name": "source", "type": "text" },
        {"name": "vin", "type": "text" },
        {"name": "vid", "type": "text" },
        {"name": "domain", "type": "text" },
        {"name": "seller", "type": "object", "field_mappings": [
            {"name": "id", "type": "text" },
            {"name": "name", "type": "text" },
            {"name": "address", "type": "text" },
            {"name": "zip", "type": "text" }
        ]}
    ],
    "partition_key": "seller.id"
}"#;

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

fn get_test_data(
    name: &'static str,
    raw: &'static str,
    doc_mapper: &'static str,
) -> (&'static str, usize, Vec<&'static str>, Box<DocMapper>) {
    let lines: Vec<&str> = raw.lines().map(|line| line.trim()).collect();
    (
        name,
        raw.len(),
        lines,
        serde_json::from_str(doc_mapper).unwrap(),
    )
}

fn run_bench() {
    let inputs: Vec<(&str, usize, Vec<&str>, Box<DocMapper>)> = vec![
        (get_test_data(
            "flat_json",
            SIMPLE_JSON_TEST_DATA,
            DOC_MAPPER_CONF_SIMPLE_JSON,
        )),
        (get_test_data("routing_json", ROUTING_TEST_DATA, ROUTING_DOC_MAPPER_CONF)),
    ];

    let mut runner: BenchRunner = BenchRunner::new();

    runner.config().set_num_iter_for_bench(1);
    runner.config().set_num_iter_for_group(100);
    runner
        .add_plugin(CacheTrasher::default())
        .add_plugin(BPUTrasher::default())
        .add_plugin(PeakMemAllocPlugin::new(GLOBAL));

    for (input_name, size, data, doc_mapper) in inputs.iter() {
        let dynamic_doc_mapper: DocMapper =
            serde_json::from_str(r#"{ "mode": "dynamic" }"#).unwrap();
        let mut group = runner.new_group();
        group.set_name(input_name);
        group.set_input_size(*size);
        group.register_with_input("doc_mapper", data, |lines| {
            for line in lines {
                black_box(doc_mapper.doc_from_json_str(line).unwrap());
            }
            Some(())
        });

        group.register_with_input("doc_mapper_dynamic", data, |lines| {
            for line in lines {
                black_box(dynamic_doc_mapper.doc_from_json_str(line).unwrap());
            }
            Some(())
        });

        group.register_with_input("tantivy parse json", data, |lines| {
            let schema = doc_mapper.schema();
            for line in lines {
                let _doc = black_box(TantivyDocument::parse_json(&schema, line).unwrap());
            }
            Some(())
        });
        group.run();
    }
}

fn main() {
    run_bench();
}
