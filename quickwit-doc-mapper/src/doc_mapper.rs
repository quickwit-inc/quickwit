// Copyright (C) 2021 Quickwit, Inc.
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

use std::collections::BTreeSet;
use std::fmt::Debug;

use dyn_clone::{clone_trait_object, DynClone};
use quickwit_proto::SearchRequest;
use tantivy::query::Query;
use tantivy::schema::{Field, Schema};
use tantivy::Document;

use crate::{DocParsingError, QueryParserError, SortBy};

/// The `DocMapper` trait defines the way of defining how a (json) document,
/// and the fields it contains, are stored and indexed.
///
/// The `DocMapper` trait is in charge of implementing :
///
/// - a way to build a tantivy::Document from a json payload
/// - a way to build a tantivy::Query from a SearchRequest
/// - a way to build a tantivy:Schema
#[typetag::serde(tag = "type")]
pub trait DocMapper: Send + Sync + Debug + DynClone + 'static {
    /// Returns the document built from an owned JSON string.
    ///
    /// (we pass by value here, as the value can be used as is in the _source field.)
    fn doc_from_json(&self, doc_json: String) -> Result<Document, DocParsingError>;

    /// Returns the schema.
    ///
    /// Considering schema evolution, splits within an index can have different schema
    /// over time. The schema returned here represents the most up-to-date schema of the index.
    fn schema(&self) -> Schema;

    /// Returns the query.
    ///
    /// Considering schema evolution, splits within an index can have different schema
    /// over time. So `split_schema` is the schema of the split the query is targeting.
    fn query(
        &self,
        split_schema: Schema,
        request: &SearchRequest,
    ) -> Result<Box<dyn Query>, QueryParserError>;

    /// Returns the default sort
    fn sort_by(&self) -> SortBy {
        SortBy::DocId
    }

    /// Returns the timestamp field.
    /// Considering schema evolution, splits within an index can have different schema
    /// over time. So `split_schema` is the schema of the split being operated on.
    fn timestamp_field(&self, split_schema: &Schema) -> Option<Field> {
        self.timestamp_field_name()
            .and_then(|field_name| split_schema.get_field(&field_name))
    }

    /// Returns the timestamp field name.
    fn timestamp_field_name(&self) -> Option<String> {
        None
    }

    /// Returns the tag field names
    fn tag_field_names(&self) -> BTreeSet<String> {
        Default::default()
    }

    /// Returns the demux field name.
    fn demux_field_name(&self) -> Option<String> {
        None
    }
}

clone_trait_object!(DocMapper);

#[cfg(test)]
mod tests {
    use quickwit_proto::SearchRequest;
    use tantivy::schema::Cardinality;

    use crate::default_doc_mapper::{FieldMappingType, QuickwitJsonOptions};
    use crate::{DefaultDocMapperBuilder, DocMapper, FieldMappingEntry};

    const JSON_DEFAULT_DOC_MAPPER: &str = r#"
        {
            "type": "default",
            "default_search_fields": [],
            "tag_fields": [],
            "field_mappings": []
        }"#;

    #[test]
    fn test_deserialize_doc_mapper() -> anyhow::Result<()> {
        let deserialized_default_doc_mapper =
            serde_json::from_str::<Box<dyn DocMapper>>(JSON_DEFAULT_DOC_MAPPER)?;
        let expected_default_doc_mapper = DefaultDocMapperBuilder::default().try_build()?;
        assert_eq!(
            format!("{:?}", deserialized_default_doc_mapper),
            format!("{:?}", expected_default_doc_mapper),
        );
        Ok(())
    }

    #[test]
    fn test_serdeserialize_doc_mapper() -> anyhow::Result<()> {
        let deserialized_default_doc_mapper =
            serde_json::from_str::<Box<dyn DocMapper>>(JSON_DEFAULT_DOC_MAPPER)?;
        let expected_default_doc_mapper = DefaultDocMapperBuilder::default().try_build()?;
        assert_eq!(
            format!("{:?}", deserialized_default_doc_mapper),
            format!("{:?}", expected_default_doc_mapper),
        );

        let serialized_doc_mapper = serde_json::to_string(&deserialized_default_doc_mapper)?;
        let deserialized_default_doc_mapper =
            serde_json::from_str::<Box<dyn DocMapper>>(&serialized_doc_mapper)?;
        let serialized_doc_mapper_2 = serde_json::to_string(&deserialized_default_doc_mapper)?;

        assert_eq!(serialized_doc_mapper, serialized_doc_mapper_2);

        Ok(())
    }

    #[test]
    fn test_doc_mapper_query_with_json_field() {
        let mut doc_mapper_builder = DefaultDocMapperBuilder::default();
        doc_mapper_builder.field_mappings.push(FieldMappingEntry {
            name: "json_field".to_string(),
            mapping_type: FieldMappingType::Json(
                QuickwitJsonOptions::default(),
                Cardinality::SingleValue,
            ),
        });
        let doc_mapper = doc_mapper_builder.try_build().unwrap();
        let schema = doc_mapper.schema();
        let search_request = SearchRequest {
            index_id: "quickwit-index".to_string(),
            query: "json_field.toto.titi:hello".to_string(),
            search_fields: vec![],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
        };
        let query = doc_mapper.query(schema, &search_request).unwrap();
        assert_eq!(
            format!("{:?}", query),
            r#"TermQuery(Term(type=Json, field=0, path=toto.titi, vtype=Str, "hello"))"#
        );
    }

    #[test]
    fn test_doc_mapper_query_with_json_field_default_search_fields() {
        let mut doc_mapper_builder = DefaultDocMapperBuilder::default();
        doc_mapper_builder.field_mappings.push(FieldMappingEntry {
            name: "json_field".to_string(),
            mapping_type: FieldMappingType::Json(
                QuickwitJsonOptions::default(),
                Cardinality::SingleValue,
            ),
        });
        doc_mapper_builder
            .default_search_fields
            .push("json_field".to_string());
        let doc_mapper = doc_mapper_builder.try_build().unwrap();
        let schema = doc_mapper.schema();
        let search_request = SearchRequest {
            index_id: "quickwit-index".to_string(),
            query: "toto.titi:hello".to_string(),
            search_fields: vec![],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
        };
        let query = doc_mapper.query(schema, &search_request).unwrap();
        assert_eq!(
            format!("{:?}", query),
            r#"TermQuery(Term(type=Json, field=0, path=toto.titi, vtype=Str, "hello"))"#
        );
    }

    #[test]
    fn test_doc_mapper_query_with_json_field_ambiguous_term() {
        let doc_mapper_builder = DefaultDocMapperBuilder {
            field_mappings: vec![FieldMappingEntry {
                name: "json_field".to_string(),
                mapping_type: FieldMappingType::Json(
                    QuickwitJsonOptions::default(),
                    Cardinality::SingleValue,
                ),
            }],
            default_search_fields: vec!["json_field".to_string()],
            ..Default::default()
        };
        let doc_mapper = doc_mapper_builder.try_build().unwrap();
        let schema = doc_mapper.schema();
        let search_request = SearchRequest {
            index_id: "quickwit-index".to_string(),
            query: "toto:5".to_string(),
            search_fields: vec![],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
        };
        let query = doc_mapper.query(schema, &search_request).unwrap();
        assert_eq!(
            format!("{:?}", query),
            r#"BooleanQuery { subqueries: [(Should, TermQuery(Term(type=Json, field=0, path=toto, vtype=U64, 5))), (Should, TermQuery(Term(type=Json, field=0, path=toto, vtype=Str, "5")))] }"#
        );
    }
}
