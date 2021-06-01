/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use std::convert::TryFrom;

use crate::{
    all_flatten_mapper::AllFlattenDocMapper,
    default_mapper::{DefaultDocMapper, DocMapperConfig},
    wikipedia_mapper::WikipediaMapper,
};
use serde::{Deserialize, Serialize};
use tantivy::{
    query::Query,
    schema::{DocParsingError, Schema},
    Document,
};

/// The `DocMapper` trait defines the way of defining how a (json) document,
/// and the fields it contains, are stored and indexed.
///
/// The `DocMapper` trait is in charge of implementing :
///
/// - a way to build a tantivy::Document from a json payload
/// - a way to build a tantivy::Query from a SearchRequest
/// - a way to build a tantivy:Schema
///
#[typetag::serde(tag = "type", content = "config")]
pub trait DocMapper: Send + Sync + 'static {
    /// Returns the document built from a json string.
    fn doc_from_json(&self, doc_json: &str) -> Result<Document, DocParsingError>;
    /// Returns the schema.
    fn schema(&self) -> Schema;
    /// Returns the query.
    fn query(&self, _request: SearchRequest) -> Box<dyn Query>;
}

impl TryFrom<&str> for Box<dyn DocMapper> {
    type Error = String;

    fn try_from(doc_mapper_type_str: &str) -> Result<Self, Self::Error> {
        match doc_mapper_type_str.trim().to_lowercase().as_str() {
            "all_flatten" => Ok(Box::new(WikipediaMapper::new()) as Box<dyn DocMapper>),
            "wikipedia" => Ok(Self::Wikipedia),
            "default" => Ok(Self::Default(DocMapperConfig::default())),
            _ => Err(format!(
                "Could not parse `{}` as valid doc mapper type.",
                doc_mapper_type_str
            )),
        }
    }
}



// TODO: this is a placeholder, to be removed when it will be implementend in the search-api crate
pub struct SearchRequest {}

/// A `DocMapperType` describe a set of rules to build a document, query and schema.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum DocMapperType {
    /// Default doc mapper which is build from a config file
    Default(DocMapperConfig),
    /// All flatten doc mapper which indexes everything in one single field
    AllFlatten,
    /// Wikipedia doc mapper
    Wikipedia,
}

impl TryFrom<&str> for DocMapperType {
    type Error = String;

    fn try_from(doc_mapper_type_str: &str) -> Result<Self, Self::Error> {
        match doc_mapper_type_str.trim().to_lowercase().as_str() {
            "all_flatten" => Ok(Self::AllFlatten),
            "wikipedia" => Ok(Self::Wikipedia),
            "default" => Ok(Self::Default(DocMapperConfig::default())),
            _ => Err(format!(
                "Could not parse `{}` as valid doc mapper type.",
                doc_mapper_type_str
            )),
        }
    }
}




/// Build a doc mapper given the doc mapper type.
pub fn build_doc_mapper(mapper_type: DocMapperType) -> anyhow::Result<Box<dyn DocMapper>> {
    match mapper_type {
        DocMapperType::Default(config) => {
            DefaultDocMapper::new(config).map(|mapper| Box::new(mapper) as Box<dyn DocMapper>)
        }
        DocMapperType::AllFlatten => {
            AllFlattenDocMapper::new().map(|mapper| Box::new(mapper) as Box<dyn DocMapper>)
        }
        DocMapperType::Wikipedia => {
            WikipediaMapper::new().map(|mapper| Box::new(mapper) as Box<dyn DocMapper>)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DocMapperType;

    const JSON_ALL_FLATTEN_DOC_MAPPER: &str = r#"
        {
            "type": "all_flatten"
        }"#;

    const JSON_DEFAULT_DOC_MAPPER: &str = r#"
        {
            "type": "default",
            "config": {
                "store_source": true,
                "ignore_unknown_fields": false,
                "properties": []
            }
        }"#;

    #[test]
    fn test_deserialize_doc_mapper_type() -> anyhow::Result<()> {
        let all_flatten_mapper =
            serde_json::from_str::<DocMapperType>(JSON_ALL_FLATTEN_DOC_MAPPER)?;
        let default_mapper = serde_json::from_str::<DocMapperType>(JSON_DEFAULT_DOC_MAPPER)?;
        match all_flatten_mapper {
            DocMapperType::AllFlatten => (),
            _ => panic!("Wrong doc mapper, should be AllFlatten."),
        }
        match default_mapper {
            DocMapperType::Default(config) => {
                assert_eq!(config.store_source, true);
                assert_eq!(config.ignore_unknown_fields, false);
            }
            _ => panic!("Wrong doc mapper, should be Default."),
        }

        Ok(())
    }
}
