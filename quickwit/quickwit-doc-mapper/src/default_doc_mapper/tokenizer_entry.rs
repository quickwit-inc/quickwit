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

use anyhow::Context;
use quickwit_query::DEFAULT_REMOVE_TOKEN_LENGTH;
use serde::{Deserialize, Serialize};
use tantivy::tokenizer::{
    AsciiFoldingFilter, LowerCaser, NgramTokenizer, RegexTokenizer, RemoveLongFilter, TextAnalyzer, SimpleTokenizer,
};

/// A `TokenizerEntry` defines a custom tokenizer with its name and configuration.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
pub struct TokenizerEntry {
    /// Tokenizer name.
    pub name: String,
    /// Tokenizer configuration.
    #[serde(flatten)]
    pub(crate) config: TokenizerConfig,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
pub struct TokenizerConfig {
    #[serde(flatten)]
    tokenizer_type: TokenizerType,
    #[serde(default)]
    filters: Vec<TokenFilterType>,
}

impl TokenizerConfig {
    pub fn text_analyzer(&self) -> anyhow::Result<TextAnalyzer> {
        let mut text_analyzer_builder = match &self.tokenizer_type {
            TokenizerType::Simple => TextAnalyzer::builder(SimpleTokenizer::default()).dynamic(),
            TokenizerType::Ngram(options) => {
                let tokenizer =
                    NgramTokenizer::new(options.min_gram, options.max_gram, options.prefix_only)
                        .with_context(|| "Invalid ngram tokenizer".to_string())?;
                TextAnalyzer::builder(tokenizer).dynamic()
            }
            TokenizerType::Regex(options) => {
                let tokenizer = RegexTokenizer::new(&options.pattern)
                    .with_context(|| "Invalid regex tokenizer".to_string())?;
                TextAnalyzer::builder(tokenizer).dynamic()
            }
        };
        for filter in &self.filters {
            match filter.tantivy_token_filter_enum() {
                TantivyTokenFilterEnum::RemoveLong(token_filter) => {
                    text_analyzer_builder = text_analyzer_builder.filter_dynamic(token_filter);
                }
                TantivyTokenFilterEnum::LowerCaser(token_filter) => {
                    text_analyzer_builder = text_analyzer_builder.filter_dynamic(token_filter);
                }
                TantivyTokenFilterEnum::AsciiFolding(token_filter) => {
                    text_analyzer_builder = text_analyzer_builder.filter_dynamic(token_filter);
                }
            }
        }
        Ok(text_analyzer_builder.build())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TokenFilterType {
    RemoveLong,
    LowerCaser,
    AsciiFolding,
}

/// Tantivy token filter enum to build
/// a `TextAnalyzer` with dynamic token filters.
enum TantivyTokenFilterEnum {
    RemoveLong(RemoveLongFilter),
    LowerCaser(LowerCaser),
    AsciiFolding(AsciiFoldingFilter),
}

impl TokenFilterType {
    fn tantivy_token_filter_enum(&self) -> TantivyTokenFilterEnum {
        match &self {
            Self::RemoveLong => TantivyTokenFilterEnum::RemoveLong(RemoveLongFilter::limit(
                DEFAULT_REMOVE_TOKEN_LENGTH,
            )),
            Self::LowerCaser => TantivyTokenFilterEnum::LowerCaser(LowerCaser),
            Self::AsciiFolding => TantivyTokenFilterEnum::AsciiFolding(AsciiFoldingFilter),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TokenizerType {
    Simple,
    Ngram(NgramTokenizerOption),
    Regex(RegexTokenizerOption),
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NgramTokenizerOption {
    pub min_gram: usize,
    pub max_gram: usize,
    #[serde(default)]
    pub prefix_only: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RegexTokenizerOption {
    pub pattern: String,
}

#[cfg(test)]
mod tests {
    use super::{NgramTokenizerOption, TokenizerType};
    use crate::TokenizerEntry;

    #[test]
    fn test_deserialize_tokenizer_entry() {
        let result: Result<TokenizerEntry, serde_json::Error> =
            serde_json::from_str::<TokenizerEntry>(
                r#"
            {
                "name": "my_tokenizer",
                "type": "ngram",
                "filters": [
                    "remove_long",
                    "lower_caser",
                    "ascii_folding"
                ],
                "min_gram": 1,
                "max_gram": 3
            }
            "#,
            );
        assert!(result.is_ok());
        let tokenizer_config_entry = result.unwrap();
        assert_eq!(tokenizer_config_entry.config.filters.len(), 3);
        match tokenizer_config_entry.config.tokenizer_type {
            TokenizerType::Ngram(options) => {
                assert_eq!(
                    options,
                    NgramTokenizerOption {
                        min_gram: 1,
                        max_gram: 3,
                        prefix_only: false,
                    }
                )
            }
            _ => panic!("Unexpected tokenizer type"),
        }
    }

    #[test]
    fn test_deserialize_tokenizer_entry_failed_with_wrong_key() {
        let result: Result<TokenizerEntry, serde_json::Error> =
            serde_json::from_str::<TokenizerEntry>(
                r#"
            {
                "name": "my_tokenizer",
                "type": "ngram",
                "filters": [
                    "remove_long",
                    "lower_caser",
                    "ascii_folding"
                ],
                "min_gram": 1,
                "max_gram": 3,
                "abc": 123
            }
            "#,
            );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unknown field `abc`"));
    }

    #[test]
    fn test_deserialize_tokenizer_entry_regex() {
        let result: Result<TokenizerEntry, serde_json::Error> =
            serde_json::from_str::<TokenizerEntry>(
                r#"
            {
                "name": "my_tokenizer",
                "type": "regex",
                "pattern": "(my_pattern)"
            }
            "#,
            );
        assert!(result.is_ok());
    }
}
