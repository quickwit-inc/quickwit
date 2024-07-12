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

use std::ops::Bound;
use std::str::FromStr;

use quickwit_datetime::StrptimeParser;
use serde::Deserialize;

use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::elastic_query_dsl::ConvertableToQueryAst;
use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::QueryAst;
use crate::JsonLiteral;

#[derive(Deserialize, Debug, Default, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct RangeQueryParams {
    #[serde(default)]
    gt: Option<JsonLiteral>,
    #[serde(default)]
    gte: Option<JsonLiteral>,
    #[serde(default)]
    lt: Option<JsonLiteral>,
    #[serde(default)]
    lte: Option<JsonLiteral>,
    #[serde(default)]
    boost: Option<NotNaNf32>,
    #[serde(default)]
    format: Option<JsonLiteral>,
}

pub type RangeQuery = OneFieldMap<RangeQueryParams>;

impl ConvertableToQueryAst for RangeQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let field = self.field;
        let RangeQueryParams {
            gt,
            gte,
            lt,
            lte,
            boost,
            format,
        } = self.value;
        let (gt, gte, lt, lte) = if let Some(JsonLiteral::String(fmt)) = format {
            let parser = StrptimeParser::from_str(&fmt).map_err(|reason| {
                anyhow::anyhow!("failed to create parser from : {}; reason: {}", fmt, reason)
            })?;
            (
                gt.map(|v| parse_and_convert(v, &parser)).transpose()?,
                gte.map(|v| parse_and_convert(v, &parser)).transpose()?,
                lt.map(|v| parse_and_convert(v, &parser)).transpose()?,
                lte.map(|v| parse_and_convert(v, &parser)).transpose()?,
            )
        } else {
            (gt, gte, lt, lte)
        };

        let range_query_ast = crate::query_ast::RangeQuery {
            field,
            lower_bound: match (gt, gte) {
                (Some(_gt), Some(_gte)) => {
                    anyhow::bail!("both gt and gte are set")
                }
                (Some(gt), None) => Bound::Excluded(gt),
                (None, Some(gte)) => Bound::Included(gte),
                (None, None) => Bound::Unbounded,
            },
            upper_bound: match (lt, lte) {
                (Some(_lt), Some(_lte)) => {
                    anyhow::bail!("both lt and lte are set")
                }
                (Some(lt), None) => Bound::Excluded(lt),
                (None, Some(lte)) => Bound::Included(lte),
                (None, None) => Bound::Unbounded,
            },
        };
        let ast: QueryAst = range_query_ast.into();
        Ok(ast.boost(boost))
    }
}

fn parse_and_convert(literal: JsonLiteral, parser: &StrptimeParser) -> anyhow::Result<JsonLiteral> {
    if let JsonLiteral::String(date_time_str) = literal {
        let parsed_date_time = parser
            .parse_date_time(&date_time_str)
            .map_err(|reason| anyhow::anyhow!("Failed to parse date time: {}", reason))?;
        Ok(JsonLiteral::String(parsed_date_time.to_string()))
    } else {
        Ok(literal)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use quickwit_datetime::StrptimeParser;

    use crate::elastic_query_dsl::range_query::parse_and_convert;
    use crate::JsonLiteral;

    #[test]
    fn test_parse_and_convert() -> anyhow::Result<()> {
        let parser = StrptimeParser::from_str("%Y-%m-%d %H:%M:%S").unwrap();

        // valid datetime
        let input = JsonLiteral::String("2022-12-30 05:45:00".to_string());
        let result = parse_and_convert(input, &parser)?;
        assert_eq!(
            result,
            JsonLiteral::String("2022-12-30 5:45:00.0 +00:00:00".to_string())
        );

        // invalid datetime
        let input = JsonLiteral::String("invalid datetime".to_string());
        let result = parse_and_convert(input, &parser);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Failed to parse date time"));

        // non_string(number) input
        let input = JsonLiteral::Number(27.into());
        let result = parse_and_convert(input.clone(), &parser)?;
        assert_eq!(result, input);

        Ok(())
    }
}
