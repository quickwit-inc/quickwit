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

use quickwit_config::{IndexTemplate, IndexTemplateId};
use quickwit_proto::metastore::MetastoreResult;

use super::index_id_matcher::IndexIdMatcher;

struct InnerMatcher {
    template_id: IndexTemplateId,
    priority: usize,
    matcher: IndexIdMatcher,
}

impl InnerMatcher {
    /// Compares two matchers by (-<priority>, <template ID>)
    fn cmp_by_priority_desc(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .cmp(&other.priority)
            .reverse()
            .then_with(|| self.template_id.cmp(&other.template_id))
    }

    fn is_match(&self, index_id: &str) -> bool {
        self.matcher.is_match(index_id)
    }
}

/// Finds the best matching index template for a given index ID. The matching algorithm is naive and
/// should be improved to support a large number of templates, should the need arise. It maintains a
/// list of index templates matchers sorted by priority and performs a linear search returning the
/// first match.
#[derive(Default)]
pub(super) struct IndexTemplateMatcher {
    inner_matchers: Vec<InnerMatcher>,
}

impl IndexTemplateMatcher {
    pub fn try_from_index_templates<'a>(
        templates: impl Iterator<Item = &'a IndexTemplate> + 'a,
    ) -> MetastoreResult<Self> {
        let mut inner_matchers = Vec::new();

        for template in templates {
            let matcher = IndexIdMatcher::try_from_index_id_patterns(&template.index_id_patterns)?;
            let inner_matcher = InnerMatcher {
                template_id: template.template_id.clone(),
                priority: template.priority,
                matcher,
            };
            inner_matchers.push(inner_matcher);
        }
        let mut matcher = Self { inner_matchers };
        matcher.sort_by_priority_desc();

        Ok(matcher)
    }

    pub fn insert(&mut self, template: &IndexTemplate) -> MetastoreResult<()> {
        let matcher = IndexIdMatcher::try_from_index_id_patterns(&template.index_id_patterns)?;
        let inner_matcher = InnerMatcher {
            template_id: template.template_id.clone(),
            priority: template.priority,
            matcher,
        };
        self.inner_matchers.push(inner_matcher);
        self.sort_by_priority_desc();

        Ok(())
    }

    pub fn remove(&mut self, template_id: &str) {
        self.inner_matchers
            .retain(|matcher| matcher.template_id != *template_id);
    }

    pub fn find_match(&self, index_id: &str) -> Option<IndexTemplateId> {
        self.inner_matchers
            .iter()
            .find(|inner_matcher| inner_matcher.is_match(index_id))
            .map(|inner_matcher| inner_matcher.template_id.clone())
    }

    fn sort_by_priority_desc(&mut self) {
        self.inner_matchers
            .sort_unstable_by(InnerMatcher::cmp_by_priority_desc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_template_matcher() {
        let index_template_bar =
            IndexTemplate::for_test("test-template-bar", &["test-index-bar*"], 0);
        let index_template_foo =
            IndexTemplate::for_test("test-template-foo", &["test-index-foo*"], 100);
        let index_template_foobar =
            IndexTemplate::for_test("test-template-foobar", &["test-index-foobar*"], 200);

        let mut matcher = IndexTemplateMatcher::default();
        matcher.insert(&index_template_foo).unwrap();
        matcher.insert(&index_template_bar).unwrap();

        assert_eq!(
            matcher.find_match("test-index-bar-1").unwrap(),
            "test-template-bar"
        );
        assert_eq!(
            matcher.find_match("test-index-foobar").unwrap(),
            "test-template-foo"
        );
        assert_eq!(
            matcher.find_match("test-index-foo").unwrap(),
            "test-template-foo"
        );

        matcher.insert(&index_template_foobar).unwrap();
        assert_eq!(
            matcher.find_match("test-index-foobar").unwrap(),
            "test-template-foobar"
        );

        matcher.remove("test-template-foobar");
        assert_eq!(
            matcher.find_match("test-index-foobar").unwrap(),
            "test-template-foo"
        );

        matcher.remove("test-template-foo");
        assert!(matcher.find_match("test-index-foobar").is_none())
    }
}
