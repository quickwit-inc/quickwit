// Copyright (C) 2022 Quickwit, Inc.
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

pub mod file_backed_metastore;
pub mod grpc_metastore;
mod index_metadata;
#[cfg(feature = "postgres")]
pub mod postgresql_metastore;
#[cfg(feature = "postgres")]
mod postgresql_model;

use std::ops::{Bound, Deref, DerefMut};

use async_trait::async_trait;
pub use index_metadata::IndexMetadata;
use quickwit_common::uri::Uri;
use quickwit_config::SourceConfig;
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_proto::metastore_api::{DeleteQuery, DeleteTask};

use crate::checkpoint::IndexCheckpointDelta;
use crate::{MetastoreError, MetastoreResult, Split, SplitMetadata, SplitState};

/// Metastore meant to manage Quickwit's indexes, their splits and delete tasks.
///
/// I. Index and splits management.
///
/// Quickwit needs a way to ensure that we can cleanup unused files,
/// and this process needs to be resilient to any fail-stop failures.
/// We rely on atomically transitioning the status of splits.
///
/// The split state goes through the following life cycle:
/// 1. `Staged`
///   - Start uploading the split files.
/// 2. `Published`
///   - Uploading the split files is complete and the split is searchable.
/// 3. `MarkedForDeletion`
///   - Mark the split for deletion.
///
/// If a split has a file in the storage, it MUST be registered in the metastore,
/// and its state can be as follows:
/// - `Staged`: The split is almost ready. Some of its files may have been uploaded in the storage.
/// - `Published`: The split is ready and published.
/// - `MarkedForDeletion`: The split is marked for deletion.
///
/// Before creating any file, we need to stage the split. If there is a failure, upon recovery, we
/// schedule for deletion all the staged splits. A client may not necessarily remove files from
/// storage right after marking it for deletion. A CLI client may delete files right away, but a
/// more serious deployment should probably only delete those files after a grace period so that the
/// running search queries can complete.
///
/// II. Delete tasks management.
///
/// A delete task is defined on a given index and by a search query. It can be
/// applied to all the splits of the index.
///
/// Quickwit needs a way to track that a delete task has been applied to a split. This is ensured
/// by two mecanisms:
/// - On creation of a delete task, we give to the task a monotically increasing opstamp (uniqueness
///   and monotonically increasing must be true at the index level).
/// - When a delete task is executed on a split, that is when the documents matched by the search
///   query are removed from the splits, we update the split's `delete_opstamp` to the value of the
///   task's opstamp. This marks the split as "up-to-date" regarding this delete task. If new delete
///   tasks are added, we will know that we need to run these delete tasks on the splits as its
///   `delete_optstamp` will be inferior to the `opstamp` of the new tasks.
///
/// For splits created after a given delete task, Quickwit's indexing ensures that these splits
/// are created with a `delete_optstamp` equal the lastest opstamp of the tasks of the
/// corresponding index.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait Metastore: Send + Sync + 'static {
    /// Checks whether the metastore is available.
    async fn check_connectivity(&self) -> anyhow::Result<()>;

    /// Returns whether an index exists in the metastore.
    async fn index_exists(&self, index_id: &str) -> MetastoreResult<bool> {
        match self.index_metadata(index_id).await {
            Ok(_) => Ok(true),
            Err(MetastoreError::IndexDoesNotExist { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Creates an index.
    ///
    /// This API creates a new index in the metastore.
    /// An error will occur if an index that already exists in the storage is specified.
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()>;

    /// List indexes.
    ///
    /// This API lists the indexes stored in the metastore and returns a collection of
    /// [`IndexMetadata`].
    async fn list_indexes_metadatas(&self) -> MetastoreResult<Vec<IndexMetadata>>;

    /// Returns the [`IndexMetadata`] for a given index.
    /// TODO consider merging with list_splits to remove one round-trip
    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata>;

    /// Deletes an index.
    ///
    /// This API removes the specified  from the metastore, but does not remove the index from the
    /// storage. An error will occur if an index that does not exist in the storage is
    /// specified.
    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()>;

    /// Stages a split.
    ///
    /// A split needs to be staged before uploading any of its files to the storage.
    /// An error will occur if an index that does not exist in the storage is specified, or if you
    /// specify a split that already exists.
    async fn stage_split(
        &self,
        index_id: &str,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<()>;

    /// Publishes a list of splits.
    ///
    /// This API only updates the state of the split from [`SplitState::Staged`] to
    /// [`SplitState::Published`]. At this point, the split files are assumed to have already
    /// been uploaded. If the split is already published, this API call returns a success.
    /// An error will occur if you specify an index or split that does not exist in the storage.
    ///
    /// This method can be used to advance the checkpoint, by supplying an empty array for
    /// `split_ids`.
    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    ) -> MetastoreResult<()>;

    /// Lists the splits.
    ///
    /// Returns a list of splits that intersects the given `time_range`, `split_state`, and `tag`.
    /// Regardless of the time range filter, if a split has no timestamp it is always returned.
    /// An error will occur if an index that does not exist in the storage is specified.
    async fn list_splits<'a>(&self, query: ListSplitsQuery<'a>) -> MetastoreResult<Vec<Split>>;

    /// Lists all the splits without filtering.
    ///
    /// Returns a list of all splits currently known to the metastore regardless of their state.
    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<Split>> {
        let query = ListSplitsQuery::for_index(index_id);
        self.list_splits(query).await
    }

    /// Lists splits with `split.delete_opstamp` < `delete_opstamp` for a given `index_id`.
    /// These splits are called "stale" as they have an `delete_opstamp` strictly inferior
    /// to the given `delete_opstamp`.
    async fn list_stale_splits(
        &self,
        index_id: &str,
        delete_opstamp: u64,
        num_splits: usize,
    ) -> MetastoreResult<Vec<Split>> {
        let mut query = ListSplitsQuery::for_index(index_id);
        query.with_delete_opstamp_lt(delete_opstamp);
        query.with_split_state(SplitState::Published);

        let mut splits = self.list_splits(query).await?;
        splits.sort_by(|split_left, split_right| {
            split_left
                .split_metadata
                .delete_opstamp
                .cmp(&split_right.split_metadata.delete_opstamp)
                .then_with(|| {
                    split_left
                        .publish_timestamp
                        .cmp(&split_right.publish_timestamp)
                })
        });
        splits.truncate(num_splits);
        Ok(splits)
    }

    /// Marks a list of splits for deletion.
    ///
    /// This API will change the state to [`SplitState::MarkedForDeletion`] so that it is not
    /// referenced by the client anymore. It actually does not remove the split from storage. An
    /// error will occur if you specify an index or split that does not exist in the storage.
    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()>;

    /// Deletes a list of splits.
    ///
    /// This API only accepts splits that are in [`SplitState::Staged`] or
    /// [`SplitState::MarkedForDeletion`] state. This removes the split metadata from the
    /// metastore, but does not remove the split from storage. An error will occur if you
    /// specify an index or split that does not exist in the storage.
    async fn delete_splits<'a>(&self, index_id: &str, split_ids: &[&'a str])
        -> MetastoreResult<()>;

    /// Adds a new source. Fails with
    /// [`SourceAlreadyExists`](crate::MetastoreError::SourceAlreadyExists) if a source with the
    /// same ID is already defined for the index.
    ///
    /// If a checkpoint is already registered for the source, it is kept.
    async fn add_source(&self, index_id: &str, source: SourceConfig) -> MetastoreResult<()>;

    /// Enables or Disables a source.
    /// Fails with `SourceDoesNotExist` error if the specified source doesn't exist.
    async fn toggle_source(
        &self,
        index_id: &str,
        source_id: &str,
        enable: bool,
    ) -> MetastoreResult<()>;

    /// Deletes a source. Fails with
    /// [`SourceDoesNotExist`](crate::MetastoreError::SourceDoesNotExist) if the specified source
    /// does not exist.
    ///
    /// The checkpoint associated to the source is deleted as well.
    /// If the checkpoint is missing, this does not trigger an error.
    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()>;

    /// Resets the checkpoint of a source identified by `index_id` and `source_id`.
    async fn reset_source_checkpoint(&self, index_id: &str, source_id: &str)
        -> MetastoreResult<()>;

    /// Returns the metastore uri.
    fn uri(&self) -> &Uri;

    /// Gets the last delete opstamp for a given `index_id`.
    async fn last_delete_opstamp(&self, index_id: &str) -> MetastoreResult<u64>;

    /// Creates a [`DeleteTask`] from a [`DeleteQuery`].
    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask>;

    /// Updates splits `split_metadata.delete_opstamp` to the value `delete_opstamp`.
    async fn update_splits_delete_opstamp<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        delete_opstamp: u64,
    ) -> MetastoreResult<()>;

    /// Lists [`DeleteTask`] with `delete_task.opstamp` > `opstamp_start` for a given `index_id`.
    async fn list_delete_tasks(
        &self,
        index_id: &str,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// A query builder for filtering splits within the metastore.
pub struct ListSplitsQuery<'a> {
    /// The index to get splits from.
    pub index: &'a str,

    /// The maximum number of splits to retrieve.
    pub limit: Option<usize>,

    /// The number of splits to skip.
    pub offset: Option<usize>,

    /// A specific split state(s) to filter by.
    pub split_states: Vec<SplitState>,

    /// A specific set of tag(s) to filter by.
    pub tags: Option<TagFilterAst>,

    /// A set filters which can have the common set of equality operators. (`le`, `lt`, `ge`, `gt`)
    pub equality_filters: EqualityFieldFilters,
}

impl<'a> Deref for ListSplitsQuery<'a> {
    type Target = EqualityFieldFilters;

    fn deref(&self) -> &Self::Target {
        &self.equality_filters
    }
}

impl<'a> DerefMut for ListSplitsQuery<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.equality_filters
    }
}

#[allow(unused_attributes)]
impl<'a> ListSplitsQuery<'a> {
    /// Create a new [ListSplitsQuery] for a specific index.
    pub fn for_index(index: &'a str) -> Self {
        Self {
            index,
            limit: None,
            offset: None,
            split_states: vec![],
            tags: None,
            equality_filters: EqualityFieldFilters::default(),
        }
    }

    /// Sets the maximum number of splits to retrieve.
    pub fn with_limit(&mut self, n: usize) {
        self.limit = Some(n);
    }

    /// Sets the number of splits to skip.
    pub fn with_offset(&mut self, n: usize) {
        self.offset = Some(n);
    }

    /// Select splits which have the given split state.
    pub fn with_split_state(&mut self, state: SplitState) {
        self.split_states.push(state);
    }

    /// Select splits which have the any of the following split state.
    pub fn with_split_states(&mut self, states: impl AsRef<[SplitState]>) {
        self.split_states.extend_from_slice(states.as_ref());
    }

    /// Select splits which match the given tag filter.
    pub fn with_tags_filter(&mut self, tags: TagFilterAst) {
        self.tags = Some(tags);
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// A range containing the upper and lower bounds to filter documents by.
pub struct FilterRange<T> {
    /// The lower bound of the filter.
    pub start: Bound<T>,
    /// The upper bound of the filter.
    pub end: Bound<T>,
}

impl<T: PartialEq + PartialOrd> FilterRange<T> {
    /// Checks if both the upper and lower bound are `Bound::Unbounded`.
    pub fn is_unbounded(&self) -> bool {
        self.start == Bound::Unbounded && self.end == Bound::Unbounded
    }

    /// Checks if the provided value lied within the upper and lower bounds
    /// of the range.
    pub fn contains(&self, value: &T) -> bool {
        if self.is_unbounded() {
            return true;
        }

        let lower_check = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(left) => left <= value,
            Bound::Excluded(left) => left < value,
        };

        let upper_check = match &self.end {
            Bound::Unbounded => true,
            Bound::Included(left) => left >= value,
            Bound::Excluded(left) => left > value,
        };

        lower_check && upper_check
    }
}

// The `Default` derive implementation imposes a restriction
// for `T` to also implement Default when this is not required.
impl<T> Default for FilterRange<T> {
    fn default() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

macro_rules! define_equality_filters {
    ($name:ident { $($field:ident : $tp:ty),* $(,)?}) => {
        #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
        /// A collection of filter ranges which has individual setters for each range bound.
        pub struct $name {
            $(
                pub $field: FilterRange<$tp>,
            )*
        }

        paste::paste! {
            #[allow(unused)]
            impl $name {
                $(
                    /// Set the field's lower bound to match values that are
                    /// *less than or equal to* the provided value.
                    pub fn [<with_ $field _le>](&mut self, v: $tp) {
                        self.$field.end = std::ops::Bound::Included(v);
                    }

                    /// Set the field's lower bound to match values that are
                    /// *less than* the provided value.
                    pub fn [<with_ $field _lt>](&mut self, v: $tp) {
                        self.$field.end = std::ops::Bound::Excluded(v);
                    }

                    /// Set the field's upper bound to match values that are
                    /// *greater than or equal to* the provided value.
                    pub fn [<with_ $field _ge>](&mut self, v: $tp) {
                        self.$field.start = std::ops::Bound::Included(v);
                    }

                    /// Set the field's upper bound to match values that are
                    /// *greater than* the provided value.
                    pub fn [<with_ $field _gt>](&mut self, v: $tp) {
                        self.$field.start = std::ops::Bound::Excluded(v);
                    }
                )*
            }
        }
    }
}

define_equality_filters!(EqualityFieldFilters {
    time_range: i64,
    delete_opstamp: u64,
    update_timestamp: i64,
});

#[cfg(test)]
mod list_splits_query_tests {
    use super::*;

    define_equality_filters!(TestFilter { age: u64 });

    #[test]
    fn test_derived_setters() {
        let filter = TestFilter::default();
        assert_eq!(
            filter.age.start,
            Bound::Unbounded,
            "Lower bound should be unbounded."
        );
        assert_eq!(
            filter.age.end,
            Bound::Unbounded,
            "Upper bound should be unbounded."
        );

        let mut filter = TestFilter::default();
        filter.with_age_lt(18);
        assert_eq!(
            filter.age.start,
            Bound::Unbounded,
            "Lower bound should be unbounded."
        );
        assert_eq!(
            filter.age.end,
            Bound::Excluded(18),
            "Upper bound should match."
        );

        let mut filter = TestFilter::default();
        filter.with_age_le(18);
        assert_eq!(
            filter.age.start,
            Bound::Unbounded,
            "Lower bound should be unbounded."
        );
        assert_eq!(
            filter.age.end,
            Bound::Included(18),
            "Upper bound should match."
        );

        let mut filter = TestFilter::default();
        filter.with_age_gt(18);
        assert_eq!(
            filter.age.start,
            Bound::Excluded(18),
            "Lower bound should be unbounded."
        );
        assert_eq!(
            filter.age.end,
            Bound::Unbounded,
            "Upper bound should match."
        );

        let mut filter = TestFilter::default();
        filter.with_age_ge(18);
        assert_eq!(
            filter.age.start,
            Bound::Included(18),
            "Lower bound should match."
        );
        assert_eq!(
            filter.age.end,
            Bound::Unbounded,
            "Upper bound should be unbounded."
        );
    }

    #[test]
    fn test_is_in_range_lt() {
        let mut filter = TestFilter::default();
        filter.with_age_lt(18);

        assert!(
            filter.age.contains(&15),
            "Value (15) should be within range."
        );
        assert!(
            filter.age.contains(&17),
            "Value (17) should be within range."
        );
        assert!(
            filter.age.contains(&0),
            "Value (0) should be within range."
        );

        assert!(
            !filter.age.contains(&18),
            "Value (18) should not be within range."
        );
        assert!(
            !filter.age.contains(&900),
            "Value (900) should not be within range."
        );
    }

    #[test]
    fn test_is_in_range_le() {
        let mut filter = TestFilter::default();
        filter.with_age_le(18);

        assert!(
            filter.age.contains(&15),
            "Value (15) should be within range."
        );
        assert!(
            filter.age.contains(&17),
            "Value (17) should be within range."
        );
        assert!(
            filter.age.contains(&0),
            "Value (0) should be within range."
        );
        assert!(
            filter.age.contains(&18),
            "Value (18) should be within range."
        );

        assert!(
            !filter.age.contains(&19),
            "Value (19) should not be within range."
        );
        assert!(
            !filter.age.contains(&900),
            "Value (900) should not be within range."
        );
    }

    #[test]
    fn test_is_in_range_gt() {
        let mut filter = TestFilter::default();
        filter.with_age_gt(18);

        assert!(
            !filter.age.contains(&15),
            "Value (15) should not be within range."
        );
        assert!(
            !filter.age.contains(&17),
            "Value (17) should not be within range."
        );
        assert!(
            !filter.age.contains(&0),
            "Value (0) should not be within range."
        );
        assert!(
            !filter.age.contains(&18),
            "Value (18) should not be within range."
        );

        assert!(
            filter.age.contains(&19),
            "Value (19) should be within range."
        );
        assert!(
            filter.age.contains(&900),
            "Value (900) should be within range."
        );
    }

    #[test]
    fn test_is_in_range_ge() {
        let mut filter = TestFilter::default();
        filter.with_age_ge(18);

        assert!(
            !filter.age.contains(&15),
            "Value (15) should not be within range."
        );
        assert!(
            !filter.age.contains(&17),
            "Value (17) should not be within range."
        );
        assert!(
            !filter.age.contains(&0),
            "Value (0) should not be within range."
        );

        assert!(
            filter.age.contains(&18),
            "Value (18) should be within range."
        );
        assert!(
            filter.age.contains(&19),
            "Value (19) should be within range."
        );
        assert!(
            filter.age.contains(&900),
            "Value (900) should be within range."
        );
    }

    #[test]
    fn test_is_in_range_upper_and_lower_bounds() {
        let mut filter = TestFilter::default();
        filter.with_age_ge(18);
        filter.with_age_lt(30);

        assert!(
            !filter.age.contains(&17),
            "Value (17) should not be within range."
        );
        assert!(
            !filter.age.contains(&30),
            "Value (30) should not be within range."
        );
        assert!(
            !filter.age.contains(&31),
            "Value (31) should not be within range."
        );
        assert!(
            !filter.age.contains(&900),
            "Value (900) should not be within range."
        );

        assert!(
            filter.age.contains(&18),
            "Value (18) should be within range."
        );
        assert!(
            filter.age.contains(&29),
            "Value (29) should be within range."
        );
    }

    #[test]
    fn test_is_in_range_unbounded() {
        let filter = TestFilter::default();

        assert!(
            filter.age.contains(&0),
            "Value (0) should be within range."
        );
        assert!(
            filter.age.contains(&31),
            "Value (31) should be within range."
        );
        assert!(
            filter.age.contains(&900),
            "Value (900) should be within range."
        );
    }
}
