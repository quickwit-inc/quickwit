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

use quickwit_actors::ActorHandle;
use quickwit_metastore::MetastoreError;
use quickwit_storage::StorageResolverError;
use thiserror::Error;
use tracing::error;

use crate::actors::GarbageCollector;

#[derive(Error, Debug)]
pub enum JanitorServiceError {
    #[error("Failed to resolve the storage `{0}`.")]
    StorageError(#[from] StorageResolverError),
    #[error("Metastore error `{0}`.")]
    MetastoreError(#[from] MetastoreError),
}

pub struct JanitorService {
    _node_id: String,
    _garbage_collector_handle: ActorHandle<GarbageCollector>,
}

impl JanitorService {
    pub fn new(node_id: String, garbage_collector_handle: ActorHandle<GarbageCollector>) -> Self {
        Self {
            _node_id: node_id,
            _garbage_collector_handle: garbage_collector_handle,
        }
    }
}
