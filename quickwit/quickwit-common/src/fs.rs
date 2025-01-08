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

use std::path::{Path, PathBuf};

use bytesize::ByteSize;
use sysinfo::{Disk, DiskRefreshKind};
use tokio;

/// Deletes the contents of a directory.
pub async fn empty_dir<P: AsRef<Path>>(path: P) -> anyhow::Result<()> {
    let mut entries = tokio::fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            tokio::fs::remove_dir_all(entry.path()).await?
        } else {
            tokio::fs::remove_file(entry.path()).await?;
        }
    }
    Ok(())
}

/// Helper function to get the indexer split cache path.
pub fn get_cache_directory_path(data_dir_path: &Path) -> PathBuf {
    data_dir_path.join("indexer-split-cache").join("splits")
}

/// Get the total size of the disk containing the given directory, or `None` if
/// it couldn't be determined.
pub fn get_disk_size(dir_path: &Path) -> Option<ByteSize> {
    let disks = sysinfo::Disks::new_with_refreshed_list_specifics(
        DiskRefreshKind::nothing().with_storage(),
    );
    let mut best_match: Option<&Disk> = None;
    for disk in disks.list() {
        if dir_path.starts_with(disk.mount_point()) {
            match best_match {
                Some(best_disk) if disk.mount_point().starts_with(best_disk.mount_point()) => {
                    best_match = Some(disk);
                }
                None => {
                    best_match = Some(disk);
                }
                _ => {}
            }
        }
        if disk.mount_point().starts_with(dir_path) && disk.mount_point() != dir_path {
            // if a disk is mounted within the directory, we can't determine the
            // size of the directories disk
            return None;
        }
    }
    best_match.map(|disk| ByteSize::b(disk.total_space()))
}

#[cfg(test)]
mod tests {
    use tempfile;

    use super::*;

    #[tokio::test]
    async fn test_empty_dir() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;

        let file_path = temp_dir.path().join("file");
        tokio::fs::File::create(file_path).await?;

        let subdir = temp_dir.path().join("subdir");
        tokio::fs::create_dir(&subdir).await?;

        let subfile_path = subdir.join("subfile");
        tokio::fs::File::create(subfile_path).await?;

        empty_dir(temp_dir.path()).await?;
        assert!(tokio::fs::read_dir(temp_dir.path())
            .await?
            .next_entry()
            .await?
            .is_none());
        Ok(())
    }
}
