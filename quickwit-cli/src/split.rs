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

use std::io::stdout;
use std::ops::Range;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{bail, Context};
use chrono::{NaiveDate, NaiveDateTime};
use clap::ArgMatches;
use humansize::{file_size_opts, FileSize};
use quickwit_common::uri::normalize_uri;
use quickwit_directories::{
    get_hotcache_from_split, read_split_footer, BundleDirectory, HotDirectory,
};
use quickwit_metastore::{MetastoreUriResolver, SplitState};
use quickwit_storage::{quickwit_storage_uri_resolver, BundleStorage, Storage};
use tracing::debug;

use crate::Printer;

#[derive(Debug, Eq, PartialEq)]
pub struct ListSplitArgs {
    pub metastore_uri: String,
    pub index_id: String,
    pub state: SplitState,
    pub from: Option<i64>,
    pub to: Option<i64>,
    pub tags: Vec<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DescribeSplitArgs {
    metastore_uri: String,
    index_id: String,
    split_id: String,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ExtractSplitArgs {
    metastore_uri: String,
    index_id: String,
    split_id: String,
    target_folder: PathBuf,
}

impl ExtractSplitArgs {
    pub fn new(
        metastore_uri: String,
        index_id: String,
        split_id: String,
        target_folder: PathBuf,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            metastore_uri,
            index_id,
            split_id,
            target_folder,
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum SplitCliCommand {
    ListSplit(ListSplitArgs),
    DescribeSplit(DescribeSplitArgs),
    ExtractSplit(ExtractSplitArgs),
}

impl SplitCliCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "list" => Self::parse_list_args(submatches),
            "describe" => Self::parse_describe_args(submatches),
            "extract" => Self::parse_extract_split_args(submatches),
            _ => bail!("Subcommand '{}' is not implemented", subcommand),
        }
    }

    fn parse_list_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .context("'metastore-uri' is a required arg")
            .map(normalize_uri)??;
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg")?
            .to_string();

        let state = matches
            .value_of("state")
            .context("'state' is a required arg")
            .map(SplitState::from_str)?
            .map_err(|err_str| anyhow::anyhow!(err_str))?;

        let from = if let Some(date_str) = matches.value_of("from") {
            let from_date_time = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                .map(|date| date.and_hms(0, 0, 0))
                .or_else(|_err| NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S"))
                .context("'from' should be of the format `2020-10-31` or `2020-10-31T02:00:00`")?;
            Some(from_date_time.timestamp())
        } else {
            None
        };

        let to = if let Some(date_str) = matches.value_of("to") {
            let to_date_time = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                .map(|date| date.and_hms(0, 0, 0))
                .or_else(|_err| NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S"))
                .context("'to' should be of the format `2020-10-31` or `2020-10-31T02:00:00`")?;
            Some(to_date_time.timestamp())
        } else {
            None
        };

        let tags = matches.values_of("tags").map_or(vec![], |values| {
            values.into_iter().map(str::to_string).collect::<Vec<_>>()
        });

        Ok(Self::ListSplit(ListSplitArgs {
            metastore_uri,
            index_id,
            state,
            from,
            to,
            tags,
        }))
    }

    fn parse_describe_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg")?
            .to_string();
        let split_id = matches
            .value_of("split-id")
            .context("'split-id' is a required arg")?
            .to_string();
        let metastore_uri = matches
            .value_of("metastore-uri")
            .context("'metastore-uri' is a required arg")
            .map(normalize_uri)??;

        Ok(Self::DescribeSplit(DescribeSplitArgs {
            metastore_uri,
            index_id,
            split_id,
        }))
    }

    fn parse_extract_split_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg")?
            .to_string();
        let split_id = matches
            .value_of("split-id")
            .context("'split-id' is a required arg")?
            .to_string();
        let metastore_uri = matches
            .value_of("metastore-uri")
            .context("'metastore-uri' is a required arg")
            .map(normalize_uri)??;

        let target_folder = matches
            .value_of("target-folder")
            .map(PathBuf::from)
            .context("'target-folder' is a required arg")?;

        Ok(Self::ExtractSplit(ExtractSplitArgs::new(
            metastore_uri,
            index_id,
            split_id,
            target_folder,
        )?))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::ListSplit(args) => list_split_cli(args).await,
            Self::DescribeSplit(args) => describe_split_cli(args).await,
            Self::ExtractSplit(args) => extract_split_cli(args).await,
        }
    }
}

pub async fn list_split_cli(args: ListSplitArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "list-split");

    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let time_range_opt = match (args.from, args.to) {
        (None, None) => None,
        (None, Some(to)) => Some(Range {
            start: i64::MIN,
            end: to,
        }),
        (Some(from), None) => Some(Range {
            start: from,
            end: i64::MAX,
        }),
        (Some(from), Some(to)) => Some(Range {
            start: from,
            end: to,
        }),
    };
    let splits = metastore
        .list_splits(&args.index_id, args.state, time_range_opt, &args.tags)
        .await?;

    let mut stdout_handle = stdout();
    let mut printer = Printer {
        stdout: &mut stdout_handle,
    };
    for split in splits {
        printer.print_header("Id")?;
        printer.print_value(format_args!("{:>7}", split.split_metadata.split_id))?;
        printer.print_header("Created at")?;
        printer.print_value(format_args!(
            "{:>5}",
            NaiveDateTime::from_timestamp(split.split_metadata.create_timestamp, 0)
        ))?;
        printer.print_header("Updated at")?;
        printer.print_value(format_args!(
            "{:>3}",
            NaiveDateTime::from_timestamp(split.update_timestamp, 0)
        ))?;
        printer.print_header("Num docs")?;
        printer.print_value(format_args!("{:>7}", split.split_metadata.num_docs))?;
        printer.print_header("Size")?;
        printer.print_value(format_args!(
            "{:>5}MB",
            split.split_metadata.original_size_in_bytes / 1_000_000
        ))?;
        printer.print_header("Demux ops")?;
        printer.print_value(format_args!("{:>7}", split.split_metadata.demux_num_ops))?;
        printer.print_header("Time range")?;
        if let Some(time_range) = split.split_metadata.time_range {
            printer.print_value(format_args!("[{:?}]\n", time_range))?;
        } else {
            printer.print_value(format_args!("[*]\n"))?;
        }
        printer.flush()?;
    }

    Ok(())
}

pub async fn describe_split_cli(args: DescribeSplitArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "describe-split");

    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let index_metadata = metastore.index_metadata(&args.index_id).await?;
    let index_storage = storage_uri_resolver.resolve(&index_metadata.index_uri)?;

    let split_file = PathBuf::from(format!("{}.split", args.split_id));
    let (split_footer, _) = read_split_footer(index_storage, &split_file).await?;

    let stats = BundleDirectory::get_stats_split(split_footer.clone())?;
    let hotcache_bytes = get_hotcache_from_split(split_footer)?;

    for (path, size) in stats {
        let readable_size = size.file_size(file_size_opts::DECIMAL).unwrap();
        println!("{:?} {}", path, readable_size);
    }
    let hotcache_stats = HotDirectory::get_stats_per_file(hotcache_bytes)?;
    for (path, size) in hotcache_stats {
        let readable_size = size.file_size(file_size_opts::DECIMAL).unwrap();
        println!("HotCache {:?} {}", path, readable_size);
    }
    Ok(())
}

pub async fn extract_split_cli(args: ExtractSplitArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "extract-split");

    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let index_metadata = metastore.index_metadata(&args.index_id).await?;
    let index_storage = storage_uri_resolver.resolve(&index_metadata.index_uri)?;

    let split_file = PathBuf::from(format!("{}.split", args.split_id));
    let (_, bundle_footer) = read_split_footer(index_storage.clone(), &split_file).await?;

    let (_hotcache_bytes, bundle_storage) = BundleStorage::open_from_split_data_with_owned_bytes(
        index_storage,
        split_file,
        bundle_footer,
    )?;

    std::fs::create_dir_all(args.target_folder.to_owned())?;

    for path in bundle_storage.iter_files() {
        let mut out_path = args.target_folder.to_owned();
        out_path.push(path.to_owned());
        println!("Copying {:?}", out_path);
        bundle_storage.copy_to_file(path, &out_path).await?;
    }

    Ok(())
}
