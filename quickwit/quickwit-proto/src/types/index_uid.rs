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

use std::fmt;
use std::str::FromStr;

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
pub use ulid::Ulid;

use crate::types::IndexId;

/// Index identifiers that uniquely identify not only the index, but also
/// its incarnation allowing to distinguish between deleted and recreated indexes.
/// It is represented as a string in index_id:incarnation_id format.
#[derive(Clone, Debug, Default, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct IndexUid {
    index_id: IndexId,
    incarnation_id: Ulid,
}

impl FromStr for IndexUid {
    type Err = InvalidIndexUid;

    fn from_str(index_uid_str: &str) -> Result<Self, Self::Err> {
        let (index_id, incarnation_id) = match index_uid_str.split_once(':') {
            Some((index_id, "")) => (index_id, Ulid::nil()), // TODO reject
            Some((index_id, ulid)) => {
                let ulid = Ulid::from_string(ulid).map_err(|_| InvalidIndexUid {
                    invalid_index_uid_str: index_uid_str.to_string(),
                })?;
                (index_id, ulid)
            }
            None => (index_uid_str, Ulid::nil()), // TODO reject
        };
        Ok(IndexUid {
            index_id: index_id.to_string(),
            incarnation_id,
        })
    }
}

// It is super lame, but for backward compatibility reasons we accept having a missing ulid part.
// TODO DEPRECATED ME and remove
impl<'de> Deserialize<'de> for IndexUid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let index_uid_str: String = String::deserialize(deserializer)?;
        let index_uid = IndexUid::from_str(&index_uid_str).map_err(D::Error::custom)?;
        Ok(index_uid)
    }
}

impl Serialize for IndexUid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

impl ::prost::Message for IndexUid {
    fn encode_raw<B>(&self, buf: &mut B)
    where B: ::prost::bytes::BufMut {
        if !self.index_id.is_empty() {
            ::prost::encoding::string::encode(1u32, &self.index_id, buf);
        }
        let (ulid_high, ulid_low): (u64, u64) = self.incarnation_id.into();

        if ulid_high != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &ulid_high, buf);
        }
        if ulid_low != 0u64 {
            ::prost::encoding::uint64::encode(3u32, &ulid_low, buf);
        }
    }

    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: ::prost::encoding::WireType,
        buf: &mut B,
        ctx: ::prost::encoding::DecodeContext,
    ) -> ::core::result::Result<(), ::prost::DecodeError>
    where
        B: ::prost::bytes::Buf,
    {
        const STRUCT_NAME: &str = "IndexUid";

        match tag {
            1u32 => {
                let value = &mut self.index_id;
                ::prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "index_id");
                    error
                })
            }
            2u32 => {
                let (mut ulid_high, ulid_low) = self.incarnation_id.into();
                ::prost::encoding::uint64::merge(wire_type, &mut ulid_high, buf, ctx).map_err(
                    |mut error| {
                        error.push(STRUCT_NAME, "incarnation_id_high");
                        error
                    },
                )?;
                self.incarnation_id = (ulid_high, ulid_low).into();
                Ok(())
            }
            3u32 => {
                let (ulid_high, mut ulid_low) = self.incarnation_id.into();
                ::prost::encoding::uint64::merge(wire_type, &mut ulid_low, buf, ctx).map_err(
                    |mut error| {
                        error.push(STRUCT_NAME, "incarnation_id_low");
                        error
                    },
                )?;
                self.incarnation_id = (ulid_high, ulid_low).into();
                Ok(())
            }
            _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        let mut len = 0;

        if !self.index_id.is_empty() {
            len += ::prost::encoding::string::encoded_len(1u32, &self.index_id);
        }
        let (ulid_high, ulid_low): (u64, u64) = self.incarnation_id.into();

        if ulid_high != 0u64 {
            len += ::prost::encoding::uint64::encoded_len(2u32, &ulid_high);
        }
        if ulid_low != 0u64 {
            len += ::prost::encoding::uint64::encoded_len(3u32, &ulid_low);
        }
        len
    }

    fn clear(&mut self) {
        self.index_id.clear();
        self.incarnation_id = Ulid::nil();
    }
}

impl fmt::Display for IndexUid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.index_id, self.incarnation_id)
    }
}

impl IndexUid {
    /// Creates a new index uid from index_id.
    /// A random ULID will be used as incarnation
    pub fn new_with_random_ulid(index_id: &str) -> Self {
        Self::from_parts(index_id, Ulid::new())
    }

    pub fn from_parts(index_id: &str, incarnation_id: impl Into<Ulid>) -> Self {
        assert!(!index_id.contains(':'), "index ID may not contain `:`");
        let incarnation_id = incarnation_id.into();
        IndexUid {
            index_id: index_id.to_string(),
            incarnation_id,
        }
    }

    pub fn parse(string: &str) -> Result<Self, InvalidIndexUid> {
        string.parse()
    }

    pub fn index_id(&self) -> &str {
        &self.index_id
    }

    pub fn incarnation_id(&self) -> &Ulid {
        &self.incarnation_id
    }

    pub fn is_empty(&self) -> bool {
        self.index_id.is_empty()
    }
}

impl From<IndexUid> for String {
    fn from(val: IndexUid) -> Self {
        val.to_string()
    }
}

#[derive(Error, Debug)]
#[error("invalid index uid `{invalid_index_uid_str}`")]
pub struct InvalidIndexUid {
    pub invalid_index_uid_str: String,
}

impl TryFrom<String> for IndexUid {
    type Error = InvalidIndexUid;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}
