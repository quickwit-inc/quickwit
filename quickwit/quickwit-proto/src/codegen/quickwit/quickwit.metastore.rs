#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateIndexRequest {
    #[prost(string, tag = "2")]
    pub index_config_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateIndexResponse {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListIndexesMetadataRequest {
    #[prost(string, tag = "1")]
    pub query_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListIndexesMetadataResponse {
    #[prost(string, tag = "1")]
    pub indexes_metadata_serialized_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteIndexRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexMetadataRequest {
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexMetadataResponse {
    #[prost(string, tag = "1")]
    pub index_metadata_serialized_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListAllSplitsRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSplitsRequest {
    #[prost(string, tag = "1")]
    pub query_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSplitsResponse {
    #[prost(string, tag = "1")]
    pub splits_serialized_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageSplitsRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub split_metadata_list_serialized_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PublishSplitsRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub staged_split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "3")]
    pub replaced_split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "4")]
    pub index_checkpoint_delta_json_opt: ::core::option::Option<
        ::prost::alloc::string::String,
    >,
    #[prost(string, optional, tag = "5")]
    pub publish_token_opt: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MarkSplitsForDeletionRequest {
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "3")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSplitsRequest {
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "3")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddSourceRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_config_json: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ToggleSourceRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(bool, tag = "3")]
    pub enable: bool,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSourceRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResetSourceCheckpointRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteTask {
    #[prost(int64, tag = "1")]
    pub create_timestamp: i64,
    #[prost(uint64, tag = "2")]
    pub opstamp: u64,
    #[prost(message, optional, tag = "3")]
    pub delete_query: ::core::option::Option<DeleteQuery>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteQuery {
    /// Index ID.
    #[prost(string, tag = "1")]
    #[serde(alias = "index_id")]
    pub index_uid: ::prost::alloc::string::String,
    /// If set, restrict search to documents with a `timestamp >= start_timestamp`.
    #[prost(int64, optional, tag = "2")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_timestamp: ::core::option::Option<i64>,
    /// If set, restrict search to documents with a `timestamp < end_timestamp``.
    #[prost(int64, optional, tag = "3")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_timestamp: ::core::option::Option<i64>,
    /// Query text. The query language is that of tantivy.
    /// Query AST serialized in JSON
    #[prost(string, tag = "6")]
    #[serde(alias = "query")]
    pub query_ast: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateSplitsDeleteOpstampRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(uint64, tag = "3")]
    pub delete_opstamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateSplitsDeleteOpstampResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LastDeleteOpstampRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LastDeleteOpstampResponse {
    #[prost(uint64, tag = "1")]
    pub last_delete_opstamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListStaleSplitsRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub delete_opstamp: u64,
    #[prost(uint64, tag = "3")]
    pub num_splits: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDeleteTasksRequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub opstamp_start: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDeleteTasksResponse {
    #[prost(message, repeated, tag = "1")]
    pub delete_tasks: ::prost::alloc::vec::Vec<DeleteTask>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<OpenShardsSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "4")]
    pub follower_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint64, tag = "5")]
    pub next_shard_id: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub subresponses: ::prost::alloc::vec::Vec<OpenShardsSubresponse>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpenShardsSubresponse {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub open_shards: ::prost::alloc::vec::Vec<super::ingest::Shard>,
    #[prost(uint64, tag = "4")]
    pub next_shard_id: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcquireShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<AcquireShardsSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcquireShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, repeated, tag = "3")]
    pub shard_ids: ::prost::alloc::vec::Vec<u64>,
    #[prost(string, tag = "4")]
    pub publish_token: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcquireShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub subresponses: ::prost::alloc::vec::Vec<AcquireShardsSubresponse>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcquireShardsSubresponse {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub acquired_shards: ::prost::alloc::vec::Vec<super::ingest::Shard>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloseShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<CloseShardsSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloseShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(enumeration = "super::ingest::ShardState", tag = "4")]
    pub shard_state: i32,
    #[prost(uint64, optional, tag = "5")]
    pub replication_position_inclusive: ::core::option::Option<u64>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloseShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub successes: ::prost::alloc::vec::Vec<CloseShardsSuccess>,
    #[prost(message, repeated, tag = "2")]
    pub failures: ::prost::alloc::vec::Vec<CloseShardsFailure>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloseShardsSuccess {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(string, tag = "4")]
    pub leader_id: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "5")]
    pub follower_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, tag = "6")]
    pub publish_position_inclusive: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloseShardsFailure {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub shard_id: u64,
    #[prost(enumeration = "CloseShardsFailureKind", tag = "4")]
    pub failure_kind: i32,
    #[prost(string, tag = "5")]
    pub failure_message: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<DeleteShardsSubrequest>,
    #[prost(bool, tag = "2")]
    pub force: bool,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, repeated, tag = "3")]
    pub shard_ids: ::prost::alloc::vec::Vec<u64>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardsResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsRequest {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<ListShardsSubrequest>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsSubrequest {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(enumeration = "super::ingest::ShardState", optional, tag = "3")]
    pub shard_state: ::core::option::Option<i32>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsResponse {
    #[prost(message, repeated, tag = "1")]
    pub subresponses: ::prost::alloc::vec::Vec<ListShardsSubresponse>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardsSubresponse {
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub shards: ::prost::alloc::vec::Vec<super::ingest::Shard>,
    #[prost(uint64, tag = "4")]
    pub next_shard_id: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SourceType {
    Cli = 0,
    File = 1,
    GcpPubsub = 2,
    IngestV1 = 3,
    IngestV2 = 4,
    Kafka = 5,
    Kinesis = 6,
    Nats = 7,
    Pulsar = 8,
    Vec = 9,
    Void = 10,
}
impl SourceType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            SourceType::Cli => "CLI",
            SourceType::File => "FILE",
            SourceType::GcpPubsub => "GCP_PUBSUB",
            SourceType::IngestV1 => "INGEST_V1",
            SourceType::IngestV2 => "INGEST_V2",
            SourceType::Kafka => "KAFKA",
            SourceType::Kinesis => "KINESIS",
            SourceType::Nats => "NATS",
            SourceType::Pulsar => "PULSAR",
            SourceType::Vec => "VEC",
            SourceType::Void => "VOID",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CLI" => Some(Self::Cli),
            "FILE" => Some(Self::File),
            "GCP_PUBSUB" => Some(Self::GcpPubsub),
            "INGEST_V1" => Some(Self::IngestV1),
            "INGEST_V2" => Some(Self::IngestV2),
            "KAFKA" => Some(Self::Kafka),
            "KINESIS" => Some(Self::Kinesis),
            "NATS" => Some(Self::Nats),
            "PULSAR" => Some(Self::Pulsar),
            "VEC" => Some(Self::Vec),
            "VOID" => Some(Self::Void),
            _ => None,
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CloseShardsFailureKind {
    InvalidArgument = 0,
    NotFound = 1,
}
impl CloseShardsFailureKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CloseShardsFailureKind::InvalidArgument => "INVALID_ARGUMENT",
            CloseShardsFailureKind::NotFound => "NOT_FOUND",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "INVALID_ARGUMENT" => Some(Self::InvalidArgument),
            "NOT_FOUND" => Some(Self::NotFound),
            _ => None,
        }
    }
}
/// BEGIN quickwit-codegen
use tower::{Layer, Service, ServiceExt};
use quickwit_common::metrics::{PrometheusLabels, OwnedPrometheusLabels};
impl PrometheusLabels<1> for CreateIndexRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("create_index")])
    }
}
impl PrometheusLabels<1> for IndexMetadataRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("index_metadata")])
    }
}
impl PrometheusLabels<1> for ListIndexesMetadataRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("list_indexes_metadata")])
    }
}
impl PrometheusLabels<1> for DeleteIndexRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("delete_index")])
    }
}
impl PrometheusLabels<1> for ListAllSplitsRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("list_all_splits")])
    }
}
impl PrometheusLabels<1> for ListSplitsRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("list_splits")])
    }
}
impl PrometheusLabels<1> for StageSplitsRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("stage_splits")])
    }
}
impl PrometheusLabels<1> for PublishSplitsRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("publish_splits")])
    }
}
impl PrometheusLabels<1> for MarkSplitsForDeletionRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([
            std::borrow::Cow::Borrowed("mark_splits_for_deletion"),
        ])
    }
}
impl PrometheusLabels<1> for DeleteSplitsRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("delete_splits")])
    }
}
impl PrometheusLabels<1> for AddSourceRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("add_source")])
    }
}
impl PrometheusLabels<1> for ToggleSourceRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("toggle_source")])
    }
}
impl PrometheusLabels<1> for DeleteSourceRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("delete_source")])
    }
}
impl PrometheusLabels<1> for ResetSourceCheckpointRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([
            std::borrow::Cow::Borrowed("reset_source_checkpoint"),
        ])
    }
}
impl PrometheusLabels<1> for LastDeleteOpstampRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("last_delete_opstamp")])
    }
}
impl PrometheusLabels<1> for DeleteQuery {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("delete_query")])
    }
}
impl PrometheusLabels<1> for UpdateSplitsDeleteOpstampRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([
            std::borrow::Cow::Borrowed("update_splits_delete_opstamp"),
        ])
    }
}
impl PrometheusLabels<1> for ListDeleteTasksRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("list_delete_tasks")])
    }
}
impl PrometheusLabels<1> for ListStaleSplitsRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("list_stale_splits")])
    }
}
impl PrometheusLabels<1> for OpenShardsRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("open_shards")])
    }
}
impl PrometheusLabels<1> for AcquireShardsRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("acquire_shards")])
    }
}
impl PrometheusLabels<1> for CloseShardsRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("close_shards")])
    }
}
impl PrometheusLabels<1> for DeleteShardsRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("delete_shards")])
    }
}
impl PrometheusLabels<1> for ListShardsRequest {
    fn labels(&self) -> OwnedPrometheusLabels<1usize> {
        OwnedPrometheusLabels::new([std::borrow::Cow::Borrowed("list_shards")])
    }
}
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait MetastoreService: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    /// Creates an index.
    async fn create_index(
        &mut self,
        request: CreateIndexRequest,
    ) -> crate::metastore::MetastoreResult<CreateIndexResponse>;
    /// Gets an index metadata.
    async fn index_metadata(
        &mut self,
        request: IndexMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse>;
    /// Gets an indexes metadatas.
    async fn list_indexes_metadatas(
        &mut self,
        request: ListIndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexesMetadataResponse>;
    /// Deletes an index
    async fn delete_index(
        &mut self,
        request: DeleteIndexRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Gets all splits from index.
    async fn list_all_splits(
        &mut self,
        request: ListAllSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse>;
    /// Gets splits from index.
    async fn list_splits(
        &mut self,
        request: ListSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse>;
    /// Stages several splits.
    async fn stage_splits(
        &mut self,
        request: StageSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Publishes split.
    async fn publish_splits(
        &mut self,
        request: PublishSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Marks splits for deletion.
    async fn mark_splits_for_deletion(
        &mut self,
        request: MarkSplitsForDeletionRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Deletes splits.
    async fn delete_splits(
        &mut self,
        request: DeleteSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Adds source.
    async fn add_source(
        &mut self,
        request: AddSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Toggles source.
    async fn toggle_source(
        &mut self,
        request: ToggleSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Removes source.
    async fn delete_source(
        &mut self,
        request: DeleteSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Resets source checkpoint.
    async fn reset_source_checkpoint(
        &mut self,
        request: ResetSourceCheckpointRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse>;
    /// Gets last opstamp for a given `index_id`.
    async fn last_delete_opstamp(
        &mut self,
        request: LastDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<LastDeleteOpstampResponse>;
    /// Creates a delete task.
    async fn create_delete_task(
        &mut self,
        request: DeleteQuery,
    ) -> crate::metastore::MetastoreResult<DeleteTask>;
    /// Updates splits `delete_opstamp`.
    async fn update_splits_delete_opstamp(
        &mut self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<UpdateSplitsDeleteOpstampResponse>;
    /// Lists delete tasks with `delete_task.opstamp` > `opstamp_start` for a given `index_id`.
    async fn list_delete_tasks(
        &mut self,
        request: ListDeleteTasksRequest,
    ) -> crate::metastore::MetastoreResult<ListDeleteTasksResponse>;
    #[doc = "/ Lists splits with `split.delete_opstamp` < `delete_opstamp` for a given `index_id`."]
    async fn list_stale_splits(
        &mut self,
        request: ListStaleSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse>;
    async fn open_shards(
        &mut self,
        request: OpenShardsRequest,
    ) -> crate::metastore::MetastoreResult<OpenShardsResponse>;
    /// Acquires a set of shards for indexing. This RPC locks the shards for publishing thanks to a publish token and only
    /// the last indexer that has acquired the shards is allowed to publish. The response returns for each subrequest the
    /// list of acquired shards along with the positions to index from.
    async fn acquire_shards(
        &mut self,
        request: AcquireShardsRequest,
    ) -> crate::metastore::MetastoreResult<AcquireShardsResponse>;
    async fn close_shards(
        &mut self,
        request: CloseShardsRequest,
    ) -> crate::metastore::MetastoreResult<CloseShardsResponse>;
    async fn delete_shards(
        &mut self,
        request: DeleteShardsRequest,
    ) -> crate::metastore::MetastoreResult<DeleteShardsResponse>;
    async fn list_shards(
        &mut self,
        request: ListShardsRequest,
    ) -> crate::metastore::MetastoreResult<ListShardsResponse>;
    async fn check_connectivity(&mut self) -> anyhow::Result<()>;
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri>;
}
dyn_clone::clone_trait_object!(MetastoreService);
#[cfg(any(test, feature = "testsuite"))]
impl Clone for MockMetastoreService {
    fn clone(&self) -> Self {
        MockMetastoreService::new()
    }
}
#[derive(Debug, Clone)]
pub struct MetastoreServiceClient {
    inner: Box<dyn MetastoreService>,
}
impl MetastoreServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: MetastoreService,
    {
        #[cfg(any(test, feature = "testsuite"))]
        assert!(
            std::any::TypeId::of:: < T > () != std::any::TypeId::of:: <
            MockMetastoreService > (),
            "`MockMetastoreService` must be wrapped in a `MockMetastoreServiceWrapper`. Use `MockMetastoreService::from(mock)` to instantiate the client."
        );
        Self { inner: Box::new(instance) }
    }
    pub fn as_grpc_service(
        &self,
    ) -> metastore_service_grpc_server::MetastoreServiceGrpcServer<
        MetastoreServiceGrpcServerAdapter,
    > {
        let adapter = MetastoreServiceGrpcServerAdapter::new(self.clone());
        metastore_service_grpc_server::MetastoreServiceGrpcServer::new(adapter)
    }
    pub fn from_channel(
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
    ) -> Self {
        let (_, connection_keys_watcher) = tokio::sync::watch::channel(
            std::collections::HashSet::from_iter([addr]),
        );
        let adapter = MetastoreServiceGrpcClientAdapter::new(
            metastore_service_grpc_client::MetastoreServiceGrpcClient::new(channel),
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_balance_channel(
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
    ) -> MetastoreServiceClient {
        let connection_keys_watcher = balance_channel.connection_keys_watcher();
        let adapter = MetastoreServiceGrpcClientAdapter::new(
            metastore_service_grpc_client::MetastoreServiceGrpcClient::new(
                balance_channel,
            ),
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        MetastoreServiceMailbox<A>: MetastoreService,
    {
        MetastoreServiceClient::new(MetastoreServiceMailbox::new(mailbox))
    }
    pub fn tower() -> MetastoreServiceTowerBlockBuilder {
        MetastoreServiceTowerBlockBuilder::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mock() -> MockMetastoreService {
        MockMetastoreService::new()
    }
}
#[async_trait::async_trait]
impl MetastoreService for MetastoreServiceClient {
    async fn create_index(
        &mut self,
        request: CreateIndexRequest,
    ) -> crate::metastore::MetastoreResult<CreateIndexResponse> {
        self.inner.create_index(request).await
    }
    async fn index_metadata(
        &mut self,
        request: IndexMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse> {
        self.inner.index_metadata(request).await
    }
    async fn list_indexes_metadatas(
        &mut self,
        request: ListIndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexesMetadataResponse> {
        self.inner.list_indexes_metadatas(request).await
    }
    async fn delete_index(
        &mut self,
        request: DeleteIndexRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.delete_index(request).await
    }
    async fn list_all_splits(
        &mut self,
        request: ListAllSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.inner.list_all_splits(request).await
    }
    async fn list_splits(
        &mut self,
        request: ListSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.inner.list_splits(request).await
    }
    async fn stage_splits(
        &mut self,
        request: StageSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.stage_splits(request).await
    }
    async fn publish_splits(
        &mut self,
        request: PublishSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.publish_splits(request).await
    }
    async fn mark_splits_for_deletion(
        &mut self,
        request: MarkSplitsForDeletionRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.mark_splits_for_deletion(request).await
    }
    async fn delete_splits(
        &mut self,
        request: DeleteSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.delete_splits(request).await
    }
    async fn add_source(
        &mut self,
        request: AddSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.add_source(request).await
    }
    async fn toggle_source(
        &mut self,
        request: ToggleSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.toggle_source(request).await
    }
    async fn delete_source(
        &mut self,
        request: DeleteSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.delete_source(request).await
    }
    async fn reset_source_checkpoint(
        &mut self,
        request: ResetSourceCheckpointRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner.reset_source_checkpoint(request).await
    }
    async fn last_delete_opstamp(
        &mut self,
        request: LastDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<LastDeleteOpstampResponse> {
        self.inner.last_delete_opstamp(request).await
    }
    async fn create_delete_task(
        &mut self,
        request: DeleteQuery,
    ) -> crate::metastore::MetastoreResult<DeleteTask> {
        self.inner.create_delete_task(request).await
    }
    async fn update_splits_delete_opstamp(
        &mut self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        self.inner.update_splits_delete_opstamp(request).await
    }
    async fn list_delete_tasks(
        &mut self,
        request: ListDeleteTasksRequest,
    ) -> crate::metastore::MetastoreResult<ListDeleteTasksResponse> {
        self.inner.list_delete_tasks(request).await
    }
    async fn list_stale_splits(
        &mut self,
        request: ListStaleSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.inner.list_stale_splits(request).await
    }
    async fn open_shards(
        &mut self,
        request: OpenShardsRequest,
    ) -> crate::metastore::MetastoreResult<OpenShardsResponse> {
        self.inner.open_shards(request).await
    }
    async fn acquire_shards(
        &mut self,
        request: AcquireShardsRequest,
    ) -> crate::metastore::MetastoreResult<AcquireShardsResponse> {
        self.inner.acquire_shards(request).await
    }
    async fn close_shards(
        &mut self,
        request: CloseShardsRequest,
    ) -> crate::metastore::MetastoreResult<CloseShardsResponse> {
        self.inner.close_shards(request).await
    }
    async fn delete_shards(
        &mut self,
        request: DeleteShardsRequest,
    ) -> crate::metastore::MetastoreResult<DeleteShardsResponse> {
        self.inner.delete_shards(request).await
    }
    async fn list_shards(
        &mut self,
        request: ListShardsRequest,
    ) -> crate::metastore::MetastoreResult<ListShardsResponse> {
        self.inner.list_shards(request).await
    }
    async fn check_connectivity(&mut self) -> anyhow::Result<()> {
        self.inner.check_connectivity().await
    }
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        self.inner.endpoints()
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod metastore_service_mock {
    use super::*;
    #[derive(Debug, Clone)]
    struct MockMetastoreServiceWrapper {
        inner: std::sync::Arc<tokio::sync::Mutex<MockMetastoreService>>,
    }
    #[async_trait::async_trait]
    impl MetastoreService for MockMetastoreServiceWrapper {
        async fn create_index(
            &mut self,
            request: super::CreateIndexRequest,
        ) -> crate::metastore::MetastoreResult<super::CreateIndexResponse> {
            self.inner.lock().await.create_index(request).await
        }
        async fn index_metadata(
            &mut self,
            request: super::IndexMetadataRequest,
        ) -> crate::metastore::MetastoreResult<super::IndexMetadataResponse> {
            self.inner.lock().await.index_metadata(request).await
        }
        async fn list_indexes_metadatas(
            &mut self,
            request: super::ListIndexesMetadataRequest,
        ) -> crate::metastore::MetastoreResult<super::ListIndexesMetadataResponse> {
            self.inner.lock().await.list_indexes_metadatas(request).await
        }
        async fn delete_index(
            &mut self,
            request: super::DeleteIndexRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.delete_index(request).await
        }
        async fn list_all_splits(
            &mut self,
            request: super::ListAllSplitsRequest,
        ) -> crate::metastore::MetastoreResult<super::ListSplitsResponse> {
            self.inner.lock().await.list_all_splits(request).await
        }
        async fn list_splits(
            &mut self,
            request: super::ListSplitsRequest,
        ) -> crate::metastore::MetastoreResult<super::ListSplitsResponse> {
            self.inner.lock().await.list_splits(request).await
        }
        async fn stage_splits(
            &mut self,
            request: super::StageSplitsRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.stage_splits(request).await
        }
        async fn publish_splits(
            &mut self,
            request: super::PublishSplitsRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.publish_splits(request).await
        }
        async fn mark_splits_for_deletion(
            &mut self,
            request: super::MarkSplitsForDeletionRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.mark_splits_for_deletion(request).await
        }
        async fn delete_splits(
            &mut self,
            request: super::DeleteSplitsRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.delete_splits(request).await
        }
        async fn add_source(
            &mut self,
            request: super::AddSourceRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.add_source(request).await
        }
        async fn toggle_source(
            &mut self,
            request: super::ToggleSourceRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.toggle_source(request).await
        }
        async fn delete_source(
            &mut self,
            request: super::DeleteSourceRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.delete_source(request).await
        }
        async fn reset_source_checkpoint(
            &mut self,
            request: super::ResetSourceCheckpointRequest,
        ) -> crate::metastore::MetastoreResult<super::EmptyResponse> {
            self.inner.lock().await.reset_source_checkpoint(request).await
        }
        async fn last_delete_opstamp(
            &mut self,
            request: super::LastDeleteOpstampRequest,
        ) -> crate::metastore::MetastoreResult<super::LastDeleteOpstampResponse> {
            self.inner.lock().await.last_delete_opstamp(request).await
        }
        async fn create_delete_task(
            &mut self,
            request: super::DeleteQuery,
        ) -> crate::metastore::MetastoreResult<super::DeleteTask> {
            self.inner.lock().await.create_delete_task(request).await
        }
        async fn update_splits_delete_opstamp(
            &mut self,
            request: super::UpdateSplitsDeleteOpstampRequest,
        ) -> crate::metastore::MetastoreResult<
            super::UpdateSplitsDeleteOpstampResponse,
        > {
            self.inner.lock().await.update_splits_delete_opstamp(request).await
        }
        async fn list_delete_tasks(
            &mut self,
            request: super::ListDeleteTasksRequest,
        ) -> crate::metastore::MetastoreResult<super::ListDeleteTasksResponse> {
            self.inner.lock().await.list_delete_tasks(request).await
        }
        async fn list_stale_splits(
            &mut self,
            request: super::ListStaleSplitsRequest,
        ) -> crate::metastore::MetastoreResult<super::ListSplitsResponse> {
            self.inner.lock().await.list_stale_splits(request).await
        }
        async fn open_shards(
            &mut self,
            request: super::OpenShardsRequest,
        ) -> crate::metastore::MetastoreResult<super::OpenShardsResponse> {
            self.inner.lock().await.open_shards(request).await
        }
        async fn acquire_shards(
            &mut self,
            request: super::AcquireShardsRequest,
        ) -> crate::metastore::MetastoreResult<super::AcquireShardsResponse> {
            self.inner.lock().await.acquire_shards(request).await
        }
        async fn close_shards(
            &mut self,
            request: super::CloseShardsRequest,
        ) -> crate::metastore::MetastoreResult<super::CloseShardsResponse> {
            self.inner.lock().await.close_shards(request).await
        }
        async fn delete_shards(
            &mut self,
            request: super::DeleteShardsRequest,
        ) -> crate::metastore::MetastoreResult<super::DeleteShardsResponse> {
            self.inner.lock().await.delete_shards(request).await
        }
        async fn list_shards(
            &mut self,
            request: super::ListShardsRequest,
        ) -> crate::metastore::MetastoreResult<super::ListShardsResponse> {
            self.inner.lock().await.list_shards(request).await
        }
        async fn check_connectivity(&mut self) -> anyhow::Result<()> {
            self.inner.lock().await.check_connectivity().await
        }
        fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
            futures::executor::block_on(self.inner.lock()).endpoints()
        }
    }
    impl From<MockMetastoreService> for MetastoreServiceClient {
        fn from(mock: MockMetastoreService) -> Self {
            let mock_wrapper = MockMetastoreServiceWrapper {
                inner: std::sync::Arc::new(tokio::sync::Mutex::new(mock)),
            };
            MetastoreServiceClient::new(mock_wrapper)
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<CreateIndexRequest> for Box<dyn MetastoreService> {
    type Response = CreateIndexResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: CreateIndexRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.create_index(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<IndexMetadataRequest> for Box<dyn MetastoreService> {
    type Response = IndexMetadataResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: IndexMetadataRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.index_metadata(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListIndexesMetadataRequest> for Box<dyn MetastoreService> {
    type Response = ListIndexesMetadataResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListIndexesMetadataRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.list_indexes_metadatas(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<DeleteIndexRequest> for Box<dyn MetastoreService> {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: DeleteIndexRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.delete_index(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListAllSplitsRequest> for Box<dyn MetastoreService> {
    type Response = ListSplitsResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListAllSplitsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.list_all_splits(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListSplitsRequest> for Box<dyn MetastoreService> {
    type Response = ListSplitsResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListSplitsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.list_splits(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<StageSplitsRequest> for Box<dyn MetastoreService> {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: StageSplitsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.stage_splits(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<PublishSplitsRequest> for Box<dyn MetastoreService> {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: PublishSplitsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.publish_splits(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<MarkSplitsForDeletionRequest> for Box<dyn MetastoreService> {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: MarkSplitsForDeletionRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.mark_splits_for_deletion(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<DeleteSplitsRequest> for Box<dyn MetastoreService> {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: DeleteSplitsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.delete_splits(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<AddSourceRequest> for Box<dyn MetastoreService> {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: AddSourceRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.add_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ToggleSourceRequest> for Box<dyn MetastoreService> {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ToggleSourceRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.toggle_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<DeleteSourceRequest> for Box<dyn MetastoreService> {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: DeleteSourceRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.delete_source(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ResetSourceCheckpointRequest> for Box<dyn MetastoreService> {
    type Response = EmptyResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ResetSourceCheckpointRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.reset_source_checkpoint(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<LastDeleteOpstampRequest> for Box<dyn MetastoreService> {
    type Response = LastDeleteOpstampResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: LastDeleteOpstampRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.last_delete_opstamp(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<DeleteQuery> for Box<dyn MetastoreService> {
    type Response = DeleteTask;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: DeleteQuery) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.create_delete_task(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<UpdateSplitsDeleteOpstampRequest> for Box<dyn MetastoreService> {
    type Response = UpdateSplitsDeleteOpstampResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: UpdateSplitsDeleteOpstampRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.update_splits_delete_opstamp(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListDeleteTasksRequest> for Box<dyn MetastoreService> {
    type Response = ListDeleteTasksResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListDeleteTasksRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.list_delete_tasks(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListStaleSplitsRequest> for Box<dyn MetastoreService> {
    type Response = ListSplitsResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListStaleSplitsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.list_stale_splits(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<OpenShardsRequest> for Box<dyn MetastoreService> {
    type Response = OpenShardsResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: OpenShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.open_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<AcquireShardsRequest> for Box<dyn MetastoreService> {
    type Response = AcquireShardsResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: AcquireShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.acquire_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<CloseShardsRequest> for Box<dyn MetastoreService> {
    type Response = CloseShardsResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: CloseShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.close_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<DeleteShardsRequest> for Box<dyn MetastoreService> {
    type Response = DeleteShardsResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: DeleteShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.delete_shards(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<ListShardsRequest> for Box<dyn MetastoreService> {
    type Response = ListShardsResponse;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ListShardsRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.list_shards(request).await };
        Box::pin(fut)
    }
}
/// A tower block is a set of towers. Each tower is stack of layers (middlewares) that are applied to a service.
#[derive(Debug)]
struct MetastoreServiceTowerBlock {
    inner: Box<dyn MetastoreService>,
    create_index_svc: quickwit_common::tower::BoxService<
        CreateIndexRequest,
        CreateIndexResponse,
        crate::metastore::MetastoreError,
    >,
    index_metadata_svc: quickwit_common::tower::BoxService<
        IndexMetadataRequest,
        IndexMetadataResponse,
        crate::metastore::MetastoreError,
    >,
    list_indexes_metadatas_svc: quickwit_common::tower::BoxService<
        ListIndexesMetadataRequest,
        ListIndexesMetadataResponse,
        crate::metastore::MetastoreError,
    >,
    delete_index_svc: quickwit_common::tower::BoxService<
        DeleteIndexRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    list_all_splits_svc: quickwit_common::tower::BoxService<
        ListAllSplitsRequest,
        ListSplitsResponse,
        crate::metastore::MetastoreError,
    >,
    list_splits_svc: quickwit_common::tower::BoxService<
        ListSplitsRequest,
        ListSplitsResponse,
        crate::metastore::MetastoreError,
    >,
    stage_splits_svc: quickwit_common::tower::BoxService<
        StageSplitsRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    publish_splits_svc: quickwit_common::tower::BoxService<
        PublishSplitsRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    mark_splits_for_deletion_svc: quickwit_common::tower::BoxService<
        MarkSplitsForDeletionRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    delete_splits_svc: quickwit_common::tower::BoxService<
        DeleteSplitsRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    add_source_svc: quickwit_common::tower::BoxService<
        AddSourceRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    toggle_source_svc: quickwit_common::tower::BoxService<
        ToggleSourceRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    delete_source_svc: quickwit_common::tower::BoxService<
        DeleteSourceRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    reset_source_checkpoint_svc: quickwit_common::tower::BoxService<
        ResetSourceCheckpointRequest,
        EmptyResponse,
        crate::metastore::MetastoreError,
    >,
    last_delete_opstamp_svc: quickwit_common::tower::BoxService<
        LastDeleteOpstampRequest,
        LastDeleteOpstampResponse,
        crate::metastore::MetastoreError,
    >,
    create_delete_task_svc: quickwit_common::tower::BoxService<
        DeleteQuery,
        DeleteTask,
        crate::metastore::MetastoreError,
    >,
    update_splits_delete_opstamp_svc: quickwit_common::tower::BoxService<
        UpdateSplitsDeleteOpstampRequest,
        UpdateSplitsDeleteOpstampResponse,
        crate::metastore::MetastoreError,
    >,
    list_delete_tasks_svc: quickwit_common::tower::BoxService<
        ListDeleteTasksRequest,
        ListDeleteTasksResponse,
        crate::metastore::MetastoreError,
    >,
    list_stale_splits_svc: quickwit_common::tower::BoxService<
        ListStaleSplitsRequest,
        ListSplitsResponse,
        crate::metastore::MetastoreError,
    >,
    open_shards_svc: quickwit_common::tower::BoxService<
        OpenShardsRequest,
        OpenShardsResponse,
        crate::metastore::MetastoreError,
    >,
    acquire_shards_svc: quickwit_common::tower::BoxService<
        AcquireShardsRequest,
        AcquireShardsResponse,
        crate::metastore::MetastoreError,
    >,
    close_shards_svc: quickwit_common::tower::BoxService<
        CloseShardsRequest,
        CloseShardsResponse,
        crate::metastore::MetastoreError,
    >,
    delete_shards_svc: quickwit_common::tower::BoxService<
        DeleteShardsRequest,
        DeleteShardsResponse,
        crate::metastore::MetastoreError,
    >,
    list_shards_svc: quickwit_common::tower::BoxService<
        ListShardsRequest,
        ListShardsResponse,
        crate::metastore::MetastoreError,
    >,
}
impl Clone for MetastoreServiceTowerBlock {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            create_index_svc: self.create_index_svc.clone(),
            index_metadata_svc: self.index_metadata_svc.clone(),
            list_indexes_metadatas_svc: self.list_indexes_metadatas_svc.clone(),
            delete_index_svc: self.delete_index_svc.clone(),
            list_all_splits_svc: self.list_all_splits_svc.clone(),
            list_splits_svc: self.list_splits_svc.clone(),
            stage_splits_svc: self.stage_splits_svc.clone(),
            publish_splits_svc: self.publish_splits_svc.clone(),
            mark_splits_for_deletion_svc: self.mark_splits_for_deletion_svc.clone(),
            delete_splits_svc: self.delete_splits_svc.clone(),
            add_source_svc: self.add_source_svc.clone(),
            toggle_source_svc: self.toggle_source_svc.clone(),
            delete_source_svc: self.delete_source_svc.clone(),
            reset_source_checkpoint_svc: self.reset_source_checkpoint_svc.clone(),
            last_delete_opstamp_svc: self.last_delete_opstamp_svc.clone(),
            create_delete_task_svc: self.create_delete_task_svc.clone(),
            update_splits_delete_opstamp_svc: self
                .update_splits_delete_opstamp_svc
                .clone(),
            list_delete_tasks_svc: self.list_delete_tasks_svc.clone(),
            list_stale_splits_svc: self.list_stale_splits_svc.clone(),
            open_shards_svc: self.open_shards_svc.clone(),
            acquire_shards_svc: self.acquire_shards_svc.clone(),
            close_shards_svc: self.close_shards_svc.clone(),
            delete_shards_svc: self.delete_shards_svc.clone(),
            list_shards_svc: self.list_shards_svc.clone(),
        }
    }
}
#[async_trait::async_trait]
impl MetastoreService for MetastoreServiceTowerBlock {
    async fn create_index(
        &mut self,
        request: CreateIndexRequest,
    ) -> crate::metastore::MetastoreResult<CreateIndexResponse> {
        self.create_index_svc.ready().await?.call(request).await
    }
    async fn index_metadata(
        &mut self,
        request: IndexMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse> {
        self.index_metadata_svc.ready().await?.call(request).await
    }
    async fn list_indexes_metadatas(
        &mut self,
        request: ListIndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexesMetadataResponse> {
        self.list_indexes_metadatas_svc.ready().await?.call(request).await
    }
    async fn delete_index(
        &mut self,
        request: DeleteIndexRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.delete_index_svc.ready().await?.call(request).await
    }
    async fn list_all_splits(
        &mut self,
        request: ListAllSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.list_all_splits_svc.ready().await?.call(request).await
    }
    async fn list_splits(
        &mut self,
        request: ListSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.list_splits_svc.ready().await?.call(request).await
    }
    async fn stage_splits(
        &mut self,
        request: StageSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.stage_splits_svc.ready().await?.call(request).await
    }
    async fn publish_splits(
        &mut self,
        request: PublishSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.publish_splits_svc.ready().await?.call(request).await
    }
    async fn mark_splits_for_deletion(
        &mut self,
        request: MarkSplitsForDeletionRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.mark_splits_for_deletion_svc.ready().await?.call(request).await
    }
    async fn delete_splits(
        &mut self,
        request: DeleteSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.delete_splits_svc.ready().await?.call(request).await
    }
    async fn add_source(
        &mut self,
        request: AddSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.add_source_svc.ready().await?.call(request).await
    }
    async fn toggle_source(
        &mut self,
        request: ToggleSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.toggle_source_svc.ready().await?.call(request).await
    }
    async fn delete_source(
        &mut self,
        request: DeleteSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.delete_source_svc.ready().await?.call(request).await
    }
    async fn reset_source_checkpoint(
        &mut self,
        request: ResetSourceCheckpointRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.reset_source_checkpoint_svc.ready().await?.call(request).await
    }
    async fn last_delete_opstamp(
        &mut self,
        request: LastDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<LastDeleteOpstampResponse> {
        self.last_delete_opstamp_svc.ready().await?.call(request).await
    }
    async fn create_delete_task(
        &mut self,
        request: DeleteQuery,
    ) -> crate::metastore::MetastoreResult<DeleteTask> {
        self.create_delete_task_svc.ready().await?.call(request).await
    }
    async fn update_splits_delete_opstamp(
        &mut self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        self.update_splits_delete_opstamp_svc.ready().await?.call(request).await
    }
    async fn list_delete_tasks(
        &mut self,
        request: ListDeleteTasksRequest,
    ) -> crate::metastore::MetastoreResult<ListDeleteTasksResponse> {
        self.list_delete_tasks_svc.ready().await?.call(request).await
    }
    async fn list_stale_splits(
        &mut self,
        request: ListStaleSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.list_stale_splits_svc.ready().await?.call(request).await
    }
    async fn open_shards(
        &mut self,
        request: OpenShardsRequest,
    ) -> crate::metastore::MetastoreResult<OpenShardsResponse> {
        self.open_shards_svc.ready().await?.call(request).await
    }
    async fn acquire_shards(
        &mut self,
        request: AcquireShardsRequest,
    ) -> crate::metastore::MetastoreResult<AcquireShardsResponse> {
        self.acquire_shards_svc.ready().await?.call(request).await
    }
    async fn close_shards(
        &mut self,
        request: CloseShardsRequest,
    ) -> crate::metastore::MetastoreResult<CloseShardsResponse> {
        self.close_shards_svc.ready().await?.call(request).await
    }
    async fn delete_shards(
        &mut self,
        request: DeleteShardsRequest,
    ) -> crate::metastore::MetastoreResult<DeleteShardsResponse> {
        self.delete_shards_svc.ready().await?.call(request).await
    }
    async fn list_shards(
        &mut self,
        request: ListShardsRequest,
    ) -> crate::metastore::MetastoreResult<ListShardsResponse> {
        self.list_shards_svc.ready().await?.call(request).await
    }
    async fn check_connectivity(&mut self) -> anyhow::Result<()> {
        self.inner.check_connectivity().await
    }
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        self.inner.endpoints()
    }
}
#[derive(Debug, Default)]
pub struct MetastoreServiceTowerBlockBuilder {
    #[allow(clippy::type_complexity)]
    create_index_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            CreateIndexRequest,
            CreateIndexResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    index_metadata_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            IndexMetadataRequest,
            IndexMetadataResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    list_indexes_metadatas_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            ListIndexesMetadataRequest,
            ListIndexesMetadataResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    delete_index_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            DeleteIndexRequest,
            EmptyResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    list_all_splits_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            ListAllSplitsRequest,
            ListSplitsResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    list_splits_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            ListSplitsRequest,
            ListSplitsResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    stage_splits_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            StageSplitsRequest,
            EmptyResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    publish_splits_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            PublishSplitsRequest,
            EmptyResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    mark_splits_for_deletion_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            MarkSplitsForDeletionRequest,
            EmptyResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    delete_splits_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            DeleteSplitsRequest,
            EmptyResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    add_source_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            AddSourceRequest,
            EmptyResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    toggle_source_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            ToggleSourceRequest,
            EmptyResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    delete_source_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            DeleteSourceRequest,
            EmptyResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    reset_source_checkpoint_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            ResetSourceCheckpointRequest,
            EmptyResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    last_delete_opstamp_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            LastDeleteOpstampRequest,
            LastDeleteOpstampResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    create_delete_task_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            DeleteQuery,
            DeleteTask,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    update_splits_delete_opstamp_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            UpdateSplitsDeleteOpstampRequest,
            UpdateSplitsDeleteOpstampResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    list_delete_tasks_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            ListDeleteTasksRequest,
            ListDeleteTasksResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    list_stale_splits_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            ListStaleSplitsRequest,
            ListSplitsResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    open_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            OpenShardsRequest,
            OpenShardsResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    acquire_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            AcquireShardsRequest,
            AcquireShardsResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    close_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            CloseShardsRequest,
            CloseShardsResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    delete_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            DeleteShardsRequest,
            DeleteShardsResponse,
            crate::metastore::MetastoreError,
        >,
    >,
    #[allow(clippy::type_complexity)]
    list_shards_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn MetastoreService>,
            ListShardsRequest,
            ListShardsResponse,
            crate::metastore::MetastoreError,
        >,
    >,
}
impl MetastoreServiceTowerBlockBuilder {
    pub fn shared_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Clone + Send + Sync + 'static,
        L::Service: tower::Service<
                CreateIndexRequest,
                Response = CreateIndexResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<CreateIndexRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                IndexMetadataRequest,
                Response = IndexMetadataResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<IndexMetadataRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                ListIndexesMetadataRequest,
                Response = ListIndexesMetadataResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            ListIndexesMetadataRequest,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                DeleteIndexRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteIndexRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                ListAllSplitsRequest,
                Response = ListSplitsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListAllSplitsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                ListSplitsRequest,
                Response = ListSplitsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListSplitsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                StageSplitsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<StageSplitsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                PublishSplitsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<PublishSplitsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                MarkSplitsForDeletionRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            MarkSplitsForDeletionRequest,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                DeleteSplitsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteSplitsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                AddSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<AddSourceRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                ToggleSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ToggleSourceRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                DeleteSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteSourceRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                ResetSourceCheckpointRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            ResetSourceCheckpointRequest,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                LastDeleteOpstampRequest,
                Response = LastDeleteOpstampResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<LastDeleteOpstampRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                DeleteQuery,
                Response = DeleteTask,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteQuery>>::Future: Send + 'static,
        L::Service: tower::Service<
                UpdateSplitsDeleteOpstampRequest,
                Response = UpdateSplitsDeleteOpstampResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            UpdateSplitsDeleteOpstampRequest,
        >>::Future: Send + 'static,
        L::Service: tower::Service<
                ListDeleteTasksRequest,
                Response = ListDeleteTasksResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListDeleteTasksRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                ListStaleSplitsRequest,
                Response = ListSplitsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListStaleSplitsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                OpenShardsRequest,
                Response = OpenShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<OpenShardsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                AcquireShardsRequest,
                Response = AcquireShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<AcquireShardsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                CloseShardsRequest,
                Response = CloseShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<CloseShardsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                DeleteShardsRequest,
                Response = DeleteShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteShardsRequest>>::Future: Send + 'static,
        L::Service: tower::Service<
                ListShardsRequest,
                Response = ListShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListShardsRequest>>::Future: Send + 'static,
    {
        self
            .create_index_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .index_metadata_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .list_indexes_metadatas_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .delete_index_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .list_all_splits_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .list_splits_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .stage_splits_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .publish_splits_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .mark_splits_for_deletion_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .delete_splits_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .add_source_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .toggle_source_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .delete_source_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .reset_source_checkpoint_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .last_delete_opstamp_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .create_delete_task_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .update_splits_delete_opstamp_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .list_delete_tasks_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .list_stale_splits_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .open_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .acquire_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .close_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self
            .delete_shards_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer.clone()),
        );
        self.list_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn create_index_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                CreateIndexRequest,
                Response = CreateIndexResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<CreateIndexRequest>>::Future: Send + 'static,
    {
        self.create_index_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn index_metadata_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                IndexMetadataRequest,
                Response = IndexMetadataResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<IndexMetadataRequest>>::Future: Send + 'static,
    {
        self.index_metadata_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn list_indexes_metadatas_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                ListIndexesMetadataRequest,
                Response = ListIndexesMetadataResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            ListIndexesMetadataRequest,
        >>::Future: Send + 'static,
    {
        self
            .list_indexes_metadatas_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn delete_index_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                DeleteIndexRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteIndexRequest>>::Future: Send + 'static,
    {
        self.delete_index_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn list_all_splits_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                ListAllSplitsRequest,
                Response = ListSplitsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListAllSplitsRequest>>::Future: Send + 'static,
    {
        self.list_all_splits_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn list_splits_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                ListSplitsRequest,
                Response = ListSplitsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListSplitsRequest>>::Future: Send + 'static,
    {
        self.list_splits_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn stage_splits_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                StageSplitsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<StageSplitsRequest>>::Future: Send + 'static,
    {
        self.stage_splits_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn publish_splits_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                PublishSplitsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<PublishSplitsRequest>>::Future: Send + 'static,
    {
        self.publish_splits_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn mark_splits_for_deletion_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                MarkSplitsForDeletionRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            MarkSplitsForDeletionRequest,
        >>::Future: Send + 'static,
    {
        self
            .mark_splits_for_deletion_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn delete_splits_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                DeleteSplitsRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteSplitsRequest>>::Future: Send + 'static,
    {
        self.delete_splits_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn add_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                AddSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<AddSourceRequest>>::Future: Send + 'static,
    {
        self.add_source_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn toggle_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                ToggleSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ToggleSourceRequest>>::Future: Send + 'static,
    {
        self.toggle_source_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn delete_source_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                DeleteSourceRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteSourceRequest>>::Future: Send + 'static,
    {
        self.delete_source_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn reset_source_checkpoint_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                ResetSourceCheckpointRequest,
                Response = EmptyResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            ResetSourceCheckpointRequest,
        >>::Future: Send + 'static,
    {
        self
            .reset_source_checkpoint_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn last_delete_opstamp_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                LastDeleteOpstampRequest,
                Response = LastDeleteOpstampResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<LastDeleteOpstampRequest>>::Future: Send + 'static,
    {
        self
            .last_delete_opstamp_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn create_delete_task_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                DeleteQuery,
                Response = DeleteTask,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteQuery>>::Future: Send + 'static,
    {
        self
            .create_delete_task_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn update_splits_delete_opstamp_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                UpdateSplitsDeleteOpstampRequest,
                Response = UpdateSplitsDeleteOpstampResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            UpdateSplitsDeleteOpstampRequest,
        >>::Future: Send + 'static,
    {
        self
            .update_splits_delete_opstamp_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn list_delete_tasks_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                ListDeleteTasksRequest,
                Response = ListDeleteTasksResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListDeleteTasksRequest>>::Future: Send + 'static,
    {
        self
            .list_delete_tasks_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn list_stale_splits_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                ListStaleSplitsRequest,
                Response = ListSplitsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListStaleSplitsRequest>>::Future: Send + 'static,
    {
        self
            .list_stale_splits_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn open_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                OpenShardsRequest,
                Response = OpenShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<OpenShardsRequest>>::Future: Send + 'static,
    {
        self.open_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn acquire_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                AcquireShardsRequest,
                Response = AcquireShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<AcquireShardsRequest>>::Future: Send + 'static,
    {
        self.acquire_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn close_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                CloseShardsRequest,
                Response = CloseShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<CloseShardsRequest>>::Future: Send + 'static,
    {
        self.close_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn delete_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                DeleteShardsRequest,
                Response = DeleteShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<DeleteShardsRequest>>::Future: Send + 'static,
    {
        self.delete_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn list_shards_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn MetastoreService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                ListShardsRequest,
                Response = ListShardsResponse,
                Error = crate::metastore::MetastoreError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ListShardsRequest>>::Future: Send + 'static,
    {
        self.list_shards_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn build<T>(self, instance: T) -> MetastoreServiceClient
    where
        T: MetastoreService,
    {
        self.build_from_boxed(Box::new(instance))
    }
    pub fn build_from_channel(
        self,
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
    ) -> MetastoreServiceClient {
        self.build_from_boxed(
            Box::new(MetastoreServiceClient::from_channel(addr, channel)),
        )
    }
    pub fn build_from_balance_channel(
        self,
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
    ) -> MetastoreServiceClient {
        self.build_from_boxed(
            Box::new(MetastoreServiceClient::from_balance_channel(balance_channel)),
        )
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> MetastoreServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        MetastoreServiceMailbox<A>: MetastoreService,
    {
        self.build_from_boxed(Box::new(MetastoreServiceMailbox::new(mailbox)))
    }
    fn build_from_boxed(
        self,
        boxed_instance: Box<dyn MetastoreService>,
    ) -> MetastoreServiceClient {
        let create_index_svc = if let Some(layer) = self.create_index_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let index_metadata_svc = if let Some(layer) = self.index_metadata_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let list_indexes_metadatas_svc = if let Some(layer)
            = self.list_indexes_metadatas_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let delete_index_svc = if let Some(layer) = self.delete_index_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let list_all_splits_svc = if let Some(layer) = self.list_all_splits_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let list_splits_svc = if let Some(layer) = self.list_splits_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let stage_splits_svc = if let Some(layer) = self.stage_splits_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let publish_splits_svc = if let Some(layer) = self.publish_splits_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let mark_splits_for_deletion_svc = if let Some(layer)
            = self.mark_splits_for_deletion_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let delete_splits_svc = if let Some(layer) = self.delete_splits_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let add_source_svc = if let Some(layer) = self.add_source_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let toggle_source_svc = if let Some(layer) = self.toggle_source_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let delete_source_svc = if let Some(layer) = self.delete_source_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let reset_source_checkpoint_svc = if let Some(layer)
            = self.reset_source_checkpoint_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let last_delete_opstamp_svc = if let Some(layer) = self.last_delete_opstamp_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let create_delete_task_svc = if let Some(layer) = self.create_delete_task_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let update_splits_delete_opstamp_svc = if let Some(layer)
            = self.update_splits_delete_opstamp_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let list_delete_tasks_svc = if let Some(layer) = self.list_delete_tasks_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let list_stale_splits_svc = if let Some(layer) = self.list_stale_splits_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let open_shards_svc = if let Some(layer) = self.open_shards_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let acquire_shards_svc = if let Some(layer) = self.acquire_shards_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let close_shards_svc = if let Some(layer) = self.close_shards_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let delete_shards_svc = if let Some(layer) = self.delete_shards_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let list_shards_svc = if let Some(layer) = self.list_shards_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let tower_block = MetastoreServiceTowerBlock {
            inner: boxed_instance.clone(),
            create_index_svc,
            index_metadata_svc,
            list_indexes_metadatas_svc,
            delete_index_svc,
            list_all_splits_svc,
            list_splits_svc,
            stage_splits_svc,
            publish_splits_svc,
            mark_splits_for_deletion_svc,
            delete_splits_svc,
            add_source_svc,
            toggle_source_svc,
            delete_source_svc,
            reset_source_checkpoint_svc,
            last_delete_opstamp_svc,
            create_delete_task_svc,
            update_splits_delete_opstamp_svc,
            list_delete_tasks_svc,
            list_stale_splits_svc,
            open_shards_svc,
            acquire_shards_svc,
            close_shards_svc,
            delete_shards_svc,
            list_shards_svc,
        };
        MetastoreServiceClient::new(tower_block)
    }
}
#[derive(Debug, Clone)]
struct MailboxAdapter<A: quickwit_actors::Actor, E> {
    inner: quickwit_actors::Mailbox<A>,
    phantom: std::marker::PhantomData<E>,
}
impl<A, E> std::ops::Deref for MailboxAdapter<A, E>
where
    A: quickwit_actors::Actor,
{
    type Target = quickwit_actors::Mailbox<A>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
#[derive(Debug)]
pub struct MetastoreServiceMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::metastore::MetastoreError>,
}
impl<A: quickwit_actors::Actor> MetastoreServiceMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for MetastoreServiceMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A, M, T, E> tower::Service<M> for MetastoreServiceMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send
        + 'static,
    M: std::fmt::Debug + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Debug + Send + 'static,
    crate::metastore::MetastoreError: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::metastore::MetastoreError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        //! This does not work with balance middlewares such as `tower::balance::pool::Pool` because
        //! this always returns `Poll::Ready`. The fix is to acquire a permit from the
        //! mailbox in `poll_ready` and consume it in `call`.
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, message: M) -> Self::Future {
        let mailbox = self.inner.clone();
        let fut = async move {
            mailbox.ask_for_res(message).await.map_err(|error| error.into())
        };
        Box::pin(fut)
    }
}
#[async_trait::async_trait]
impl<A> MetastoreService for MetastoreServiceMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug,
    MetastoreServiceMailbox<
        A,
    >: tower::Service<
            CreateIndexRequest,
            Response = CreateIndexResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<CreateIndexResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            IndexMetadataRequest,
            Response = IndexMetadataResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<IndexMetadataResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            ListIndexesMetadataRequest,
            Response = ListIndexesMetadataResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<
                ListIndexesMetadataResponse,
                crate::metastore::MetastoreError,
            >,
        >
        + tower::Service<
            DeleteIndexRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            ListAllSplitsRequest,
            Response = ListSplitsResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<ListSplitsResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            ListSplitsRequest,
            Response = ListSplitsResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<ListSplitsResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            StageSplitsRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            PublishSplitsRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            MarkSplitsForDeletionRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            DeleteSplitsRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            AddSourceRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            ToggleSourceRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            DeleteSourceRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            ResetSourceCheckpointRequest,
            Response = EmptyResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<EmptyResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            LastDeleteOpstampRequest,
            Response = LastDeleteOpstampResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<
                LastDeleteOpstampResponse,
                crate::metastore::MetastoreError,
            >,
        >
        + tower::Service<
            DeleteQuery,
            Response = DeleteTask,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<DeleteTask, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            UpdateSplitsDeleteOpstampRequest,
            Response = UpdateSplitsDeleteOpstampResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<
                UpdateSplitsDeleteOpstampResponse,
                crate::metastore::MetastoreError,
            >,
        >
        + tower::Service<
            ListDeleteTasksRequest,
            Response = ListDeleteTasksResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<ListDeleteTasksResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            ListStaleSplitsRequest,
            Response = ListSplitsResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<ListSplitsResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            OpenShardsRequest,
            Response = OpenShardsResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<OpenShardsResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            AcquireShardsRequest,
            Response = AcquireShardsResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<AcquireShardsResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            CloseShardsRequest,
            Response = CloseShardsResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<CloseShardsResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            DeleteShardsRequest,
            Response = DeleteShardsResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<DeleteShardsResponse, crate::metastore::MetastoreError>,
        >
        + tower::Service<
            ListShardsRequest,
            Response = ListShardsResponse,
            Error = crate::metastore::MetastoreError,
            Future = BoxFuture<ListShardsResponse, crate::metastore::MetastoreError>,
        >,
{
    async fn create_index(
        &mut self,
        request: CreateIndexRequest,
    ) -> crate::metastore::MetastoreResult<CreateIndexResponse> {
        self.call(request).await
    }
    async fn index_metadata(
        &mut self,
        request: IndexMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse> {
        self.call(request).await
    }
    async fn list_indexes_metadatas(
        &mut self,
        request: ListIndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexesMetadataResponse> {
        self.call(request).await
    }
    async fn delete_index(
        &mut self,
        request: DeleteIndexRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.call(request).await
    }
    async fn list_all_splits(
        &mut self,
        request: ListAllSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.call(request).await
    }
    async fn list_splits(
        &mut self,
        request: ListSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.call(request).await
    }
    async fn stage_splits(
        &mut self,
        request: StageSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.call(request).await
    }
    async fn publish_splits(
        &mut self,
        request: PublishSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.call(request).await
    }
    async fn mark_splits_for_deletion(
        &mut self,
        request: MarkSplitsForDeletionRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.call(request).await
    }
    async fn delete_splits(
        &mut self,
        request: DeleteSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.call(request).await
    }
    async fn add_source(
        &mut self,
        request: AddSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.call(request).await
    }
    async fn toggle_source(
        &mut self,
        request: ToggleSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.call(request).await
    }
    async fn delete_source(
        &mut self,
        request: DeleteSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.call(request).await
    }
    async fn reset_source_checkpoint(
        &mut self,
        request: ResetSourceCheckpointRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.call(request).await
    }
    async fn last_delete_opstamp(
        &mut self,
        request: LastDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<LastDeleteOpstampResponse> {
        self.call(request).await
    }
    async fn create_delete_task(
        &mut self,
        request: DeleteQuery,
    ) -> crate::metastore::MetastoreResult<DeleteTask> {
        self.call(request).await
    }
    async fn update_splits_delete_opstamp(
        &mut self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        self.call(request).await
    }
    async fn list_delete_tasks(
        &mut self,
        request: ListDeleteTasksRequest,
    ) -> crate::metastore::MetastoreResult<ListDeleteTasksResponse> {
        self.call(request).await
    }
    async fn list_stale_splits(
        &mut self,
        request: ListStaleSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.call(request).await
    }
    async fn open_shards(
        &mut self,
        request: OpenShardsRequest,
    ) -> crate::metastore::MetastoreResult<OpenShardsResponse> {
        self.call(request).await
    }
    async fn acquire_shards(
        &mut self,
        request: AcquireShardsRequest,
    ) -> crate::metastore::MetastoreResult<AcquireShardsResponse> {
        self.call(request).await
    }
    async fn close_shards(
        &mut self,
        request: CloseShardsRequest,
    ) -> crate::metastore::MetastoreResult<CloseShardsResponse> {
        self.call(request).await
    }
    async fn delete_shards(
        &mut self,
        request: DeleteShardsRequest,
    ) -> crate::metastore::MetastoreResult<DeleteShardsResponse> {
        self.call(request).await
    }
    async fn list_shards(
        &mut self,
        request: ListShardsRequest,
    ) -> crate::metastore::MetastoreResult<ListShardsResponse> {
        self.call(request).await
    }
    async fn check_connectivity(&mut self) -> anyhow::Result<()> {
        if self.inner.is_disconnected() {
            anyhow::bail!("actor `{}` is disconnected", self.inner.actor_instance_id())
        }
        Ok(())
    }
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        vec![
            quickwit_common::uri::Uri::from_well_formed(format!("actor://localhost/{}",
            self.inner.actor_instance_id()))
        ]
    }
}
#[derive(Debug, Clone)]
pub struct MetastoreServiceGrpcClientAdapter<T> {
    inner: T,
    #[allow(dead_code)]
    connection_addrs_rx: tokio::sync::watch::Receiver<
        std::collections::HashSet<std::net::SocketAddr>,
    >,
}
impl<T> MetastoreServiceGrpcClientAdapter<T> {
    pub fn new(
        instance: T,
        connection_addrs_rx: tokio::sync::watch::Receiver<
            std::collections::HashSet<std::net::SocketAddr>,
        >,
    ) -> Self {
        Self {
            inner: instance,
            connection_addrs_rx,
        }
    }
}
#[async_trait::async_trait]
impl<T> MetastoreService
for MetastoreServiceGrpcClientAdapter<
    metastore_service_grpc_client::MetastoreServiceGrpcClient<T>,
>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn create_index(
        &mut self,
        request: CreateIndexRequest,
    ) -> crate::metastore::MetastoreResult<CreateIndexResponse> {
        self.inner
            .create_index(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn index_metadata(
        &mut self,
        request: IndexMetadataRequest,
    ) -> crate::metastore::MetastoreResult<IndexMetadataResponse> {
        self.inner
            .index_metadata(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn list_indexes_metadatas(
        &mut self,
        request: ListIndexesMetadataRequest,
    ) -> crate::metastore::MetastoreResult<ListIndexesMetadataResponse> {
        self.inner
            .list_indexes_metadatas(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn delete_index(
        &mut self,
        request: DeleteIndexRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .delete_index(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn list_all_splits(
        &mut self,
        request: ListAllSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.inner
            .list_all_splits(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn list_splits(
        &mut self,
        request: ListSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.inner
            .list_splits(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn stage_splits(
        &mut self,
        request: StageSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .stage_splits(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn publish_splits(
        &mut self,
        request: PublishSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .publish_splits(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn mark_splits_for_deletion(
        &mut self,
        request: MarkSplitsForDeletionRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .mark_splits_for_deletion(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn delete_splits(
        &mut self,
        request: DeleteSplitsRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .delete_splits(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn add_source(
        &mut self,
        request: AddSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .add_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn toggle_source(
        &mut self,
        request: ToggleSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .toggle_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn delete_source(
        &mut self,
        request: DeleteSourceRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .delete_source(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn reset_source_checkpoint(
        &mut self,
        request: ResetSourceCheckpointRequest,
    ) -> crate::metastore::MetastoreResult<EmptyResponse> {
        self.inner
            .reset_source_checkpoint(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn last_delete_opstamp(
        &mut self,
        request: LastDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<LastDeleteOpstampResponse> {
        self.inner
            .last_delete_opstamp(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn create_delete_task(
        &mut self,
        request: DeleteQuery,
    ) -> crate::metastore::MetastoreResult<DeleteTask> {
        self.inner
            .create_delete_task(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn update_splits_delete_opstamp(
        &mut self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> crate::metastore::MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        self.inner
            .update_splits_delete_opstamp(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn list_delete_tasks(
        &mut self,
        request: ListDeleteTasksRequest,
    ) -> crate::metastore::MetastoreResult<ListDeleteTasksResponse> {
        self.inner
            .list_delete_tasks(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn list_stale_splits(
        &mut self,
        request: ListStaleSplitsRequest,
    ) -> crate::metastore::MetastoreResult<ListSplitsResponse> {
        self.inner
            .list_stale_splits(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn open_shards(
        &mut self,
        request: OpenShardsRequest,
    ) -> crate::metastore::MetastoreResult<OpenShardsResponse> {
        self.inner
            .open_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn acquire_shards(
        &mut self,
        request: AcquireShardsRequest,
    ) -> crate::metastore::MetastoreResult<AcquireShardsResponse> {
        self.inner
            .acquire_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn close_shards(
        &mut self,
        request: CloseShardsRequest,
    ) -> crate::metastore::MetastoreResult<CloseShardsResponse> {
        self.inner
            .close_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn delete_shards(
        &mut self,
        request: DeleteShardsRequest,
    ) -> crate::metastore::MetastoreResult<DeleteShardsResponse> {
        self.inner
            .delete_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn list_shards(
        &mut self,
        request: ListShardsRequest,
    ) -> crate::metastore::MetastoreResult<ListShardsResponse> {
        self.inner
            .list_shards(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn check_connectivity(&mut self) -> anyhow::Result<()> {
        if self.connection_addrs_rx.borrow().len() == 0 {
            anyhow::bail!("no server currently available")
        }
        Ok(())
    }
    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        self.connection_addrs_rx
            .borrow()
            .iter()
            .map(|addr| quickwit_common::uri::Uri::from_well_formed(
                format!(
                    r"grpc://{}/{}.{}", addr, "quickwit.metastore", "MetastoreService"
                ),
            ))
            .collect()
    }
}
#[derive(Debug)]
pub struct MetastoreServiceGrpcServerAdapter {
    inner: Box<dyn MetastoreService>,
}
impl MetastoreServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: MetastoreService,
    {
        Self { inner: Box::new(instance) }
    }
}
#[async_trait::async_trait]
impl metastore_service_grpc_server::MetastoreServiceGrpc
for MetastoreServiceGrpcServerAdapter {
    async fn create_index(
        &self,
        request: tonic::Request<CreateIndexRequest>,
    ) -> Result<tonic::Response<CreateIndexResponse>, tonic::Status> {
        self.inner
            .clone()
            .create_index(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn index_metadata(
        &self,
        request: tonic::Request<IndexMetadataRequest>,
    ) -> Result<tonic::Response<IndexMetadataResponse>, tonic::Status> {
        self.inner
            .clone()
            .index_metadata(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn list_indexes_metadatas(
        &self,
        request: tonic::Request<ListIndexesMetadataRequest>,
    ) -> Result<tonic::Response<ListIndexesMetadataResponse>, tonic::Status> {
        self.inner
            .clone()
            .list_indexes_metadatas(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn delete_index(
        &self,
        request: tonic::Request<DeleteIndexRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .delete_index(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn list_all_splits(
        &self,
        request: tonic::Request<ListAllSplitsRequest>,
    ) -> Result<tonic::Response<ListSplitsResponse>, tonic::Status> {
        self.inner
            .clone()
            .list_all_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn list_splits(
        &self,
        request: tonic::Request<ListSplitsRequest>,
    ) -> Result<tonic::Response<ListSplitsResponse>, tonic::Status> {
        self.inner
            .clone()
            .list_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn stage_splits(
        &self,
        request: tonic::Request<StageSplitsRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .stage_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn publish_splits(
        &self,
        request: tonic::Request<PublishSplitsRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .publish_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn mark_splits_for_deletion(
        &self,
        request: tonic::Request<MarkSplitsForDeletionRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .mark_splits_for_deletion(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn delete_splits(
        &self,
        request: tonic::Request<DeleteSplitsRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .delete_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn add_source(
        &self,
        request: tonic::Request<AddSourceRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .add_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn toggle_source(
        &self,
        request: tonic::Request<ToggleSourceRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .toggle_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn delete_source(
        &self,
        request: tonic::Request<DeleteSourceRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .delete_source(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn reset_source_checkpoint(
        &self,
        request: tonic::Request<ResetSourceCheckpointRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        self.inner
            .clone()
            .reset_source_checkpoint(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn last_delete_opstamp(
        &self,
        request: tonic::Request<LastDeleteOpstampRequest>,
    ) -> Result<tonic::Response<LastDeleteOpstampResponse>, tonic::Status> {
        self.inner
            .clone()
            .last_delete_opstamp(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn create_delete_task(
        &self,
        request: tonic::Request<DeleteQuery>,
    ) -> Result<tonic::Response<DeleteTask>, tonic::Status> {
        self.inner
            .clone()
            .create_delete_task(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn update_splits_delete_opstamp(
        &self,
        request: tonic::Request<UpdateSplitsDeleteOpstampRequest>,
    ) -> Result<tonic::Response<UpdateSplitsDeleteOpstampResponse>, tonic::Status> {
        self.inner
            .clone()
            .update_splits_delete_opstamp(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn list_delete_tasks(
        &self,
        request: tonic::Request<ListDeleteTasksRequest>,
    ) -> Result<tonic::Response<ListDeleteTasksResponse>, tonic::Status> {
        self.inner
            .clone()
            .list_delete_tasks(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn list_stale_splits(
        &self,
        request: tonic::Request<ListStaleSplitsRequest>,
    ) -> Result<tonic::Response<ListSplitsResponse>, tonic::Status> {
        self.inner
            .clone()
            .list_stale_splits(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn open_shards(
        &self,
        request: tonic::Request<OpenShardsRequest>,
    ) -> Result<tonic::Response<OpenShardsResponse>, tonic::Status> {
        self.inner
            .clone()
            .open_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn acquire_shards(
        &self,
        request: tonic::Request<AcquireShardsRequest>,
    ) -> Result<tonic::Response<AcquireShardsResponse>, tonic::Status> {
        self.inner
            .clone()
            .acquire_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn close_shards(
        &self,
        request: tonic::Request<CloseShardsRequest>,
    ) -> Result<tonic::Response<CloseShardsResponse>, tonic::Status> {
        self.inner
            .clone()
            .close_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn delete_shards(
        &self,
        request: tonic::Request<DeleteShardsRequest>,
    ) -> Result<tonic::Response<DeleteShardsResponse>, tonic::Status> {
        self.inner
            .clone()
            .delete_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
    async fn list_shards(
        &self,
        request: tonic::Request<ListShardsRequest>,
    ) -> Result<tonic::Response<ListShardsResponse>, tonic::Status> {
        self.inner
            .clone()
            .list_shards(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
}
/// Generated client implementations.
pub mod metastore_service_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct MetastoreServiceGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MetastoreServiceGrpcClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> MetastoreServiceGrpcClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> MetastoreServiceGrpcClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            MetastoreServiceGrpcClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// Creates an index.
        pub async fn create_index(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateIndexResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/create_index",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "create_index",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Gets an index metadata.
        pub async fn index_metadata(
            &mut self,
            request: impl tonic::IntoRequest<super::IndexMetadataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::IndexMetadataResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/index_metadata",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "index_metadata",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Gets an indexes metadatas.
        pub async fn list_indexes_metadatas(
            &mut self,
            request: impl tonic::IntoRequest<super::ListIndexesMetadataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListIndexesMetadataResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/list_indexes_metadatas",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "list_indexes_metadatas",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Deletes an index
        pub async fn delete_index(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteIndexRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/delete_index",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "delete_index",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Gets all splits from index.
        pub async fn list_all_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::ListAllSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSplitsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/list_all_splits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "list_all_splits",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Gets splits from index.
        pub async fn list_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::ListSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSplitsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/list_splits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "list_splits"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Stages several splits.
        pub async fn stage_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::StageSplitsRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/stage_splits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "stage_splits",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Publishes split.
        pub async fn publish_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::PublishSplitsRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/publish_splits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "publish_splits",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Marks splits for deletion.
        pub async fn mark_splits_for_deletion(
            &mut self,
            request: impl tonic::IntoRequest<super::MarkSplitsForDeletionRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/mark_splits_for_deletion",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "mark_splits_for_deletion",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Deletes splits.
        pub async fn delete_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteSplitsRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/delete_splits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "delete_splits",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Adds source.
        pub async fn add_source(
            &mut self,
            request: impl tonic::IntoRequest<super::AddSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/add_source",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "add_source"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Toggles source.
        pub async fn toggle_source(
            &mut self,
            request: impl tonic::IntoRequest<super::ToggleSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/toggle_source",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "toggle_source",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Removes source.
        pub async fn delete_source(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/delete_source",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "delete_source",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Resets source checkpoint.
        pub async fn reset_source_checkpoint(
            &mut self,
            request: impl tonic::IntoRequest<super::ResetSourceCheckpointRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/reset_source_checkpoint",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "reset_source_checkpoint",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Gets last opstamp for a given `index_id`.
        pub async fn last_delete_opstamp(
            &mut self,
            request: impl tonic::IntoRequest<super::LastDeleteOpstampRequest>,
        ) -> std::result::Result<
            tonic::Response<super::LastDeleteOpstampResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/last_delete_opstamp",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "last_delete_opstamp",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Creates a delete task.
        pub async fn create_delete_task(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteQuery>,
        ) -> std::result::Result<tonic::Response<super::DeleteTask>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/create_delete_task",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "create_delete_task",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Updates splits `delete_opstamp`.
        pub async fn update_splits_delete_opstamp(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateSplitsDeleteOpstampRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateSplitsDeleteOpstampResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/update_splits_delete_opstamp",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "update_splits_delete_opstamp",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Lists delete tasks with `delete_task.opstamp` > `opstamp_start` for a given `index_id`.
        pub async fn list_delete_tasks(
            &mut self,
            request: impl tonic::IntoRequest<super::ListDeleteTasksRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListDeleteTasksResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/list_delete_tasks",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "list_delete_tasks",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// / Lists splits with `split.delete_opstamp` < `delete_opstamp` for a given `index_id`.
        pub async fn list_stale_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::ListStaleSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSplitsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/list_stale_splits",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "list_stale_splits",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn open_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::OpenShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::OpenShardsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/OpenShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "OpenShards"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Acquires a set of shards for indexing. This RPC locks the shards for publishing thanks to a publish token and only
        /// the last indexer that has acquired the shards is allowed to publish. The response returns for each subrequest the
        /// list of acquired shards along with the positions to index from.
        pub async fn acquire_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::AcquireShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AcquireShardsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/AcquireShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "AcquireShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn close_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::CloseShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CloseShardsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/CloseShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "CloseShards"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn delete_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteShardsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/DeleteShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.metastore.MetastoreService",
                        "DeleteShards",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn list_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::ListShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListShardsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit.metastore.MetastoreService/ListShards",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("quickwit.metastore.MetastoreService", "ListShards"),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod metastore_service_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with MetastoreServiceGrpcServer.
    #[async_trait]
    pub trait MetastoreServiceGrpc: Send + Sync + 'static {
        /// Creates an index.
        async fn create_index(
            &self,
            request: tonic::Request<super::CreateIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateIndexResponse>,
            tonic::Status,
        >;
        /// Gets an index metadata.
        async fn index_metadata(
            &self,
            request: tonic::Request<super::IndexMetadataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::IndexMetadataResponse>,
            tonic::Status,
        >;
        /// Gets an indexes metadatas.
        async fn list_indexes_metadatas(
            &self,
            request: tonic::Request<super::ListIndexesMetadataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListIndexesMetadataResponse>,
            tonic::Status,
        >;
        /// Deletes an index
        async fn delete_index(
            &self,
            request: tonic::Request<super::DeleteIndexRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Gets all splits from index.
        async fn list_all_splits(
            &self,
            request: tonic::Request<super::ListAllSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSplitsResponse>,
            tonic::Status,
        >;
        /// Gets splits from index.
        async fn list_splits(
            &self,
            request: tonic::Request<super::ListSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSplitsResponse>,
            tonic::Status,
        >;
        /// Stages several splits.
        async fn stage_splits(
            &self,
            request: tonic::Request<super::StageSplitsRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Publishes split.
        async fn publish_splits(
            &self,
            request: tonic::Request<super::PublishSplitsRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Marks splits for deletion.
        async fn mark_splits_for_deletion(
            &self,
            request: tonic::Request<super::MarkSplitsForDeletionRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Deletes splits.
        async fn delete_splits(
            &self,
            request: tonic::Request<super::DeleteSplitsRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Adds source.
        async fn add_source(
            &self,
            request: tonic::Request<super::AddSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Toggles source.
        async fn toggle_source(
            &self,
            request: tonic::Request<super::ToggleSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Removes source.
        async fn delete_source(
            &self,
            request: tonic::Request<super::DeleteSourceRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Resets source checkpoint.
        async fn reset_source_checkpoint(
            &self,
            request: tonic::Request<super::ResetSourceCheckpointRequest>,
        ) -> std::result::Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
        /// Gets last opstamp for a given `index_id`.
        async fn last_delete_opstamp(
            &self,
            request: tonic::Request<super::LastDeleteOpstampRequest>,
        ) -> std::result::Result<
            tonic::Response<super::LastDeleteOpstampResponse>,
            tonic::Status,
        >;
        /// Creates a delete task.
        async fn create_delete_task(
            &self,
            request: tonic::Request<super::DeleteQuery>,
        ) -> std::result::Result<tonic::Response<super::DeleteTask>, tonic::Status>;
        /// Updates splits `delete_opstamp`.
        async fn update_splits_delete_opstamp(
            &self,
            request: tonic::Request<super::UpdateSplitsDeleteOpstampRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateSplitsDeleteOpstampResponse>,
            tonic::Status,
        >;
        /// Lists delete tasks with `delete_task.opstamp` > `opstamp_start` for a given `index_id`.
        async fn list_delete_tasks(
            &self,
            request: tonic::Request<super::ListDeleteTasksRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListDeleteTasksResponse>,
            tonic::Status,
        >;
        /// / Lists splits with `split.delete_opstamp` < `delete_opstamp` for a given `index_id`.
        async fn list_stale_splits(
            &self,
            request: tonic::Request<super::ListStaleSplitsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSplitsResponse>,
            tonic::Status,
        >;
        async fn open_shards(
            &self,
            request: tonic::Request<super::OpenShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::OpenShardsResponse>,
            tonic::Status,
        >;
        /// Acquires a set of shards for indexing. This RPC locks the shards for publishing thanks to a publish token and only
        /// the last indexer that has acquired the shards is allowed to publish. The response returns for each subrequest the
        /// list of acquired shards along with the positions to index from.
        async fn acquire_shards(
            &self,
            request: tonic::Request<super::AcquireShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AcquireShardsResponse>,
            tonic::Status,
        >;
        async fn close_shards(
            &self,
            request: tonic::Request<super::CloseShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CloseShardsResponse>,
            tonic::Status,
        >;
        async fn delete_shards(
            &self,
            request: tonic::Request<super::DeleteShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteShardsResponse>,
            tonic::Status,
        >;
        async fn list_shards(
            &self,
            request: tonic::Request<super::ListShardsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListShardsResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct MetastoreServiceGrpcServer<T: MetastoreServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: MetastoreServiceGrpc> MetastoreServiceGrpcServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>>
    for MetastoreServiceGrpcServer<T>
    where
        T: MetastoreServiceGrpc,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/quickwit.metastore.MetastoreService/create_index" => {
                    #[allow(non_camel_case_types)]
                    struct create_indexSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::CreateIndexRequest>
                    for create_indexSvc<T> {
                        type Response = super::CreateIndexResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateIndexRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).create_index(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = create_indexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/index_metadata" => {
                    #[allow(non_camel_case_types)]
                    struct index_metadataSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::IndexMetadataRequest>
                    for index_metadataSvc<T> {
                        type Response = super::IndexMetadataResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::IndexMetadataRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).index_metadata(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = index_metadataSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/list_indexes_metadatas" => {
                    #[allow(non_camel_case_types)]
                    struct list_indexes_metadatasSvc<T: MetastoreServiceGrpc>(
                        pub Arc<T>,
                    );
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ListIndexesMetadataRequest>
                    for list_indexes_metadatasSvc<T> {
                        type Response = super::ListIndexesMetadataResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListIndexesMetadataRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).list_indexes_metadatas(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = list_indexes_metadatasSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/delete_index" => {
                    #[allow(non_camel_case_types)]
                    struct delete_indexSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::DeleteIndexRequest>
                    for delete_indexSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteIndexRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).delete_index(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = delete_indexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/list_all_splits" => {
                    #[allow(non_camel_case_types)]
                    struct list_all_splitsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ListAllSplitsRequest>
                    for list_all_splitsSvc<T> {
                        type Response = super::ListSplitsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListAllSplitsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).list_all_splits(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = list_all_splitsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/list_splits" => {
                    #[allow(non_camel_case_types)]
                    struct list_splitsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ListSplitsRequest>
                    for list_splitsSvc<T> {
                        type Response = super::ListSplitsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListSplitsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).list_splits(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = list_splitsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/stage_splits" => {
                    #[allow(non_camel_case_types)]
                    struct stage_splitsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::StageSplitsRequest>
                    for stage_splitsSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StageSplitsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).stage_splits(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = stage_splitsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/publish_splits" => {
                    #[allow(non_camel_case_types)]
                    struct publish_splitsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::PublishSplitsRequest>
                    for publish_splitsSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PublishSplitsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).publish_splits(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = publish_splitsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/mark_splits_for_deletion" => {
                    #[allow(non_camel_case_types)]
                    struct mark_splits_for_deletionSvc<T: MetastoreServiceGrpc>(
                        pub Arc<T>,
                    );
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::MarkSplitsForDeletionRequest>
                    for mark_splits_for_deletionSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MarkSplitsForDeletionRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).mark_splits_for_deletion(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = mark_splits_for_deletionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/delete_splits" => {
                    #[allow(non_camel_case_types)]
                    struct delete_splitsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::DeleteSplitsRequest>
                    for delete_splitsSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteSplitsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).delete_splits(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = delete_splitsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/add_source" => {
                    #[allow(non_camel_case_types)]
                    struct add_sourceSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::AddSourceRequest>
                    for add_sourceSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AddSourceRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).add_source(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = add_sourceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/toggle_source" => {
                    #[allow(non_camel_case_types)]
                    struct toggle_sourceSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ToggleSourceRequest>
                    for toggle_sourceSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ToggleSourceRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).toggle_source(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = toggle_sourceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/delete_source" => {
                    #[allow(non_camel_case_types)]
                    struct delete_sourceSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::DeleteSourceRequest>
                    for delete_sourceSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteSourceRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).delete_source(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = delete_sourceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/reset_source_checkpoint" => {
                    #[allow(non_camel_case_types)]
                    struct reset_source_checkpointSvc<T: MetastoreServiceGrpc>(
                        pub Arc<T>,
                    );
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ResetSourceCheckpointRequest>
                    for reset_source_checkpointSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ResetSourceCheckpointRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).reset_source_checkpoint(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = reset_source_checkpointSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/last_delete_opstamp" => {
                    #[allow(non_camel_case_types)]
                    struct last_delete_opstampSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::LastDeleteOpstampRequest>
                    for last_delete_opstampSvc<T> {
                        type Response = super::LastDeleteOpstampResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LastDeleteOpstampRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).last_delete_opstamp(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = last_delete_opstampSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/create_delete_task" => {
                    #[allow(non_camel_case_types)]
                    struct create_delete_taskSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::DeleteQuery>
                    for create_delete_taskSvc<T> {
                        type Response = super::DeleteTask;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteQuery>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).create_delete_task(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = create_delete_taskSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/update_splits_delete_opstamp" => {
                    #[allow(non_camel_case_types)]
                    struct update_splits_delete_opstampSvc<T: MetastoreServiceGrpc>(
                        pub Arc<T>,
                    );
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<
                        super::UpdateSplitsDeleteOpstampRequest,
                    > for update_splits_delete_opstampSvc<T> {
                        type Response = super::UpdateSplitsDeleteOpstampResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::UpdateSplitsDeleteOpstampRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).update_splits_delete_opstamp(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = update_splits_delete_opstampSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/list_delete_tasks" => {
                    #[allow(non_camel_case_types)]
                    struct list_delete_tasksSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ListDeleteTasksRequest>
                    for list_delete_tasksSvc<T> {
                        type Response = super::ListDeleteTasksResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListDeleteTasksRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).list_delete_tasks(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = list_delete_tasksSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/list_stale_splits" => {
                    #[allow(non_camel_case_types)]
                    struct list_stale_splitsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ListStaleSplitsRequest>
                    for list_stale_splitsSvc<T> {
                        type Response = super::ListSplitsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListStaleSplitsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).list_stale_splits(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = list_stale_splitsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/OpenShards" => {
                    #[allow(non_camel_case_types)]
                    struct OpenShardsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::OpenShardsRequest>
                    for OpenShardsSvc<T> {
                        type Response = super::OpenShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::OpenShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).open_shards(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = OpenShardsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/AcquireShards" => {
                    #[allow(non_camel_case_types)]
                    struct AcquireShardsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::AcquireShardsRequest>
                    for AcquireShardsSvc<T> {
                        type Response = super::AcquireShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AcquireShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).acquire_shards(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AcquireShardsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/CloseShards" => {
                    #[allow(non_camel_case_types)]
                    struct CloseShardsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::CloseShardsRequest>
                    for CloseShardsSvc<T> {
                        type Response = super::CloseShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CloseShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).close_shards(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CloseShardsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/DeleteShards" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteShardsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::DeleteShardsRequest>
                    for DeleteShardsSvc<T> {
                        type Response = super::DeleteShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).delete_shards(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteShardsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit.metastore.MetastoreService/ListShards" => {
                    #[allow(non_camel_case_types)]
                    struct ListShardsSvc<T: MetastoreServiceGrpc>(pub Arc<T>);
                    impl<
                        T: MetastoreServiceGrpc,
                    > tonic::server::UnaryService<super::ListShardsRequest>
                    for ListShardsSvc<T> {
                        type Response = super::ListShardsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListShardsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).list_shards(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListShardsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: MetastoreServiceGrpc> Clone for MetastoreServiceGrpcServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: MetastoreServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: MetastoreServiceGrpc> tonic::server::NamedService
    for MetastoreServiceGrpcServer<T> {
        const NAME: &'static str = "quickwit.metastore.MetastoreService";
    }
}
