use crate::blocks::primitives::info::Info;
use crate::blocks::primitives::member_type::MemberType;

/// Represents an OSM relation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Relation {
    /// Relation ID
    pub id: i64,

    /// Array of key indices into the string table
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keys: Vec<u32>,

    /// Array of value indices into the string table (parallel to keys)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vals: Vec<u32>,

    /// Relation metadata (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<Info>,

    /// Array of role string indices (parallel to memids and types)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles_sid: Vec<i32>,

    /// Delta-encoded member IDs
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub memids: Vec<i64>,

    /// Member types (0=node, 1=way, 2=relation) parallel to memids and roles_sid
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub types: Vec<MemberType>,
}
