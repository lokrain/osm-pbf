use crate::blocks::primitives::info::Info;

/// Represents an OSM node in sparse format.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Node {
    /// Node ID
    pub id: i64,

    /// Array of key indices into the string table
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keys: Vec<u32>,

    /// Array of value indices into the string table (parallel to keys)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vals: Vec<u32>,

    /// Node metadata (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<Info>,

    /// Latitude in nanodegrees
    pub lat: i64,

    /// Longitude in nanodegrees
    pub lon: i64,
}
