use crate::blocks::primitives::dense_info::DenseInfo;

/// Represents dense node storage format for efficient bulk node storage.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DenseNodes {
    /// Delta-encoded node IDs
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub id: Vec<i64>,

    /// Metadata for each node (parallel to id array)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub denseinfo: Option<DenseInfo>,

    /// Delta-encoded latitudes in nanodegrees
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lat: Vec<i64>,

    /// Delta-encoded longitudes in nanodegrees
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lon: Vec<i64>,

    /// Packed key-value pairs: [key1, val1, key2, val2, ..., 0] for each node
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keys_vals: Vec<i32>,
}

impl Default for DenseNodes {
    fn default() -> Self {
        Self {
            id: Vec::new(),
            denseinfo: None,
            lat: Vec::new(),
            lon: Vec::new(),
            keys_vals: Vec::new(),
        }
    }
}

