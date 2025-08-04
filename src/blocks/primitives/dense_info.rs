/// Dense version of Info for bulk node storage.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[derive(Default)]
pub struct DenseInfo {
    /// Delta-encoded versions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub version: Vec<i32>,

    /// Delta-encoded timestamps
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timestamp: Vec<i64>,

    /// Delta-encoded changeset IDs
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changeset: Vec<i64>,

    /// Delta-encoded user IDs
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub uid: Vec<i32>,

    /// Delta-encoded username string indices
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub user_sid: Vec<i32>,

    /// Visibility flags for each node
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visible: Vec<bool>,
}


