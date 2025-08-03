/// Represents metadata information for OSM objects.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Info {
    /// Object version
    #[serde(default)]
    pub version: i32,

    /// Timestamp in milliseconds since epoch
    #[serde(default)]
    pub timestamp: i64,

    /// Changeset ID that created this version
    #[serde(default)]
    pub changeset: i64,

    /// User ID (index into string table)
    #[serde(default)]
    pub uid: i32,

    /// Username (index into string table)
    #[serde(default)]
    pub user_sid: u32,

    /// Whether this object is visible (not deleted)
    #[serde(default = "Info::default_visible")]
    pub visible: bool,
}

impl Info {
    fn default_visible() -> bool {
        true
    }
}

impl Default for Info {
    fn default() -> Self {
        Self {
            version: 0,
            timestamp: 0,
            changeset: 0,
            uid: 0,
            user_sid: 0,
            visible: true,
        }
    }
}
