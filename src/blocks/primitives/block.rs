use crate::blocks::string_table::StringTable;
use crate::blocks::primitives::group::PrimitiveGroup;

/// Represents a block of OSM primitives, including nodes, ways, and relations.
/// Stores coordinate and date granularity, offsets, and references to string and primitive tables.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PrimitiveBlock {
    pub stringtable: StringTable,
    pub primitivegroup: Vec<PrimitiveGroup>,

    /// Granularity, units of nanodegrees, used to store coordinates in this block.
    #[serde(default = "PrimitiveBlock::default_granularity")]
    pub granularity: i32,

    /// Offset value between the output coordinates and the granularity grid in units of nanodegrees.
    #[serde(default)]
    pub lat_offset: i64,

    #[serde(default)]
    pub lon_offset: i64,

    /// Granularity of dates, normally represented in units of milliseconds since the 1970 epoch.
    #[serde(default = "PrimitiveBlock::default_date_granularity")]
    pub date_granularity: i32,
}

impl PrimitiveBlock {
    /// Default coordinate granularity (nanodegrees).
    pub const DEFAULT_GRANULARITY: i32 = 100;
    /// Default date granularity (milliseconds since epoch).
    pub const DEFAULT_DATE_GRANULARITY: i32 = 1000;

    /// Returns the default coordinate granularity.
    pub fn default_granularity() -> i32 {
        Self::DEFAULT_GRANULARITY
    }
    /// Returns the default date granularity.
    pub fn default_date_granularity() -> i32 {
        Self::DEFAULT_DATE_GRANULARITY
    }
}

impl Default for PrimitiveBlock {
    fn default() -> Self {
        Self {
            stringtable: StringTable::default(),
            primitivegroup: Vec::new(),
            granularity: Self::DEFAULT_GRANULARITY,
            lat_offset: 0,
            lon_offset: 0,
            date_granularity: Self::DEFAULT_DATE_GRANULARITY,
        }
    }
}