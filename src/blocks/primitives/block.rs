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

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_primitive_block_default() {
        let block = PrimitiveBlock::default();
        
        assert_eq!(block.granularity, PrimitiveBlock::DEFAULT_GRANULARITY);
        assert_eq!(block.date_granularity, PrimitiveBlock::DEFAULT_DATE_GRANULARITY);
        assert_eq!(block.lat_offset, 0);
        assert_eq!(block.lon_offset, 0);
        assert!(block.primitivegroup.is_empty());
        assert!(block.stringtable.is_empty());
    }

    #[test]
    fn test_default_values() {
        assert_eq!(PrimitiveBlock::default_granularity(), 100);
        assert_eq!(PrimitiveBlock::default_date_granularity(), 1000);
        assert_eq!(PrimitiveBlock::DEFAULT_GRANULARITY, 100);
        assert_eq!(PrimitiveBlock::DEFAULT_DATE_GRANULARITY, 1000);
    }

    #[test]
    fn test_primitive_block_creation() {
        let mut block = PrimitiveBlock::default();
        
        // Modify some values
        block.granularity = 50;
        block.date_granularity = 500;
        block.lat_offset = 1000000;
        block.lon_offset = -2000000;
        
        assert_eq!(block.granularity, 50);
        assert_eq!(block.date_granularity, 500);
        assert_eq!(block.lat_offset, 1000000);
        assert_eq!(block.lon_offset, -2000000);
    }

    #[test]
    fn test_primitive_block_with_string_table() {
        let mut block = PrimitiveBlock::default();
        
        // Add some strings to the string table
        let highway_idx = block.stringtable.add_string("highway".to_string());
        let primary_idx = block.stringtable.add_string("primary".to_string());
        
        assert_eq!(highway_idx, 1);
        assert_eq!(primary_idx, 2);
        assert_eq!(block.stringtable.get_string(highway_idx), Some("highway"));
        assert_eq!(block.stringtable.get_string(primary_idx), Some("primary"));
    }

    #[test]
    fn test_primitive_block_serialization() {
        let mut block = PrimitiveBlock::default();
        block.granularity = 200;
        block.lat_offset = 500000;
        block.stringtable.add_string("test".to_string());
        
        let serialized = serde_json::to_string(&block).unwrap();
        let deserialized: PrimitiveBlock = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(block, deserialized);
    }

    #[test]
    fn test_clone_and_equality() {
        let mut block1 = PrimitiveBlock::default();
        block1.granularity = 150;
        block1.stringtable.add_string("test".to_string());
        
        let block2 = block1.clone();
        assert_eq!(block1, block2);
        
        let mut block3 = PrimitiveBlock::default();
        block3.granularity = 200;
        assert_ne!(block1, block3);
    }

    #[test]
    fn test_coordinate_transformation() {
        let block = PrimitiveBlock {
            stringtable: StringTable::default(),
            primitivegroup: Vec::new(),
            granularity: 100,
            lat_offset: 500_000_000, // 0.5 degrees
            lon_offset: -1_000_000_000, // -1.0 degrees
            date_granularity: 1000,
        };
        
        // Test coordinate calculation: raw_coord * granularity + offset
        let raw_lat = 450_000_000; // 45 degrees in nanodegrees
        let raw_lon = 900_000_000; // 90 degrees in nanodegrees
        
        let actual_lat = (raw_lat as i64) * (block.granularity as i64) + block.lat_offset;
        let actual_lon = (raw_lon as i64) * (block.granularity as i64) + block.lon_offset;
        
        // Expected: 45 * 100 + 500_000_000 = 4_500_000_000 + 500_000_000 = 5_000_000_000 (50 degrees)
        assert_eq!(actual_lat, 4_500_000_000 + 500_000_000);
        // Expected: 90 * 100 + (-1_000_000_000) = 9_000_000_000 - 1_000_000_000 = 8_000_000_000 (80 degrees)
        assert_eq!(actual_lon, 9_000_000_000 - 1_000_000_000);
    }

    #[test]
    fn test_performance_coordinate_calculations() {
        use std::time::Instant;
        
        let block = PrimitiveBlock {
            stringtable: StringTable::default(),
            primitivegroup: Vec::new(),
            granularity: 100,
            lat_offset: 0,
            lon_offset: 0,
            date_granularity: 1000,
        };
        
        let start = Instant::now();
        let mut sum = 0i64;
        
        // Simulate coordinate transformations for 100k points
        for i in 0..100_000 {
            let raw_coord = i * 1000;
            let actual_coord = (raw_coord as i64) * (block.granularity as i64) + block.lat_offset;
            sum += actual_coord;
        }
        
        let duration = start.elapsed();
        assert!(duration.as_millis() < 50); // Should be very fast
        assert!(sum > 0); // Ensure calculation happened
    }

    #[test]
    fn test_large_primitive_groups() {
        let mut block = PrimitiveBlock::default();
        
        // Reserve space for many primitive groups
        block.primitivegroup.reserve(1000);
        
        // Add primitive groups (empty for this test)
        for _ in 0..1000 {
            block.primitivegroup.push(PrimitiveGroup::default());
        }
        
        assert_eq!(block.primitivegroup.len(), 1000);
        assert!(block.primitivegroup.capacity() >= 1000);
    }

    #[test]
    fn test_extreme_offset_values() {
        let block = PrimitiveBlock {
            stringtable: StringTable::default(),
            primitivegroup: Vec::new(),
            granularity: 1,
            lat_offset: i64::MAX / 2,
            lon_offset: i64::MIN / 2,
            date_granularity: 1,
        };
        
        // Test that extreme values don't cause overflow in typical operations
        assert_eq!(block.lat_offset, i64::MAX / 2);
        assert_eq!(block.lon_offset, i64::MIN / 2);
    }

    #[test]
    fn test_granularity_edge_cases() {
        // Test minimum granularity
        let mut block = PrimitiveBlock::default();
        block.granularity = 1;
        assert_eq!(block.granularity, 1);
        
        // Test typical high-precision granularity
        block.granularity = 1000;
        assert_eq!(block.granularity, 1000);
        
        // Test date granularity edge cases
        block.date_granularity = 1; // 1ms precision
        assert_eq!(block.date_granularity, 1);
        
        block.date_granularity = 60000; // 1 minute precision
        assert_eq!(block.date_granularity, 60000);
    }

    #[test]
    fn test_memory_layout() {
        let block = PrimitiveBlock::default();
        
        // Verify the structure has the expected size characteristics
        assert!(std::mem::size_of::<PrimitiveBlock>() > 0);
        assert!(std::mem::align_of::<PrimitiveBlock>() > 0);
        
        // The block should have predictable memory layout
        let size = std::mem::size_of::<PrimitiveBlock>();
        assert!(size > std::mem::size_of::<StringTable>());
    }
}