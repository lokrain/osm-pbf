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

impl Node {
    /// Creates a new Node with the given ID and coordinates.
    pub fn new(id: i64, lat: i64, lon: i64) -> Self {
        Self {
            id,
            keys: Vec::new(),
            vals: Vec::new(),
            info: None,
            lat,
            lon,
        }
    }

    /// Adds a tag to the node using string table indices.
    pub fn add_tag(&mut self, key: u32, value: u32) {
        self.keys.push(key);
        self.vals.push(value);
    }

    /// Returns the number of tags on this node.
    pub fn tag_count(&self) -> usize {
        debug_assert_eq!(self.keys.len(), self.vals.len());
        self.keys.len()
    }

    /// Gets a tag by index, returning (key_index, value_index).
    pub fn get_tag(&self, index: usize) -> Option<(u32, u32)> {
        if index < self.keys.len() && index < self.vals.len() {
            Some((self.keys[index], self.vals[index]))
        } else {
            None
        }
    }

    /// Converts latitude from internal representation to degrees.
    pub fn lat_degrees(&self) -> f64 {
        self.lat as f64 * 1e-9
    }

    /// Converts longitude from internal representation to degrees.
    pub fn lon_degrees(&self) -> f64 {
        self.lon as f64 * 1e-9
    }

    /// Returns true if this node has any tags.
    pub fn has_tags(&self) -> bool {
        !self.keys.is_empty()
    }

    /// Clears all tags from this node.
    pub fn clear_tags(&mut self) {
        self.keys.clear();
        self.vals.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_node_creation() {
        let node = Node::new(123, 450_000_000, 90_000_000); // 45°N, 9°E
        
        assert_eq!(node.id, 123);
        assert_eq!(node.lat, 450_000_000);
        assert_eq!(node.lon, 90_000_000);
        assert!(node.keys.is_empty());
        assert!(node.vals.is_empty());
        assert!(node.info.is_none());
        assert!(!node.has_tags());
    }

    #[test]
    fn test_coordinate_conversion() {
        let node = Node::new(1, 450_000_000, -90_000_000); // 45°N, 9°W
        
        assert!((node.lat_degrees() - 45.0).abs() < 1e-10);
        assert!((node.lon_degrees() - (-9.0)).abs() < 1e-10);
    }

    #[test]
    fn test_add_tags() {
        let mut node = Node::new(1, 0, 0);
        
        node.add_tag(1, 2); // highway -> primary
        node.add_tag(3, 4); // name -> "Main Street"
        
        assert_eq!(node.tag_count(), 2);
        assert!(node.has_tags());
        assert_eq!(node.get_tag(0), Some((1, 2)));
        assert_eq!(node.get_tag(1), Some((3, 4)));
        assert_eq!(node.get_tag(2), None);
    }

    #[test]
    fn test_clear_tags() {
        let mut node = Node::new(1, 0, 0);
        node.add_tag(1, 2);
        node.add_tag(3, 4);
        
        assert!(node.has_tags());
        node.clear_tags();
        assert!(!node.has_tags());
        assert_eq!(node.tag_count(), 0);
    }

    #[test]
    fn test_node_with_info() {
        let mut node = Node::new(1, 0, 0);
        node.info = Some(Info {
            version: Some(1),
            timestamp: Some(1609459200),
            changeset: Some(12345),
            uid: Some(678),
            user_sid: Some(5),
            visible: None,
        });
        
        assert!(node.info.is_some());
        let info = node.info.as_ref().unwrap();
        assert_eq!(info.version, Some(1));
        assert_eq!(info.changeset, Some(12345));
    }

    #[test]
    fn test_extreme_coordinates() {
        // Test maximum valid coordinates
        let max_lat = 900_000_000; // 90°N
        let max_lon = 1_800_000_000; // 180°E
        let node_max = Node::new(1, max_lat, max_lon);
        
        assert!((node_max.lat_degrees() - 90.0).abs() < 1e-10);
        assert!((node_max.lon_degrees() - 180.0).abs() < 1e-10);
        
        // Test minimum valid coordinates
        let min_lat = -900_000_000; // 90°S
        let min_lon = -1_800_000_000; // 180°W
        let node_min = Node::new(2, min_lat, min_lon);
        
        assert!((node_min.lat_degrees() - (-90.0)).abs() < 1e-10);
        assert!((node_min.lon_degrees() - (-180.0)).abs() < 1e-10);
    }

    #[test]
    fn test_serialization() {
        let mut node = Node::new(123, 450_000_000, 90_000_000);
        node.add_tag(1, 2);
        
        let serialized = serde_json::to_string(&node).unwrap();
        let deserialized: Node = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(node, deserialized);
    }

    #[test]
    fn test_clone_and_equality() {
        let mut node1 = Node::new(1, 100, 200);
        node1.add_tag(1, 2);
        
        let node2 = node1.clone();
        assert_eq!(node1, node2);
        
        let node3 = Node::new(2, 100, 200);
        assert_ne!(node1, node3);
    }

    #[test]
    fn test_performance_tag_operations() {
        use std::time::Instant;
        
        let start = Instant::now();
        let mut node = Node::new(1, 0, 0);
        
        // Add 1000 tags
        for i in 0..1000 {
            node.add_tag(i as u32, (i + 1000) as u32);
        }
        
        let duration = start.elapsed();
        assert!(duration.as_millis() < 10); // Should be very fast
        assert_eq!(node.tag_count(), 1000);
        
        // Test tag access performance
        let access_start = Instant::now();
        for i in 0..1000 {
            let tag = node.get_tag(i).unwrap();
            assert_eq!(tag.0, i as u32);
            assert_eq!(tag.1, (i + 1000) as u32);
        }
        let access_duration = access_start.elapsed();
        assert!(access_duration.as_millis() < 5);
    }

    #[test]
    fn test_performance_coordinate_conversion() {
        use std::time::Instant;
        
        let nodes: Vec<Node> = (0..10_000)
            .map(|i| Node::new(i, (i * 100) as i64, (i * 200) as i64))
            .collect();
        
        let start = Instant::now();
        let mut sum = 0.0;
        for node in &nodes {
            sum += node.lat_degrees() + node.lon_degrees();
        }
        
        let duration = start.elapsed();
        assert!(duration.as_millis() < 20); // Should process 10k conversions quickly
        assert!(sum.abs() > 0.0); // Ensure calculation happened
    }

    #[test]
    fn test_high_precision_coordinates() {
        // Test nanodegree precision
        let precise_lat = 450_123_456; // ~45.0123456°
        let precise_lon = 90_987_654;  // ~9.0987654°
        
        let node = Node::new(1, precise_lat, precise_lon);
        let lat_deg = node.lat_degrees();
        let lon_deg = node.lon_degrees();
        
        // Should maintain high precision
        assert!((lat_deg - 45.0123456).abs() < 1e-7);
        assert!((lon_deg - 9.0987654).abs() < 1e-7);
    }

    #[test]
    fn test_empty_tag_arrays() {
        let node = Node::new(1, 0, 0);
        
        assert_eq!(node.keys.len(), 0);
        assert_eq!(node.vals.len(), 0);
        assert_eq!(node.tag_count(), 0);
        assert!(!node.has_tags());
    }

    #[test]
    fn test_large_node_ids() {
        let large_id = i64::MAX;
        let node = Node::new(large_id, 0, 0);
        assert_eq!(node.id, large_id);
        
        let negative_id = i64::MIN;
        let node_neg = Node::new(negative_id, 0, 0);
        assert_eq!(node_neg.id, negative_id);
    }

    #[test]
    fn test_tag_consistency() {
        let mut node = Node::new(1, 0, 0);
        
        // Add multiple tags and verify consistency
        for i in 0..100 {
            node.add_tag(i, i + 100);
        }
        
        // Verify all tags are correctly stored
        for i in 0..100 {
            let tag = node.get_tag(i).unwrap();
            assert_eq!(tag.0, i);
            assert_eq!(tag.1, i + 100);
        }
        
        // Keys and vals should have same length
        assert_eq!(node.keys.len(), node.vals.len());
    }

    #[test]
    fn test_memory_efficiency() {
        let node = Node::new(1, 0, 0);
        
        // Check that empty vectors don't waste too much space
        assert_eq!(node.keys.len(), 0);
        assert_eq!(node.vals.len(), 0);
        
        // The struct should be reasonably sized
        let size = std::mem::size_of::<Node>();
        assert!(size > 0);
        assert!(size < 1024); // Should be reasonably compact
    }
}
