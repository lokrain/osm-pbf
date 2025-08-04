/// Represents a string table used in OSM PBF format.
/// String tables contain an array of UTF-8 strings which are referenced by index
/// from other parts of the PBF data structure to reduce redundancy.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StringTable {
    /// Array of UTF-8 strings. Index 0 is always empty/null string.
    pub s: Vec<String>,
}

impl StringTable {
    /// Creates a new StringTable with an empty string at index 0.
    pub fn new() -> Self {
        Self {
            s: vec![String::new()], // Index 0 is always empty
        }
    }

    /// Adds a string to the table and returns its index.
    pub fn add_string(&mut self, string: String) -> usize {
        self.s.push(string);
        self.s.len() - 1
    }

    /// Gets a string by index. Returns None if index is out of bounds.
    pub fn get_string(&self, index: usize) -> Option<&str> {
        self.s.get(index).map(|s| s.as_str())
    }

    /// Gets a string by index, returning empty string if index is 0 or out of bounds.
    pub fn get_string_or_empty(&self, index: usize) -> &str {
        if index == 0 || index >= self.s.len() {
            ""
        } else {
            &self.s[index]
        }
    }

    /// Returns the number of strings in the table.
    pub fn len(&self) -> usize {
        self.s.len()
    }

    /// Returns true if the table only contains the empty string at index 0.
    pub fn is_empty(&self) -> bool {
        self.s.len() <= 1
    }
}

impl Default for StringTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;

    #[test]
    fn test_new_string_table() {
        let st = StringTable::new();
        assert_eq!(st.len(), 1);
        assert_eq!(st.get_string(0), Some(""));
        assert!(st.is_empty());
    }

    #[test]
    fn test_default_string_table() {
        let st = StringTable::default();
        assert_eq!(st.len(), 1);
        assert_eq!(st.get_string(0), Some(""));
        assert!(st.is_empty());
    }

    #[test]
    fn test_add_and_get_string() {
        let mut st = StringTable::new();
        let index = st.add_string("test".to_string());
        assert_eq!(index, 1);
        assert_eq!(st.get_string(1), Some("test"));
        assert_eq!(st.len(), 2);
        assert!(!st.is_empty());
    }

    #[test]
    fn test_get_string_or_empty() {
        let mut st = StringTable::new();
        st.add_string("test".to_string());
        
        assert_eq!(st.get_string_or_empty(0), "");
        assert_eq!(st.get_string_or_empty(1), "test");
        assert_eq!(st.get_string_or_empty(999), "");
    }

    #[test]
    fn test_multiple_strings() {
        let mut st = StringTable::new();
        
        let idx1 = st.add_string("highway".to_string());
        let idx2 = st.add_string("primary".to_string());
        let idx3 = st.add_string("name".to_string());
        let idx4 = st.add_string("Main Street".to_string());
        
        assert_eq!(idx1, 1);
        assert_eq!(idx2, 2);
        assert_eq!(idx3, 3);
        assert_eq!(idx4, 4);
        
        assert_eq!(st.get_string(1), Some("highway"));
        assert_eq!(st.get_string(2), Some("primary"));
        assert_eq!(st.get_string(3), Some("name"));
        assert_eq!(st.get_string(4), Some("Main Street"));
        
        assert_eq!(st.len(), 5);
    }

    #[test]
    fn test_unicode_strings() {
        let mut st = StringTable::new();
        
        let unicode_strings = vec![
            "Ñoño".to_string(),
            "测试".to_string(),
            "🏠🛣️".to_string(),
            "Москва".to_string(),
            "🇺🇸".to_string(),
        ];
        
        let mut indices = Vec::new();
        for s in &unicode_strings {
            indices.push(st.add_string(s.clone()));
        }
        
        for (i, expected) in unicode_strings.iter().enumerate() {
            assert_eq!(st.get_string(indices[i]), Some(expected.as_str()));
        }
    }

    #[test]
    fn test_empty_and_whitespace_strings() {
        let mut st = StringTable::new();
        
        let idx1 = st.add_string("".to_string());
        let idx2 = st.add_string(" ".to_string());
        let idx3 = st.add_string("\t".to_string());
        let idx4 = st.add_string("\n".to_string());
        let idx5 = st.add_string("   spaces   ".to_string());
        
        assert_eq!(st.get_string(idx1), Some(""));
        assert_eq!(st.get_string(idx2), Some(" "));
        assert_eq!(st.get_string(idx3), Some("\t"));
        assert_eq!(st.get_string(idx4), Some("\n"));
        assert_eq!(st.get_string(idx5), Some("   spaces   "));
    }

    #[test]
    fn test_very_long_strings() {
        let mut st = StringTable::new();
        
        let long_string = "a".repeat(10000);
        let idx = st.add_string(long_string.clone());
        
        assert_eq!(st.get_string(idx), Some(long_string.as_str()));
    }

    #[test]
    fn test_out_of_bounds_access() {
        let mut st = StringTable::new();
        st.add_string("test".to_string());
        
        assert_eq!(st.get_string(999), None);
        assert_eq!(st.get_string_or_empty(999), "");
    }

    #[test]
    fn test_serialization() {
        let mut st = StringTable::new();
        st.add_string("highway".to_string());
        st.add_string("primary".to_string());
        
        let serialized = serde_json::to_string(&st).unwrap();
        let deserialized: StringTable = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(st, deserialized);
        assert_eq!(deserialized.get_string(1), Some("highway"));
        assert_eq!(deserialized.get_string(2), Some("primary"));
    }

    #[test]
    fn test_clone_and_equality() {
        let mut st1 = StringTable::new();
        st1.add_string("test".to_string());
        
        let st2 = st1.clone();
        assert_eq!(st1, st2);
        
        let mut st3 = StringTable::new();
        st3.add_string("different".to_string());
        assert_ne!(st1, st3);
    }

    #[test]
    fn test_performance_large_string_table() {
        use std::time::Instant;
        
        let start = Instant::now();
        let mut st = StringTable::new();
        
        // Add 10,000 strings
        for i in 0..10_000 {
            st.add_string(format!("string_{}", i));
        }
        
        let duration = start.elapsed();
        assert!(duration.as_millis() < 100); // Should be fast
        assert_eq!(st.len(), 10_001); // +1 for empty string at index 0
        
        // Test random access performance
        let access_start = Instant::now();
        for i in 1..=10_000 {
            let s = st.get_string(i).unwrap();
            assert!(s.starts_with("string_"));
        }
        let access_duration = access_start.elapsed();
        assert!(access_duration.as_millis() < 50); // Random access should be very fast
    }

    #[test]
    fn test_performance_string_deduplication_simulation() {
        use std::time::Instant;
        
        // Simulate common OSM tag deduplication scenario
        let common_keys = vec!["highway", "name", "surface", "maxspeed", "oneway"];
        let common_values = vec!["primary", "secondary", "residential", "footway", "yes", "no"];
        
        let start = Instant::now();
        let mut st = StringTable::new();
        let mut key_indices = HashMap::new();
        let mut value_indices = HashMap::new();
        
        // Add common keys and values
        for key in &common_keys {
            let idx = st.add_string(key.to_string());
            key_indices.insert(key.to_string(), idx);
        }
        
        for value in &common_values {
            let idx = st.add_string(value.to_string());
            value_indices.insert(value.to_string(), idx);
        }
        
        let duration = start.elapsed();
        assert!(duration.as_micros() < 1000); // Should be very fast
        
        // Verify all keys and values are accessible
        for key in &common_keys {
            let idx = key_indices[*key];
            assert_eq!(st.get_string(idx), Some(*key));
        }
        
        for value in &common_values {
            let idx = value_indices[*value];
            assert_eq!(st.get_string(idx), Some(*value));
        }
    }

    #[test]
    fn test_memory_efficiency_indicators() {
        let mut st = StringTable::new();
        
        // Add some strings
        let strings = vec![
            "highway", "primary", "name", "Main Street",
            "surface", "asphalt", "maxspeed", "50",
        ];
        
        for s in strings {
            st.add_string(s.to_string());
        }
        
        // The string table should maintain proper indexing
        assert_eq!(st.len(), 9); // 8 strings + 1 empty at index 0
        
        // All strings should be accessible by their index
        for i in 0..st.len() {
            assert!(st.get_string(i).is_some());
        }
    }

    #[test]
    fn test_edge_case_index_zero() {
        let st = StringTable::new();
        
        // Index 0 should always be empty string
        assert_eq!(st.get_string(0), Some(""));
        assert_eq!(st.get_string_or_empty(0), "");
        
        // Even after adding strings, index 0 should remain empty
        let mut st = StringTable::new();
        st.add_string("test".to_string());
        assert_eq!(st.get_string(0), Some(""));
    }

    #[test]
    fn test_string_table_capacity_growth() {
        let mut st = StringTable::new();
        let initial_capacity = st.s.capacity();
        
        // Add strings until capacity grows
        for i in 0..100 {
            st.add_string(format!("string_{}", i));
        }
        
        assert!(st.s.capacity() >= initial_capacity);
        assert_eq!(st.len(), 101); // 100 strings + 1 empty
    }
}
