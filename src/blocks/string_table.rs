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

    #[test]
    fn test_new_string_table() {
        let st = StringTable::new();
        assert_eq!(st.len(), 1);
        assert_eq!(st.get_string(0), Some(""));
    }

    #[test]
    fn test_add_and_get_string() {
        let mut st = StringTable::new();
        let index = st.add_string("test".to_string());
        assert_eq!(index, 1);
        assert_eq!(st.get_string(1), Some("test"));
        assert_eq!(st.len(), 2);
    }

    #[test]
    fn test_get_string_or_empty() {
        let mut st = StringTable::new();
        st.add_string("test".to_string());
        
        assert_eq!(st.get_string_or_empty(0), "");
        assert_eq!(st.get_string_or_empty(1), "test");
        assert_eq!(st.get_string_or_empty(999), "");
    }
}
