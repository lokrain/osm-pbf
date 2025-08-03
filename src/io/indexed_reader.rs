use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use bytes::Bytes;
use crate::io::blob::{Blob, BlobType, BlobError, Result};

/// Index entry for a blob, containing metadata for fast access
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobIndex {
    /// Byte offset in the file
    pub offset: u64,
    /// Size of the blob in bytes
    pub size: u32,
    /// Type of blob (OSMHeader, OSMData, etc.)
    pub blob_type: BlobType,
    /// ID range for primitive blocks (min_id, max_id)
    pub id_range: Option<(i64, i64)>,
    /// Element counts by type (nodes, ways, relations)
    pub element_counts: ElementCounts,
}

/// Counts of different OSM elements in a blob
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ElementCounts {
    pub nodes: u32,
    pub ways: u32,
    pub relations: u32,
    pub changesets: u32,
}

/// Filter criteria for selecting OSM elements
#[derive(Debug, Clone)]
pub struct ElementFilter {
    /// Include nodes
    pub include_nodes: bool,
    /// Include ways
    pub include_ways: bool,
    /// Include relations
    pub include_relations: bool,
    /// Include changesets
    pub include_changesets: bool,
    /// Filter by specific ID ranges
    pub id_ranges: Vec<(i64, i64)>,
    /// Filter by tags (key-value pairs)
    pub tag_filters: HashMap<String, Option<String>>, // None means any value
    /// Resolve dependencies (fetch referenced nodes for ways, etc.)
    pub resolve_dependencies: bool,
}

impl Default for ElementFilter {
    fn default() -> Self {
        Self {
            include_nodes: true,
            include_ways: true,
            include_relations: true,
            include_changesets: false,
            id_ranges: Vec::new(),
            tag_filters: HashMap::new(),
            resolve_dependencies: false,
        }
    }
}

impl ElementFilter {
    /// Create a filter for all element types
    pub fn all() -> Self {
        Self::default()
    }
    
    /// Create a filter for only nodes
    pub fn nodes_only() -> Self {
        Self {
            include_nodes: true,
            include_ways: false,
            include_relations: false,
            include_changesets: false,
            ..Default::default()
        }
    }
    
    /// Create a filter for only ways (with optional dependency resolution)
    pub fn ways_only(resolve_dependencies: bool) -> Self {
        Self {
            include_nodes: resolve_dependencies,
            include_ways: true,
            include_relations: false,
            include_changesets: false,
            resolve_dependencies,
            ..Default::default()
        }
    }
    
    /// Add an ID range filter
    pub fn with_id_range(mut self, min_id: i64, max_id: i64) -> Self {
        self.id_ranges.push((min_id, max_id));
        self
    }
    
    /// Add a tag filter (key must exist with any value)
    pub fn with_tag_key(mut self, key: String) -> Self {
        self.tag_filters.insert(key, None);
        self
    }
    
    /// Add a tag filter (key must have specific value)
    pub fn with_tag(mut self, key: String, value: String) -> Self {
        self.tag_filters.insert(key, Some(value));
        self
    }
}

/// Performant structure for random-access and filtered streaming of OSM PBF data
pub struct IndexedReader<R: Read + Seek> {
    /// The underlying reader
    reader: R,
    /// Index of all blobs in the file
    blob_index: Vec<BlobIndex>,
    /// Header blob (if any)
    header_blob: Option<BlobIndex>,
    /// Quick lookup for blobs by offset
    offset_to_index: HashMap<u64, usize>,
}

impl<R: Read + Seek> IndexedReader<R> {
    /// Create a new IndexedReader and build the index
    pub fn new(reader: R) -> Result<Self> {
        let mut indexed_reader = Self {
            reader,
            blob_index: Vec::new(),
            header_blob: None,
            offset_to_index: HashMap::new(),
        };
        
        indexed_reader.build_index()?;
        Ok(indexed_reader)
    }
    
    /// Build the in-memory index by scanning all blobs
    fn build_index(&mut self) -> Result<()> {
        self.reader.seek(SeekFrom::Start(0))?;
        let mut current_offset = 0u64;
        
        loop {
            // Try to read the next blob
            match self.read_blob_header_at_offset(current_offset) {
                Ok(Some((header, blob_size))) => {
                    let index_entry = BlobIndex {
                        offset: current_offset,
                        size: blob_size,
                        blob_type: header.blob_type,
                        id_range: None, // Will be filled when we actually read the blob
                        element_counts: ElementCounts::default(),
                    };
                    
                    // Store header blob separately
                    if matches!(index_entry.blob_type, BlobType::OSMHeader) {
                        self.header_blob = Some(index_entry.clone());
                    }
                    
                    let index = self.blob_index.len();
                    self.offset_to_index.insert(current_offset, index);
                    self.blob_index.push(index_entry);
                    
                    // Move to next blob
                    current_offset += 4 + blob_size as u64; // 4 bytes for size + blob data
                }
                Ok(None) => break, // End of file
                Err(e) => {
                    // For robust error handling, log the error but continue if possible
                    eprintln!("Warning: Error reading blob at offset {}: {}", current_offset, e);
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    /// Read just the blob header at a specific offset (for indexing)
    fn read_blob_header_at_offset(&mut self, offset: u64) -> Result<Option<(crate::io::blob::BlobHeader, u32)>> {
        self.reader.seek(SeekFrom::Start(offset))?;
        
        // Read blob size (4 bytes, big-endian)
        let mut size_bytes = [0u8; 4];
        match self.reader.read_exact(&mut size_bytes) {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(BlobError::Io(e)),
        }
        
        let blob_size = u32::from_be_bytes(size_bytes);
        
        // For now, we'll create a simplified header
        // In a full implementation, this would parse the actual protobuf header
        let header = crate::io::blob::BlobHeader::new(
            BlobType::OSMData, // Default, would be parsed from actual data
            blob_size
        );
        
        Ok(Some((header, blob_size)))
    }
    
    /// Get the header blob if it exists
    pub fn header_blob(&self) -> Option<&BlobIndex> {
        self.header_blob.as_ref()
    }
    
    /// Get the number of indexed blobs
    pub fn blob_count(&self) -> usize {
        self.blob_index.len()
    }
    
    /// Get blob index by position
    pub fn get_blob_index(&self, index: usize) -> Option<&BlobIndex> {
        self.blob_index.get(index)
    }
    
    /// Read a specific blob by its index
    pub fn read_blob_by_index(&mut self, index: usize) -> Result<Option<Blob>> {
        let blob_index = self.blob_index.get(index).ok_or_else(|| {
            BlobError::InvalidFormat(format!("Blob index {} out of range", index))
        })?;
        
        self.read_blob_at_offset(blob_index.offset)
    }
    
    /// Read a blob at a specific file offset
    pub fn read_blob_at_offset(&mut self, offset: u64) -> Result<Option<Blob>> {
        self.reader.seek(SeekFrom::Start(offset))?;
        
        // Read blob size
        let mut size_bytes = [0u8; 4];
        match self.reader.read_exact(&mut size_bytes) {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(BlobError::Io(e)),
        }
        
        let blob_size = u32::from_be_bytes(size_bytes);
        
        // Read blob data
        let mut blob_data = vec![0u8; blob_size as usize];
        self.reader.read_exact(&mut blob_data)?;
        
        // For now, create a simple raw blob
        // In full implementation, this would parse the protobuf structure
        let blob = Blob::new_raw(
            BlobType::OSMData,
            Bytes::from(blob_data),
            offset
        )?;
        
        Ok(Some(blob))
    }
    
    /// Stream blobs that match the given filter
    pub fn stream_filtered(&mut self, filter: &ElementFilter) -> FilteredBlobIterator<R> {
        FilteredBlobIterator::new(self, filter)
    }
    
    /// Get statistics about the indexed file
    pub fn statistics(&self) -> IndexStatistics {
        let mut stats = IndexStatistics::default();
        
        for blob_index in &self.blob_index {
            match blob_index.blob_type {
                BlobType::OSMHeader => stats.header_blobs += 1,
                BlobType::OSMData => stats.data_blobs += 1,
                BlobType::Unknown(_) => stats.unknown_blobs += 1,
            }
            
            stats.total_nodes += blob_index.element_counts.nodes as u64;
            stats.total_ways += blob_index.element_counts.ways as u64;
            stats.total_relations += blob_index.element_counts.relations as u64;
            stats.total_changesets += blob_index.element_counts.changesets as u64;
        }
        
        stats.total_blobs = self.blob_index.len() as u64;
        stats
    }
    
    /// Find blobs that potentially contain elements in the given ID range
    pub fn find_blobs_for_id_range(&self, min_id: i64, max_id: i64) -> Vec<usize> {
        self.blob_index
            .iter()
            .enumerate()
            .filter_map(|(index, blob)| {
                if let Some((blob_min, blob_max)) = blob.id_range {
                    // Check if ranges overlap
                    if blob_min <= max_id && blob_max >= min_id {
                        Some(index)
                    } else {
                        None
                    }
                } else {
                    // If we don't know the range, include it to be safe
                    Some(index)
                }
            })
            .collect()
    }
}

/// Iterator for streaming filtered blobs
pub struct FilteredBlobIterator<'a, R: Read + Seek> {
    reader: &'a mut IndexedReader<R>,
    filter: ElementFilter,
    current_index: usize,
}

impl<'a, R: Read + Seek> FilteredBlobIterator<'a, R> {
    fn new(reader: &'a mut IndexedReader<R>, filter: &ElementFilter) -> Self {
        Self {
            reader,
            filter: filter.clone(),
            current_index: 0,
        }
    }
}

impl<'a, R: Read + Seek> Iterator for FilteredBlobIterator<'a, R> {
    type Item = Result<Blob>;
    
    fn next(&mut self) -> Option<Self::Item> {
        while self.current_index < self.reader.blob_count() {
            let blob_index = self.reader.get_blob_index(self.current_index)?;
            self.current_index += 1;
            
            // Apply filter logic
            let should_include = match blob_index.blob_type {
                BlobType::OSMHeader => true, // Always include headers
                BlobType::OSMData => {
                    // Check if this blob might contain elements we're interested in
                    let has_relevant_elements = 
                        (self.filter.include_nodes && blob_index.element_counts.nodes > 0) ||
                        (self.filter.include_ways && blob_index.element_counts.ways > 0) ||
                        (self.filter.include_relations && blob_index.element_counts.relations > 0) ||
                        (self.filter.include_changesets && blob_index.element_counts.changesets > 0);
                    
                    has_relevant_elements
                }
                BlobType::Unknown(_) => false, // Skip unknown types by default
            };
            
            if should_include {
                match self.reader.read_blob_by_index(self.current_index - 1) {
                    Ok(Some(blob)) => return Some(Ok(blob)),
                    Ok(None) => continue,
                    Err(e) => return Some(Err(e)),
                }
            }
        }
        
        None
    }
}

/// Statistics about the indexed PBF file
#[derive(Debug, Clone, Default)]
pub struct IndexStatistics {
    pub total_blobs: u64,
    pub header_blobs: u64,
    pub data_blobs: u64,
    pub unknown_blobs: u64,
    pub total_nodes: u64,
    pub total_ways: u64,
    pub total_relations: u64,
    pub total_changesets: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    
    #[test]
    fn test_element_filter_creation() {
        let filter = ElementFilter::nodes_only();
        assert!(filter.include_nodes);
        assert!(!filter.include_ways);
        assert!(!filter.include_relations);
        
        let filter = ElementFilter::ways_only(true);
        assert!(filter.include_nodes); // Dependencies resolved
        assert!(filter.include_ways);
        assert!(!filter.include_relations);
        assert!(filter.resolve_dependencies);
    }
    
    #[test]
    fn test_element_filter_with_tags() {
        let filter = ElementFilter::all()
            .with_tag_key("highway".to_string())
            .with_tag("name".to_string(), "Main Street".to_string());
        
        assert_eq!(filter.tag_filters.get("highway"), Some(&None));
        assert_eq!(filter.tag_filters.get("name"), Some(&Some("Main Street".to_string())));
    }
    
    #[test]
    fn test_indexed_reader_empty() {
        let empty_data = Vec::new();
        let cursor = Cursor::new(empty_data);
        let reader = IndexedReader::new(cursor).unwrap();
        
        assert_eq!(reader.blob_count(), 0);
        assert!(reader.header_blob().is_none());
    }
    
    #[test]
    fn test_element_counts() {
        let counts = ElementCounts {
            nodes: 100,
            ways: 50,
            relations: 10,
            changesets: 5,
        };
        
        assert_eq!(counts.nodes, 100);
        assert_eq!(counts.ways, 50);
    }
}
