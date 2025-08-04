use std::borrow::Cow; 

use crate::blocks::nano_degree::NanoDegree;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[derive(Default)]
pub struct HeaderBlock<'a> {
    pub required_features: Vec<Cow<'a, str>>,
    pub optional_features: Vec<Cow<'a, str>>,
    pub writing_program: &'a str,
    pub source: &'a str, // from the bbox field 

    /// Replication timestamp, expressed in seconds since the epoch,
    pub osmosis_replication_timestamp: Option<OsmosisReplicationTimestamp>,

    // Replication sequence number (sequenceNumber in state.txt).
    pub osmosis_replication_sequence_number: Option<OsmosisSequenceNumber>,

    /// Replication base URL (from Osmosis' configuration.txt file).
    pub osmosis_replication_base_url: Option<&'a str>,
}

/// The bounding box field in the OSM header. BBOX, as used in the OSM
/// header. Always nanodegrees (1e-9 deg), not affected by granularity rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct HeaderBBox {
    pub min_lon: NanoDegree,
    pub max_lon: NanoDegree,
    pub min_lat: NanoDegree,
    pub max_lat: NanoDegree,
}

/// Replication timestamp, expressed in seconds since the epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct OsmosisReplicationTimestamp(i64);

impl OsmosisReplicationTimestamp {
    /// Creates a new OsmosisReplicationTimestamp if the value is valid (non-negative).
    pub fn new(secs: i64) -> Option<Self> {
        if secs >= 0 {
            Some(OsmosisReplicationTimestamp(secs))
        } else {
            None
        }
    }

    /// Returns the timestamp as seconds since the epoch.
    pub fn as_secs(&self) -> i64 {
        self.0
    }
}

/// Replication sequence number (sequenceNumber in state.txt).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct OsmosisSequenceNumber(i64);

impl OsmosisSequenceNumber {
    /// Creates a new OsmosisSequenceNumber if the value is valid (non-negative).
    pub fn new(seq: i64) -> Option<Self> {
        if seq >= 0 {
            Some(OsmosisSequenceNumber(seq))
        } else {
            None
        }
    }

    /// Returns the sequence number.
    pub fn as_seq(&self) -> i64 {
        self.0
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::blocks::nano_degree::NanoDegree;
    use pretty_assertions::assert_eq;

    #[test]
    fn construct_header_bbox() {
        let bbox = HeaderBBox {
            min_lon: NanoDegree(0),
            max_lon: NanoDegree(100),
            min_lat: NanoDegree(-50),
            max_lat: NanoDegree(50),
        };
        assert_eq!(bbox.min_lon, NanoDegree(0));
        assert_eq!(bbox.max_lon, NanoDegree(100));
        assert_eq!(bbox.min_lat, NanoDegree(-50));
        assert_eq!(bbox.max_lat, NanoDegree(50));
    }

    #[test]
    fn test_osmosis_replication_timestamp_creation() {
        // Valid timestamps
        assert!(OsmosisReplicationTimestamp::new(0).is_some());
        assert!(OsmosisReplicationTimestamp::new(1609459200).is_some()); // 2021-01-01
        assert!(OsmosisReplicationTimestamp::new(i64::MAX).is_some());
        
        // Invalid timestamps
        assert!(OsmosisReplicationTimestamp::new(-1).is_none());
        assert!(OsmosisReplicationTimestamp::new(-1000).is_none());
    }

    #[test]
    fn test_osmosis_replication_timestamp_as_secs() {
        let timestamp = OsmosisReplicationTimestamp::new(1609459200).unwrap();
        assert_eq!(timestamp.as_secs(), 1609459200);
    }

    #[test]
    fn test_osmosis_sequence_number_creation() {
        // Valid sequence numbers
        assert!(OsmosisSequenceNumber::new(0).is_some());
        assert!(OsmosisSequenceNumber::new(12345).is_some());
        assert!(OsmosisSequenceNumber::new(i64::MAX).is_some());
        
        // Invalid sequence numbers
        assert!(OsmosisSequenceNumber::new(-1).is_none());
        assert!(OsmosisSequenceNumber::new(-1000).is_none());
    }

    #[test]
    fn test_osmosis_sequence_number_as_seq() {
        let seq = OsmosisSequenceNumber::new(98765).unwrap();
        assert_eq!(seq.as_seq(), 98765);
    }

    #[test]
    fn test_header_block_default() {
        let header = HeaderBlock::default();
        assert!(header.required_features.is_empty());
        assert!(header.optional_features.is_empty());
        assert_eq!(header.writing_program, "");
        assert_eq!(header.source, "");
        assert!(header.osmosis_replication_timestamp.is_none());
        assert!(header.osmosis_replication_sequence_number.is_none());
        assert!(header.osmosis_replication_base_url.is_none());
    }

    #[test]
    fn test_header_block_with_features() {
        let mut header = HeaderBlock::default();
        header.required_features.push("OsmSchema-V0.6".into());
        header.optional_features.push("DenseNodes".into());
        header.writing_program = "osm2pbf";
        header.source = "OpenStreetMap contributors";

        assert_eq!(header.required_features.len(), 1);
        assert_eq!(header.optional_features.len(), 1);
        assert_eq!(header.writing_program, "osm2pbf");
        assert_eq!(header.source, "OpenStreetMap contributors");
    }

    #[test]
    fn test_header_block_with_replication_info() {
        let mut header = HeaderBlock::default();
        header.osmosis_replication_timestamp = OsmosisReplicationTimestamp::new(1609459200);
        header.osmosis_replication_sequence_number = OsmosisSequenceNumber::new(1234);
        header.osmosis_replication_base_url = Some("https://planet.openstreetmap.org/replication/minute/");

        assert!(header.osmosis_replication_timestamp.is_some());
        assert!(header.osmosis_replication_sequence_number.is_some());
        assert!(header.osmosis_replication_base_url.is_some());
        
        assert_eq!(header.osmosis_replication_timestamp.unwrap().as_secs(), 1609459200);
        assert_eq!(header.osmosis_replication_sequence_number.unwrap().as_seq(), 1234);
        assert_eq!(header.osmosis_replication_base_url.unwrap(), "https://planet.openstreetmap.org/replication/minute/");
    }

    #[test]
    fn test_header_bbox_serialization() {
        let bbox = HeaderBBox {
            min_lon: NanoDegree(-1_000_000_000),
            max_lon: NanoDegree(1_000_000_000),
            min_lat: NanoDegree(-500_000_000),
            max_lat: NanoDegree(500_000_000),
        };

        // Test serialization roundtrip
        let serialized = serde_json::to_string(&bbox).unwrap();
        let deserialized: HeaderBBox = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(bbox, deserialized);
    }

    #[test]
    fn test_performance_header_block_creation() {
        use std::time::Instant;
        
        let start = Instant::now();
        let mut headers = Vec::with_capacity(100_000);
        
        /// Performance target: Create 100k headers in under 50ms
        for i in 0..100_000 {
            let mut header = HeaderBlock::default();
            header.required_features.push(format!("Feature-{}", i).into());
            header.osmosis_replication_sequence_number = OsmosisSequenceNumber::new(i as i64);
            headers.push(header);
        }
        
        let duration = start.elapsed();
        assert!(duration.as_millis() < 50, "Creating 100k headers took {:?}, expected < 50ms", duration);
        assert_eq!(headers.len(), 100_000);
        
        /// Performance target: Memory usage optimization
        let memory_per_header = std::mem::size_of::<HeaderBlock>();
        assert!(memory_per_header < 200, "HeaderBlock size is {} bytes, should be < 200", memory_per_header);
    }

    #[test]
    fn test_performance_header_bbox_operations() {
        use std::time::Instant;
        
        let start = Instant::now();
        let mut bboxes = Vec::with_capacity(1_000_000);
        
        /// Performance target: Create 1M bboxes in under 20ms
        for i in 0..1_000_000 {
            let bbox = HeaderBBox {
                min_lon: NanoDegree(-(i as i64) * 1_000_000),
                max_lon: NanoDegree((i as i64) * 1_000_000),
                min_lat: NanoDegree(-(i as i64) * 500_000),
                max_lat: NanoDegree((i as i64) * 500_000),
            };
            bboxes.push(bbox);
        }
        
        let duration = start.elapsed();
        assert!(duration.as_millis() < 20, "Creating 1M bboxes took {:?}, expected < 20ms", duration);
        assert_eq!(bboxes.len(), 1_000_000);
        
        /// Performance target: Zero-cost coordinate access
        let access_start = Instant::now();
        let mut sum = 0i64;
        for bbox in &bboxes[..10_000] {
            sum += bbox.min_lon.0 + bbox.max_lon.0 + bbox.min_lat.0 + bbox.max_lat.0;
        }
        let access_duration = access_start.elapsed();
        assert!(access_duration.as_micros() < 500, "Accessing 10k bbox coordinates took {:?}, expected < 500µs", access_duration);
        assert_ne!(sum, 0); // Ensure computation wasn't optimized away
    }

    #[test]
    fn test_performance_string_interning_simulation() {
        use std::time::Instant;
        use std::collections::HashMap;
        
        /// Performance test: Simulate string interning for common OSM features
        let common_features = vec![
            "OsmSchema-V0.6", "DenseNodes", "HistoricalInformation",
            "LocationsOnWays", "Relations", "Ways", "Nodes"
        ];
        
        let start = Instant::now();
        let mut headers = Vec::with_capacity(50_000);
        let mut feature_cache: HashMap<&str, Cow<str>> = HashMap::new();
        
        for i in 0..50_000 {
            let mut header = HeaderBlock::default();
            
            // Simulate efficient string reuse
            for &feature in &common_features {
                let interned = feature_cache.entry(feature)
                    .or_insert_with(|| feature.into())
                    .clone();
                header.required_features.push(interned);
            }
            
            headers.push(header);
        }
        
        let duration = start.elapsed();
        assert!(duration.as_millis() < 100, "String interning simulation took {:?}, expected < 100ms", duration);
        assert_eq!(headers.len(), 50_000);
        assert_eq!(feature_cache.len(), common_features.len());
    }

    #[test]
    fn test_performance_timestamp_validation() {
        use std::time::Instant;
        
        /// Performance test: Timestamp validation should be extremely fast
        let timestamps = (0..1_000_000i64).collect::<Vec<_>>();
        
        let start = Instant::now();
        let valid_timestamps: Vec<_> = timestamps
            .iter()
            .filter_map(|&ts| OsmosisReplicationTimestamp::new(ts))
            .collect();
        let duration = start.elapsed();
        
        assert!(duration.as_millis() < 30, "Validating 1M timestamps took {:?}, expected < 30ms", duration);
        assert_eq!(valid_timestamps.len(), 1_000_000);
        
        /// Performance test: Invalid timestamp rejection
        let invalid_start = Instant::now();
        let invalid_timestamps: Vec<_> = (-1_000_000..0i64)
            .filter_map(|ts| OsmosisReplicationTimestamp::new(ts))
            .collect();
        let invalid_duration = invalid_start.elapsed();
        
        assert!(invalid_duration.as_millis() < 20, "Rejecting 1M invalid timestamps took {:?}, expected < 20ms", invalid_duration);
        assert_eq!(invalid_timestamps.len(), 0);
    }

    #[test]
    fn test_performance_sequence_number_validation() {
        use std::time::Instant;
        
        /// Performance test: Sequence number validation with large ranges
        let start = Instant::now();
        let mut valid_count = 0;
        let mut invalid_count = 0;
        
        for i in -500_000..500_000i64 {
            match OsmosisSequenceNumber::new(i) {
                Some(_) => valid_count += 1,
                None => invalid_count += 1,
            }
        }
        
        let duration = start.elapsed();
        assert!(duration.as_millis() < 50, "Validating 1M sequence numbers took {:?}, expected < 50ms", duration);
        assert_eq!(valid_count, 500_000);
        assert_eq!(invalid_count, 500_000);
    }

    #[test]
    fn test_performance_header_serialization() {
        use std::time::Instant;
        
        /// Performance test: Serialization/deserialization throughput
        let mut header = HeaderBlock::default();
        header.required_features = vec!["OsmSchema-V0.6".into(), "DenseNodes".into()];
        header.optional_features = vec!["HistoricalInformation".into()];
        header.writing_program = "osmosis-0.47";
        header.source = "OpenStreetMap contributors";
        header.osmosis_replication_timestamp = OsmosisReplicationTimestamp::new(1609459200);
        header.osmosis_replication_sequence_number = OsmosisSequenceNumber::new(12345);
        
        // Serialization performance
        let ser_start = Instant::now();
        let mut serialized_headers = Vec::with_capacity(10_000);
        for _ in 0..10_000 {
            let serialized = serde_json::to_string(&header).unwrap();
            serialized_headers.push(serialized);
        }
        let ser_duration = ser_start.elapsed();
        
        assert!(ser_duration.as_millis() < 100, "Serializing 10k headers took {:?}, expected < 100ms", ser_duration);
        
        // Deserialization performance
        let deser_start = Instant::now();
        let mut deserialized_headers = Vec::with_capacity(10_000);
        for serialized in &serialized_headers {
            let deserialized: HeaderBlock = serde_json::from_str(serialized).unwrap();
            deserialized_headers.push(deserialized);
        }
        let deser_duration = deser_start.elapsed();
        
        assert!(deser_duration.as_millis() < 150, "Deserializing 10k headers took {:?}, expected < 150ms", deser_duration);
        assert_eq!(deserialized_headers.len(), 10_000);
    }

    #[test]
    fn test_performance_memory_layout_optimization() {
        use std::mem;
        
        /// Performance test: Ensure optimal memory layout
        let header_size = mem::size_of::<HeaderBlock>();
        let bbox_size = mem::size_of::<HeaderBBox>();
        let timestamp_size = mem::size_of::<OsmosisReplicationTimestamp>();
        let sequence_size = mem::size_of::<OsmosisSequenceNumber>();
        
        // Memory efficiency assertions
        assert!(header_size <= 200, "HeaderBlock size is {} bytes, should be ≤ 200", header_size);
        assert!(bbox_size <= 32, "HeaderBBox size is {} bytes, should be ≤ 32", bbox_size);
        assert_eq!(timestamp_size, 8, "OsmosisReplicationTimestamp should be exactly 8 bytes");
        assert_eq!(sequence_size, 8, "OsmosisSequenceNumber should be exactly 8 bytes");
        
        /// Performance test: Memory alignment
        assert_eq!(mem::align_of::<HeaderBBox>(), 8, "HeaderBBox should be 8-byte aligned");
        assert_eq!(mem::align_of::<OsmosisReplicationTimestamp>(), 8, "Timestamp should be 8-byte aligned");
        assert_eq!(mem::align_of::<OsmosisSequenceNumber>(), 8, "Sequence number should be 8-byte aligned");
    }

    #[test]
    fn test_performance_batch_operations() {
        use std::time::Instant;
        
        /// Performance test: Batch processing of headers for planet-scale data
        let start = Instant::now();
        let batch_size = 100_000;
        let mut total_features = 0;
        
        for batch in 0..10 {
            let mut headers = Vec::with_capacity(batch_size);
            
            for i in 0..batch_size {
                let mut header = HeaderBlock::default();
                header.required_features.push(format!("Batch-{}-Feature-{}", batch, i).into());
                
                if i % 1000 == 0 {
                    header.osmosis_replication_timestamp = OsmosisReplicationTimestamp::new(i as i64);
                }
                
                total_features += header.required_features.len();
                headers.push(header);
            }
        }
        
        let duration = start.elapsed();
        assert!(duration.as_millis() < 500, "Processing 1M headers in batches took {:?}, expected < 500ms", duration);
        assert_eq!(total_features, 1_000_000);
    }

    #[test]
    fn test_osmosis_timestamp_edge_cases() {
        // Test boundary values
        assert!(OsmosisReplicationTimestamp::new(0).is_some());
        assert!(OsmosisReplicationTimestamp::new(i64::MAX).is_some());
        assert!(OsmosisReplicationTimestamp::new(-1).is_none());
        
        // Test Unix epoch boundaries
        let epoch = OsmosisReplicationTimestamp::new(0).unwrap();
        assert_eq!(epoch.as_secs(), 0);
        
        // Test year 2038 problem boundary (32-bit signed int)
        let y2038 = OsmosisReplicationTimestamp::new(2147483647).unwrap();
        assert_eq!(y2038.as_secs(), 2147483647);
    }

    #[test]
    fn test_sequence_number_edge_cases() {
        // Test boundary values
        assert!(OsmosisSequenceNumber::new(0).is_some());
        assert!(OsmosisSequenceNumber::new(i64::MAX).is_some());
        assert!(OsmosisSequenceNumber::new(-1).is_none());
        
        // Test typical replication sequence ranges
        let seq_low = OsmosisSequenceNumber::new(1).unwrap();
        assert_eq!(seq_low.as_seq(), 1);
        
        let seq_high = OsmosisSequenceNumber::new(999999999).unwrap();
        assert_eq!(seq_high.as_seq(), 999999999);
    }
}
