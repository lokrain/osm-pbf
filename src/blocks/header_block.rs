use std::borrow::Cow; 

use crate::blocks::nano_degree::NanoDegree;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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

impl<'a> Default for HeaderBlock<'a> {
    fn default() -> Self {
        Self {
            required_features: Vec::new(),
            optional_features: Vec::new(),
            writing_program: "",
            source: "",
            osmosis_replication_timestamp: None,
            osmosis_replication_sequence_number: None,
            osmosis_replication_base_url: None,
        }
    }
}
