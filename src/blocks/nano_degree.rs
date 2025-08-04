
/// Represents a value in nanodegrees (1e-9 degrees).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NanoDegree(pub i64);

impl NanoDegree {
    /// Creates a new NanoDegree from a value in nanodegrees.
    pub fn new(nd: i64) -> Self {
        assert!(
            (-1_800_000_000..=1_800_000_000).contains(&nd),
            "NanoDegree must be in the range [-180e7, 180e7] (longitude/latitude bounds)"
        );

        NanoDegree(nd)
    }

    /// Converts the NanoDegree to degrees.
    pub fn to_degrees(self) -> f64 {
        self.0 as f64 * 1e-9
    }

    /// Creates a NanoDegree from a value in degrees.
    pub fn from_degrees(deg: f64) -> Self {
        let nd = (deg * 1e9) as i64;
        NanoDegree::new(nd)
    }

    /// Creates a NanoDegree from latitude in degrees.
    /// Validates latitude range [-90, 90].
    pub fn from_latitude(lat: f64) -> Result<Self, &'static str> {
        if !(-90.0..=90.0).contains(&lat) {
            return Err("Latitude must be in range [-90, 90]");
        }
        Ok(NanoDegree((lat * 1e9) as i64))
    }

    /// Creates a NanoDegree from longitude in degrees.
    /// Validates longitude range [-180, 180].
    pub fn from_longitude(lon: f64) -> Result<Self, &'static str> {
        if !(-180.0..=180.0).contains(&lon) {
            return Err("Longitude must be in range [-180, 180]");
        }
        Ok(NanoDegree((lon * 1e9) as i64))
    }

    /// Returns the raw nanodegree value.
    pub fn raw(self) -> i64 {
        self.0
    }

    /// Returns true if this represents a valid latitude.
    pub fn is_valid_latitude(self) -> bool {
        (-900_000_000..=900_000_000).contains(&self.0)
    }

    /// Returns true if this represents a valid longitude.
    pub fn is_valid_longitude(self) -> bool {
        (-1_800_000_000..=1_800_000_000).contains(&self.0)
    }
}

// Implement From<f64> for NanoDegree
impl From<f64> for NanoDegree {
    fn from(deg: f64) -> Self {
        NanoDegree::from_degrees(deg)
    }
}

// Implement From<NanoDegree> for f64
impl From<NanoDegree> for f64 {
    fn from(nd: NanoDegree) -> Self {
        nd.to_degrees()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_nano_degree_creation() {
        let nd = NanoDegree::new(900_000_000); // 90 degrees
        assert_eq!(nd.0, 900_000_000);
        assert_eq!(nd.raw(), 900_000_000);
    }

    #[test]
    fn test_nano_degree_to_degrees() {
        let nd = NanoDegree::new(900_000_000); // 90 degrees
        assert!((nd.to_degrees() - 90.0).abs() < 1e-10);
        
        let nd = NanoDegree::new(-1_800_000_000); // -180 degrees
        assert!((nd.to_degrees() - (-180.0)).abs() < 1e-10);
        
        let nd = NanoDegree::new(0);
        assert_eq!(nd.to_degrees(), 0.0);
    }

    #[test]
    fn test_nano_degree_from_degrees() {
        let nd = NanoDegree::from_degrees(90.0);
        assert_eq!(nd.0, 900_000_000);
        
        let nd = NanoDegree::from_degrees(-180.0);
        assert_eq!(nd.0, -1_800_000_000);
        
        let nd = NanoDegree::from_degrees(0.0);
        assert_eq!(nd.0, 0);
    }

    #[test]
    fn test_latitude_validation() {
        // Valid latitudes
        assert!(NanoDegree::from_latitude(90.0).is_ok());
        assert!(NanoDegree::from_latitude(-90.0).is_ok());
        assert!(NanoDegree::from_latitude(0.0).is_ok());
        assert!(NanoDegree::from_latitude(45.5).is_ok());
        
        // Invalid latitudes
        assert!(NanoDegree::from_latitude(90.1).is_err());
        assert!(NanoDegree::from_latitude(-90.1).is_err());
        assert!(NanoDegree::from_latitude(180.0).is_err());
    }

    #[test]
    fn test_longitude_validation() {
        // Valid longitudes
        assert!(NanoDegree::from_longitude(180.0).is_ok());
        assert!(NanoDegree::from_longitude(-180.0).is_ok());
        assert!(NanoDegree::from_longitude(0.0).is_ok());
        assert!(NanoDegree::from_longitude(123.45).is_ok());
        
        // Invalid longitudes
        assert!(NanoDegree::from_longitude(180.1).is_err());
        assert!(NanoDegree::from_longitude(-180.1).is_err());
        assert!(NanoDegree::from_longitude(360.0).is_err());
    }

    #[test]
    fn test_is_valid_latitude() {
        let valid_lat = NanoDegree::new(900_000_000); // 90 degrees
        assert!(valid_lat.is_valid_latitude());
        
        let valid_lat = NanoDegree::new(-900_000_000); // -90 degrees
        assert!(valid_lat.is_valid_latitude());
        
        let invalid_lat = NanoDegree::new(1_000_000_000); // 100 degrees
        assert!(!invalid_lat.is_valid_latitude());
    }

    #[test]
    fn test_is_valid_longitude() {
        let valid_lon = NanoDegree::new(1_800_000_000); // 180 degrees
        assert!(valid_lon.is_valid_longitude());
        
        let valid_lon = NanoDegree::new(-1_800_000_000); // -180 degrees
        assert!(valid_lon.is_valid_longitude());
        
        let valid_lon = NanoDegree::new(0); // 0 degrees
        assert!(valid_lon.is_valid_longitude());
    }

    #[test]
    fn test_from_trait_implementation() {
        let nd: NanoDegree = 90.0.into();
        assert_eq!(nd.0, 900_000_000);
        
        let deg: f64 = nd.into();
        assert!((deg - 90.0).abs() < 1e-10);
    }

    #[test]
    fn test_serialization() {
        let nd = NanoDegree::new(123_456_789);
        let serialized = serde_json::to_string(&nd).unwrap();
        let deserialized: NanoDegree = serde_json::from_str(&serialized).unwrap();
        assert_eq!(nd, deserialized);
    }

    #[test]
    fn test_precision_and_rounding() {
        // Test high precision coordinates
        let precise_coord = 12.123456789;
        let nd = NanoDegree::from_degrees(precise_coord);
        let back_to_degrees = nd.to_degrees();
        
        // Should maintain reasonable precision (within nanodegree accuracy)
        assert!((back_to_degrees - precise_coord).abs() < 1e-8);
    }

    #[test]
    #[should_panic(expected = "NanoDegree must be in the range")]
    fn test_panic_on_invalid_range() {
        NanoDegree::new(2_000_000_000); // Beyond valid range
    }

    #[test]
    fn test_performance_conversion_operations() {
        use std::time::Instant;
        
        let start = Instant::now();
        let mut sum = 0.0;
        
        for i in 0..100_000 {
            let degrees = (i as f64) / 1000.0 - 50.0; // Range around [-50, 50]
            if degrees.abs() <= 90.0 {
                let nd = NanoDegree::from_degrees(degrees);
                sum += nd.to_degrees();
            }
        }
        
        let duration = start.elapsed();
        assert!(duration.as_millis() < 50); // Should be very fast
        assert!(sum.abs() > 0.0); // Ensure calculation happened
    }

    #[test]
    fn test_performance_batch_validation() {
        use std::time::Instant;
        
        let coords: Vec<f64> = (0..10_000)
            .map(|i| (i as f64) / 100.0 - 50.0) // Range [-50, 50]
            .collect();
        
        let start = Instant::now();
        let valid_coords: Vec<NanoDegree> = coords
            .iter()
            .filter_map(|&coord| {
                if coord.abs() <= 90.0 {
                    Some(NanoDegree::from_degrees(coord))
                } else {
                    None
                }
            })
            .collect();
        
        let duration = start.elapsed();
        assert!(duration.as_millis() < 20); // Should process 10k coords quickly
        assert!(!valid_coords.is_empty());
    }

    #[test]
    fn test_edge_case_coordinates() {
        // Test exact boundary values
        let max_lat = NanoDegree::from_latitude(90.0).unwrap();
        assert!(max_lat.is_valid_latitude());
        
        let min_lat = NanoDegree::from_latitude(-90.0).unwrap();
        assert!(min_lat.is_valid_latitude());
        
        let max_lon = NanoDegree::from_longitude(180.0).unwrap();
        assert!(max_lon.is_valid_longitude());
        
        let min_lon = NanoDegree::from_longitude(-180.0).unwrap();
        assert!(min_lon.is_valid_longitude());
        
        // Test zero
        let zero = NanoDegree::new(0);
        assert!(zero.is_valid_latitude());
        assert!(zero.is_valid_longitude());
    }

    #[test]
    fn test_equality_and_hashing() {
        use std::collections::HashSet;
        
        let nd1 = NanoDegree::new(123_456_789);
        let nd2 = NanoDegree::new(123_456_789);
        let nd3 = NanoDegree::new(987_654_321);
        
        assert_eq!(nd1, nd2);
        assert_ne!(nd1, nd3);
        
        let mut set = HashSet::new();
        set.insert(nd1);
        set.insert(nd2); // Should not add duplicate
        set.insert(nd3);
        
        assert_eq!(set.len(), 2);
    }
}