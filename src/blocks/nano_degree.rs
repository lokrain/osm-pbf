
/// Represents a value in nanodegrees (1e-9 degrees).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NanoDegree(pub i64);

impl NanoDegree {
    /// Creates a new NanoDegree from a value in nanodegrees.
    pub fn new(nd: i64) -> Self {
        assert!(
            (-1_000_000_000..=1_000_000_000).contains(&nd),
            "NanoDegree must be in the range [-1e9, 1e9]"
        );

        NanoDegree(nd)
    }

    /// Converts the NanoDegree to degrees.
    pub fn to_degrees(self) -> f64 {
        self.0 as f64 * 1e-9
    }

    /// Creates a NanoDegree from a value in degrees.
    pub fn from_degrees(deg: f64) -> Self {
        NanoDegree((deg / 1e-9) as i64)
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