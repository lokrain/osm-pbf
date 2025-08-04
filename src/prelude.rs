pub use crate::blocks::prelude::*;
pub use crate::io::prelude::*;

// Re-export the high-level Reader for convenience
pub use crate::io::reader::{Reader, OsmElement};

// Re-export memory-mapped reader when available
#[cfg(feature = "mmap")]
pub use crate::io::mmap_blob::MmapBlobReader;
