use bytes::Bytes;
use thiserror::Error;
use std::str::FromStr;

/// Maximum size for a BlobHeader: 64 KiB (65,536 bytes)
pub const MAX_BLOB_HEADER_SIZE: usize = 65_536;

/// Maximum size for a Blob (uncompressed): 32 MiB (33,554,432 bytes)
pub const MAX_BLOB_MESSAGE_SIZE: usize = 33_554_432;

/// Errors that can occur when working with Blobs
#[derive(Error, Debug)]
pub enum BlobError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Blob header too large: {size} bytes (max: {max} bytes)")]
    HeaderTooLarge { size: usize, max: usize },
    
    #[error("Blob message too large: {size} bytes (max: {max} bytes)")]
    MessageTooLarge { size: usize, max: usize },
    
    #[error("Invalid blob format: {0}")]
    InvalidFormat(String),
    
    #[error("Compression error: {0}")]
    Compression(String),
    
    #[error("Unknown blob type: {0}")]
    UnknownType(String),
}

pub type Result<T> = std::result::Result<T, BlobError>;

/// Represents the type of data contained in a Blob
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobType {
    /// OSM file metadata (HeaderBlock)
    OSMHeader,
    /// Actual OSM map elements (PrimitiveBlock)
    OSMData,
    /// Non-standard blob with custom identifier
    Unknown(String),
}

impl FromStr for BlobType {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result< BlobType, ()> {
        Ok(match s {
            "OSMHeader" => BlobType::OSMHeader,
            "OSMData" => BlobType::OSMData,
            other => BlobType::Unknown(other.to_string()),
        })
    }
}

impl BlobType {
    /// Returns the string identifier for this BlobType
    pub fn as_str(&self) -> &str {
        match self {
            BlobType::OSMHeader => "OSMHeader",
            BlobType::OSMData => "OSMData",
            BlobType::Unknown(s) => s,
        }
    }
}

/// Header for a Blob, containing metadata about the blob's content
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobHeader {
    /// Type of data in the blob
    pub blob_type: BlobType,
    /// Size of the blob data in bytes
    pub datasize: u32,
    /// Optional index data (for future use)
    pub indexdata: Option<Bytes>,
}

impl BlobHeader {
    /// Creates a new BlobHeader
    pub fn new(blob_type: BlobType, datasize: u32) -> Self {
        Self {
            blob_type,
            datasize,
            indexdata: None,
        }
    }
    
    /// Validates that the header size doesn't exceed limits
    pub fn validate_size(&self, header_size: usize) -> Result<()> {
        if header_size > MAX_BLOB_HEADER_SIZE {
            return Err(BlobError::HeaderTooLarge {
                size: header_size,
                max: MAX_BLOB_HEADER_SIZE,
            });
        }
        Ok(())
    }
}

/// A Blob represents a binary data block in the OSM PBF file
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob {
    /// Header containing metadata about this blob
    pub header: BlobHeader,
    /// The actual data, either raw or compressed
    pub data: BlobData,
    /// Byte offset in the file for precise navigation
    pub offset: u64,
}

/// Represents the data contained in a Blob, which can be compressed or raw
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobData {
    /// Raw, uncompressed data
    Raw(Bytes),
    /// Zlib-compressed data with original size
    ZlibData { 
        compressed: Bytes, 
        raw_size: u32 
    },
    /// LZMA-compressed data with original size (for future use)
    LzmaData { 
        compressed: Bytes, 
        raw_size: u32 
    },
    /// Bzip2-compressed data with original size (for future use)
    Bzip2Data { 
        compressed: Bytes, 
        raw_size: u32 
    },
}

impl BlobData {
    /// Returns the uncompressed size of the data
    pub fn raw_size(&self) -> u32 {
        match self {
            BlobData::Raw(data) => data.len() as u32,
            BlobData::ZlibData { raw_size, .. } => *raw_size,
            BlobData::LzmaData { raw_size, .. } => *raw_size,
            BlobData::Bzip2Data { raw_size, .. } => *raw_size,
        }
    }
    
    /// Returns true if the data is compressed
    pub fn is_compressed(&self) -> bool {
        !matches!(self, BlobData::Raw(_))
    }
    
    /// Validates that the uncompressed size doesn't exceed limits
    pub fn validate_size(&self) -> Result<()> {
        let size = self.raw_size() as usize;
        if size > MAX_BLOB_MESSAGE_SIZE {
            return Err(BlobError::MessageTooLarge {
                size,
                max: MAX_BLOB_MESSAGE_SIZE,
            });
        }
        Ok(())
    }
}

impl Blob {
    /// Creates a new Blob with raw data
    pub fn new_raw(blob_type: BlobType, data: Bytes, offset: u64) -> Result<Self> {
        let header = BlobHeader::new(blob_type, data.len() as u32);
        let blob_data = BlobData::Raw(data);
        
        // Validate sizes
        blob_data.validate_size()?;
        
        Ok(Self {
            header,
            data: blob_data,
            offset,
        })
    }
    
    /// Creates a new Blob with zlib-compressed data
    pub fn new_zlib(blob_type: BlobType, compressed: Bytes, raw_size: u32, offset: u64) -> Result<Self> {
        let header = BlobHeader::new(blob_type, compressed.len() as u32);
        let blob_data = BlobData::ZlibData { compressed, raw_size };
        
        // Validate sizes
        blob_data.validate_size()?;
        
        Ok(Self {
            header,
            data: blob_data,
            offset,
        })
    }
    
    /// Returns the type of data contained in this blob
    pub fn blob_type(&self) -> &BlobType {
        &self.header.blob_type
    }
    
    /// Returns the byte offset of this blob in the file
    pub fn offset(&self) -> u64 {
        self.offset
    }
    
    /// Returns the size of the compressed/raw data
    pub fn compressed_size(&self) -> u32 {
        self.header.datasize
    }
    
    /// Returns the size of the uncompressed data
    pub fn raw_size(&self) -> u32 {
        self.data.raw_size()
    }
    
    /// Returns true if this blob contains compressed data
    pub fn is_compressed(&self) -> bool {
        self.data.is_compressed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_blob_type_conversion() {
        use std::str::FromStr;
        assert_eq!(BlobType::from_str("OSMHeader").unwrap(), BlobType::OSMHeader);
        assert_eq!(BlobType::from_str("OSMData").unwrap(), BlobType::OSMData);
        assert_eq!(BlobType::from_str("Custom").unwrap(), BlobType::Unknown("Custom".to_string()));
        
        assert_eq!(BlobType::OSMHeader.as_str(), "OSMHeader");
        assert_eq!(BlobType::OSMData.as_str(), "OSMData");
        assert_eq!(BlobType::Unknown("Custom".to_string()).as_str(), "Custom");
    }
    
    #[test]
    fn test_blob_creation() {
        let data = Bytes::from("Hello, OSM!");
        let blob = Blob::new_raw(BlobType::OSMData, data.clone(), 1024).unwrap();
        
        assert_eq!(blob.blob_type(), &BlobType::OSMData);
        assert_eq!(blob.offset(), 1024);
        assert_eq!(blob.compressed_size(), data.len() as u32);
        assert_eq!(blob.raw_size(), data.len() as u32);
        assert!(!blob.is_compressed());
    }
    
    #[test]
    fn test_blob_size_validation() {
        // Test that oversized blobs are rejected
        let large_data = Bytes::from(vec![0u8; MAX_BLOB_MESSAGE_SIZE + 1]);
        let result = Blob::new_raw(BlobType::OSMData, large_data, 0);
        
        assert!(result.is_err());
        match result.unwrap_err() {
            BlobError::MessageTooLarge { size, max } => {
                assert_eq!(size, MAX_BLOB_MESSAGE_SIZE + 1);
                assert_eq!(max, MAX_BLOB_MESSAGE_SIZE);
            }
            _ => panic!("Expected MessageTooLarge error"),
        }
    }
    
    #[test]
    fn test_compressed_blob() {
        let compressed = Bytes::from("compressed data");
        let raw_size = 1000;
        let blob = Blob::new_zlib(BlobType::OSMHeader, compressed.clone(), raw_size, 2048).unwrap();
        
        assert_eq!(blob.blob_type(), &BlobType::OSMHeader);
        assert_eq!(blob.offset(), 2048);
        assert_eq!(blob.compressed_size(), compressed.len() as u32);
        assert_eq!(blob.raw_size(), raw_size);
        assert!(blob.is_compressed());
    }
}
