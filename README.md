# OSM PBF - High-Performance OpenStreetMap Data Processing Library

A Rust library for reading and processing OpenStreetMap PBF (Protocol Buffer Format) files with enterprise-grade performance and developer-friendly APIs.

## Features

### 🚀 High-Level Zero-Boilerplate API
- **Reader**: One-liner access to OSM elements with streaming and parallel processing
- **Element Filtering**: Efficient filtering by type, ID ranges, and tags
- **Business-Grade Parallelism**: Rayon-backed parallel processing for maximum throughput

### ⚡ Performance-Oriented Architecture
- **IndexedReader**: Random access with efficient blob indexing
- **Streaming**: Memory-efficient processing of planet-scale datasets
- **Low-Level Control**: Direct blob access for custom processing pipelines

### 🏗️ Robust Foundation
- **Binary Data Handling**: Native support for PBF compression (zlib, lzma, bzip2)
- **Error Handling**: Comprehensive error types with detailed context
- **Extensible Design**: Pluggable for any `Read + Seek` source

## Quick Start

### Basic Usage

```rust
use osm_pbf::{Reader, OsmElement};
use std::fs::File;

// Open and process a PBF file
let file = File::open("map.osm.pbf")?;
let mut reader = Reader::new(file)?;

// Count all nodes
let mut node_count = 0;
reader.for_each(|element| {
    if matches!(element, OsmElement::Node(_)) {
        node_count += 1;
    }
    Ok(())
})?;

println!("Found {} nodes", node_count);
```

### Filtered Processing

```rust
use osm_pbf::{Reader, ElementFilter, OsmElement};
use std::fs::File;

let file = File::open("map.osm.pbf")?;
let mut reader = Reader::new(file)?;

// Process only highways
let filter = ElementFilter::ways_only(false)
    .with_tag_key("highway".to_string());

reader.for_each_filtered(&filter, |element| {
    if let OsmElement::Way(way) = element {
        println!("Highway way: {}", way.id);
    }
    Ok(())
})?;
```

### Parallel Processing

```rust
use osm_pbf::{Reader, ParallelConfig, OsmElement};
use std::fs::File;

let file = File::open("large_map.osm.pbf")?;
let mut reader = Reader::new(file)?;

let config = ParallelConfig::default();

// Parallel count of highway ways
let highway_count = reader.par_map_reduce(
    &config,
    |element| match element {
        OsmElement::Way(way) if way.keys.contains(&1) => 1u64, // Assuming key 1 is "highway"
        _ => 0u64,
    },
    || 0u64,
    |a, b| a + b,
    0u64,
)?;

println!("Total highways: {}", highway_count);
```

## Advanced Usage

### IndexedReader for Random Access

```rust
use osm_pbf::IndexedReader;
use std::fs::File;

let file = File::open("map.osm.pbf")?;
let mut indexed_reader = IndexedReader::new(file)?;

// Get file statistics
let stats = indexed_reader.statistics();
println!("File contains {} blobs", stats.total_blobs);

// Random access to specific blobs
if let Some(blob) = indexed_reader.read_blob_by_index(0)? {
    println!("First blob has {} bytes", blob.size());
}

// Stream with filtering
let filter = ElementFilter::nodes_only();
for blob_result in indexed_reader.stream_filtered(&filter) {
    let blob = blob_result?;
    // Process blob...
}
```

### Low-Level Blob Access

```rust
use osm_pbf::{Blob, BlobType};
use bytes::Bytes;

// Create a blob
let data = Bytes::from(vec![1, 2, 3, 4]);
let blob = Blob::new_raw(BlobType::OSMData, data, 0)?;

println!("Blob type: {:?}", blob.blob_type());
println!("Blob size: {}", blob.size());
```

## API Overview

### Core Types

- **`Reader<R>`**: High-level, zero-boilerplate entry point
- **`IndexedReader<R>`**: Efficient random access and streaming
- **`Blob`**: Binary data block with compression support
- **`OsmElement`**: Unified enum for all OSM element types
- **`ElementFilter`**: Configurable filtering for selective processing

### Element Types

```rust
pub enum OsmElement {
    Node(Node),
    Way(Way),
    Relation(Relation),
    ChangeSet(ChangeSet),
}
```

### Processing Patterns

1. **Sequential Streaming**: `for_each()` - Memory efficient, single-threaded
2. **Filtered Streaming**: `for_each_filtered()` - Apply filters during extraction
3. **Parallel Processing**: `par_map_reduce()` - Leverage all CPU cores
4. **Collection**: `collect_filtered()` - Load small datasets into memory
5. **Specialized**: `nodes()`, `ways()` - Type-specific extraction

## Dependencies

- **serde**: Serialization support
- **bytes**: Efficient binary data handling
- **rayon**: Parallel processing
- **thiserror**: Ergonomic error handling
- **url**: URL parsing utilities
- **tokio** (optional): Async I/O support

## Architecture

```
osm-pbf/
├── io/
│   ├── reader.rs          # High-level API
│   ├── indexed_reader.rs  # Indexed access
│   └── blob.rs           # Binary data handling
├── blocks/
│   ├── primitives/       # OSM data structures
│   └── headers/          # File metadata
└── prelude.rs           # Convenient imports
```

## Performance Characteristics

- **Memory Efficient**: Streaming design, minimal allocations
- **CPU Optimized**: Parallel processing with work-stealing
- **I/O Efficient**: Indexed access, minimal seeking
- **Scalable**: Handles planet-scale datasets (50GB+)

## Error Handling

All operations return `Result<T, BlobError>` with detailed error context:

```rust
use osm_pbf::BlobError;

match reader.for_each(|_| Ok(())) {
    Ok(stats) => println!("Processed {} elements", stats.elements_processed),
    Err(BlobError::Io(e)) => eprintln!("I/O error: {}", e),
    Err(BlobError::InvalidFormat(msg)) => eprintln!("Format error: {}", msg),
    Err(BlobError::SizeLimit(msg)) => eprintln!("Size limit: {}", msg),
}
```

## Contributing

This library is designed for enterprise-scale OSM data processing. Contributions are welcome for:

- Performance optimizations
- Additional compression formats
- Enhanced filtering capabilities
- Documentation improvements

## License

Licensed under the MIT license.