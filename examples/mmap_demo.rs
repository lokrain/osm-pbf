// Example demonstrating the memory-mapped blob reader for high-performance OSM data access
// 
// This example shows how to use MmapBlobReader for zero-copy, enterprise-grade throughput

#[cfg(feature = "mmap")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use osm_pbf::{MmapBlobReader, ParallelMmapBlobReader, ElementFilter};
    use std::io::Write;
    use tempfile::NamedTempFile;
    use rayon::prelude::*;

    println!("=== Memory-Mapped OSM PBF Reader Demo ===\n");

    // Create a temporary test file with some sample data
    let mut temp_file = NamedTempFile::new()?;
    
    // Write sample blob data (simplified structure)
    for i in 0..5 {
        let test_data = format!("Sample OSM blob data #{}", i).into_bytes();
        let blob_size = test_data.len() as u32;
        
        // Write blob size (4 bytes, big-endian)
        temp_file.write_all(&blob_size.to_be_bytes())?;
        // Write blob data
        temp_file.write_all(&test_data)?;
    }
    temp_file.flush()?;

    // Example 1: Basic memory-mapped reading
    println!("1. Basic Memory-Mapped Reading:");
    let reader = MmapBlobReader::from_file(temp_file.reopen()?)?;
    
    println!("   - File size: {} bytes", reader.file_size());
    println!("   - Blob count: {}", reader.blob_count());
    println!("   - Supports parallel access: {}", reader.supports_parallel_access());
    
    // Example 2: Zero-copy blob access
    println!("\n2. Zero-Copy Blob Access:");
    for i in 0..reader.blob_count().min(3) {
        if let Some(blob) = reader.read_blob_by_index(i)? {
            println!("   - Blob {}: {} bytes at offset {}", 
                     i, blob.raw_size(), blob.offset());
        }
    }
    
    // Example 3: Statistics
    println!("\n3. File Statistics:");
    let stats = reader.statistics();
    println!("   - Total blobs: {}", stats.total_blobs);
    println!("   - Header blobs: {}", stats.header_blobs);
    println!("   - Data blobs: {}", stats.data_blobs);
    
    // Example 4: Streaming with filtering (same API as IndexedReader)
    println!("\n4. Filtered Streaming:");
    let filter = ElementFilter::all(); // Accept all elements
    let mut blob_count = 0;
    
    for blob_result in reader.stream_filtered(&filter) {
        match blob_result {
            Ok(blob) => {
                blob_count += 1;
                println!("   - Streamed blob: {} bytes", blob.raw_size());
            }
            Err(e) => println!("   - Error: {}", e),
        }
    }
    println!("   - Total streamed blobs: {}", blob_count);
    
    // Example 5: Parallel processing
    println!("\n5. Parallel Processing:");
    let parallel_reader = ParallelMmapBlobReader::from_reader(&reader);
    
    // Process blobs in parallel using Rayon
    let blob_indices: Vec<usize> = (0..reader.blob_count()).collect();
    let total_size: u32 = blob_indices
        .par_iter()
        .map(|&index| {
            parallel_reader
                .read_blob_by_index(index)
                .unwrap_or(None)
                .map(|blob| blob.raw_size())
                .unwrap_or(0)
        })
        .sum();
    
    println!("   - Total size of all blobs (parallel): {} bytes", total_size);
    
    // Example 6: Advanced - Raw slice access
    println!("\n6. Advanced Raw Access:");
    if reader.file_size() > 0 {
        // Get first 10 bytes as raw slice (zero-copy)
        let slice = reader.get_raw_slice(0, 10.min(reader.file_size() as usize))?;
        println!("   - First 10 bytes: {:?}", slice);
    }
    
    // Example 7: ID range lookup (for indexed access)
    println!("\n7. ID Range Lookup:");
    let blob_indices = reader.find_blobs_for_id_range(1000, 2000);
    println!("   - Blobs potentially containing IDs 1000-2000: {:?}", blob_indices);
    
    println!("\n=== Performance Benefits ===");
    println!("✓ Zero-copy memory mapping");
    println!("✓ OS page cache utilization");
    println!("✓ Parallel-safe concurrent access");
    println!("✓ Minimal memory footprint");
    println!("✓ Same API as traditional readers");
    
    Ok(())
}

#[cfg(not(feature = "mmap"))]
fn main() {
    println!("This example requires the 'mmap' feature.");
    println!("Run with: cargo run --example mmap_demo --features mmap");
}
