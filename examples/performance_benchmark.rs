// Performance comparison between regular file I/O and memory-mapped access
// 
// This benchmark demonstrates the throughput advantages of memory-mapped readers

#[cfg(feature = "mmap")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use osm_pbf::{IndexedReader, MmapBlobReader, ElementFilter};
    use std::io::{Cursor, Write};
    use std::time::Instant;
    use tempfile::NamedTempFile;
    
    println!("=== OSM PBF Performance Benchmark ===\n");
    
    // Create a larger test file to see performance differences
    let mut temp_file = NamedTempFile::new()?;
    let blob_count = 1000;
    
    println!("Creating test file with {} blobs...", blob_count);
    for i in 0..blob_count {
        // Create larger test data to simulate real OSM blobs
        let test_data = format!("OSM PBF blob data #{} - ", i)
            .repeat(100) // Make it larger
            .into_bytes();
        let blob_size = test_data.len() as u32;
        
        temp_file.write_all(&blob_size.to_be_bytes())?;
        temp_file.write_all(&test_data)?;
    }
    temp_file.flush()?;
    
    let file_size = temp_file.as_file().metadata()?.len();
    println!("Test file size: {:.2} MB\n", file_size as f64 / 1024.0 / 1024.0);
    
    // Benchmark 1: Traditional IndexedReader
    println!("1. Traditional IndexedReader Performance:");
    let start = Instant::now();
    let mut indexed_reader = IndexedReader::new(temp_file.reopen()?)?;
    let index_time = start.elapsed();
    
    let start = Instant::now();
    let mut blob_count_regular = 0;
    let mut total_size_regular = 0u64;
    
    for i in 0..indexed_reader.blob_count() {
        if let Some(blob) = indexed_reader.read_blob_by_index(i)? {
            blob_count_regular += 1;
            total_size_regular += blob.raw_size() as u64;
        }
    }
    let read_time = start.elapsed();
    
    println!("   - Index build time: {:?}", index_time);
    println!("   - Read time: {:?}", read_time);
    println!("   - Total time: {:?}", index_time + read_time);
    println!("   - Blobs processed: {}", blob_count_regular);
    println!("   - Total data size: {:.2} MB", total_size_regular as f64 / 1024.0 / 1024.0);
    
    if read_time.as_millis() > 0 {
        let throughput = (total_size_regular as f64 / 1024.0 / 1024.0) / read_time.as_secs_f64();
        println!("   - Throughput: {:.2} MB/s", throughput);
    }
    
    // Benchmark 2: Memory-mapped reader
    println!("\n2. Memory-Mapped Reader Performance:");
    let start = Instant::now();
    let mmap_reader = MmapBlobReader::from_file(temp_file.reopen()?)?;
    let mmap_index_time = start.elapsed();
    
    let start = Instant::now();
    let mut blob_count_mmap = 0;
    let mut total_size_mmap = 0u64;
    
    for i in 0..mmap_reader.blob_count() {
        if let Some(blob) = mmap_reader.read_blob_by_index(i)? {
            blob_count_mmap += 1;
            total_size_mmap += blob.raw_size() as u64;
        }
    }
    let mmap_read_time = start.elapsed();
    
    println!("   - Index build time: {:?}", mmap_index_time);
    println!("   - Read time: {:?}", mmap_read_time);
    println!("   - Total time: {:?}", mmap_index_time + mmap_read_time);
    println!("   - Blobs processed: {}", blob_count_mmap);
    println!("   - Total data size: {:.2} MB", total_size_mmap as f64 / 1024.0 / 1024.0);
    
    if mmap_read_time.as_millis() > 0 {
        let mmap_throughput = (total_size_mmap as f64 / 1024.0 / 1024.0) / mmap_read_time.as_secs_f64();
        println!("   - Throughput: {:.2} MB/s", mmap_throughput);
    }
    
    // Benchmark 3: Streaming performance
    println!("\n3. Streaming Performance Comparison:");
    let filter = ElementFilter::all();
    
    // Regular streaming
    let start = Instant::now();
    let mut stream_count_regular = 0;
    for blob_result in indexed_reader.stream_filtered(&filter) {
        if blob_result.is_ok() {
            stream_count_regular += 1;
        }
    }
    let stream_time_regular = start.elapsed();
    
    // Memory-mapped streaming
    let start = Instant::now();
    let mut stream_count_mmap = 0;
    for blob_result in mmap_reader.stream_filtered(&filter) {
        if blob_result.is_ok() {
            stream_count_mmap += 1;
        }
    }
    let stream_time_mmap = start.elapsed();
    
    println!("   - Regular streaming: {} blobs in {:?}", stream_count_regular, stream_time_regular);
    println!("   - Memory-mapped streaming: {} blobs in {:?}", stream_count_mmap, stream_time_mmap);
    
    // Performance comparison
    println!("\n=== Performance Summary ===");
    
    if read_time > mmap_read_time {
        let speedup = read_time.as_secs_f64() / mmap_read_time.as_secs_f64();
        println!("✓ Memory-mapped reader is {:.2}x faster for random access", speedup);
    } else {
        let slowdown = mmap_read_time.as_secs_f64() / read_time.as_secs_f64();
        println!("⚠ Memory-mapped reader is {:.2}x slower (overhead for small files)", slowdown);
    }
    
    if stream_time_regular > stream_time_mmap {
        let speedup = stream_time_regular.as_secs_f64() / stream_time_mmap.as_secs_f64();
        println!("✓ Memory-mapped streaming is {:.2}x faster", speedup);
    }
    
    println!("\n=== Use Cases for Memory-Mapped Reader ===");
    println!("✓ Large files (>100MB): Significant performance gains");
    println!("✓ Random access patterns: Excellent cache locality");
    println!("✓ Parallel processing: Multiple threads can read concurrently");
    println!("✓ Read-heavy workloads: OS page cache optimization");
    println!("✓ Enterprise ETL: Minimal memory footprint with maximum throughput");
    
    Ok(())
}

#[cfg(not(feature = "mmap"))]
fn main() {
    println!("This benchmark requires the 'mmap' feature.");
    println!("Run with: cargo run --example performance_benchmark --features mmap");
}
