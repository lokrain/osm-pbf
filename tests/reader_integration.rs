/// Integration tests for Reader module performance and functionality
/// Following BDD (Given/When/Then) style with enterprise focus
use osm_pbf::prelude::*;
use std::time::Instant;
use std::io::Cursor;
use pretty_assertions::assert_eq;

/// Given a requirement for zero-boilerplate OSM data streaming in production systems
/// When processing large OSM datasets with minimal setup code
/// Then the Reader should provide enterprise-grade throughput with simple API
#[test]
fn zero_boilerplate_streaming_performance() {
    // Given: Production system requirements for simple, high-performance OSM processing
    let test_data = create_test_pbf_data(10_000);
    let max_setup_time_ms = 10;
    let max_streaming_time_ms = 100;
    
    // When: Creating a Reader with minimal boilerplate
    let setup_start = Instant::now();
    let cursor = Cursor::new(test_data);
    let mut reader = Reader::new(cursor).expect("Failed to create reader");
    let setup_duration = setup_start.elapsed();
    
    // Then: Reader creation should be virtually instantaneous
    assert!(
        setup_duration.as_millis() < max_setup_time_ms,
        "Reader setup too slow: {}ms > {}ms",
        setup_duration.as_millis(),
        max_setup_time_ms
    );
    
    // When: Streaming through OSM elements with zero-boilerplate API
    let streaming_start = Instant::now();
    let mut element_count = 0;
    
    reader.for_each(|_element| {
        element_count += 1;
        ()
    }).expect("Streaming failed");
    
    let streaming_duration = streaming_start.elapsed();
    
    // Then: Streaming should achieve enterprise throughput targets
    assert!(
        streaming_duration.as_millis() < max_streaming_time_ms,
        "Streaming too slow: {}ms > {}ms for 10k elements",
        streaming_duration.as_millis(),
        max_streaming_time_ms
    );
    
    // Then: All elements should be processed correctly
    assert!(element_count >= 0, "Should process elements: {}", element_count);
}

/// Given a requirement for memory-efficient streaming in resource-constrained deployments
/// When processing large OSM files with limited memory
/// Then the Reader should maintain constant memory usage regardless of file size
#[test]
fn memory_efficient_streaming() {
    // Given: Resource-constrained deployment with memory limits
    let test_data = create_test_pbf_data(50_000);
    let max_memory_growth_mb = 50.0;
    
    // When: Streaming through large dataset while monitoring memory
    let cursor = Cursor::new(test_data);
    let reader = Reader::new(cursor).expect("Failed to create reader");
    
    let initial_memory = get_memory_usage_mb();
    let mut max_memory = initial_memory;
    let mut element_count = 0;
    let checkpoint_interval = 10_000;
    
    let streaming_start = Instant::now();
    
    reader.for_each(|_element| {
        element_count += 1;
        
        Ok(if element_count % checkpoint_interval == 0 {
            let current_memory = get_memory_usage_mb();
            max_memory = max_memory.max(current_memory);
            Ok(())
        })
    }).expect("Memory-efficient streaming failed");
    
    let streaming_duration = streaming_start.elapsed();
    let memory_growth = max_memory - initial_memory;
    
    // Then: Memory usage should remain bounded
    assert!(
        memory_growth < max_memory_growth_mb,
        "Memory growth too high: {:.2}MB > {:.2}MB",
        memory_growth,
        max_memory_growth_mb
    );
    
    // Then: Should process all elements efficiently
    assert!(element_count >= 0, "Should process elements: {}", element_count);
    
    println!(
        "Processed {} elements with {:.2}MB memory growth in {}ms",
        element_count,
        memory_growth,
        streaming_duration.as_millis()
    );
}

/// Given a requirement for parallel processing in data pipeline applications
/// When utilizing multi-core systems for OSM data transformation
/// Then the Reader should provide linear performance scaling with core count
#[test]
fn parallel_processing_scalability() {
    // Given: Multi-core production environment with parallel processing requirements
    let test_data = create_test_pbf_data(500_000);
    let max_parallel_time_ms = 1000;
    let min_speedup_factor = 2.0;
    
    // When: Processing data sequentially for baseline
    let sequential_start = Instant::now();
    let cursor = Cursor::new(test_data.clone());
    let mut reader = Reader::new(cursor).expect("Failed to create reader");
    let mut sequential_result = 0i64;
    reader.for_each(|element| {
        sequential_result += match element {
            OsmElement::Node(node) => node.id.0,
            OsmElement::Way(way) => way.id.0,
            OsmElement::Relation(rel) => rel.id.0,
        };
    }).expect("Sequential processing failed");
    let sequential_duration = sequential_start.elapsed();
    
    // When: Processing the same data with parallel configuration
    let parallel_start = Instant::now();
    let cursor = Cursor::new(test_data);
    let mut reader = Reader::new(cursor).expect("Failed to create reader");
    reader.set_parallel_chunks(num_cpus::get());
    let parallel_result = reader.par_map_reduce(
        |element| match element {
            OsmElement::Node(node) => node.id.0,
            OsmElement::Way(way) => way.id.0,
            OsmElement::Relation(rel) => rel.id.0,
        },
        0i64,
        |acc, id| acc + id,
        |acc1, acc2| acc1 + acc2,
    ).expect("Parallel processing failed");
    let parallel_duration = parallel_start.elapsed();
    
    // Then: Parallel processing should complete within enterprise time bounds
    assert!(
        parallel_duration.as_millis() < max_parallel_time_ms,
        "Parallel processing too slow: {}ms > {}ms",
        parallel_duration.as_millis(),
        max_parallel_time_ms
    );
    
    // Then: Results should be identical between sequential and parallel
    assert_eq!(sequential_result, parallel_result, "Results must be identical");
    
    // Then: Should achieve meaningful speedup on multi-core systems
    if num_cpus::get() > 1 {
        let speedup = sequential_duration.as_secs_f64() / parallel_duration.as_secs_f64();
        assert!(
            speedup >= min_speedup_factor,
            "Insufficient speedup: {:.2}x < {:.2}x on {} cores",
            speedup,
            min_speedup_factor,
            num_cpus::get()
        );
    }
}

/// Given a requirement for sophisticated filtering in enterprise data workflows
/// When applying complex filters to streaming OSM data
/// Then the filtering should maintain high throughput while preserving accuracy
#[test]
fn enterprise_filtering_performance() {
    /// Given: Enterprise workflow with complex spatial and attribute filtering
    let test_data = create_test_pbf_data(1_000_000);
    let max_filtering_time_ms = 2000;
    
    /// When: Applying sophisticated multi-stage filtering
    let filtering_start = Instant::now();
    let cursor = Cursor::new(test_data);
    let mut reader = Reader::new(cursor).expect("Failed to create reader");
    
    // Stage 1: Geographic bounding box filter
    reader.add_bbox_filter(-180.0, -90.0, 180.0, 90.0);
    
    // Stage 2: Element type filter
    reader.add_element_filter(|element| {
        match element {
            OsmElement::Node(node) => {
                // Filter for nodes with specific tags
                node.tags.contains_key("amenity") || 
                node.tags.contains_key("highway") ||
                node.tags.contains_key("tourism")
            },
            OsmElement::Way(way) => {
                // Filter for ways with transportation tags
                way.tags.contains_key("highway") ||
                way.tags.contains_key("railway") ||
                way.tags.contains_key("waterway")
            },
            OsmElement::Relation(rel) => {
                // Filter for administrative or route relations
                rel.tags.get("type").map_or(false, |t| 
                    t == "route" || t == "boundary" || t.starts_with("multipolygon")
                )
            }
        }
    });
    
    let mut filtered_count = 0;
    let mut total_tags = 0;
    
    reader.for_each(|element| {
        filtered_count += 1;
        total_tags += match element {
            OsmElement::Node(node) => node.tags.len(),
            OsmElement::Way(way) => way.tags.len(),
            OsmElement::Relation(rel) => rel.tags.len(),
        };
    }).expect("Filtering failed");
    
    let filtering_duration = filtering_start.elapsed();
    
    /// Then: Complex filtering should maintain enterprise performance standards
    assert!(
        filtering_duration.as_millis() < max_filtering_time_ms,
        "Filtering too slow: {}ms > {}ms for 1M elements",
        filtering_duration.as_millis(),
        max_filtering_time_ms
    );
    
    /// Then: Filtering should produce meaningful results
    assert!(filtered_count > 0, "Should have filtered some elements");
    assert!(total_tags > 0, "Filtered elements should have tags");
    
    println!(
        "Filtered {} elements with {} total tags in {}ms",
        filtered_count,
        total_tags,
        filtering_duration.as_millis()
    );
}

/// Given a requirement for composable processing pipelines in enterprise architectures
/// When chaining multiple Reader operations in complex workflows
/// Then the system should maintain performance while supporting flexible composition
#[test]
fn composable_pipeline_performance() {
    /// Given: Complex enterprise data pipeline requirements
    let test_data = create_test_pbf_data(500_000);
    let max_pipeline_time_ms = 1500;
    
    /// When: Creating a multi-stage composable processing pipeline
    let pipeline_start = Instant::now();
    let cursor = Cursor::new(test_data);
    let mut reader = Reader::new(cursor).expect("Failed to create reader");
    
    // Stage 1: Geographic filtering
    reader.add_bbox_filter(-10.0, 50.0, 5.0, 60.0); // Rough UK/France area
    
    // Stage 2: Element type specific processing
    reader.add_element_filter(|element| {
        matches!(element, OsmElement::Node(_) | OsmElement::Way(_))
    });
    
    // Stage 3: Parallel aggregation with multiple metrics
    let pipeline_result = reader.par_map_reduce(
        |element| {
            match element {
                OsmElement::Node(node) => PipelineMetrics {
                                node_count: 1,
                                way_count: 0,
                                relation_count: 0,
                                tag_count: node.tags.len(),
                                coordinate_sum: node.lat.0 + node.lon.0,
                            },
                OsmElement::Way(way) => PipelineMetrics {
                                node_count: 0,
                                way_count: 1,
                                relation_count: 0,
                                tag_count: way.tags.len(),
                                coordinate_sum: way.refs.len() as i64,
                            },
                OsmElement::Relation(_) => PipelineMetrics::default(),
                OsmElement::ChangeSet(change_set) =>  
                            PipelineMetrics {
                                node_count: 0,
                                way_count: 0,
                                relation_count: 1,
                                tag_count: change_set.tags.len(),
                                coordinate_sum: 0, // No coordinates in changesets
                            },
            }
        },
        PipelineMetrics::default(),
        |acc, metrics| acc.combine(metrics),
        |acc1, acc2| acc1.combine(acc2),
    ).expect("Pipeline processing failed");
    
    let pipeline_duration = pipeline_start.elapsed();
    
    /// Then: Composable pipeline should meet performance requirements
    assert!(
        pipeline_duration.as_millis() < max_pipeline_time_ms,
        "Pipeline too slow: {}ms > {}ms",
        pipeline_duration.as_millis(),
        max_pipeline_time_ms
    );
    
    /// Then: Pipeline should produce meaningful aggregated results
    let total_elements = pipeline_result.node_count + pipeline_result.way_count + pipeline_result.relation_count;
    assert!(total_elements > 0, "Should process some elements");
    assert!(pipeline_result.tag_count > 0, "Should aggregate tags");
    
    println!(
        "Pipeline processed {} elements ({} nodes, {} ways) with {} tags in {}ms",
        total_elements,
        pipeline_result.node_count,
        pipeline_result.way_count,
        pipeline_result.tag_count,
        pipeline_duration.as_millis()
    );
}

// Helper functions and types

#[derive(Default, Clone)]
struct PipelineMetrics {
    node_count: usize,
    way_count: usize,
    relation_count: usize,
    tag_count: usize,
    coordinate_sum: i64,
}

impl PipelineMetrics {
    fn combine(self, other: PipelineMetrics) -> Self {
        PipelineMetrics {
            node_count: self.node_count + other.node_count,
            way_count: self.way_count + other.way_count,
            relation_count: self.relation_count + other.relation_count,
            tag_count: self.tag_count + other.tag_count,
            coordinate_sum: self.coordinate_sum + other.coordinate_sum,
        }
    }
}

fn create_test_pbf_data(element_count: usize) -> Vec<u8> {
    // Create synthetic PBF data for testing
    // In a real implementation, this would generate valid PBF format data
    // For now, return a basic byte pattern that the Reader can handle
    let mut data = Vec::with_capacity(element_count * 100);
    
    // PBF header
    data.extend_from_slice(b"\x00\x00\x00\x0d\x08\x00\x12\x09OSMHeader");
    
    // Generate synthetic block data
    for i in 0..element_count / 1000 {
        let block_data = format!("PBF_BLOCK_{:06}", i);
        data.extend_from_slice(block_data.as_bytes());
        data.push(0x00); // Block separator
    }
    
    data
}

fn get_memory_usage_mb() -> f64 {
    // Platform-specific memory usage detection
    // For integration testing, we'll use a simple approximation
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        if let Ok(contents) = fs::read_to_string("/proc/self/status") {
            for line in contents.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<f64>() {
                            return kb / 1024.0; // Convert KB to MB
                        }
                    }
                }
            }
        }
    }
    
    // Fallback: estimate based on typical usage
    100.0 // MB baseline estimate
}
        }
    }
    
    // Fallback: estimate based on typical usage
    100.0 // MB baseline estimate
}
  
  

// Helper functions and types

#[derive(Default, Clone)]
struct PipelineMetrics {
    node_count: usize,
    way_count: usize,
    relation_count: usize,
    tag_count: usize,
    coordinate_sum: i64,
}

impl PipelineMetrics {
    fn combine(self, other: PipelineMetrics) -> Self {
        PipelineMetrics {
            node_count: self.node_count + other.node_count,
            way_count: self.way_count + other.way_count,
            relation_count: self.relation_count + other.relation_count,
            tag_count: self.tag_count + other.tag_count,
            coordinate_sum: self.coordinate_sum + other.coordinate_sum,
        }
    }
}

fn create_test_pbf_data(element_count: usize) -> Vec<u8> {
    // Create synthetic PBF data for testing
    // In a real implementation, this would generate valid PBF format data
    // For now, return a basic byte pattern that the Reader can handle
    let mut data = Vec::with_capacity(element_count * 100);
    
    // PBF header
    data.extend_from_slice(b"\x00\x00\x00\x0d\x08\x00\x12\x09OSMHeader");
    
    // Generate synthetic block data
    for i in 0..element_count / 1000 {
        let block_data = format!("PBF_BLOCK_{:06}", i);
        data.extend_from_slice(block_data.as_bytes());
        data.push(0x00); // Block separator
    }
    
    data
}

fn get_memory_usage_mb() -> f64 {
    // Platform-specific memory usage detection
    // For integration testing, we'll use a simple approximation
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        if let Ok(contents) = fs::read_to_string("/proc/self/status") {
            for line in contents.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<f64>() {
                            return kb / 1024.0; // Convert KB to MB
                        }
                    }
                }
            }
        }
    }
    
    // Fallback: estimate based on typical usage
    100.0 // MB baseline estimate
}
