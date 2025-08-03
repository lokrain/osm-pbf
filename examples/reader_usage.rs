// Example usage of the high-level Reader API
// 
// This example demonstrates the zero-boilerplate entry point for OSM PBF processing

use osm_pbf::{Reader, OsmElement, ElementFilter};
use std::io::Cursor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example 1: Create a reader from any Read + Seek source
    let empty_pbf_data = Vec::new(); // In reality, this would be actual PBF data
    let cursor = Cursor::new(empty_pbf_data);
    let mut reader = Reader::new(cursor)?;

    println!("Created reader with {} blobs", reader.statistics().total_blobs);

    // Example 2: Sequential streaming with for_each
    let mut node_count = 0;
    let mut way_count = 0;
    
    let _stats = reader.for_each(|element| {
        match element {
            OsmElement::Node(_) => node_count += 1,
            OsmElement::Way(_) => way_count += 1,
            OsmElement::Relation(_) => {},
            OsmElement::ChangeSet(_) => {},
        }
        Ok(()) // Continue processing
    })?;

    println!("Found {} nodes and {} ways", node_count, way_count);

    // Example 3: Filtered processing (ways only)
    let cursor2 = Cursor::new(Vec::new());
    let mut reader2 = Reader::new(cursor2)?;
    
    let filter = ElementFilter::ways_only(false) // Don't resolve dependencies
        .with_tag_key("highway".to_string());

    let _stats = reader2.for_each_filtered(&filter, |element| {
        if let OsmElement::Way(way) = element {
            println!("Highway way ID: {}", way.id);
        }
        Ok(())
    })?;

    // Example 4: Count all elements
    let cursor3 = Cursor::new(Vec::new());
    let mut reader3 = Reader::new(cursor3)?;
    
    let (nodes, ways, relations, changesets) = reader3.count_elements()?;
    println!("Totals: {} nodes, {} ways, {} relations, {} changesets", 
             nodes, ways, relations, changesets);

    // Example 5: Collect small datasets
    let cursor4 = Cursor::new(Vec::new());
    let mut reader4 = Reader::new(cursor4)?;
    
    let node_filter = ElementFilter::nodes_only();
    let (_elements, stats) = reader4.collect_filtered(&node_filter)?;
    println!("Collected {} elements", stats.elements_processed);

    Ok(())
}
