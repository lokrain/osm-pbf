use crate::blocks::primitives::node::Node;
use crate::blocks::primitives::dense_nodes::DenseNodes;
use crate::blocks::primitives::way::Way;
use crate::blocks::primitives::relation::Relation;
use crate::blocks::primitives::changeset::ChangeSet;

/// Represents a group of OSM primitives (nodes, ways, or relations) in a PBF block.
/// A primitive group contains either dense nodes or a collection of nodes, ways, or relations.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PrimitiveGroup {
    /// Collection of nodes (sparse format)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<Node>,

    /// Dense node representation (more efficient for many nodes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dense: Option<DenseNodes>,

    /// Collection of ways
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ways: Vec<Way>,

    /// Collection of relations
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<Relation>,

    /// Collection of changesets
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changesets: Vec<ChangeSet>,
}

impl Default for PrimitiveGroup {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            dense: None,
            ways: Vec::new(),
            relations: Vec::new(),
            changesets: Vec::new(),
        }
    }
}
