/// Represents member types in relations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(i32)]
pub enum MemberType {
    Node = 0,
    Way = 1,
    Relation = 2,
}
