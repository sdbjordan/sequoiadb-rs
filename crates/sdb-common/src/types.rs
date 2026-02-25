/// Unique identifier for a node within a replica group.
pub type NodeId = u16;

/// Identifier for a replica group.
pub type GroupId = u32;

/// Identifier for a collection space.
pub type CollectionSpaceId = u32;

/// Identifier for a collection within a collection space.
pub type CollectionId = u16;

/// Log sequence number for WAL ordering.
pub type Lsn = u64;

/// Identifier for a record within a storage unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordId {
    pub extent_id: u32,
    pub offset: u32,
}

/// Identifier for an extent within a storage unit.
pub type ExtentId = u32;

/// Identifier for a page within a storage unit.
pub type PageId = u32;

/// Request identifier for correlating messages.
pub type RequestId = u64;

/// Cluster-wide unique identifier for a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeAddress {
    pub group_id: GroupId,
    pub node_id: NodeId,
}
