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

/// Read preference for routing queries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReadPreference {
    /// All reads go to primary (default).
    #[default]
    Primary,
    /// Reads go to a secondary if available, else primary.
    SecondaryPreferred,
    /// Reads must go to a secondary; fail if none available.
    Secondary,
    /// Reads go to the node with lowest latency (not yet measured, acts as SecondaryPreferred).
    Nearest,
}

impl ReadPreference {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "primary" => Some(Self::Primary),
            "secondarypreferred" | "secondary_preferred" => Some(Self::SecondaryPreferred),
            "secondary" => Some(Self::Secondary),
            "nearest" => Some(Self::Nearest),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Primary => "Primary",
            Self::SecondaryPreferred => "SecondaryPreferred",
            Self::Secondary => "Secondary",
            Self::Nearest => "Nearest",
        }
    }

    pub fn allows_secondary(&self) -> bool {
        matches!(self, Self::Secondary | Self::SecondaryPreferred | Self::Nearest)
    }
}
