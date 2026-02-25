use sdb_bson::Document;
use sdb_common::{CollectionId, CollectionSpaceId, GroupId};

/// Metadata for a collection space.
#[derive(Debug, Clone)]
pub struct CollectionSpaceMeta {
    pub id: CollectionSpaceId,
    pub name: String,
    pub page_size: u32,
    pub collections: Vec<CollectionMeta>,
}

/// Metadata for a collection.
#[derive(Debug, Clone)]
pub struct CollectionMeta {
    pub cs_id: CollectionSpaceId,
    pub cl_id: CollectionId,
    pub name: String,
    pub shard_key: Option<Document>,
    pub replicated: bool,
    pub groups: Vec<GroupId>,
}

/// Metadata for a replica group.
#[derive(Debug, Clone)]
pub struct GroupMeta {
    pub id: GroupId,
    pub name: String,
    pub primary_node: Option<sdb_common::NodeId>,
    pub nodes: Vec<NodeMeta>,
}

/// Metadata for a node within a group.
#[derive(Debug, Clone)]
pub struct NodeMeta {
    pub node_id: sdb_common::NodeId,
    pub host: String,
    pub port: u16,
}
