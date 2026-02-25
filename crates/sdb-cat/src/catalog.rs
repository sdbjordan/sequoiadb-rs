use crate::metadata::{CollectionMeta, CollectionSpaceMeta, GroupMeta};
use sdb_common::{CollectionSpaceId, GroupId, Result};
use std::collections::HashMap;

/// Manages cluster-wide catalog metadata.
pub struct CatalogManager {
    spaces: HashMap<String, CollectionSpaceMeta>,
    groups: HashMap<GroupId, GroupMeta>,
}

impl CatalogManager {
    pub fn new() -> Self {
        Self {
            spaces: HashMap::new(),
            groups: HashMap::new(),
        }
    }

    pub fn get_collection_space(&self, name: &str) -> Result<&CollectionSpaceMeta> {
        self.spaces
            .get(name)
            .ok_or(sdb_common::SdbError::CollectionSpaceNotFound)
    }

    pub fn get_collection(&self, cs_name: &str, cl_name: &str) -> Result<&CollectionMeta> {
        let cs = self.get_collection_space(cs_name)?;
        cs.collections
            .iter()
            .find(|c| c.name == cl_name)
            .ok_or(sdb_common::SdbError::CollectionNotFound)
    }

    pub fn get_group(&self, id: GroupId) -> Result<&GroupMeta> {
        self.groups
            .get(&id)
            .ok_or(sdb_common::SdbError::NodeNotFound)
    }

    pub fn create_collection_space(
        &mut self,
        _id: CollectionSpaceId,
        meta: CollectionSpaceMeta,
    ) -> Result<()> {
        if self.spaces.contains_key(&meta.name) {
            return Err(sdb_common::SdbError::CollectionSpaceAlreadyExists);
        }
        self.spaces.insert(meta.name.clone(), meta);
        Ok(())
    }

    pub fn register_group(&mut self, meta: GroupMeta) -> Result<()> {
        self.groups.insert(meta.id, meta);
        Ok(())
    }
}

impl Default for CatalogManager {
    fn default() -> Self {
        Self::new()
    }
}
