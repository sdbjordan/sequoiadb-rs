/// RBAC permission actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Find,
    Insert,
    Update,
    Delete,
    CreateCL,
    DropCL,
    CreateCS,
    DropCS,
    CreateIndex,
    DropIndex,
    Admin,
}

/// Resource that a permission applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resource {
    Cluster,
    CollectionSpace(String),
    Collection(String),
}

impl Resource {
    /// Check if this resource covers the target resource.
    /// Cluster covers everything; CollectionSpace("cs") covers Collection("cs.cl").
    pub fn covers(&self, target: &Resource) -> bool {
        match (self, target) {
            (Resource::Cluster, _) => true,
            (Resource::CollectionSpace(cs), Resource::Collection(cl)) => {
                cl.starts_with(cs.as_str()) && cl.as_bytes().get(cs.len()) == Some(&b'.')
            }
            (a, b) => a == b,
        }
    }
}

/// A named role with a set of permissions.
#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub permissions: Vec<(Permission, Resource)>,
}

impl Role {
    pub fn new(name: impl Into<String>, permissions: Vec<(Permission, Resource)>) -> Self {
        Self {
            name: name.into(),
            permissions,
        }
    }

    /// Check if this role grants the given permission on the given resource.
    pub fn has_permission(&self, perm: Permission, resource: &Resource) -> bool {
        self.permissions.iter().any(|(p, r)| {
            (*p == perm || *p == Permission::Admin) && r.covers(resource)
        })
    }
}
