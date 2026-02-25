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
#[derive(Debug, Clone)]
pub enum Resource {
    Cluster,
    CollectionSpace(String),
    Collection(String),
}

/// A named role with a set of permissions.
#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub permissions: Vec<(Permission, Resource)>,
}
