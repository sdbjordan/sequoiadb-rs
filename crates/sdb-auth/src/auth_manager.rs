use sdb_common::Result;
use crate::user::User;
use crate::role::{Permission, Resource, Role};

/// Authentication and authorization manager.
pub struct AuthManager {
    users: Vec<User>,
    roles: Vec<Role>,
}

impl AuthManager {
    pub fn new() -> Self {
        Self {
            users: Vec::new(),
            roles: Vec::new(),
        }
    }

    pub fn authenticate(&self, _username: &str, _password: &[u8]) -> Result<()> {
        // Stub
        Err(sdb_common::SdbError::AuthFailed)
    }

    pub fn authorize(&self, _username: &str, _perm: Permission, _resource: &Resource) -> Result<()> {
        // Stub
        Err(sdb_common::SdbError::PermissionDenied)
    }

    pub fn create_user(&mut self, user: User) -> Result<()> {
        self.users.push(user);
        Ok(())
    }

    pub fn create_role(&mut self, role: Role) -> Result<()> {
        self.roles.push(role);
        Ok(())
    }
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}
