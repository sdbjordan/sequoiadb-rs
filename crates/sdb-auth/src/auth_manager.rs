use crate::role::{Permission, Resource, Role};
use crate::user::User;
use sdb_common::{Result, SdbError};

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

    /// Authenticate a user by username and password.
    pub fn authenticate(&self, username: &str, password: &[u8]) -> Result<()> {
        let user = self.find_user(username).ok_or(SdbError::AuthFailed)?;
        let hash = User::hash_password(username, password);
        if user.password_hash == hash {
            Ok(())
        } else {
            Err(SdbError::AuthFailed)
        }
    }

    /// Check if a user has the required permission on a resource.
    pub fn authorize(
        &self,
        username: &str,
        perm: Permission,
        resource: &Resource,
    ) -> Result<()> {
        let user = self.find_user(username).ok_or(SdbError::PermissionDenied)?;
        for role_name in &user.roles {
            if let Some(role) = self.find_role(role_name) {
                if role.has_permission(perm, resource) {
                    return Ok(());
                }
            }
        }
        Err(SdbError::PermissionDenied)
    }

    /// Create a new user. Fails if name already exists.
    pub fn create_user(&mut self, user: User) -> Result<()> {
        if self.find_user(&user.name).is_some() {
            return Err(SdbError::InvalidArg);
        }
        self.users.push(user);
        Ok(())
    }

    /// Remove a user by name.
    pub fn drop_user(&mut self, username: &str) -> Result<()> {
        let pos = self.users.iter().position(|u| u.name == username)
            .ok_or(SdbError::NotFound)?;
        self.users.remove(pos);
        Ok(())
    }

    /// Create a new role. Fails if name already exists.
    pub fn create_role(&mut self, role: Role) -> Result<()> {
        if self.find_role(&role.name).is_some() {
            return Err(SdbError::InvalidArg);
        }
        self.roles.push(role);
        Ok(())
    }

    /// Remove a role by name.
    pub fn drop_role(&mut self, role_name: &str) -> Result<()> {
        let pos = self.roles.iter().position(|r| r.name == role_name)
            .ok_or(SdbError::NotFound)?;
        self.roles.remove(pos);
        Ok(())
    }

    pub fn find_user(&self, username: &str) -> Option<&User> {
        self.users.iter().find(|u| u.name == username)
    }

    pub fn find_role(&self, role_name: &str) -> Option<&Role> {
        self.roles.iter().find(|r| r.name == role_name)
    }

    pub fn list_users(&self) -> Vec<&str> {
        self.users.iter().map(|u| u.name.as_str()).collect()
    }

    pub fn list_roles(&self) -> Vec<&str> {
        self.roles.iter().map(|r| r.name.as_str()).collect()
    }
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> AuthManager {
        let mut mgr = AuthManager::new();
        mgr.create_role(Role::new("admin", vec![
            (Permission::Admin, Resource::Cluster),
        ])).unwrap();
        mgr.create_role(Role::new("reader", vec![
            (Permission::Find, Resource::Cluster),
        ])).unwrap();
        mgr.create_role(Role::new("cs_writer", vec![
            (Permission::Find, Resource::CollectionSpace("mycs".into())),
            (Permission::Insert, Resource::CollectionSpace("mycs".into())),
            (Permission::Update, Resource::CollectionSpace("mycs".into())),
            (Permission::Delete, Resource::CollectionSpace("mycs".into())),
        ])).unwrap();

        let hash = User::hash_password("alice", b"secret123");
        mgr.create_user(User::new("alice", hash, vec!["admin".into()])).unwrap();

        let hash = User::hash_password("bob", b"pass");
        mgr.create_user(User::new("bob", hash, vec!["reader".into()])).unwrap();

        let hash = User::hash_password("carol", b"pw");
        mgr.create_user(User::new("carol", hash, vec!["cs_writer".into()])).unwrap();

        mgr
    }

    #[test]
    fn authenticate_success() {
        let mgr = setup();
        assert!(mgr.authenticate("alice", b"secret123").is_ok());
    }

    #[test]
    fn authenticate_wrong_password() {
        let mgr = setup();
        assert!(mgr.authenticate("alice", b"wrong").is_err());
    }

    #[test]
    fn authenticate_nonexistent_user() {
        let mgr = setup();
        assert!(mgr.authenticate("nobody", b"x").is_err());
    }

    #[test]
    fn authorize_admin_can_do_everything() {
        let mgr = setup();
        assert!(mgr.authorize("alice", Permission::Find, &Resource::Cluster).is_ok());
        assert!(mgr.authorize("alice", Permission::Insert, &Resource::Collection("any.cl".into())).is_ok());
        assert!(mgr.authorize("alice", Permission::DropCS, &Resource::CollectionSpace("x".into())).is_ok());
    }

    #[test]
    fn authorize_reader_can_only_find() {
        let mgr = setup();
        assert!(mgr.authorize("bob", Permission::Find, &Resource::Collection("any.cl".into())).is_ok());
        assert!(mgr.authorize("bob", Permission::Insert, &Resource::Collection("any.cl".into())).is_err());
    }

    #[test]
    fn authorize_cs_writer_scoped() {
        let mgr = setup();
        // Can write to mycs.cl
        assert!(mgr.authorize("carol", Permission::Insert, &Resource::Collection("mycs.cl".into())).is_ok());
        // Cannot write to other_cs.cl
        assert!(mgr.authorize("carol", Permission::Insert, &Resource::Collection("other.cl".into())).is_err());
    }

    #[test]
    fn duplicate_user_rejected() {
        let mut mgr = setup();
        let hash = User::hash_password("alice", b"x");
        assert!(mgr.create_user(User::new("alice", hash, vec![])).is_err());
    }

    #[test]
    fn drop_user() {
        let mut mgr = setup();
        mgr.drop_user("bob").unwrap();
        assert!(mgr.find_user("bob").is_none());
        assert!(mgr.authenticate("bob", b"pass").is_err());
    }

    #[test]
    fn drop_role() {
        let mut mgr = setup();
        mgr.drop_role("reader").unwrap();
        assert!(mgr.find_role("reader").is_none());
    }

    #[test]
    fn list_users_and_roles() {
        let mgr = setup();
        assert_eq!(mgr.list_users().len(), 3);
        assert_eq!(mgr.list_roles().len(), 3);
    }
}
