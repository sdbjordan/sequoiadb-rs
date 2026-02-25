/// A database user.
#[derive(Debug, Clone)]
pub struct User {
    pub name: String,
    pub password_hash: Vec<u8>,
    pub roles: Vec<String>,
}
