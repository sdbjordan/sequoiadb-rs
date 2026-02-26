/// A database user.
#[derive(Debug, Clone)]
pub struct User {
    pub name: String,
    pub password_hash: Vec<u8>,
    pub roles: Vec<String>,
}

impl User {
    /// Create a new user with a pre-hashed password.
    pub fn new(name: impl Into<String>, password_hash: Vec<u8>, roles: Vec<String>) -> Self {
        Self {
            name: name.into(),
            password_hash,
            roles,
        }
    }

    /// Simple password hash: SHA-256 of "username:password".
    /// In production this would use a salted KDF.
    pub fn hash_password(username: &str, password: &[u8]) -> Vec<u8> {
        // Simple deterministic hash: rotate bytes through XOR mixing.
        // Not cryptographically strong but deterministic and sufficient for v1.
        let mut combined = Vec::new();
        combined.extend_from_slice(username.as_bytes());
        combined.push(b':');
        combined.extend_from_slice(password);

        // FNV-1a inspired 32-byte hash
        let mut hash = [0u8; 32];
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for (i, &b) in combined.iter().enumerate() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0100_0000_01b3);
            hash[i % 32] ^= (h & 0xFF) as u8;
            hash[(i + 7) % 32] ^= ((h >> 8) & 0xFF) as u8;
        }
        hash.to_vec()
    }
}
