use std::sync::atomic::{AtomicU64, Ordering};

/// A client session on the coordinator node.
pub struct CoordSession {
    pub session_id: u64,
    pub authenticated: bool,
    pub user: Option<String>,
    pub current_cs: Option<String>,
}

impl CoordSession {
    pub fn new(session_id: u64) -> Self {
        Self {
            session_id,
            authenticated: false,
            user: None,
            current_cs: None,
        }
    }

    /// Mark the session as authenticated.
    pub fn authenticate(&mut self, username: &str) {
        self.authenticated = true;
        self.user = Some(username.to_string());
    }

    /// Log out the session.
    pub fn logout(&mut self) {
        self.authenticated = false;
        self.user = None;
    }
}

/// Session ID generator — atomic counter for unique session IDs.
pub struct SessionIdGen {
    next: AtomicU64,
}

impl SessionIdGen {
    pub fn new() -> Self {
        Self { next: AtomicU64::new(1) }
    }

    pub fn next_id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for SessionIdGen {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_lifecycle() {
        let mut s = CoordSession::new(1);
        assert!(!s.authenticated);
        assert!(s.user.is_none());

        s.authenticate("alice");
        assert!(s.authenticated);
        assert_eq!(s.user.as_deref(), Some("alice"));

        s.logout();
        assert!(!s.authenticated);
        assert!(s.user.is_none());
    }

    #[test]
    fn session_id_gen() {
        let gen = SessionIdGen::new();
        assert_eq!(gen.next_id(), 1);
        assert_eq!(gen.next_id(), 2);
        assert_eq!(gen.next_id(), 3);
    }
}
