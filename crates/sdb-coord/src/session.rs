/// A client session on the coordinator node.
pub struct CoordSession {
    pub session_id: u64,
    pub authenticated: bool,
    pub user: Option<String>,
}

impl CoordSession {
    pub fn new(session_id: u64) -> Self {
        Self {
            session_id,
            authenticated: false,
            user: None,
        }
    }
}
