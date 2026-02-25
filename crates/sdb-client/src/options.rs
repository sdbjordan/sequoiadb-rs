/// Connection options for the SequoiaDB client.
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connect_timeout_ms: u64,
    pub max_retry: u32,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 11810,
            username: None,
            password: None,
            connect_timeout_ms: 10_000,
            max_retry: 3,
        }
    }
}
