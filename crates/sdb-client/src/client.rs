use sdb_common::Result;
use crate::collection::Collection;
use crate::options::ConnectOptions;

/// SequoiaDB client — main entry point for the driver.
pub struct Client {
    _options: ConnectOptions,
    connected: bool,
}

impl Client {
    pub async fn connect(options: ConnectOptions) -> Result<Self> {
        // Stub: does not actually connect
        Ok(Self {
            _options: options,
            connected: true,
        })
    }

    pub fn get_collection(&self, cs_name: &str, cl_name: &str) -> Result<Collection> {
        if !self.connected {
            return Err(sdb_common::SdbError::NetworkClose);
        }
        Ok(Collection::new(format!("{}.{}", cs_name, cl_name)))
    }

    pub async fn create_collection_space(&self, _name: &str) -> Result<()> {
        // Stub
        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<()> {
        self.connected = false;
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        self.connected
    }
}
