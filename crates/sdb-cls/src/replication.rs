use std::collections::HashMap;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use sdb_common::{Lsn, NodeAddress, Result, SdbError};
use sdb_msg::header::MsgHeader;
use sdb_msg::opcode::OpCode;

/// Replica sync status for a single peer.
#[derive(Debug, Clone)]
pub struct ReplicaStatus {
    pub node: NodeAddress,
    pub synced_lsn: Lsn,
    pub is_alive: bool,
}

/// Replication agent — handles log shipping to replicas.
pub struct ReplicationAgent {
    pub local_node: NodeAddress,
    pub local_lsn: Lsn,
    pub replicas: HashMap<u16, ReplicaStatus>,
    pub peer_addrs: HashMap<u16, String>,
}

impl ReplicationAgent {
    pub fn new(local_node: NodeAddress) -> Self {
        Self {
            local_node,
            local_lsn: 0,
            replicas: HashMap::new(),
            peer_addrs: HashMap::new(),
        }
    }

    /// Add a replica peer.
    pub fn add_replica(&mut self, node: NodeAddress) {
        self.replicas.insert(node.node_id, ReplicaStatus {
            node,
            synced_lsn: 0,
            is_alive: true,
        });
    }

    /// Remove a replica peer.
    pub fn remove_replica(&mut self, node_id: u16) -> Result<()> {
        self.replicas.remove(&node_id).ok_or(SdbError::NodeNotFound)?;
        Ok(())
    }

    /// Record that the local node has advanced to a new LSN.
    pub fn advance_local_lsn(&mut self, lsn: Lsn) {
        self.local_lsn = lsn;
    }

    /// Simulate sending logs to a replica. Returns the new synced LSN.
    /// In a real system this would ship WAL records over the network.
    pub async fn sync_to(&mut self, target_id: u16, up_to_lsn: Lsn) -> Result<Lsn> {
        let replica = self.replicas.get_mut(&target_id).ok_or(SdbError::NodeNotFound)?;
        if !replica.is_alive {
            return Err(SdbError::NetworkError);
        }
        // Simulate: instantly sync up to the requested LSN (capped at local)
        let new_lsn = up_to_lsn.min(self.local_lsn);
        replica.synced_lsn = new_lsn;
        Ok(new_lsn)
    }

    /// Sync all replicas to the current local LSN.
    pub async fn sync_all(&mut self) -> Result<()> {
        let target_lsn = self.local_lsn;
        let node_ids: Vec<u16> = self.replicas.keys().copied().collect();
        for nid in node_ids {
            let _ = self.sync_to(nid, target_lsn).await;
        }
        Ok(())
    }

    /// Mark a replica as alive or dead.
    pub fn set_replica_alive(&mut self, node_id: u16, alive: bool) -> Result<()> {
        let replica = self.replicas.get_mut(&node_id).ok_or(SdbError::NodeNotFound)?;
        replica.is_alive = alive;
        Ok(())
    }

    /// Get the minimum synced LSN across all alive replicas.
    pub fn min_synced_lsn(&self) -> Lsn {
        self.replicas.values()
            .filter(|r| r.is_alive)
            .map(|r| r.synced_lsn)
            .min()
            .unwrap_or(0)
    }

    /// Get the number of replicas that have synced up to at least the given LSN.
    pub fn count_synced(&self, lsn: Lsn) -> usize {
        self.replicas.values()
            .filter(|r| r.is_alive && r.synced_lsn >= lsn)
            .count()
    }

    /// Check if a write is durable (majority of replicas have synced).
    pub fn is_majority_synced(&self, lsn: Lsn) -> bool {
        let total = self.replicas.len() + 1; // include self
        let synced = self.count_synced(lsn) + 1; // self is always synced
        synced > total / 2
    }

    /// Push WAL frames to a specific peer via TCP.
    /// Uses WalIterator (which has its own File handle, no WAL lock needed).
    pub async fn push_wal_to_peer(
        &mut self,
        peer_id: u16,
        wal_path: &str,
        from_lsn: Lsn,
        flushed_lsn: Lsn,
    ) -> Result<Lsn> {
        let addr = self.peer_addrs.get(&peer_id).ok_or(SdbError::NodeNotFound)?.clone();
        let replica = self.replicas.get(&peer_id).ok_or(SdbError::NodeNotFound)?;
        if !replica.is_alive {
            return Err(SdbError::NetworkError);
        }

        // Open a WAL iterator from disk (no lock needed)
        let wal = sdb_dps::WriteAheadLog::open(wal_path).map_err(|_| SdbError::IoError)?;
        let iter = wal.scan_from(from_lsn)?;

        // Collect frames up to flushed_lsn
        let mut frames: Vec<Vec<u8>> = Vec::new();
        let mut last_lsn = from_lsn;
        for record_result in iter {
            let record = record_result?;
            if record.lsn > flushed_lsn {
                break;
            }
            last_lsn = record.lsn;
            // Serialize as BSON doc with op metadata
            let mut doc = sdb_bson::Document::new();
            doc.insert("lsn", sdb_bson::Value::Int64(record.lsn as i64));
            doc.insert("prev_lsn", sdb_bson::Value::Int64(record.prev_lsn as i64));
            doc.insert("txn_id", sdb_bson::Value::Int64(record.txn_id as i64));
            doc.insert("op", sdb_bson::Value::Int32(record.op as i32));
            doc.insert("data", sdb_bson::Value::Binary(record.data.clone()));
            if let Ok(bytes) = doc.to_bytes() {
                frames.push(bytes);
            }
        }

        if frames.is_empty() {
            return Ok(from_lsn);
        }

        // Build payload: num_records(4B) + [frame_len(4B) + frame_bytes]*
        let mut payload = Vec::new();
        payload.extend_from_slice(&(frames.len() as u32).to_le_bytes());
        for frame in &frames {
            payload.extend_from_slice(&(frame.len() as u32).to_le_bytes());
            payload.extend_from_slice(frame);
        }

        // Build message header
        let mut header = MsgHeader::new_request(OpCode::ReplSync as i32, 0);
        header.msg_len = (MsgHeader::SIZE + payload.len()) as i32;
        let mut msg = Vec::new();
        header.encode(&mut msg);
        msg.extend_from_slice(&payload);

        // Send over TCP
        let mut stream = TcpStream::connect(&addr)
            .await
            .map_err(|_| SdbError::NetworkError)?;
        stream
            .write_all(&msg)
            .await
            .map_err(|_| SdbError::NetworkError)?;
        stream
            .flush()
            .await
            .map_err(|_| SdbError::NetworkError)?;

        // Read ack reply
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|_| SdbError::NetworkError)?;
        let reply_len = i32::from_le_bytes(len_buf) as usize;
        let mut reply_buf = vec![0u8; reply_len];
        reply_buf[..4].copy_from_slice(&len_buf);
        stream
            .read_exact(&mut reply_buf[4..])
            .await
            .map_err(|_| SdbError::NetworkError)?;

        // Update synced_lsn
        if let Some(replica) = self.replicas.get_mut(&peer_id) {
            replica.synced_lsn = last_lsn;
        }

        Ok(last_lsn)
    }

    /// Push WAL to all alive peers in parallel, return whether majority acked.
    /// Times out after 1 second, degrading gracefully.
    pub async fn replicate_and_check_majority(
        &mut self,
        wal_path: &str,
        write_lsn: Lsn,
        flushed_lsn: Lsn,
    ) -> bool {
        if self.peer_addrs.is_empty() {
            return true; // no peers = majority of 1
        }

        let peers: Vec<(u16, String)> = self
            .peer_addrs
            .iter()
            .filter(|(id, _)| {
                self.replicas
                    .get(id)
                    .map(|r| r.is_alive)
                    .unwrap_or(false)
            })
            .map(|(id, addr)| (*id, addr.clone()))
            .collect();

        let from_lsns: HashMap<u16, Lsn> = self
            .replicas
            .iter()
            .map(|(id, r)| (*id, r.synced_lsn))
            .collect();

        let wal_path = wal_path.to_string();
        let mut handles = Vec::new();

        for (peer_id, addr) in &peers {
            let from_lsn = from_lsns.get(peer_id).copied().unwrap_or(0);
            let peer_id = *peer_id;
            let addr = addr.clone();
            let wal_path = wal_path.clone();
            let flushed = flushed_lsn;

            handles.push(tokio::spawn(async move {
                // Open a fresh WAL reader for this peer
                let wal = match sdb_dps::WriteAheadLog::open(&wal_path) {
                    Ok(w) => w,
                    Err(_) => return (peer_id, Err(SdbError::IoError)),
                };
                let iter = match wal.scan_from(from_lsn) {
                    Ok(i) => i,
                    Err(e) => return (peer_id, Err(e)),
                };

                let mut frames: Vec<Vec<u8>> = Vec::new();
                let mut last_lsn = from_lsn;
                for record_result in iter {
                    match record_result {
                        Ok(record) => {
                            if record.lsn > flushed {
                                break;
                            }
                            last_lsn = record.lsn;
                            let mut doc = sdb_bson::Document::new();
                            doc.insert("lsn", sdb_bson::Value::Int64(record.lsn as i64));
                            doc.insert("prev_lsn", sdb_bson::Value::Int64(record.prev_lsn as i64));
                            doc.insert("txn_id", sdb_bson::Value::Int64(record.txn_id as i64));
                            doc.insert("op", sdb_bson::Value::Int32(record.op as i32));
                            doc.insert("data", sdb_bson::Value::Binary(record.data.clone()));
                            if let Ok(bytes) = doc.to_bytes() {
                                frames.push(bytes);
                            }
                        }
                        Err(_) => break,
                    }
                }

                if frames.is_empty() {
                    return (peer_id, Ok(from_lsn));
                }

                let mut payload = Vec::new();
                payload.extend_from_slice(&(frames.len() as u32).to_le_bytes());
                for frame in &frames {
                    payload.extend_from_slice(&(frame.len() as u32).to_le_bytes());
                    payload.extend_from_slice(frame);
                }

                let mut header = MsgHeader::new_request(OpCode::ReplSync as i32, 0);
                header.msg_len = (MsgHeader::SIZE + payload.len()) as i32;
                let mut msg = Vec::new();
                header.encode(&mut msg);
                msg.extend_from_slice(&payload);

                let stream_result = TcpStream::connect(&addr).await;
                let mut stream = match stream_result {
                    Ok(s) => s,
                    Err(_) => return (peer_id, Err(SdbError::NetworkError)),
                };

                if stream.write_all(&msg).await.is_err() {
                    return (peer_id, Err(SdbError::NetworkError));
                }
                let _ = stream.flush().await;

                // Read ack
                let mut len_buf = [0u8; 4];
                if stream.read_exact(&mut len_buf).await.is_err() {
                    return (peer_id, Err(SdbError::NetworkError));
                }
                let reply_len = i32::from_le_bytes(len_buf) as usize;
                let mut reply_buf = vec![0u8; reply_len];
                reply_buf[..4].copy_from_slice(&len_buf);
                if stream.read_exact(&mut reply_buf[4..]).await.is_err() {
                    return (peer_id, Err(SdbError::NetworkError));
                }

                (peer_id, Ok(last_lsn))
            }));
        }

        // Wait with 1s timeout
        let results = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            futures_join_all(handles),
        )
        .await;

        match results {
            Ok(join_results) => {
                for join_result in join_results {
                    if let Ok((peer_id, Ok(lsn))) = join_result {
                        if let Some(replica) = self.replicas.get_mut(&peer_id) {
                            replica.synced_lsn = lsn;
                        }
                    }
                }
            }
            Err(_) => {
                // Timeout — some peers may not have acked
                tracing::warn!("Replication timeout, some peers may be behind");
            }
        }

        self.is_majority_synced(write_lsn)
    }
}

/// Join all JoinHandles (avoids requiring futures crate dependency).
async fn futures_join_all<T: Send + 'static>(
    handles: Vec<tokio::task::JoinHandle<T>>,
) -> Vec<std::result::Result<T, tokio::task::JoinError>> {
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(handle.await);
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(node: u16) -> NodeAddress {
        NodeAddress { group_id: 1, node_id: node }
    }

    #[tokio::test]
    async fn add_and_sync_replica() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        agent.advance_local_lsn(100);

        let lsn = agent.sync_to(2, 100).await.unwrap();
        assert_eq!(lsn, 100);
        assert_eq!(agent.replicas[&2].synced_lsn, 100);
    }

    #[tokio::test]
    async fn sync_capped_at_local_lsn() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        agent.advance_local_lsn(50);

        let lsn = agent.sync_to(2, 100).await.unwrap();
        assert_eq!(lsn, 50); // capped
    }

    #[tokio::test]
    async fn sync_dead_replica_fails() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        agent.set_replica_alive(2, false).unwrap();
        agent.advance_local_lsn(100);

        assert!(agent.sync_to(2, 100).await.is_err());
    }

    #[tokio::test]
    async fn sync_all() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        agent.add_replica(addr(3));
        agent.advance_local_lsn(200);

        agent.sync_all().await.unwrap();
        assert_eq!(agent.replicas[&2].synced_lsn, 200);
        assert_eq!(agent.replicas[&3].synced_lsn, 200);
    }

    #[test]
    fn min_synced_lsn() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        agent.add_replica(addr(3));
        agent.replicas.get_mut(&2).unwrap().synced_lsn = 100;
        agent.replicas.get_mut(&3).unwrap().synced_lsn = 50;
        assert_eq!(agent.min_synced_lsn(), 50);
    }

    #[test]
    fn majority_synced() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        agent.add_replica(addr(3));
        agent.advance_local_lsn(100);
        // self(100) + node2(0) + node3(0) — only self synced
        assert!(!agent.is_majority_synced(100));

        agent.replicas.get_mut(&2).unwrap().synced_lsn = 100;
        // self(100) + node2(100) + node3(0) — 2/3 = majority
        assert!(agent.is_majority_synced(100));
    }

    #[test]
    fn remove_replica() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        assert!(agent.remove_replica(2).is_ok());
        assert!(agent.remove_replica(2).is_err());
    }
}
