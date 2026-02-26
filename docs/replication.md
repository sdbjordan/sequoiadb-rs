# Replication (sdb-cls)

Replication provides high availability through a Raft-inspired election protocol and push-based WAL replication within each data group.

## Replica Group Topology

```
Data Group N
┌──────────────────────────────────────────┐
│                                          │
│  Primary ──WAL push──▶ Secondary 1       │
│     │                                    │
│     └──WAL push──▶ Secondary 2           │
│                                          │
│  Election: term-based voting             │
│  Heartbeat: 500ms loop, 3s timeout       │
│                                          │
└──────────────────────────────────────────┘
```

Each data group is an independent replica set. Nodes within a group communicate via the same TCP port (11810), using OpCodes in the 4000 range to distinguish replication traffic from client traffic.

## Election Protocol

### State Machine

```
                 timeout
  Unknown ──────────────▶ Candidate
     ▲                       │
     │                       │ majority votes
     │ heartbeat             ▼
     │               ┌── Primary
     │               │
  Secondary ◀────────┘
     ▲        step down / higher term
     │
     └── receives heartbeat from valid primary
```

**States**: `Unknown` → `Candidate` → `Primary` or `Secondary`

### Election Manager

```rust
pub struct ElectionManager {
    local_node: NodeAddress,
    state: ElectionState,
    primary: Option<NodeId>,
    term: u64,                      // starts at 0
    peers: Vec<NodeAddress>,
    votes_received: u32,
    last_heartbeat: Instant,
    heartbeat_timeout: Duration,    // 3s + node_id × 200ms
    voted_for: Option<NodeId>,
    local_lsn: Lsn,
}
```

### Election Flow

1. **Timeout detection** (checked every 500ms):
   - If not primary AND `elapsed > heartbeat_timeout` AND has peers → start election

2. **Start election** (`start_election()`):
   - Increment `term` by 1
   - Set state to `Candidate`
   - Vote for self (`votes_received = 1`, `voted_for = self`)
   - If single node (no peers): immediately promote to Primary

3. **Request votes**:
   - Send `ReplVoteReq` to each peer: `{ term, candidate_id, last_lsn }`
   - Each peer evaluates `should_grant_vote()`

4. **Vote granting** (`should_grant_vote()`):
   - Reject if `candidate_term < self.term`
   - If `candidate_term > self.term`: update own term, step down to Secondary
   - Reject if already voted for someone else this term
   - Reject if `candidate_lsn < local_lsn` (log completeness)
   - On grant: record vote, reset heartbeat timer

5. **Collect votes** (`receive_vote()`):
   - Increment `votes_received`
   - If `votes_received >= majority` (= `total_nodes / 2 + 1`): promote to Primary

6. **Heartbeat** (primary sends every 500ms):
   - `ReplHeartbeat`: `{ term, primary_id, commit_lsn }`
   - Receivers: if `term >= self.term`, accept heartbeat, set state to Secondary

### Jitter

Heartbeat timeout = `3000ms + (node_id × 200ms)`. This staggering reduces the chance of multiple nodes starting elections simultaneously (split vote prevention).

### Dynamic Membership

- `$add member { NodeId, Host, Port }` — adds peer to election manager and replication agent at runtime
- `$remove member { NodeId }` — removes peer
- `$get members` — lists local node + all peers with status

## WAL Replication

### Push Model

The primary pushes WAL frames to secondaries after each write:

```
Primary                          Secondary
   │                                 │
   │──── ReplSync (WAL frames) ────▶│
   │                                 │ apply frames
   │◀──── Reply (acked_lsn) ────────│
   │                                 │
```

### Replication Agent

```rust
pub struct ReplicationAgent {
    local_node: NodeAddress,
    local_lsn: Lsn,
    replicas: HashMap<u16, ReplicaStatus>,
    peer_addrs: HashMap<u16, String>,  // node_id -> "host:port"
}

pub struct ReplicaStatus {
    node: NodeAddress,
    synced_lsn: Lsn,
    is_alive: bool,
    fail_count: u32,
    next_retry: Option<Instant>,
}
```

### Push Flow (`push_wal_to_peer`)

1. Open a fresh `WalIterator` from disk (independent file handle, no WAL lock)
2. Collect frames from `from_lsn` to `flushed_lsn`
3. Serialize each `LogRecord` as BSON: `{ lsn, prev_lsn, txn_id, op, data }`
4. Build payload: `[num_records: u32 LE] [frame_len: u32 LE + frame_bytes]*`
5. Wrap in `MsgHeader` with opcode `ReplSync` (4001)
6. Send via TCP, read ack reply
7. Update `synced_lsn` for the peer

### Parallel Replication (`replicate_and_check_majority`)

1. Spawn one tokio task per alive (or retry-ready) peer
2. Each task independently reads WAL and sends frames
3. Wait with **1 second timeout** (`tokio::time::timeout`)
4. On success: update `synced_lsn`, record success
5. On failure/timeout: record failure (triggers exponential backoff)
6. Return `is_majority_synced(write_lsn)`

### Majority Acknowledgment

After replicating, the agent checks: `(count_synced + 1_self) > total_nodes / 2`. If majority is not reached within 1 second, the write still succeeds (degraded mode) — the timeout prevents blocking clients.

### Exponential Backoff

On failure:
- `fail_count += 1`
- `next_retry = now + min(30s, 2^fail_count seconds)`
- `is_alive = false`

On success:
- `fail_count = 0`
- `next_retry = None`
- `is_alive = true`

The election loop checks `peers_needing_catchup()` — peers that are `!is_alive` but whose `next_retry` has passed — and attempts reconnection.

### Secondary Receiving Frames (`handle_repl_sync`)

When a secondary receives `ReplSync`:
1. Decode the WAL frames from the payload
2. For each frame, replay into local catalog (same logic as WAL recovery)
3. Write to local WAL via `wal.append()`
4. Return `acked_lsn` in reply

## Write Guard

The `DataNodeHandler` enforces primary-only writes:

```rust
fn check_primary(&self) -> Result<()> {
    match &self.election {
        None => Ok(()),  // no replication = always accept
        Some(em) => if em.lock().unwrap().is_primary() { Ok(()) }
                    else { Err(SdbError::NotPrimary) }
    }
}
```

Called before every Insert, Update, and Delete operation.

## Read Preference

Reads can be routed to secondaries based on per-connection `ReadPreference`:

| Mode | Behavior |
|------|----------|
| `Primary` | Always read from primary (default) |
| `SecondaryPreferred` | Try secondary first, fallback to primary |
| `Secondary` | Must read from secondary, fail if none |
| `Nearest` | Lowest latency (acts as SecondaryPreferred in v1) |

## Replication OpCodes

| OpCode | Value | Direction | Payload |
|--------|-------|-----------|---------|
| `ReplSync` | 4001 | Primary → Secondary | WAL frames |
| `ReplConsistency` | 4002 | — | Reserved |
| `ReplVoteReq` | 4003 | Candidate → Peers | `{ term, candidate_id, last_lsn }` |
| `ReplVoteReply` | 4004 | Peer → Candidate | `{ term, voter_id, granted }` |
| `ReplHeartbeat` | 4005 | Primary → All | `{ term, primary_id, commit_lsn }` |
| `ReplMemberChange` | 4006 | Any → Any | Dynamic membership changes |
