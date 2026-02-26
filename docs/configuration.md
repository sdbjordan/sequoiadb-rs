# Configuration Reference

SequoiaDB-RS is configured via CLI arguments, with optional TOML file support. CLI arguments override TOML values.

## CLI Arguments

```
sdb-server [OPTIONS]
```

| Flag | Short | Long | Default | Description |
|------|-------|------|---------|-------------|
| Role | `-r` | `--role` | `data` | Node role: `data`, `coord`, `catalog` (or `cat`) |
| Port | `-p` | `--port` | `11810` | TCP bind port |
| DB path | `-d` | `--db-path` | `/opt/sequoiadb/database` | Data directory |
| Config | `-c` | `--config` | — | Path to TOML config file |
| Catalog | — | `--catalog-addr` | — | Catalog node address (`host:port`) |
| Data addr | — | `--data-addr` | `[]` | Data node addresses (repeatable, for coord) |
| Group ID | — | `--group-id` | `1` | Replica group ID |
| Node ID | — | `--node-id` | `1` | Node ID within group |
| Repl peer | — | `--repl-peer` | `[]` | Replica peer (repeatable, format below) |

### Repl Peer Format

```
--repl-peer "node_id@host:port"    # explicit node ID
--repl-peer "host:port"            # auto-assigns node_id starting at 2
```

### Examples

**Single data node**:
```bash
sdb-server -r data -p 11810 -d /data/sdb
```

**3-node replica set**:
```bash
# Node 1 (will auto-elect as primary)
sdb-server -r data -p 11810 -d /data/n1 --group-id 1 --node-id 1 \
  --repl-peer "2@127.0.0.1:11811" --repl-peer "3@127.0.0.1:11812"

# Node 2
sdb-server -r data -p 11811 -d /data/n2 --group-id 1 --node-id 2 \
  --repl-peer "1@127.0.0.1:11810" --repl-peer "3@127.0.0.1:11812"

# Node 3
sdb-server -r data -p 11812 -d /data/n3 --group-id 1 --node-id 3 \
  --repl-peer "1@127.0.0.1:11810" --repl-peer "2@127.0.0.1:11811"
```

**Coordinator with 3 data groups**:
```bash
sdb-server -r coord -p 11800 -d /data/coord \
  --data-addr "127.0.0.1:11810" \
  --data-addr "127.0.0.1:11811" \
  --data-addr "127.0.0.1:11812"
```

**Catalog node**:
```bash
sdb-server -r catalog -p 11900 -d /data/catalog
```

## TOML Configuration File

All fields are optional. CLI arguments override TOML values.

```toml
role = "Data"                      # "Data", "Coord", or "Catalog"
host = "0.0.0.0"                   # Bind address
port = 11810                       # Bind port
db_path = "/opt/sequoiadb/database"
log_path = "/opt/sequoiadb/log"

# Cluster
catalog_addr = "127.0.0.1:11900"   # Catalog node address
data_addrs = []                    # Data node addresses (for coord)
group_id = 1                       # Replica group ID
node_id = 1                        # Node ID within group
repl_peers = []                    # Replica peers ["host:port", ...]
repl_enabled = false               # Enable replication

# Performance
max_pool_size = 100                # Max connection pool size
page_size = 65536                  # Storage page size (bytes)
log_file_size = 67108864           # WAL file size limit (64 MB)
log_file_num = 20                  # Max WAL files

# Features
transaction_on = true              # Enable transactions
monitoring_enabled = true          # Enable metrics/snapshots

# TLS (requires "tls" feature)
tls_enabled = false
tls_cert_path = "/path/to/cert.pem"
tls_key_path = "/path/to/key.pem"
tls_ca_path = "/path/to/ca.pem"

# Replication
repl_port = 11800                  # Replication port (reserved)
shard_port = 11820                 # Shard port (reserved)
```

### Loading

```rust
let config = NodeConfig::load_from_file("config.toml")?;
```

## Config Precedence

1. Default values (compiled into `NodeConfig::default()`)
2. TOML file (if `--config` specified)
3. CLI arguments (highest priority)

## Data Directory Structure

```
db_path/
├── wal/
│   └── wal.sdb              # Write-ahead log file
├── coord_state.json          # Coordinator shard config (coord only)
├── catalog_meta.json         # Catalog metadata (catalog only)
└── <cs_name>/
    └── <cl_name>.dat         # DataStore fallback persistence
```

## Connection Pools

### Coordinator → Data Node (DataNodeClient)

| Parameter | Value |
|-----------|-------|
| Max connections | 10 |
| Idle timeout | 60 seconds |

### Client Driver (sdb-client)

| Parameter | Default | Config Field |
|-----------|---------|-------------|
| Max pool size | 10 | `max_pool_size` |
| Connect timeout | 10,000 ms | `connect_timeout_ms` |
| Max retry | 3 | `max_retry` |

## Server Defaults

| Parameter | Default |
|-----------|---------|
| Cursor batch size | 100 documents |
| Heartbeat timeout | 3s + (node_id × 200ms) |
| Election loop interval | 500ms |
| Replication timeout | 1 second |
| Backoff max | 30 seconds |
| Graceful shutdown drain | 5 seconds |
