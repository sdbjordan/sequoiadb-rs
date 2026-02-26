# Authentication & Authorization (sdb-auth)

SequoiaDB-RS provides per-connection authentication with role-based access control (RBAC).

## User Model

```rust
pub struct User {
    pub name: String,
    pub password_hash: Vec<u8>,  // 32 bytes
    pub roles: Vec<String>,
}
```

### Password Hashing

FNV-1a inspired 32-byte hash (v1 — not cryptographically strong):

1. Combine `username:password` bytes
2. Initialize `h = 0xcbf29ce484222325` (u64)
3. For each byte: `h ^= byte; h = h.wrapping_mul(0x0100000001b3)`
4. Distribute into 32-byte array: `hash[i % 32] ^= (h & 0xFF)`, `hash[(i+7) % 32] ^= ((h >> 8) & 0xFF)`

### Authentication Flow

1. Client sends `$authenticate { User: "admin", Passwd: "password" }` via QueryReq
2. Server hashes the provided password
3. Compares with stored hash
4. On success: marks the connection as authenticated in the session map
5. On failure: returns `SdbError::AuthFailed` (wire code -179)

## Bootstrap Mode

When no users exist in the system, authentication is not required. This allows initial setup:

1. Fresh server starts → no users → `auth_required()` returns false
2. Client connects → all operations allowed without `$authenticate`
3. Client sends `$create user { User: "admin", Passwd: "...", Roles: [...] }`
4. First user created → `auth_required()` returns true
5. All subsequent connections must authenticate

## Role-Based Access Control

### Permissions

```rust
pub enum Permission {
    Find, Insert, Update, Delete,
    CreateCL, DropCL, CreateCS, DropCS,
    CreateIndex, DropIndex,
    Admin,  // grants all permissions
}
```

### Resources

```rust
pub enum Resource {
    Cluster,                    // covers everything
    CollectionSpace(String),    // covers all CLs in the CS
    Collection(String),         // covers specific "cs.cl"
}
```

**Hierarchy**: `Cluster` covers all resources. `CollectionSpace("cs")` covers `Collection("cs.cl")` (prefix match with dot boundary check).

### Role Definition

```rust
pub struct Role {
    pub name: String,
    pub permissions: Vec<(Permission, Resource)>,
}
```

A role grants a permission if any `(perm, resource)` pair matches where:
- `perm == requested_perm || perm == Admin`
- `resource.covers(requested_resource)`

## AuthManager

```rust
pub struct AuthManager {
    users: Vec<User>,
    roles: Vec<Role>,
}
```

| Operation | Method |
|-----------|--------|
| Authenticate | `authenticate(username, password) -> bool` |
| Authorize | `authorize(username, perm, resource) -> bool` |
| Create user | `create_user(user) -> Result<()>` (fails if exists) |
| Drop user | `drop_user(username) -> Result<()>` |
| Create role | `create_role(role) -> Result<()>` |
| Drop role | `drop_role(name) -> Result<()>` |

## Server Integration

The `DataNodeHandler` tracks per-connection auth state:

```rust
sessions: Arc<Mutex<HashMap<SocketAddr, bool>>>  // addr -> authenticated
```

### Command Auth Requirements

| Command | Auth Required |
|---------|--------------|
| `$authenticate` | Never |
| `$create user` | No (bootstrap), Yes (after first user) |
| All DDL commands | Yes |
| All DML (Insert/Update/Delete/Query) | Yes |
| `$snapshot *` | Yes |
| GetMore / KillContext | No |
| Transaction ops | No |
| Replication ops | No |

### Wire Commands

```
$create user  { User: "admin", Passwd: "secret", Roles: ["admin"] }
$authenticate { User: "admin", Passwd: "secret" }
$drop user    { User: "admin" }
```
