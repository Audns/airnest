# airnest ✈️

Silent, async SQLite persistence for Rust. Derive once, store forever.

```rust
#[persistent]
#[derive(Serialize, Deserialize, Clone)]
pub struct Session {
    pub messages: Vec<Message>,
    pub created_at: u64,
}

let store = Store::open("app.db").await?;
let session = Session::new(vec![], 0);
store.save(&session).await?;

let loaded = store.load(&session).await?; // Option<Session>
```

No schema files. No migrations. No SQL. Just `#[persistent]` and go.

---

## Install

```toml
[dependencies]
airnest = { version = "0.1.1" }
serde    = { version = "1", features = ["derive"] }
```

`serde` is required as a dependency because the macro generates
`Serialize` / `Deserialize` implementations for your structs.

---

## Core concepts

### 1. `#[persistent]`

Mark any struct as persistable. The macro injects a UUIDv7 `id` field,
generates a `new()` constructor, and auto-derives `Serialize` and `Deserialize`
if they are not already present.

```rust
use airnest::persistent;

#[persistent]                              // ← must be the outermost attribute
#[derive(Clone)]
pub struct WorkflowState {
    pub status: WorkflowStatus,
    pub steps:  Vec<Step>,
}

let state = WorkflowState::new(
    WorkflowStatus::Running,
    vec![],
);
println!("{:?}", state.id());   // AirId<WorkflowState>
```

`#[persistent]` must sit **above** `#[derive(...)]` so the `id` field exists before derives run.
You can still add `#[derive(Serialize, Deserialize)]` explicitly when you need custom serde
attributes such as `#[serde(rename)]` or `#[serde(default)]`.

### 2. `Store`

One store, one file, all types:

```rust
// Persistent storage
let store = Store::open("agent.db").await?;

// In-memory (tests, ephemeral state)
let store = Store::in_memory().await?;
```

`Store` is cheap to clone — the underlying connection is `Arc`-wrapped. Pass it around freely.

### 3. Operations

```rust
// Upsert (insert or overwrite by embedded id)
store.save(&value).await?;

// Load by id — accepts AirId or &Value
let value = store.load(&existing_value).await?;
let value = store.load(existing_value.id()).await?;

// Delete — accepts AirId or &Value
store.delete(&existing_value).await?;

// Check existence — accepts AirId or &Value
if store.exists(&existing_value).await? { ... }

// Scan all values of a type, ordered by save time
let all: Vec<MyType> = store.scan::<MyType>().await?;

// Alias for scan — "load everything into memory"
let all: Vec<MyType> = store.load_all::<MyType>().await?;

// Count
let n: i64 = store.count::<MyType>().await?;
```

`load`, `delete`, `exists`, and `update` all accept either an `AirId<T>` or a `&T`. The latter reads the embedded id automatically, so you rarely need to thread `.id()` through your code.

### 4. Convenience helpers

```rust
// Load → mutate → save in one call
store.update(&existing_value, |v| v.status = Status::Done).await?;
```

### 5. Atomic batch writes

Persist multiple values of different types in one SQLite transaction:

```rust
let mut batch = StoreBatch::new();
batch.push(&session)?;
batch.push(&workflow)?;
batch.push(&tool_context)?;
store.save_batch(batch).await?;
```

If any push fails (encode error), the batch is never committed.

---

## Indexed columns

By default, the full struct is stored as a compact binary blob. If you need to
query across values — filter by status, sort by priority, range by timestamp —
declare index fields:

```rust
#[persistent(index(status, priority, created_at))]
#[derive(Serialize, Deserialize, Clone)]
pub struct Job {
    pub status:     String,   // "pending" | "running" | "done"
    pub priority:   i32,
    pub created_at: u64,
    pub payload:    Vec<u8>,  // not indexed — lives only in the blob
}
```

Each indexed field becomes a real SQLite `TEXT` column alongside the blob,
updated atomically on every `save`. Query them via `query_raw` or `pool()`:

```rust
// Typed helper — decodes the blob column automatically
let pending: Vec<Job> = store
    .query_raw::<Job>(
        r#"SELECT v FROM "Job"
           WHERE "status" = 'pending'
           ORDER BY "priority" ASC"#,
    )
    .await?;

// Full escape hatch — raw pool access for anything else
let count: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM "Job" WHERE "status" != 'done'"#,
    )
    .fetch_one(store.pool())
    .await?;
```

Any type that implements `Display` can be an index column: `String`, `i32`,
`u64`, `bool`, custom enums with `Display`, etc.

---

## Schema evolution

airnest uses **bitcode** — a bitwise binary serialization format. This means:

| Change | Strategy |
|--------|----------|
| Add a field | Wrap it in `Option<T>`. Old blobs decode to `None`. |
| Remove a field | Write a migration (load old, re-save new). |
| Rename a field | No impact — bitcode encodes by position, not name. |
| Change a field type | Write a migration. |

### Adding a field

```rust
// V1 (already stored)
#[persistent]
#[derive(Serialize, Deserialize, Clone)]
pub struct Session {
    pub data: String,
}

// V2 — wrap the new field in Option
#[persistent]
#[derive(Serialize, Deserialize, Clone)]
pub struct Session {
    pub data: String,
    pub tags: Option<Vec<String>>,  // None for old blobs, Some(...) for new
}
```

Old blobs decode as `tags: None`. New saves carry the value. No migration needed.

### Writing a migration

For breaking changes, run a migration at startup:

```rust
// One-time migration: re-encode all rows under the new schema
async fn migrate_sessions(store: &Store) -> Result<(), StoreError> {
    let all: Vec<SessionV1> = store.load_all::<SessionV1>().await?;
    for old in all {
        let new = SessionV2::from(old);
        store.save(&new).await?;
    }
    Ok(())
}
```

---

## Design notes

**Why not sqlx?** The current crates.io index requires Rust ≥ 1.85 for sqlx's
transitive dependencies. airnest uses `rusqlite` (bundled SQLite, zero system
deps) and `tokio::task::spawn_blocking` to keep the async contract without
requiring a bleeding-edge toolchain.

**Why bitcode?** It's a very compact, fast binary serialization format for
Rust — competitive with or smaller than bincode and faster than JSON, MessagePack,
or CBOR. For agent state (large message histories, tool call logs) this matters.
The tradeoff is positional encoding; see schema evolution above.

**Why one file?** One SQLite WAL file is simpler to back up, replicate, and
reason about than a directory of files. WAL mode means readers never block
writers, so an agent streaming output can read session state concurrently with
the loop writing tool results.

**Hybrid blobs + indexed columns** gives you the best of both worlds: compact
storage and schema freedom for the struct body, real SQLite indexes for the
fields you actually query. You only pay the column overhead where you need it.

---

## Architecture patterns for large codebases

> **Don't make everything persistent. Persist aggregates / domain entities.**

If you try to slap `#[persistent]` on every struct, you’ll create a nightmare.
Think in layers. Only the structs that represent **state worth saving** should
carry the attribute. Child structs nested inside a persistent root should be
plain `Serialize + Deserialize` values.

---

### Pattern 1 — Persistence boundary (recommended)

Create a `persistence/` folder and keep persistence decisions localized:

```text
src/
├── ui/
├── services/
├── engine/
└── persistence/
    ├── chat_session.rs
    ├── settings.rs
    └── workspace.rs
```

Only these get `#[persistent]`:

```rust
#[persistent(index(name))]
#[derive(Clone)]
pub struct Workspace {
    pub name: String,
    pub chats: Vec<ChatSession>,
}
```

`ChatSession`, `Message`, `ToolCall`, and `StreamAccumulator` inside are plain
serde structs — no nested persistence needed. This scales extremely well.

---

### Pattern 2 — Aggregate root model

Persist only the "root" of an aggregate:

```text
Workspace
└── ChatSession
    └── Message
        └── ToolCall
```

Only `Workspace` (or `ChatSession`) is `#[persistent]`. Everything below is
plain serde. This keeps your DB simple and your mental model clean.

---

### Pattern 3 — Save application state

For desktop apps, editors, AI clients, games, or local-first apps, a single
snapshot struct is often easiest:

```rust
#[persistent]
pub struct AppState {
    pub sessions: Vec<ChatSession>,
    pub settings: Settings,
    pub ui_state: UiState,
}
```

Then:

```rust
store.save(&state).await?;
```

Boom — whole app snapshot.

---

### Pattern 4 — Repository layer

Instead of calling `store` directly everywhere, wrap it:

```rust
pub struct SessionRepo {
    store: Store,
}

impl SessionRepo {
    pub async fn save(&self, session: &ChatSession) -> Result<(), StoreError> {
        self.store.save(session).await
    }

    pub async fn load(&self, id: AirId<ChatSession>) -> Result<Option<ChatSession>, StoreError> {
        self.store.load(id).await
    }
}
```

Business logic stays clean and the persistence boundary is explicit.

---

### Pattern 5 — Domain module convention

A very scalable convention:

```text
chat/
├── mod.rs
├── model.rs          // plain structs
└── persistence.rs    // #[persistent] roots
```

`model.rs`:

```rust
pub struct Message { ... }
pub struct ToolCall { ... }
pub struct StreamAccumulator { ... }
```

`persistence.rs`:

```rust
#[persistent]
pub struct ChatSession {
    pub messages: Vec<Message>,
}
```

---

### A heuristic for deciding persistence

Ask:

> "Would I ever independently load/save this?"

If **yes** → `#[persistent]`
If **no**  → plain serde

For a large app, aim for:

```text
5–20 persistent structs
hundreds of normal structs
```

rather than hundreds of persistent structs. The crate is strongest when used
this way.

---

## Full example

```rust
use airnest::{persistent, Store, StoreBatch};
use serde::{Serialize, Deserialize};

#[persistent]
#[derive(Serialize, Deserialize, Clone)]
pub struct AgentSession {
    pub workflow_id: String,
    pub messages:    Vec<String>,
    pub created_at:  u64,
}

#[persistent(index(status))]
#[derive(Serialize, Deserialize, Clone)]
pub struct WorkflowRun {
    pub status: String,   // indexed — queryable without loading all blobs
    pub steps:  Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = Store::open("agent.db").await?;

    // Save
    let session = AgentSession::new("wf1".into(), vec![], 0);
    store.save(&session).await?;

    // Load — by value reference (ergonomic)
    let loaded = store.load(&session).await?;

    // Or by id explicitly
    let loaded = store.load(session.id()).await?;

    // Atomic multi-type write
    let run = WorkflowRun::new("running".into(), vec![]);
    let mut batch = StoreBatch::new();
    batch.push(&session)?;
    batch.push(&run)?;
    store.save_batch(batch).await?;

    // Query on indexed column
    let running: Vec<WorkflowRun> = store
        .query_raw::<WorkflowRun>(r#"SELECT v FROM "WorkflowRun" WHERE "status" = 'running'"#)
        .await?;

    println!("loaded: {:?}", loaded.map(|s| s.workflow_id));
    println!("running workflows: {}", running.len());
    Ok(())
}
```

---

## License

MIT OR Apache-2.0
