# airnest

Silent, async persistence for Rust. Derive once, store forever — across SQLite, redb, and PostgreSQL.

```rust
#[persistent]
#[derive(Serialize, Deserialize, Clone)]
pub struct Session {
    pub messages: Vec<Message>,
    pub created_at: u64,
}

// SQLite, redb, or Postgres — same API, same types
let store = Store::open("app.db").await?;
let session = Session::new(vec![], 0);
store.save(&session).await?;

let loaded = store.load(&session).await?; // Option<Session>
```

No schema files. No migrations. No SQL. Just `#[persistent]` and go.

---

## Backends

| Backend   | Status     | Feature flag    | Notes                              |
|-----------|------------|-----------------|------------------------------------|
| SQLite    | Default    | _none_          | Bundled via `sqlx`, WAL mode       |
| redb      | Optional   | `redb`          | Pure-Rust embedded key/value store |
| PostgreSQL| Optional   | `postgres`      | Via `sqlx` connection pool         |

All three share the same `Store` API, the same `#[persistent]` macro, and the same `bitcode`-encoded blob layout.

---

## Install

```toml
[dependencies]
airnest = "0.1.4"
serde   = { version = "1", features = ["derive"] }
```

`serde` is required because the macro generates `Serialize` / `Deserialize` implementations for your structs.

### Optional backends

```toml
# redb embedded KV store
airnest = { version = "0.1.4", features = ["redb"] }

# PostgreSQL
airnest = { version = "0.1.4", features = ["postgres"] }

# Postcard codec (compact, no-std friendly)
airnest = { version = "0.1.4", features = ["postcard"] }
```

---

## Quick start

```rust
use airnest::{Store, persistent};
use serde::{Deserialize, Serialize};

#[persistent]
#[derive(Serialize, Deserialize, Clone)]
pub struct Note {
    pub title: String,
    pub body:  String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = Store::open("notes.db").await?;

    let note = Note::new("hello".into(), "airnest".into());
    store.save(&note).await?;

    let loaded = store.load(&note).await?;
    println!("{:?}", loaded.map(|n| n.title));
    Ok(())
}
```

---

## Core concepts

### 1. `#[persistent]`

Mark any struct as persistable. The macro injects a UUIDv7 `id` field, generates a `new()` constructor, and auto-derives `Serialize` and `Deserialize` if they are not already present.

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
// SQLite (default)
let store = Store::open("agent.db").await?;          // persistent file
let store = Store::in_memory().await?;               // tests, ephemeral state

// redb (pure-Rust KV, no SQLite)
#[cfg(feature = "redb")]
let store = Store::open_redb("agent.redb").await?;

// PostgreSQL
#[cfg(feature = "postgres")]
let store = Store::open_postgres("postgres://user:pass@host/db").await?;

// Or use the builder for fine-grained control
let store = Store::builder("app.db")
    .codec(airnest::Codec::Json)   // Bitcode | Json | Postcard (feature)
    .open()
    .await?;
```

`Store` is cheap to clone — the underlying connection is `Arc`-wrapped. Pass it around freely.

### 3. Operations

```rust
// Upsert (insert or overwrite by embedded id)
store.save(&value).await?;

// Load by id — accepts AirId or &Value
let value = store.load(&existing_value).await?;
let value = store.load(existing_value.id()).await?;

// Bulk load by ids
let many: Vec<MyType> = store.load_many(&[id1, id2, id3]).await?;

// Delete — accepts AirId or &Value
store.delete(&existing_value).await?;

// Check existence — accepts AirId or &Value
if store.exists(&existing_value).await? { ... }

// Delete every row of a type
let n_removed: u64 = store.delete_all::<MyType>().await?;

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
let updated = store
    .update(&existing_value, |v| v.status = Status::Done)
    .await?;   // Option<T> — None if the id doesn't exist
```

### 5. Atomic batch writes

Persist multiple values of different types in one transaction:

```rust
let mut batch = StoreBatch::new();
batch.push(&session)?;
batch.push(&workflow)?;
batch.push(&tool_context)?;
store.save_batch(batch).await?;

// Or, using the store's configured codec:
let mut batch = store.batch();
batch.push(&a)?;
batch.push(&b)?;
store.save_batch(batch).await?;
```

If any `push` fails (encode error), the batch is never committed.

---

## Querying

### Indexed columns

By default, the full struct is stored as a compact binary blob. If you need to query across values — filter by status, sort by priority, range by timestamp — declare index fields:

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

Each indexed field becomes a real SQL/column alongside the blob, updated atomically on every `save`.

### The query builder

`Store::find::<T>()` returns a chainable builder that works the same on every backend:

```rust
use airnest::Order;

// eq — equality filter
let pending: Vec<Job> = store
    .find::<Job>()
    .eq("status", "pending")
    .all()
    .await?;

// eq + order_by + limit
let top3: Vec<Job> = store
    .find::<Job>()
    .eq("status", "pending")
    .order_by("priority", Order::Asc)
    .limit(3)
    .all()
    .await?;

// in_ — multi-value filter
let found: Vec<Job> = store
    .find::<Job>()
    .in_("status", &["pending", "running"])
    .all()
    .await?;

// first() — at most one row
let first = store.find::<Job>().eq("status", "running").first().await?;

// count() — without decoding blobs
let n = store.find::<Job>().eq("status", "pending").count().await?;
```

### Typed query API (macro-generated)

For every indexed field, `#[persistent]` generates a typed method. No string column names, no type ambiguity:

```rust
let pending: Vec<Job> = Job::find(&store)
    .status("pending")
    .order_by("priority", Order::Asc)
    .limit(10)
    .all()
    .await?;

let first = Job::find(&store).status("running").first().await?;
let n     = Job::find(&store).status("pending").count().await?;
```

### Aggregates and bulk operations

```rust
// count_grouped_by — rows grouped by an indexed column
let counts: HashMap<String, i64> = store.count_grouped_by::<Job>("status").await?;
// { "pending": 2, "running": 1 }

// Or use the macro-generated static helper:
let counts = Job::count_by_status(&store).await?;

// replace_where — delete rows matching filters, then insert the given items
store
    .replace_where::<Job>(
        &[("status".into(), "pending".into())],
        &[Job::new("pending".into(), 99, "replacement".into())],
    )
    .await?;

// Typed replace builder (sugar for the above)
Job::replace_for(&store)
    .status("pending")
    .items(vec![Job::new("pending".into(), 99, "new".into())])
    .await?;

// Typed upsert — modify existing or insert new
let job = Job::upsert(&store)
    .status("special")
    .modify(|j| j.priority = 42)
    .or_insert(|| Job::new("special".into(), 0, "fallback".into()))
    .await?;
```

### Raw SQL (SQLite only)

For anything the typed builder can't express, fall back to raw SQL. The `v` blob column holds the bitcode-encoded value; indexed columns live as real TEXT/INTEGER columns.

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
    .fetch_one(store.pool().expect("sqlite pool"))
    .await?;
```

Any type that implements `Display` can be an index column: `String`, `i32`, `u64`, `bool`, custom enums with `Display`, etc.

### JSON columns

For complex values you want to query on as a single indexable column, use `#[stored(json)]`:

```rust
#[persistent(index(session_uuid))]
#[derive(Clone)]
pub struct StoredMessage {
    pub session_uuid: String,
    pub sort_order:   i64,
    #[stored(json)]
    pub content:      Message,  // serialized as JSON in a `content_json` TEXT column
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Message {
    pub text: String,
}
```

`content_json` is queryable from raw SQL just like any other index column.

---

## Schemas, codecs, and registration

### Codecs

`airnest` defaults to `bitcode` — a compact, fast positional binary format. Switch to JSON for human-readable blobs, or to Postcard for `no_std`-friendly encoding:

```rust
use airnest::Codec;

let store = Store::builder("app.db")
    .codec(Codec::Json)         // Bitcode | Json | Postcard (feature)
    .open()
    .await?;
```

> The codec is selected per-store. Decoding a row requires the same codec that encoded it, so pick one up front and stick with it.

### Pre-creating tables

`Store::init` ensures the schema exists for a type or a tuple of types — useful at application startup:

```rust
// Single type
store.init::<User>().await?;

// Tuple of types — generated via macro_rules up to 12
store.init::<(User, Job, Config)>().await?;
```

After `init`, no implicit DDL happens on first `save` — saves go straight to INSERT.

### Schema evolution

Because the default codec is bitcode (positional encoding), schema changes have well-defined rules:

| Change | Strategy |
|--------|----------|
| Add a field | Wrap it in `Option<T>`. Old blobs decode to `None`. |
| Remove a field | Write a migration (load old, re-save new). |
| Rename a field | No impact — bitcode encodes by position, not name. |
| Change a field type | Write a migration. |

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
    pub tags: Option<Vec<String>>,
}
```

Old blobs decode as `tags: None`. New saves carry the value. No migration needed.

For breaking changes, run a one-shot migration at startup:

```rust
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

## Backend notes

### SQLite (default)

`Store::open("app.db")` opens or creates a persistent SQLite file. `Store::in_memory()` opens a transient `:memory:` database. The backend runs on `sqlx` with WAL mode, so concurrent readers don't block writers.

### redb

`Store::open_redb("agent.redb")` opens a pure-Rust embedded key/value store. No SQLite dependency, single-file, fully synchronous underneath. Useful when you want a tiny, self-contained binary or are not on a platform with `sqlite3` headers.

> `query_raw` (raw SQL) is not available on redb; use the typed `find`/`count_grouped_by`/`replace_where` API instead.

### PostgreSQL

`Store::open_postgres("postgres://...")` connects to a Postgres instance via `sqlx`. The schema layout is identical to SQLite (one table per type, `id` UUIDv7, `v` blob, indexed columns as TEXT, `saved_at` timestamp). Indexes are created automatically on first use.

```rust
#[cfg(feature = "postgres")]
let store = Store::open_postgres("postgres://user:pass@host:5432/db").await?;
```

The crate ships an `examples/psql_demo.rs` and `examples/psql_inspect.sql` pair — the first seeds a database, the second shows what to query with `psql` to inspect airnest's schema, indexed columns, raw blobs, and time-ordered UUIDv7 ids.

---

## Design notes

**Hybrid blobs + indexed columns** gives you the best of both worlds: compact storage and schema freedom for the struct body, real SQL columns for the fields you actually query. You only pay the column overhead where you need it.

**Why bitcode?** It's a very compact, fast binary serialization format for Rust — competitive with or smaller than bincode and faster than JSON, MessagePack, or CBOR. For agent state (large message histories, tool call logs) this matters. The tradeoff is positional encoding; see schema evolution above.

**Why UUIDv7?** The first 48 bits are a millisecond timestamp, so ids sort chronologically without an extra index. You get globally unique identifiers for free, with the side benefit that `ORDER BY id` is also `ORDER BY created_at` (modulo clock skew).

**Backend-agnostic API.** The `Store` API is identical across SQLite, redb, and PostgreSQL. Switching is a one-line change. Internally, the crate uses a `SqlDialect` trait so the relational backends share a single source of truth for table DDL and CRUD statements.

**One file (or one connection).** SQLite and redb keep state in a single file — simpler to back up, replicate, and reason about than a directory of files. PostgreSQL just uses your existing DB. WAL mode (SQLite) means readers never block writers, so an agent streaming output can read session state concurrently with the loop writing tool results.

---

## Architecture patterns for large codebases

> **Don't make everything persistent. Persist aggregates / domain entities.**

If you try to slap `#[persistent]` on every struct, you'll create a nightmare. Think in layers. Only the structs that represent **state worth saving** should carry the attribute. Child structs nested inside a persistent root should be plain `Serialize + Deserialize` values.

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

`ChatSession`, `Message`, `ToolCall`, and `StreamAccumulator` inside are plain serde structs — no nested persistence needed. This scales extremely well.

### Pattern 2 — Aggregate root model

Persist only the "root" of an aggregate:

```text
Workspace
└── ChatSession
    └── Message
        └── ToolCall
```

Only `Workspace` (or `ChatSession`) is `#[persistent]`. Everything below is plain serde. This keeps your DB simple and your mental model clean.

### Pattern 3 — Save application state

For desktop apps, editors, AI clients, games, or local-first apps, a single snapshot struct is often easiest:

```rust
#[persistent]
pub struct AppState {
    pub sessions: Vec<ChatSession>,
    pub settings: Settings,
    pub ui_state: UiState,
}

store.save(&state).await?;
```

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

rather than hundreds of persistent structs. The crate is strongest when used this way.

---

## Full example

```rust
use airnest::{Order, Store, StoreBatch, persistent};
use serde::{Serialize, Deserialize};

#[persistent]
#[derive(Serialize, Deserialize, Clone)]
pub struct AgentSession {
    pub workflow_id: String,
    pub messages:    Vec<String>,
    pub created_at:  u64,
}

#[persistent(index(status, priority))]
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

    // Query with the typed builder (macro-generated)
    let running: Vec<WorkflowRun> = WorkflowRun::find(&store)
        .status("running")
        .order_by("priority", Order::Asc)
        .all()
        .await?;

    println!("loaded: {:?}", loaded.map(|s| s.workflow_id));
    println!("running workflows: {}", running.len());
    Ok(())
}
```

---

## Examples

The crate ships runnable examples:

| Example | What it shows |
|---------|---------------|
| `cargo run --example dual_backend` | Run the same test suite against SQLite and redb and compare results |
| `cargo run --features postgres --example psql_demo` | Seed a Postgres instance, then `psql -f examples/psql_inspect.sql` to inspect airnest's schema |
