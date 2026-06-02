//! Integration tests for airnest.
//!
//! All tests use an in-memory store — no files on disk.

use airnest::{AirId, Store, StoreBatch, persistent};
use serde::{Deserialize, Serialize};

// ── test types ────────────────────────────────────────────────────────────────

#[persistent]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct User {
    name: String,
    age: u32,
}

#[persistent(index(status, priority))]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Job {
    status: String,
    priority: i32,
    payload: String,
}

#[persistent]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
struct Config {
    value: String,
}

// Struct with *no* explicit serde derives — the macro auto-injects them.
#[persistent]
#[derive(Clone, Debug, PartialEq)]
struct AutoSerde {
    count: i32,
}

// Type with a JSON column
#[persistent(index(session_uuid))]
#[derive(Clone, Debug, PartialEq)]
struct StoredMessage {
    session_uuid: String,
    sort_order: i64,
    #[stored(json)]
    content: Message,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Message {
    text: String,
}

// ── helpers ───────────────────────────────────────────────────────────────────

async fn store() -> Store {
    Store::in_memory().await.expect("in-memory store")
}

fn user(name: &str, age: u32) -> User {
    User::new(name.into(), age)
}

fn job(status: &str, priority: i32) -> Job {
    Job::new(status.into(), priority, "data".into())
}

fn msg(session_uuid: &str, sort_order: i64, text: &str) -> StoredMessage {
    StoredMessage::new(
        session_uuid.into(),
        sort_order,
        Message { text: text.into() },
    )
}

// ── basic CRUD ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn save_and_load() {
    let s = store().await;
    let u = user("Alice", 30);
    s.save(&u).await.unwrap();
    assert_eq!(s.load(&u).await.unwrap(), Some(u.clone()));
    assert_eq!(s.load(u.id()).await.unwrap(), Some(u));
}

#[tokio::test]
async fn load_missing_returns_none() {
    let s = store().await;
    let ghost: AirId<User> = AirId::new();
    assert!(s.load(ghost).await.unwrap().is_none());
}

#[tokio::test]
async fn save_upserts_existing() {
    let s = store().await;
    let u = user("Alice", 30);
    s.save(&u).await.unwrap();

    let mut updated = u.clone();
    updated.name = "Alice Smith".into();
    updated.age = 31;
    s.save(&updated).await.unwrap();

    let reloaded = s.load(u.id()).await.unwrap().unwrap();
    assert_eq!(reloaded.name, "Alice Smith");
    assert_eq!(reloaded.age, 31);
    assert_eq!(s.count::<User>().await.unwrap(), 1);
}

#[tokio::test]
async fn delete_removes_entry() {
    let s = store().await;
    let u = user("Alice", 30);
    s.save(&u).await.unwrap();
    s.delete(u.id()).await.unwrap();
    assert!(s.load(u.id()).await.unwrap().is_none());
}

#[tokio::test]
async fn delete_nonexistent_is_noop() {
    let s = store().await;
    let ghost: AirId<User> = AirId::new();
    s.delete(ghost).await.unwrap();
}

#[tokio::test]
async fn scan_returns_all_values() {
    let s = store().await;
    let u1 = user("Alice", 30);
    let u2 = user("Bob", 25);
    let u3 = user("Carol", 40);
    s.save(&u1).await.unwrap();
    s.save(&u2).await.unwrap();
    s.save(&u3).await.unwrap();

    let rows = s.scan::<User>().await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].id(), u1.id());
    assert_eq!(rows[1].id(), u2.id());
    assert_eq!(rows[2].id(), u3.id());
}

#[tokio::test]
async fn scan_empty_returns_empty_vec() {
    let s = store().await;
    assert!(s.scan::<User>().await.unwrap().is_empty());
}

#[tokio::test]
async fn load_all_alias() {
    let s = store().await;
    s.save(&user("Alice", 30)).await.unwrap();
    assert_eq!(s.load_all::<User>().await.unwrap().len(), 1);
}

#[tokio::test]
async fn count_tracks_inserts_and_deletes() {
    let s = store().await;
    assert_eq!(s.count::<User>().await.unwrap(), 0);
    let u1 = user("Alice", 30);
    let u2 = user("Bob", 25);
    s.save(&u1).await.unwrap();
    s.save(&u2).await.unwrap();
    assert_eq!(s.count::<User>().await.unwrap(), 2);
    s.delete(u1.id()).await.unwrap();
    assert_eq!(s.count::<User>().await.unwrap(), 1);
}

#[tokio::test]
async fn exists_checks_presence() {
    let s = store().await;
    let u = user("Alice", 30);
    s.save(&u).await.unwrap();
    assert!(s.exists(u.id()).await.unwrap());
    s.delete(u.id()).await.unwrap();
    assert!(!s.exists(u.id()).await.unwrap());
}

// ── convenience helpers ───────────────────────────────────────────────────────

#[tokio::test]
async fn update_mutates_in_place() {
    let s = store().await;
    let u = user("Alice", 30);
    s.save(&u).await.unwrap();
    let updated = s.update(u.id(), |usr| usr.age = 31).await.unwrap();
    assert_eq!(updated.unwrap().age, 31);
    assert_eq!(s.load(u.id()).await.unwrap().unwrap().age, 31);
}

#[tokio::test]
async fn update_returns_none_for_missing_key() {
    let s = store().await;
    let ghost: AirId<User> = AirId::new();
    let result = s.update(ghost, |u| u.age = 99).await.unwrap();
    assert!(result.is_none());
}

// ── atomic batch ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn save_batch_persists_multiple_types() {
    let s = store().await;
    let u = user("Alice", 30);
    let j = job("pending", 5);
    let mut batch = StoreBatch::new();
    batch.push(&u).unwrap();
    batch.push(&j).unwrap();
    s.save_batch(batch).await.unwrap();

    assert_eq!(s.load(u.id()).await.unwrap(), Some(u));
    assert_eq!(s.load(j.id()).await.unwrap(), Some(j));
}

#[tokio::test]
async fn save_batch_is_atomic() {
    let s = store().await;
    let mut batch = StoreBatch::new();
    batch.push(&user("Alice", 30)).unwrap();
    batch.push(&user("Bob", 22)).unwrap();
    s.save_batch(batch).await.unwrap();
    assert_eq!(s.count::<User>().await.unwrap(), 2);
}

// ── indexed columns ───────────────────────────────────────────────────────────

#[tokio::test]
async fn indexed_columns_queryable_via_query_raw() {
    let s = store().await;
    s.save(&job("pending", 1)).await.unwrap();
    s.save(&job("running", 5)).await.unwrap();
    s.save(&job("pending", 3)).await.unwrap();

    let pending: Vec<Job> = s
        .query_raw::<Job>(
            r#"SELECT v FROM "Job" WHERE "status" = 'pending' ORDER BY "priority" ASC"#,
        )
        .await
        .unwrap();

    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].priority, 1);
    assert_eq!(pending[1].priority, 3);
}

#[tokio::test]
async fn indexed_column_updated_on_save_upsert() {
    let s = store().await;
    let j = job("pending", 1);
    s.save(&j).await.unwrap();
    s.update(j.id(), |jb| jb.status = "done".into())
        .await
        .unwrap();

    // Verify the real TEXT column was updated.
    let id_bytes = j.id().to_bytes();
    let status: String = sqlx::query_scalar(r#"SELECT "status" FROM "Job" WHERE id = ?1"#)
        .bind(&id_bytes)
        .fetch_one(s.pool())
        .await
        .unwrap();
    assert_eq!(status, "done");
}

// ── schema evolution ──────────────────────────────────────────────────────────

#[tokio::test]
async fn new_blobs_carry_option_value() {
    #[persistent]
    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct SessionV2 {
        data: String,
        tags: Option<Vec<String>>,
    }

    let s = store().await;
    let session = SessionV2::new("world".into(), Some(vec!["rust".into(), "airnest".into()]));
    s.save(&session).await.unwrap();
    let loaded = s.load(session.id()).await.unwrap().unwrap();
    assert_eq!(loaded.tags, Some(vec!["rust".into(), "airnest".into()]));
}

// ── multiple types coexist ────────────────────────────────────────────────────

#[tokio::test]
async fn multiple_types_share_one_store() {
    let s = store().await;
    s.save(&user("Alice", 30)).await.unwrap();
    s.save(&job("pending", 1)).await.unwrap();
    s.save(&Config::new("on".into())).await.unwrap();
    assert_eq!(s.count::<User>().await.unwrap(), 1);
    assert_eq!(s.count::<Job>().await.unwrap(), 1);
    assert_eq!(s.count::<Config>().await.unwrap(), 1);
}

#[tokio::test]
async fn auto_derived_serde_works() {
    let s = store().await;
    let a = AutoSerde::new(42);
    s.save(&a).await.unwrap();
    let loaded = s.load(a.id()).await.unwrap();
    assert_eq!(loaded, Some(a));
}

// ── query builder ────────────────────────────────────────────────────────────

#[tokio::test]
async fn query_builder_eq_filters() {
    let s = store().await;
    s.save(&job("pending", 1)).await.unwrap();
    s.save(&job("running", 5)).await.unwrap();
    s.save(&job("pending", 3)).await.unwrap();

    let pending: Vec<Job> = s.find::<Job>().eq("status", "pending").all().await.unwrap();
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().all(|j| j.status == "pending"));
}

#[tokio::test]
async fn query_builder_order_and_limit() {
    let s = store().await;
    s.save(&job("pending", 5)).await.unwrap();
    s.save(&job("pending", 1)).await.unwrap();
    s.save(&job("pending", 3)).await.unwrap();

    let ordered: Vec<Job> = s
        .find::<Job>()
        .eq("status", "pending")
        .order_by("priority", airnest::Order::Asc)
        .all()
        .await
        .unwrap();

    assert_eq!(ordered.len(), 3);
    assert_eq!(ordered[0].priority, 1);
    assert_eq!(ordered[1].priority, 3);
    assert_eq!(ordered[2].priority, 5);
}

#[tokio::test]
async fn query_builder_first() {
    let s = store().await;
    s.save(&job("running", 10)).await.unwrap();
    s.save(&job("running", 20)).await.unwrap();

    let first = s
        .find::<Job>()
        .eq("status", "running")
        .order_by("priority", airnest::Order::Asc)
        .first()
        .await
        .unwrap();

    assert_eq!(first.map(|j| j.priority), Some(10));
}

#[tokio::test]
async fn query_builder_count() {
    let s = store().await;
    s.save(&job("pending", 1)).await.unwrap();
    s.save(&job("pending", 2)).await.unwrap();
    s.save(&job("running", 3)).await.unwrap();

    let n = s
        .find::<Job>()
        .eq("status", "pending")
        .count()
        .await
        .unwrap();
    assert_eq!(n, 2);
}

#[tokio::test]
async fn query_builder_no_match() {
    let s = store().await;
    s.save(&job("pending", 1)).await.unwrap();

    let empty: Vec<Job> = s.find::<Job>().eq("status", "done").all().await.unwrap();
    assert!(empty.is_empty());
}

// ── typed query API (macro-generated) ────────────────────────────────────────

#[tokio::test]
async fn typed_query_api() {
    let s = store().await;
    s.save(&job("pending", 1)).await.unwrap();
    s.save(&job("running", 5)).await.unwrap();
    s.save(&job("pending", 3)).await.unwrap();

    let pending: Vec<Job> = Job::find(&s).status("pending").all().await.unwrap();
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().all(|j| j.status == "pending"));
}

#[tokio::test]
async fn typed_query_chain() {
    let s = store().await;
    s.save(&job("pending", 5)).await.unwrap();
    s.save(&job("pending", 1)).await.unwrap();
    s.save(&job("pending", 3)).await.unwrap();

    let ordered: Vec<Job> = Job::find(&s)
        .status("pending")
        .order_by("priority", airnest::Order::Asc)
        .all()
        .await
        .unwrap();

    assert_eq!(ordered[0].priority, 1);
    assert_eq!(ordered[1].priority, 3);
    assert_eq!(ordered[2].priority, 5);
}

// ── IN queries ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn query_in_filter() {
    let s = store().await;
    s.save(&job("pending", 1)).await.unwrap();
    s.save(&job("running", 5)).await.unwrap();
    s.save(&job("done", 3)).await.unwrap();

    let found: Vec<Job> = s
        .find::<Job>()
        .in_("status", &["pending", "done"])
        .all()
        .await
        .unwrap();

    assert_eq!(found.len(), 2);
    let statuses: Vec<_> = found.iter().map(|j| j.status.as_str()).collect();
    assert!(statuses.contains(&"pending"));
    assert!(statuses.contains(&"done"));
}

#[tokio::test]
async fn query_in_empty_returns_nothing() {
    let s = store().await;
    s.save(&job("pending", 1)).await.unwrap();

    let empty: Vec<Job> = s
        .find::<Job>()
        .in_("status", &[] as &[String])
        .all()
        .await
        .unwrap();
    assert!(empty.is_empty());
}

// ── JSON columns ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn json_column_roundtrip() {
    let s = store().await;
    let m = msg("sess-1", 0, "hello");
    s.save(&m).await.unwrap();

    let loaded = s.load(m.id()).await.unwrap().unwrap();
    assert_eq!(loaded.content.text, "hello");
}

#[tokio::test]
async fn json_column_creates_text_column() {
    let s = store().await;
    let m = msg("sess-1", 0, "hello");
    s.save(&m).await.unwrap();

    // Query via raw SQL against the generated content_json column
    let rows: Vec<StoredMessage> = s
        .query_raw::<StoredMessage>(
            r#"SELECT v FROM "StoredMessage" WHERE "content_json" LIKE '%hello%'"#,
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

// ── load_many ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn load_many_by_ids() {
    let s = store().await;
    let u1 = user("Alice", 30);
    let u2 = user("Bob", 25);
    let u3 = user("Carol", 40);
    s.save(&u1).await.unwrap();
    s.save(&u2).await.unwrap();
    s.save(&u3).await.unwrap();

    let loaded = s.load_many(&[u1.id(), u2.id()]).await.unwrap();
    assert_eq!(loaded.len(), 2);
}

#[tokio::test]
async fn load_many_empty_returns_empty() {
    let s = store().await;
    let loaded: Vec<User> = s.load_many::<User, AirId<User>>(&[]).await.unwrap();
    assert!(loaded.is_empty());
}

// ── count_grouped_by ─────────────────────────────────────────────────────────

#[tokio::test]
async fn count_grouped_by_column() {
    let s = store().await;
    s.save(&job("pending", 1)).await.unwrap();
    s.save(&job("pending", 2)).await.unwrap();
    s.save(&job("running", 3)).await.unwrap();

    let counts = s.count_grouped_by::<Job>("status").await.unwrap();
    assert_eq!(counts.get("pending"), Some(&2));
    assert_eq!(counts.get("running"), Some(&1));
    assert_eq!(counts.get("done"), None);
}

// ── replace builder ──────────────────────────────────────────────────────────

#[tokio::test]
async fn replace_builder_deletes_and_inserts() {
    let s = store().await;
    s.save(&job("pending", 1)).await.unwrap();
    s.save(&job("running", 5)).await.unwrap();

    Job::replace_for(&s)
        .status("pending")
        .items(vec![job("pending", 99)])
        .await
        .unwrap();

    let remaining: Vec<Job> = s.scan::<Job>().await.unwrap();
    assert_eq!(remaining.len(), 2);
    let pending: Vec<_> = remaining
        .into_iter()
        .filter(|j| j.status == "pending")
        .collect();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].priority, 99);
}

// ── upsert builder ───────────────────────────────────────────────────────────

#[tokio::test]
async fn upsert_builder_inserts_when_missing() {
    let s = store().await;

    let item = Job::upsert(&s)
        .status("special")
        .modify(|j| j.priority = 42)
        .or_insert(|| job("special", 0))
        .await
        .unwrap();

    assert_eq!(item.status, "special");
    assert_eq!(item.priority, 0);
}

#[tokio::test]
async fn upsert_builder_modifies_when_present() {
    let s = store().await;
    let original = job("special", 10);
    s.save(&original).await.unwrap();

    let item = Job::upsert(&s)
        .status("special")
        .modify(|j| j.priority = 99)
        .or_insert(|| job("special", 0))
        .await
        .unwrap();

    assert_eq!(item.status, "special");
    assert_eq!(item.priority, 99);
    assert_eq!(s.count::<Job>().await.unwrap(), 1);
}

// ── init registration ────────────────────────────────────────────────────────

#[tokio::test]
async fn init_single_type() {
    let s = store().await;
    s.init::<User>().await.unwrap();
    assert_eq!(s.count::<User>().await.unwrap(), 0);
}

#[tokio::test]
async fn init_tuple_of_types() {
    let s = store().await;
    s.init::<(User, Job, Config)>().await.unwrap();
    assert_eq!(s.count::<User>().await.unwrap(), 0);
    assert_eq!(s.count::<Job>().await.unwrap(), 0);
    assert_eq!(s.count::<Config>().await.unwrap(), 0);
}

// ── codec switching ──────────────────────────────────────────────────────────

#[tokio::test]
async fn json_codec_roundtrip() {
    let s = Store::builder(":memory:")
        .codec(airnest::Codec::Json)
        .open()
        .await
        .unwrap();

    let u = user("Alice", 30);
    s.save(&u).await.unwrap();
    let loaded = s.load(u.id()).await.unwrap().unwrap();
    assert_eq!(loaded, u);
}

#[tokio::test]
async fn batch_with_store_codec() {
    let s = Store::builder(":memory:")
        .codec(airnest::Codec::Json)
        .open()
        .await
        .unwrap();

    let mut batch = s.batch();
    batch.push(&user("Alice", 30)).unwrap();
    batch.push(&user("Bob", 25)).unwrap();
    s.save_batch(batch).await.unwrap();

    assert_eq!(s.count::<User>().await.unwrap(), 2);
}
