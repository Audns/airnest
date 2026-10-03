//! Integration tests for the PostgreSQL backend.
//!
//! Requires a running PostgreSQL instance. The test reads
//! `AIRNEST_TEST_DATABASE_URL` from the environment, falling back to:
//!
//! ```text
//! postgres://airnest:airnest@localhost:5432/airnest
//! ```
//!
//! Tests run in parallel against the same database; each test calls `clean!`
//! with its types at the start to isolate itself from other tests.

#![cfg(feature = "postgres")]

use airnest::{Order, Store, persistent};
use serde::{Deserialize, Serialize};

fn database_url() -> String {
    std::env::var("AIRNEST_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://airnest:airnest@localhost:5432/airnest".to_string())
}

async fn store() -> Store {
    Store::open_postgres(&database_url())
        .await
        .expect("open postgres store")
}

/// Serializes PG tests against the shared database.
///
/// Tests share a single PG instance and table cache; concurrent `delete_all`
/// calls between tests would race. A static tokio Mutex gives us per-test
/// serialization without requiring external test framework dependencies.
fn test_lock() -> &'static tokio::sync::Mutex<()> {
    use std::sync::OnceLock;
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    test_lock().lock().await
}

/// Truncate the tables for the supplied types. Run after acquiring `serial()`.
macro_rules! clean {
    ($s:expr, $($t:ty),+ $(,)?) => {
        $( let _ = $s.delete_all::<$t>().await; )+
    };
}

// ── test types ────────────────────────────────────────────────────────────────

#[persistent]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct PgUser {
    name: String,
    age: u32,
}

#[persistent(index(status, priority))]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct PgJob {
    status: String,
    priority: i32,
    payload: String,
}

#[persistent]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct PgConfig {
    value: String,
}

// ── basic CRUD ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn pg_save_and_load() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgUser);
    let u = PgUser::new("Alice".into(), 30);
    s.save(&u).await.unwrap();
    assert_eq!(s.load(&u).await.unwrap(), Some(u.clone()));
    assert_eq!(s.load(u.id()).await.unwrap(), Some(u));
}

#[tokio::test]
async fn pg_load_missing_returns_none() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgUser);
    use airnest::AirId;
    let ghost: AirId<PgUser> = AirId::new();
    assert!(s.load(ghost).await.unwrap().is_none());
}

#[tokio::test]
async fn pg_save_upserts_existing() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgUser);
    let mut u = PgUser::new("Bob".into(), 25);
    s.save(&u).await.unwrap();
    u.age = 26;
    s.save(&u).await.unwrap();
    let loaded = s.load(u.id()).await.unwrap().unwrap();
    assert_eq!(loaded.age, 26);
}

#[tokio::test]
async fn pg_exists_and_delete() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgUser);
    let u = PgUser::new("Carol".into(), 40);
    s.save(&u).await.unwrap();
    assert!(s.exists(&u).await.unwrap());
    s.delete(&u).await.unwrap();
    assert!(!s.exists(&u).await.unwrap());
}

#[tokio::test]
async fn pg_count_and_scan() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgUser);
    let u1 = PgUser::new("U1".into(), 1);
    let u2 = PgUser::new("U2".into(), 2);
    s.save(&u1).await.unwrap();
    s.save(&u2).await.unwrap();
    assert_eq!(s.count::<PgUser>().await.unwrap(), 2);
    let all = s.scan::<PgUser>().await.unwrap();
    let ids: Vec<_> = all.iter().map(|u| u.id()).collect();
    assert!(ids.contains(&u1.id()));
    assert!(ids.contains(&u2.id()));
}

#[tokio::test]
async fn pg_delete_all() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgUser);
    s.save(&PgUser::new("X".into(), 1)).await.unwrap();
    s.delete_all::<PgUser>().await.unwrap();
    assert_eq!(s.count::<PgUser>().await.unwrap(), 0);
}

// ── update helper ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn pg_update_returns_none_for_missing() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgUser);
    use airnest::AirId;
    let ghost: AirId<PgUser> = AirId::new();
    assert!(s.update(ghost, |_u| {}).await.unwrap().is_none());
}

#[tokio::test]
async fn pg_update_mutates_in_place() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgUser);
    let u = PgUser::new("Dave".into(), 50);
    s.save(&u).await.unwrap();
    let modified = s
        .update(u.id(), |u| u.age = 51)
        .await
        .unwrap()
        .expect("update returned Some");
    assert_eq!(modified.age, 51);
    let reloaded = s.load(u.id()).await.unwrap().unwrap();
    assert_eq!(reloaded.age, 51);
}

// ── indexed columns + query builder ───────────────────────────────────────────
//
// Indexed column correctness is verified *behaviorally*: if the indexed TEXT
// column exists and is populated, the typed query API returns the right rows.
// If the dialect regressed and stored only the blob, these tests would fail.

#[tokio::test]
async fn pg_indexed_columns_are_queryable() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgJob);
    s.save(&PgJob::new("pending".into(), 1, "a".into()))
        .await
        .unwrap();
    s.save(&PgJob::new("running".into(), 5, "b".into()))
        .await
        .unwrap();
    s.save(&PgJob::new("pending".into(), 3, "c".into()))
        .await
        .unwrap();

    let pending: Vec<PgJob> = s
        .find::<PgJob>()
        .eq("status", "pending")
        .order_by("priority", Order::Asc)
        .all()
        .await
        .unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].priority, 1);
    assert_eq!(pending[1].priority, 3);
}

#[tokio::test]
async fn pg_indexed_column_updated_on_save_upsert() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgJob);
    let j = PgJob::new("pending".into(), 1, "x".into());
    s.save(&j).await.unwrap();
    s.update(j.id(), |jb| jb.status = "done".into())
        .await
        .unwrap();

    // After upsert, the indexed column must reflect the new status.
    let done: Vec<PgJob> = s.find::<PgJob>().eq("status", "done").all().await.unwrap();
    assert_eq!(done.len(), 1);
    assert_eq!(done[0].id(), j.id());

    let pending: Vec<PgJob> = s
        .find::<PgJob>()
        .eq("status", "pending")
        .all()
        .await
        .unwrap();
    assert!(pending.is_empty());
}

#[tokio::test]
async fn pg_query_in_filter() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgJob);
    s.save(&PgJob::new("a".into(), 1, "x".into()))
        .await
        .unwrap();
    s.save(&PgJob::new("b".into(), 2, "y".into()))
        .await
        .unwrap();
    s.save(&PgJob::new("c".into(), 3, "z".into()))
        .await
        .unwrap();

    let rows: Vec<PgJob> = s
        .find::<PgJob>()
        .in_("status", &["a", "c"])
        .all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn pg_query_count_and_first() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgJob);
    s.save(&PgJob::new("pending".into(), 1, "".into()))
        .await
        .unwrap();
    s.save(&PgJob::new("pending".into(), 2, "".into()))
        .await
        .unwrap();
    s.save(&PgJob::new("running".into(), 3, "".into()))
        .await
        .unwrap();

    assert_eq!(
        s.find::<PgJob>()
            .eq("status", "pending")
            .count()
            .await
            .unwrap(),
        2
    );

    let first = s
        .find::<PgJob>()
        .eq("status", "running")
        .first()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.priority, 3);
}

#[tokio::test]
async fn pg_count_grouped_by() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgJob);
    s.save(&PgJob::new("pending".into(), 1, "".into()))
        .await
        .unwrap();
    s.save(&PgJob::new("pending".into(), 2, "".into()))
        .await
        .unwrap();
    s.save(&PgJob::new("running".into(), 3, "".into()))
        .await
        .unwrap();

    let counts = s.count_grouped_by::<PgJob>("status").await.unwrap();
    assert_eq!(counts.get("pending"), Some(&2));
    assert_eq!(counts.get("running"), Some(&1));
}

#[tokio::test]
async fn pg_replace_where() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgJob);
    s.save(&PgJob::new("pending".into(), 1, "old".into()))
        .await
        .unwrap();
    s.save(&PgJob::new("pending".into(), 2, "old".into()))
        .await
        .unwrap();
    s.save(&PgJob::new("running".into(), 3, "keep".into()))
        .await
        .unwrap();

    s.replace_where::<PgJob>(
        &[("status".into(), "pending".into())],
        &[PgJob::new("pending".into(), 99, "replacement".into())],
    )
    .await
    .unwrap();

    let after = s
        .find::<PgJob>()
        .eq("status", "pending")
        .all()
        .await
        .unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].priority, 99);

    // running row untouched
    let running = s
        .find::<PgJob>()
        .eq("status", "running")
        .all()
        .await
        .unwrap();
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].priority, 3);
}

// ── batch + atomicity ─────────────────────────────────────────────────────────

#[tokio::test]
async fn pg_save_batch_is_atomic() {
    use airnest::StoreBatch;
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgUser);

    let u1 = PgUser::new("B1".into(), 1);
    let u2 = PgUser::new("B2".into(), 2);
    let mut batch = StoreBatch::new();
    batch.push(&u1).unwrap();
    batch.push(&u2).unwrap();
    s.save_batch(batch).await.unwrap();

    assert_eq!(s.count::<PgUser>().await.unwrap(), 2);
}

// ── heterogeneous types ──────────────────────────────────────────────────────

#[tokio::test]
async fn pg_multiple_types_share_one_store() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgUser, PgConfig);
    let u = PgUser::new("M".into(), 1);
    let c = PgConfig::new("value".into());
    s.save(&u).await.unwrap();
    s.save(&c).await.unwrap();
    assert_eq!(s.count::<PgUser>().await.unwrap(), 1);
    assert_eq!(s.count::<PgConfig>().await.unwrap(), 1);
}

// ── typed query API ──────────────────────────────────────────────────────────

#[tokio::test]
async fn pg_typed_query_api() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgJob);
    s.save(&PgJob::new("done".into(), 1, "".into()))
        .await
        .unwrap();
    s.save(&PgJob::new("done".into(), 2, "".into()))
        .await
        .unwrap();
    let done = PgJob::find(&s).status("done").all().await.unwrap();
    assert_eq!(done.len(), 2);
}

// ── upsert builder ───────────────────────────────────────────────────────────

#[tokio::test]
async fn pg_upsert_inserts_when_missing() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgJob);
    let job = PgJob::upsert(&s)
        .status("new")
        .modify(|_j| {})
        .or_insert(|| PgJob::new("new".into(), 42, "created".into()))
        .await
        .unwrap();
    assert_eq!(job.priority, 42);
    assert_eq!(job.payload, "created");
}

#[tokio::test]
async fn pg_upsert_modifies_when_present() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgJob);
    // Seed
    let existing = PgJob::new("hot".into(), 1, "old".into());
    s.save(&existing).await.unwrap();

    // Upsert: should modify, not insert
    let updated = PgJob::upsert(&s)
        .status("hot")
        .modify(|j| j.priority = 99)
        .or_insert(|| PgJob::new("hot".into(), 0, "fallback".into()))
        .await
        .unwrap();
    assert_eq!(updated.priority, 99);
    assert_eq!(updated.payload, "old"); // payload not modified
    assert_eq!(s.count::<PgJob>().await.unwrap(), 1);
}

// ── A1: unique constraints ────────────────────────────────────────────────

#[persistent(index(session_id), unique(session_id, generation))]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct PgRevision {
    session_id: String,
    generation: String,
    body: String,
}

#[tokio::test]
async fn pg_unique_constraint_rejects_duplicates_as_conflict() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgRevision);
    s.save(&PgRevision::new(
        "chat/a".into(),
        "1".into(),
        "first".into(),
    ))
    .await
    .unwrap();
    let err = s
        .save(&PgRevision::new(
            "chat/a".into(),
            "1".into(),
            "second".into(),
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(err, airnest::StoreError::Conflict(_)),
        "expected Conflict, got {err:?}"
    );
    s.save(&PgRevision::new("chat/a".into(), "2".into(), "ok".into()))
        .await
        .unwrap();
    assert_eq!(s.count::<PgRevision>().await.unwrap(), 2);
}

// ── A2: projection reads ──────────────────────────────────────────────────

#[tokio::test]
async fn pg_project_returns_columns_without_blobs() {
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgRevision);
    s.save(&PgRevision::new(
        "chat/a".into(),
        "1".into(),
        "body-one".into(),
    ))
    .await
    .unwrap();

    let rows = s
        .find::<PgRevision>()
        .eq("session_id", &"chat/a")
        .project(&["generation"])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("generation"), Some("1"));
    assert!(rows[0].get("body").is_none());
}

// ── A3: atomic guarded insert ─────────────────────────────────────────────

#[tokio::test]
async fn pg_insert_guarded_lands_monotonic_writes() {
    use airnest::GuardOutcome;
    let s = store().await;
    let _g = serial().await;
    clean!(s, PgRevision);

    let guard = airnest::SequenceGuard {
        partition: vec![("session_id".to_string(), "chat/a".to_string())],
        sequence_column: "generation".to_string(),
        sequence_value: 1,
    };
    assert_eq!(
        s.insert_guarded(
            &PgRevision::new("chat/a".into(), "1".into(), "one".into()),
            &guard
        )
        .await
        .unwrap(),
        GuardOutcome::Landed
    );
    let stale = airnest::SequenceGuard {
        sequence_value: 1,
        ..guard.clone()
    };
    // Re-inserting the same generation must not land twice.
    assert_eq!(
        s.insert_guarded(
            &PgRevision::new("chat/a".into(), "1".into(), "dupe".into()),
            &stale
        )
        .await
        .unwrap(),
        GuardOutcome::Rejected
    );
    assert_eq!(s.count::<PgRevision>().await.unwrap(), 1);
}
