//! Smoke tests for the redb backend.

#![cfg(feature = "redb")]

use airnest::{Store, persistent};
use serde::{Deserialize, Serialize};

#[persistent]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Task {
    status: String,
    payload: String,
}

#[tokio::test]
async fn redb_save_and_load() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let s = Store::open_redb(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    let t = Task::new("pending".into(), "buy milk".into());
    s.save(&t).await.unwrap();

    let loaded = s.load(t.id()).await.unwrap();
    assert_eq!(loaded, Some(t));
}

#[tokio::test]
async fn redb_scan_and_count() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let s = Store::open_redb(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    let t1 = Task::new("pending".into(), "a".into());
    let t2 = Task::new("done".into(), "b".into());
    s.save(&t1).await.unwrap();
    s.save(&t2).await.unwrap();

    assert_eq!(s.count::<Task>().await.unwrap(), 2);
    let all = s.scan::<Task>().await.unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn redb_delete_and_exists() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let s = Store::open_redb(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    let t = Task::new("pending".into(), "x".into());
    s.save(&t).await.unwrap();
    assert!(s.exists(t.id()).await.unwrap());

    s.delete(t.id()).await.unwrap();
    assert!(!s.exists(t.id()).await.unwrap());
}

// ── A1: unique constraints (redb enforcement) ─────────────────────────────

#[persistent(index(session_id), unique(session_id, generation))]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Revision {
    session_id: String,
    generation: String,
    body: String,
}

#[tokio::test]
async fn redb_unique_constraint_rejects_duplicates_as_conflict() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let s = Store::open_redb(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    s.save(&Revision::new("chat/a".into(), "1".into(), "first".into()))
        .await
        .unwrap();
    let err = s
        .save(&Revision::new("chat/a".into(), "1".into(), "second".into()))
        .await
        .unwrap_err();
    assert!(
        matches!(err, airnest::StoreError::Conflict(_)),
        "expected Conflict, got {err:?}"
    );

    s.save(&Revision::new("chat/a".into(), "2".into(), "ok".into()))
        .await
        .unwrap();
    assert_eq!(s.count::<Revision>().await.unwrap(), 2);
}

#[tokio::test]
async fn redb_unique_constraint_allows_rewriting_the_same_row() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let s = Store::open_redb(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    let mut a = Revision::new("chat/a".into(), "1".into(), "first".into());
    s.save(&a).await.unwrap();
    // Same id, same unique tuple: an upsert of the row itself.
    a.body = "edited".into();
    s.save(&a).await.unwrap();
    assert_eq!(s.count::<Revision>().await.unwrap(), 1);
}

#[tokio::test]
async fn redb_unique_constraint_frees_keys_on_delete() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let s = Store::open_redb(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    let a = Revision::new("chat/a".into(), "1".into(), "first".into());
    s.save(&a).await.unwrap();
    s.delete(a.id()).await.unwrap();
    s.save(&Revision::new("chat/a".into(), "1".into(), "second".into()))
        .await
        .unwrap();
    assert_eq!(s.count::<Revision>().await.unwrap(), 1);
}

#[tokio::test]
async fn redb_unique_upsert_changing_values_releases_the_old_key() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let s = Store::open_redb(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    let mut a = Revision::new("chat/a".into(), "1".into(), "first".into());
    s.save(&a).await.unwrap();
    // Move the row to a new generation: the old tuple must be released.
    a.generation = "2".into();
    s.save(&a).await.unwrap();

    s.save(&Revision::new("chat/a".into(), "1".into(), "reused".into()))
        .await
        .unwrap();
    assert_eq!(s.count::<Revision>().await.unwrap(), 2);
}

// ── A2: projection reads ──────────────────────────────────────────────────

#[tokio::test]
async fn redb_project_returns_columns_without_blobs() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let s = Store::open_redb(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    s.save(&Revision::new("chat/a".into(), "1".into(), "body-one".into()))
        .await
        .unwrap();
    s.save(&Revision::new("chat/a".into(), "2".into(), "body-two".into()))
        .await
        .unwrap();

    let rows = s
        .find::<Revision>()
        .eq("session_id", &"chat/a")
        .project(&["generation"])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].get("body").is_none());
}

// ── A3: atomic guarded insert ─────────────────────────────────────────────

fn redb_seq_guard(session: &str, generation: i64) -> airnest::SequenceGuard {
    airnest::SequenceGuard {
        partition: vec![("session_id".to_string(), session.to_string())],
        sequence_column: "generation".to_string(),
        sequence_value: generation,
    }
}

#[tokio::test]
async fn redb_insert_guarded_lands_monotonic_writes() {
    use airnest::GuardOutcome;
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let s = Store::open_redb(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(
        s.insert_guarded(
            &Revision::new("chat/a".into(), "1".into(), "one".into()),
            &redb_seq_guard("chat/a", 1)
        )
        .await
        .unwrap(),
        GuardOutcome::Landed
    );
    assert_eq!(
        s.insert_guarded(
            &Revision::new("chat/a".into(), "2".into(), "two".into()),
            &redb_seq_guard("chat/a", 2)
        )
        .await
        .unwrap(),
        GuardOutcome::Landed
    );
    assert_eq!(
        s.insert_guarded(
            &Revision::new("chat/a".into(), "1".into(), "stale".into()),
            &redb_seq_guard("chat/a", 1)
        )
        .await
        .unwrap(),
        GuardOutcome::Rejected
    );
    assert_eq!(s.count::<Revision>().await.unwrap(), 2);
}

#[tokio::test]
async fn redb_insert_guarded_concurrent_racers_land_exactly_once() {
    use airnest::GuardOutcome;
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let s = Store::open_redb(tmp.path().to_str().unwrap())
        .await
        .unwrap();
    s.insert_guarded(
        &Revision::new("chat/a".into(), "1".into(), "one".into()),
        &redb_seq_guard("chat/a", 1),
    )
    .await
    .unwrap();

    let mut handles = Vec::new();
    for i in 0..10 {
        let s = s.clone();
        handles.push(tokio::spawn(async move {
            let r = Revision::new("chat/a".into(), "2".into(), format!("racer-{i}"));
            s.insert_guarded(&r, &redb_seq_guard("chat/a", 2)).await.unwrap()
        }));
    }
    let mut landed = 0;
    for h in handles {
        if h.await.unwrap() == GuardOutcome::Landed {
            landed += 1;
        }
    }
    assert_eq!(landed, 1, "exactly one racer may land G2");
    assert_eq!(s.count::<Revision>().await.unwrap(), 2);
}
