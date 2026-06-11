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
