//! Side-by-side example: run the same operations on SQLite and redb,
//! and assert identical results.

use airnest::{Order, Store, StoreBatch, persistent};
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
struct Task {
    status: String,
    priority: i32,
    payload: String,
}

#[persistent]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Config {
    value: String,
}

// ── shared test suite ─────────────────────────────────────────────────────────

async fn run_full_suite(store: &Store, label: &str) -> anyhow::Result<()> {
    println!("\n========== {label} ==========");

    // --- basic CRUD ---
    let alice = User::new("Alice".into(), 30);
    let bob = User::new("Bob".into(), 25);

    store.save(&alice).await?;
    store.save(&bob).await?;
    println!("  saved 2 users");

    let loaded = store.load(alice.id()).await?;
    assert_eq!(loaded, Some(alice.clone()), "load roundtrip");
    println!("  load roundtrip: ok");

    let exists = store.exists(alice.id()).await?;
    assert!(exists, "exists should be true");
    println!("  exists: ok");

    // --- upsert ---
    let mut updated = alice.clone();
    updated.age = 31;
    store.save(&updated).await?;
    let reloaded = store.load(alice.id()).await?.expect("reloaded");
    assert_eq!(reloaded.age, 31, "upsert should overwrite");
    assert_eq!(store.count::<User>().await?, 2, "still 2 users");
    println!("  upsert: ok");

    // --- scan / load_all ---
    let all = store.load_all::<User>().await?;
    let ids: Vec<_> = all.iter().map(|u| u.id()).collect();
    assert!(ids.contains(&alice.id()));
    assert!(ids.contains(&bob.id()));
    println!("  load_all: ok ({} items)", all.len());

    // --- update helper ---
    let modified = store
        .update(alice.id(), |u| u.name = "Alice Smith".into())
        .await?
        .expect("update returned Some");
    assert_eq!(modified.name, "Alice Smith");
    println!("  update helper: ok");

    // --- delete ---
    store.delete(bob.id()).await?;
    assert!(!store.exists(bob.id()).await?);
    assert_eq!(store.count::<User>().await?, 1);
    println!("  delete: ok");

    // --- delete_all ---
    store.delete_all::<User>().await?;
    assert_eq!(store.count::<User>().await?, 0);
    println!("  delete_all: ok");

    // --- multiple types ---
    let cfg = Config::new("debug".into());
    store.save(&cfg).await?;
    assert_eq!(store.count::<Config>().await?, 1);
    println!("  multiple types: ok");

    // --- indexed columns + query builder ---
    let t1 = Task::new("pending".into(), 1, "a".into());
    let t2 = Task::new("running".into(), 5, "b".into());
    let t3 = Task::new("pending".into(), 3, "c".into());
    store.save(&t1).await?;
    store.save(&t2).await?;
    store.save(&t3).await?;

    let pending: Vec<Task> = store
        .find::<Task>()
        .eq("status", "pending")
        .order_by("priority", Order::Asc)
        .all()
        .await?;
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].priority, 1);
    assert_eq!(pending[1].priority, 3);
    println!("  query eq + order_by: ok");

    let first = store.find::<Task>().eq("status", "running").first().await?;
    assert_eq!(first.map(|t| t.priority), Some(5));
    println!("  query first: ok");

    let count = store.find::<Task>().eq("status", "pending").count().await?;
    assert_eq!(count, 2);
    println!("  query count: ok");

    let in_set: Vec<Task> = store
        .find::<Task>()
        .in_("status", &["pending", "running"])
        .all()
        .await?;
    assert_eq!(in_set.len(), 3);
    println!("  query in: ok");

    // --- count_grouped_by ---
    let grouped = store.count_grouped_by::<Task>("status").await?;
    assert_eq!(grouped.get("pending"), Some(&2));
    assert_eq!(grouped.get("running"), Some(&1));
    println!("  count_grouped_by: ok");

    // --- replace_where ---
    store
        .replace_where::<Task>(
            &[("status".into(), "pending".into())],
            &[Task::new("pending".into(), 99, "replacement".into())],
        )
        .await?;
    let after_replace = store.find::<Task>().eq("status", "pending").all().await?;
    assert_eq!(after_replace.len(), 1);
    assert_eq!(after_replace[0].priority, 99);
    println!("  replace_where: ok");

    // --- load_many ---
    let t4 = Task::new("done".into(), 0, "d".into());
    let t5 = Task::new("done".into(), 0, "e".into());
    store.save(&t4).await?;
    store.save(&t5).await?;

    let many = store.load_many(&[t4.id(), t5.id()]).await?;
    assert_eq!(many.len(), 2);
    println!("  load_many: ok");

    // --- batch ---
    let mut batch = StoreBatch::new();
    batch.push(&User::new("BatchAlice".into(), 1))?;
    batch.push(&User::new("BatchBob".into(), 2))?;
    store.save_batch(batch).await?;
    assert_eq!(store.count::<User>().await?, 2);
    println!("  save_batch: ok");

    // --- typed query API (macro-generated) ---
    let running = Task::find(store).status("running").all().await?;
    assert_eq!(running.len(), 1);
    println!("  typed query API: ok");

    // --- upsert builder ---
    let upserted = Task::upsert(store)
        .status("special")
        .modify(|t| t.priority = 42)
        .or_insert(|| Task::new("special".into(), 0, "new".into()))
        .await?;
    assert_eq!(upserted.status, "special");
    assert_eq!(upserted.priority, 0);
    println!("  upsert insert: ok");

    let upserted2 = Task::upsert(store)
        .status("special")
        .modify(|t| t.priority = 99)
        .or_insert(|| Task::new("special".into(), 0, "fallback".into()))
        .await?;
    assert_eq!(upserted2.priority, 99);
    assert_eq!(store.count::<Task>().await?, 5); // running, pending(1), done(2), special
    println!("  upsert modify: ok");

    // --- empty IN returns nothing ---
    let empty: Vec<Task> = store
        .find::<Task>()
        .in_("status", &[] as &[String])
        .all()
        .await?;
    assert!(empty.is_empty());
    println!("  empty IN filter: ok");

    // --- query_raw (SQLite only) ---
    if store.pool().is_some() {
        let raw: Vec<Task> = store
            .query_raw(r#"SELECT v FROM "Task" WHERE "status" = 'done'"#)
            .await?;
        assert_eq!(raw.len(), 2);
        println!("  query_raw (sqlite only): ok");
    } else {
        let err = store.query_raw::<Task>(r#"SELECT v FROM "Task""#).await;
        assert!(err.is_err(), "query_raw should fail on redb");
        println!("  query_raw (redb, expected error): ok");
    }

    println!("  ALL CHECKS PASSED for {label}\n");
    Ok(())
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // SQLite (in-memory)
    let sqlite = Store::in_memory().await?;
    run_full_suite(&sqlite, "SQLite").await?;

    // Redb (temp file)
    let tmp = tempfile::NamedTempFile::new()?;
    let path = tmp
        .path()
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("temp file path is not valid UTF-8"))?;
    let redb = Store::open_redb(path).await?;
    run_full_suite(&redb, "Redb").await?;

    println!("\n✅ Both backends passed the full suite!");
    Ok(())
}
