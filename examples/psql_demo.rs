//! Demonstrate inspecting airnest's PostgreSQL state with `psql`.
//!
//! Run in two phases:
//!
//!   1. `cargo run --features postgres --example psql_demo`   (seeds the DB)
//!   2. `psql ... < examples/psql_inspect.sql`                (queries it)
//!
//! This binary seeds the database; the SQL file shows what to look at.

use airnest::{Store, persistent};
use serde::{Deserialize, Serialize};

#[persistent(index(role))]
#[derive(Serialize, Deserialize, Clone, Debug)]
struct DemoUser {
    name: String,
    role: String,
}

#[persistent(index(status, priority))]
#[derive(Serialize, Deserialize, Clone, Debug)]
struct DemoTask {
    title: String,
    status: String,
    priority: i32,
    payload: Vec<u8>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("AIRNEST_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://airnest:airnest@localhost:5432/airnest".to_string());

    let store = Store::open_postgres(&url).await?;

    // Clean slate (idempotent re-runs).
    let _ = store.delete_all::<DemoUser>().await;
    let _ = store.delete_all::<DemoTask>().await;

    // Seed users.
    for (name, role) in [
        ("alice", "admin"),
        ("bob", "editor"),
        ("carol", "viewer"),
        ("dave", "editor"),
    ] {
        store.save(&DemoUser::new(name.into(), role.into())).await?;
    }

    // Seed tasks with varying status / priority.
    for (title, status, priority, payload) in [
        ("Write spec", "pending", 1, b"draft".to_vec()),
        ("Review PR", "pending", 3, b"needs review".to_vec()),
        ("Deploy v1", "running", 5, b"in progress".to_vec()),
        ("Archive logs", "done", 0, b"".to_vec()),
    ] {
        store
            .save(&DemoTask::new(
                title.into(),
                status.into(),
                priority,
                payload,
            ))
            .await?;
    }

    println!("Seeded {} users, {} tasks.", 4, 4);
    println!("\nNow run:\n");
    println!("  psql 'postgres://airnest:airnest@localhost:5432/airnest' \\");
    println!("       -f examples/psql_inspect.sql");
    Ok(())
}
