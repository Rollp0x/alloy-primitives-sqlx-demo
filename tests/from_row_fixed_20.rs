use sqlx::FromRow;
use alloy_primitives::{Address, address};
use serde::{Deserialize, Serialize};
use sqlx::{SqlitePool, MySqlPool, PgPool};

use alloy_primitives::FixedBytes;

type MyFixedBytes = FixedBytes<20>;

fn convert_to_fixed_bytes(addr: Address) -> MyFixedBytes {
    FixedBytes::<20>::from_slice(addr.as_slice())
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, FromRow)]
pub struct UserInfo {
    pub id: Option<i32>,
    pub hash: MyFixedBytes,
    pub name: String,
}

#[tokio::test]
async fn test_sqlite_from_row() {
    let pool = SqlitePool::connect("sqlite::memory:")
        .await
        .expect("Failed to connect to SQLite");

    // Create test table
    sqlx::query(
        "CREATE TABLE ethereum_fixed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash BINARY(20) NOT NULL,
            name TEXT
        )"
    )
    .execute(&pool)
    .await
    .expect("Failed to create test table");

    let user_info = UserInfo {
        id: None,
        hash: convert_to_fixed_bytes(address!("0x742d35Cc6635C0532925a3b8D42cC72b5c2A9A1d")),
        name: "Test User".to_string(),
    };
    sqlx::query("INSERT INTO ethereum_fixed (hash, name) VALUES (?, ?)")
        .bind(&user_info.hash)
        .bind(&user_info.name)
        .execute(&pool)
        .await
        .expect("Failed to insert address");

    let user_info_from_db: UserInfo = sqlx::query_as("SELECT id, hash, name FROM ethereum_fixed WHERE hash = ?")
        .bind(&user_info.hash)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch user info");

    assert_eq!(user_info.name, user_info_from_db.name);
    assert_eq!(user_info.hash, user_info_from_db.hash);
}

// Helper function: setup MySQL connection and test table
async fn setup_mysql_test() -> Option<MySqlPool> {
    let database_url = std::env::var("MYSQL_DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:123456@localhost:3306/test_db".to_string());
    match MySqlPool::connect(&database_url).await {
        Ok(pool) => {
            // Drop table if exists to ensure a fresh table each time
            let _ = sqlx::query("DROP TABLE IF EXISTS ethereum_fixed").execute(&pool).await.unwrap();
            if sqlx::query(
                "CREATE TABLE IF NOT EXISTS ethereum_fixed (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    hash BINARY(20) NOT NULL,
                    name VARCHAR(255)
                )"
            )
            .execute(&pool)
            .await
            .is_ok() {
                Some(pool)
            } else {
                None
            }
        },
        Err(_) => None,
    }
}

#[tokio::test]
async fn test_mysql_from_row() {
    let Some(pool) = setup_mysql_test().await else {
        println!("⚠️  Skipping MySQL test - no connection available");
        return;
    };

    let user_info = UserInfo {
        id: None,
        hash: convert_to_fixed_bytes(address!("0x742d35Cc6635C0532925a3b8D42cC72b5c2A9A1d")),
        name: "Test User".to_string(),
    };
    sqlx::query("INSERT INTO ethereum_fixed (hash, name) VALUES (?, ?)")
        .bind(&user_info.hash)
        .bind(&user_info.name)
        .execute(&pool)
        .await
        .expect("Failed to insert address");

    let user_info_from_db: UserInfo = sqlx::query_as("SELECT id, hash, name FROM ethereum_fixed WHERE hash = ?")
        .bind(&user_info.hash)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch user info");

    assert_eq!(user_info.name, user_info_from_db.name);
    assert_eq!(user_info.hash, user_info_from_db.hash);
}

// Helper function: setup PostgreSQL connection and test table
async fn setup_postgres_test(table_suffix: &str) -> Option<PgPool> {
    let database_url = std::env::var("POSTGRES_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:123456@localhost:5432/test_db".to_string());
    match PgPool::connect(&database_url).await {
        Ok(pool) => {
            let table_name = format!("ethereum_fixed_{}", table_suffix);
            let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {}", table_name))
                .execute(&pool)
                .await;
            if sqlx::query(&format!(
                "CREATE TABLE {} (
                    id SERIAL PRIMARY KEY,
                    hash BYTEA NOT NULL,
                    name VARCHAR(255)
                )", table_name
            ))
            .execute(&pool)
            .await
            .is_ok() {
                Some(pool)
            } else {
                None
            }
        },
        Err(_) => None,
    }
}

#[tokio::test]
async fn test_postgres_from_row() {
    let Some(pool) = setup_postgres_test("fromrow").await else {
        println!("⚠️  Skipping PostgreSQL test - no connection available");
        return;
    };
    let table_name = "ethereum_fixed_fromrow";
    let user_info = UserInfo {
        id: None,
        hash: convert_to_fixed_bytes(address!("0x742d35Cc6635C0532925a3b8D42cC72b5c2A9A1d")),
        name: "Test User".to_string(),
    };
    sqlx::query(&format!("INSERT INTO {} (hash, name) VALUES ($1, $2)", table_name))
        .bind(&user_info.hash)
        .bind(&user_info.name)
        .execute(&pool)
        .await
        .expect("Failed to insert hash");

    let user_info_from_db: UserInfo = sqlx::query_as(&format!("SELECT id, hash, name FROM {} WHERE hash = $1", table_name))
        .bind(&user_info.hash)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch user info");

    assert_eq!(user_info.name, user_info_from_db.name);
    assert_eq!(user_info.hash, user_info_from_db.hash);
}
