//! Integration tests for database operations using sqlx and alloy_primitives
use sqlx::{Row, SqlitePool, MySqlPool, PgPool};
use alloy_primitives::{Address, address};
use alloy_primitives::FixedBytes;

type MyFixedBytes = FixedBytes<20>;

fn convert_to_fixed_bytes(addr: Address) -> MyFixedBytes {
    FixedBytes::<20>::from_slice(addr.as_slice())
}


// cargo test -- --test-threads=1
#[tokio::test]
async fn test_sqlite_basic_operations() {
    // Create in-memory database
    let pool = SqlitePool::connect("sqlite::memory:")
        .await
        .expect("Failed to connect to SQLite");

    // Create test table
    sqlx::query(
        "CREATE TABLE test_fixed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixed_bytes BINARY(20) NOT NULL,
            name TEXT
        )"
    )
    .execute(&pool)
    .await
    .expect("Failed to create test table");

    // Test inserting address
    let test_fixed = convert_to_fixed_bytes(address!("0x742d35Cc6635C0532925a3b8D42cC72b5c2A9A1d"));
    sqlx::query("INSERT INTO test_fixed (fixed_bytes, name) VALUES (?, ?)")
        .bind(&test_fixed)
        .bind("Test Fixed Bytes")
        .execute(&pool)
        .await
        .expect("Failed to insert fixed bytes");

    // Test querying fixed bytes
    let row = sqlx::query("SELECT fixed_bytes, name FROM test_fixed WHERE fixed_bytes = ?")
        .bind(&test_fixed)
        .fetch_one(&pool)
        .await
        .expect("Failed to select fixed bytes");

    let retrieved_fixed: MyFixedBytes = row.get("fixed_bytes");
    let name: String = row.get("name");

    assert_eq!(retrieved_fixed, test_fixed);
    assert_eq!(name, "Test Fixed Bytes");
}

// Helper function: setup MySQL connection and test table
async fn setup_mysql_test() -> Option<MySqlPool> {
    // Try to connect to local MySQL, skip test if it fails
    let database_url = std::env::var("MYSQL_DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:123456@localhost:3306/test_db".to_string());
    
    match MySqlPool::connect(&database_url).await {
        Ok(pool) => {
            // Drop table if exists to ensure a fresh table each time
            let _ = sqlx::query("DROP TABLE IF EXISTS ethereum_fixed").execute(&pool).await.unwrap();
            // Create test table
            if sqlx::query(
                "CREATE TABLE IF NOT EXISTS ethereum_fixed (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    fixed_bytes BINARY(20) NOT NULL,
                    label VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
async fn test_mysql_basic_operations() {
    let Some(pool) = setup_mysql_test().await else {
        println!("⚠️  Skipping MySQL test - no connection available");
        return;
    };

    // Test inserting fixed bytes
    let test_fixed = convert_to_fixed_bytes(
        address!("0x742d35Cc6635C0532925a3b8D42cC72b5c2A9A1d")
    );

    sqlx::query("INSERT INTO ethereum_fixed (fixed_bytes, label) VALUES (?, ?)")
        .bind(&test_fixed)
        .bind("Test Fixed Bytes")
        .execute(&pool)
        .await
        .expect("Failed to insert fixed bytes");

    // Test querying fixed bytes
    let row = sqlx::query("SELECT fixed_bytes, label FROM ethereum_fixed WHERE fixed_bytes = ?")
        .bind(&test_fixed)
        .fetch_one(&pool)
        .await
        .expect("Failed to select fixed bytes");

    let retrieved_fixed: MyFixedBytes = row.get("fixed_bytes");
    let label: String = row.get("label");

    assert_eq!(retrieved_fixed, test_fixed);
    assert_eq!(label, "Test Fixed Bytes");

    println!("✅ MySQL basic operations test passed");
}

// Helper function: setup PostgreSQL connection and test table
async fn setup_postgres_test(table_suffix: &str) -> Option<PgPool> {
    // Try to connect to local PostgreSQL, skip test if it fails
    let database_url = std::env::var("POSTGRES_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:123456@localhost:5432/test_db".to_string());
    
    match PgPool::connect(&database_url).await {
        Ok(pool) => {
            let table_name = format!("ethereum_fixed_{}", table_suffix);
            // First drop any existing table
            let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {}", table_name))
                .execute(&pool)
                .await;
            
            // Create test table
            if sqlx::query(&format!(
                "CREATE TABLE {} (
                    id SERIAL PRIMARY KEY,
                    fixed_bytes BYTEA NOT NULL,
                    label VARCHAR(255),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
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
async fn test_postgres_basic_operations() {
    let Some(pool) = setup_postgres_test("basic").await else {
        println!("⚠️  Skipping PostgreSQL test - no connection available");
        return;
    };

    let table_name = "ethereum_fixed_basic";

    // Test inserting fixed bytes
    let test_fixed = convert_to_fixed_bytes(
        address!("0x742d35Cc6635C0532925a3b8D42cC72b5c2A9A1d")
    );

    sqlx::query(&format!("INSERT INTO {} (fixed_bytes, label) VALUES ($1, $2)", table_name))
        .bind(&test_fixed)
        .bind("Test Fixed Bytes")
        .execute(&pool)
        .await
        .expect("Failed to insert fixed bytes");

    // Test querying fixed bytes
    let row = sqlx::query(&format!("SELECT fixed_bytes, label FROM {} WHERE fixed_bytes = $1", table_name))
        .bind(&test_fixed)
        .fetch_one(&pool)
        .await
        .expect("Failed to select fixed bytes");

    let retrieved_fixed: MyFixedBytes = row.get("fixed_bytes");
    let label: String = row.get("label");

    assert_eq!(retrieved_fixed, test_fixed);
    assert_eq!(label, "Test Fixed Bytes");

    println!("✅ PostgreSQL basic operations test passed");
}



#[tokio::test]
async fn test_postgres_zero_and_special_fixed() {
    let Some(pool) = setup_postgres_test("special").await else {
        println!("⚠️  Skipping PostgreSQL zero fixed bytes test - no connection available");
        return;
    };

    let table_name: &'static str = "ethereum_fixed_special";
    let special_fixed = [
        (convert_to_fixed_bytes(Address::ZERO), "Zero fixed bytes"),
        (convert_to_fixed_bytes(address!("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")), "Max fixed bytes"),
        (convert_to_fixed_bytes(address!("0xdead000000000000000000000000000000000000")), "Dead fixed bytes"),
    ];
    
    // Insert special fixed bytes
    for (addr, label) in &special_fixed {
        sqlx::query(&format!("INSERT INTO {} (fixed_bytes, label) VALUES ($1, $2)", table_name))
            .bind(addr)
            .bind(*label)
            .execute(&pool)
            .await
            .expect("Failed to insert special fixed bytes");
    }

    // Verify they can be queried back
    let rows = sqlx::query(&format!("SELECT fixed_bytes, label FROM {} ORDER BY id", table_name))
        .fetch_all(&pool)
        .await
        .expect("Failed to select special fixed bytes");

    assert_eq!(rows.len(), special_fixed.len());

    for (i, row) in rows.iter().enumerate() {
        let retrieved_addr: MyFixedBytes = row.get("fixed_bytes");
        let label: String = row.get("label");

        assert_eq!(retrieved_addr, special_fixed[i].0);
        assert_eq!(label, special_fixed[i].1);
    }

    println!("✅ PostgreSQL special fixed bytes test passed");
}

    #[tokio::test]
    async fn test_postgres_transaction_operations() {
        let Some(pool) = setup_postgres_test("transaction").await else {
            println!("⚠️  Skipping PostgreSQL transaction test - no connection available");
            return;
        };

        let table_name = "ethereum_fixed_transaction";

        // Test batch operations in transaction
        let mut tx = pool.begin().await.expect("Failed to begin transaction");

        let test_fixed_bytes = [
            (convert_to_fixed_bytes(address!("0x1111111111111111111111111111111111111111")), "fixed 1"),
            (convert_to_fixed_bytes(address!("0x2222222222222222222222222222222222222222")), "fixed 2"),
            (convert_to_fixed_bytes(address!("0x3333333333333333333333333333333333333333")), "fixed 3"),
        ];

        // Batch insert in transaction
        for (addr, label) in &test_fixed_bytes {
            sqlx::query(&format!("INSERT INTO {} (fixed_bytes, label) VALUES ($1, $2)", table_name))
                .bind(addr)
                .bind(*label)
                .execute(&mut *tx)
                .await
                .expect("Failed to insert fixed bytes in transaction");
        }

        // Commit transaction
        tx.commit().await.expect("Failed to commit transaction");

        // Verify data exists
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {}", table_name))
            .fetch_one(&pool)
            .await
            .expect("Failed to count fixed bytes");

        assert_eq!(count, 3);

        // Test querying by address range
        let range_results = sqlx::query(&format!(
            "SELECT fixed_bytes FROM {} 
             WHERE fixed_bytes >= $1 AND fixed_bytes <= $2 
             ORDER BY fixed_bytes", table_name
        ))
        .bind(&convert_to_fixed_bytes(address!("0x1000000000000000000000000000000000000000")))
        .bind(&convert_to_fixed_bytes(address!("0x2999999999999999999999999999999999999999")))
        .fetch_all(&pool)
        .await
        .expect("Failed to query address range");

        assert_eq!(range_results.len(), 2); // Should find Address 1 and Address 2
        
        println!("✅ PostgreSQL transaction operations test passed");
    }

    #[tokio::test]
    async fn test_postgres_advanced_queries() {
        let Some(pool) = setup_postgres_test("advanced").await else {
            println!("⚠️  Skipping PostgreSQL advanced queries test - no connection available");
            return;
        };

        // Create more complex test data
        let hash_data = [
            (1, convert_to_fixed_bytes(address!("0x742d35Cc6635C0532925a3b8D42cC72b5c2A9A1d")), "Primary Hash", true),
            (1, convert_to_fixed_bytes(address!("0x1234567890123456789012345678901234567890")), "Secondary Hash", false),
            (2, convert_to_fixed_bytes(Address::ZERO), "Empty Hash", true),
            (3, convert_to_fixed_bytes(address!("0xdead000000000000000000000000000000000000")), "Burn Hash", true),
        ];

        let table_name = "user_hash_advanced";

        // First drop any existing table, then create new table
        let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {}", table_name))
            .execute(&pool)
            .await;

        // Create extended table structure
        sqlx::query(&format!(
            "CREATE TABLE {} (
                user_id INTEGER,
                hash_data BYTEA NOT NULL,
                hash_name VARCHAR(255),
                is_primary BOOLEAN DEFAULT FALSE,
                balance_wei NUMERIC(78, 0) DEFAULT 0
            )", table_name
        ))
        .execute(&pool)
        .await
        .expect("Failed to create user_hash_advanced table");

        // Insert test data
        for (user_id, hash, name, is_primary) in &hash_data {
            sqlx::query(&format!(
                "INSERT INTO {} (user_id, hash_data, hash_name, is_primary) 
                 VALUES ($1, $2, $3, $4)", table_name
            ))
            .bind(user_id)
            .bind(hash)
            .bind(*name)
            .bind(is_primary)
            .execute(&pool)
            .await
            .expect("Failed to insert hash data");
        }

        // Test complex query: find all users with primary hashes that have non-zero addresses
        let active_users = sqlx::query(&format!(
            "SELECT user_id, hash_data, hash_name 
             FROM {} 
             WHERE is_primary = TRUE AND hash_data != $1
             ORDER BY user_id", table_name
        ))
        .bind(&convert_to_fixed_bytes(Address::ZERO))
        .fetch_all(&pool)
        .await
        .expect("Failed to query active users");

        assert_eq!(active_users.len(), 2); // Users 1 and 3

        // Verify results
        let user1_hash: MyFixedBytes = active_users[0].get("hash_data");
        let user3_hash: MyFixedBytes = active_users[1].get("hash_data");

        assert_eq!(user1_hash.to_string(), "0x742D35CC6635C0532925A3b8D42cC72b5c2A9a1D".to_lowercase());
        assert_eq!(user3_hash.to_string(), "0xdEad000000000000000000000000000000000000".to_lowercase());

        // Test aggregate query: count hashes per user
        let hash_counts = sqlx::query(&format!(
            "SELECT user_id, COUNT(*) as hash_count 
             FROM {} 
             GROUP BY user_id 
             ORDER BY user_id", table_name
        ))
        .fetch_all(&pool)
        .await
        .expect("Failed to query wallet counts");

        assert_eq!(hash_counts.len(), 3);

        let user1_count: i64 = hash_counts[0].get("hash_count");
        let user2_count: i64 = hash_counts[1].get("hash_count");
        let user3_count: i64 = hash_counts[2].get("hash_count");

        assert_eq!(user1_count, 2); // User 1 has 2 hashes
        assert_eq!(user2_count, 1); // User 2 has 1 hash
        assert_eq!(user3_count, 1); // User 3 has 1 hash

        println!("✅ PostgreSQL advanced queries test passed");
    }

