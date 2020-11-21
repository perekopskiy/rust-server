use sqlx::postgres::PgPool;
use std::env;
use serde::{Deserialize, Serialize};

pub type PgConn = sqlx::pool::PoolConnection<sqlx::Postgres>;

pub struct Record {
    pub id: i32,
    pub task: Vec<u8>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub task: Vec<u8>
}

pub enum CalculationResult {
    Success(Vec<u8>),
    Failure(String),
}


pub async fn create_connection() -> anyhow::Result<PgPool> {
    let pool = PgPool::connect(&env::var("DATABASE_URL")?).await?;
    Ok(pool)
}

pub async fn retrive_connection(pool: &PgPool) -> anyhow::Result<PgConn> {
    let conn = PgPool::acquire(pool).await?;
    Ok(conn)
}

pub async fn insert(conn: &mut PgConn, task : &Vec<u8>) -> anyhow::Result<i32> {
    let rec = sqlx::query!(
        r#"
INSERT INTO requests (status, task) VALUES ('pending', $1)
RETURNING id;
        "#,
        task
    )
    .fetch_one(conn)
    .await?;

    Ok(rec.id)
}

pub async fn set_status(conn: &mut PgConn, id: i32, status: &str) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
    UPDATE requests
    SET status = $1
    WHERE id = $2
    RETURNING id;
        "#,
        status,
        id
    )
    .fetch_one(conn)
    .await?;

    Ok(())
}

pub async fn get_status(conn: &mut PgConn, id: i32) -> anyhow::Result<String> {
    let rec = sqlx::query!(
        r#"
    SELECT status
    FROM requests
    WHERE id = $1;
        "#,
        id
    )
    .fetch_one(conn)
    .await?;

    Ok(rec.status)
}

pub async fn get_pending_rec(conn: &mut PgConn) -> anyhow::Result<Record> {
    let rec = sqlx::query!(
        r#"
SELECT id, task
FROM requests
WHERE status = 'pending'
ORDER BY id
LIMIT 1
        "#
    )
    .fetch_one(conn)
    .await?;

    Ok(Record{id : rec.id, task: rec.task})
}




pub async fn get_result(conn: &mut PgConn, id: i32) -> anyhow::Result<Vec<u8>> {
    let rec = sqlx::query!(
        r#"
    SELECT result
    FROM requests
    WHERE id = $1;
        "#,
        id
    )
    .fetch_one(conn)
    .await?;

    match rec.result {
        Some(result) => Ok(result),
        None => Err(anyhow::anyhow!("no result for such id"))
    }   
}

pub async fn get_error(conn: &mut PgConn, id: i32) -> anyhow::Result<String> {
    let rec = sqlx::query!(
        r#"
    SELECT error
    FROM requests
    WHERE id = $1;
        "#,
        id
    )
    .fetch_one(conn)
    .await?;

    match rec.error {
        Some(error) => Ok(error),
        None => Err(anyhow::anyhow!("no result for such id"))
    }
}

pub async fn set_result(conn: &mut PgConn, id: i32, result: &Vec<u8>) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
    UPDATE requests
    SET result = $1
    WHERE id = $2
    RETURNING id;
        "#,
        result,
        id
    )
    .fetch_one(conn)
    .await?;

    Ok(())
}

pub async fn set_error(conn: &mut PgConn, id: i32, error: &str) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
    UPDATE requests
    SET error = $1
    WHERE id = $2
    RETURNING id;
        "#,
        error,
        id
    )
    .fetch_one(conn)
    .await?;

    Ok(())
}
