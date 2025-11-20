pub mod repository;
mod repository_batch;

pub use repository_batch::insert_blocks;

use eyre::Result;
use log::info;
use sqlx::{PgPool, postgres::PgPoolOptions};

pub async fn create_pool(database_url: &str) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    info!("Database connection pool created with max 5 connections");
    Ok(pool)
}
