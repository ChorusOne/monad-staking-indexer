pub mod repository;
mod repository_batch;

pub use repository_batch::insert_blocks;

use crate::metrics::Metric;
use eyre::Result;
use log::info;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::mpsc;

pub async fn create_pool(database_url: &str, metrics_tx: mpsc::UnboundedSender<Metric>) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .after_connect(move |_conn, _meta| {
            let metrics_tx = metrics_tx.clone();
            Box::pin(async move {
                info!("Establishing a DB connection");
                let _ = metrics_tx.send(Metric::DbConnected);
                Ok(())
            })
        })
        .connect(database_url)
        .await?;

    info!("Database connection pool created with max 5 connections");
    Ok(pool)
}
