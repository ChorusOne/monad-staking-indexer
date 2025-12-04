use crate::events::EventType;
use aetos::metrics;
use axum::response::IntoResponse;
use eyre::Result;
use log::info;
use std::collections::HashMap;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq)]
pub enum Metric {
    InsertedEvents(HashMap<EventType, (u64, u64)>),
    BackfilledBlocks(u64),
    FailedToBackfill(u64),
    FailedToInsert,
    InsertTimeout,
    DbConnected,
    RpcTimeout,
    RpcConnRefused,
    TransactionsFetched(u64),
    TransactionFetchFailed(u64),
    TransactionGapsFound(u64),
    BackfilledBlockTips(u64),
    BlockTipsFetchFailed(u64),
    BackfilledDelegatorSnapshots(u64),
    DelegatorSnapshotFetchFailed(u64),
}

#[metrics(prefix = "staking")]
#[derive(Debug, Clone)]
struct MetricsState {
    #[counter(
        name = "events_inserted_total",
        help = "Total number of staking events inserted into the database",
        label = "event_type"
    )]
    inserted: HashMap<EventType, u64>,

    #[counter(
        name = "events_duplicates_total",
        help = "Total number of duplicate staking events detected",
        label = "event_type"
    )]
    duplicates: HashMap<EventType, u64>,

    #[counter(help = "Number of blocks backfilled")]
    backfilled_blocks_ok: u64,

    #[counter(help = "Number of blocks that failed to backfill")]
    backfilled_blocks_err: u64,

    #[counter(help = "Number of events that failed to be inserted")]
    insert_events_err: u64,

    #[counter(help = "Number of insert operations that timed out")]
    insert_timeout_err: u64,

    #[counter(
        name = "db_connections_total",
        help = "Total number of database connections established"
    )]
    db_connections: u64,

    #[counter(help = "Number of RPC timeout events")]
    rpc_timeout_err: u64,

    #[counter(help = "Number of RPC connection refused errors")]
    rpc_conn_refused_err: u64,

    #[counter(
        name = "transactions_fetched_total",
        help = "Total number of transactions fetched"
    )]
    transactions_fetched: u64,

    #[counter(
        name = "transaction_fetch_failed_total",
        help = "Total number of failed transaction fetches"
    )]
    transaction_fetch_failed: u64,

    #[counter(
        name = "transaction_gaps_found_total",
        help = "Total number of transaction gaps found"
    )]
    transaction_gaps_found: u64,

    #[counter(
        name = "block_tips_fetched_total",
        help = "Total number of block tips successfully fetched"
    )]
    block_tips_fetched: u64,

    #[counter(
        name = "block_tips_fetch_failed_total",
        help = "Total number of failed block tips fetches"
    )]
    block_tips_fetch_failed: u64,

    #[counter(
        name = "delegator_snapshots_fetched_total",
        help = "Total number of delegator snapshots successfully fetched"
    )]
    delegator_snapshots_fetched: u64,

    #[counter(
        name = "delegator_snapshots_fetch_failed_total",
        help = "Total number of failed delegator snapshot fetches"
    )]
    delegator_snapshots_fetch_failed: u64,
}

impl MetricsState {
    fn new() -> Self {
        Self {
            inserted: HashMap::new(),
            duplicates: HashMap::new(),
            backfilled_blocks_ok: 0,
            backfilled_blocks_err: 0,
            insert_events_err: 0,
            insert_timeout_err: 0,
            db_connections: 0,
            rpc_timeout_err: 0,
            rpc_conn_refused_err: 0,
            transactions_fetched: 0,
            transaction_fetch_failed: 0,
            transaction_gaps_found: 0,
            block_tips_fetched: 0,
            block_tips_fetch_failed: 0,
            delegator_snapshots_fetched: 0,
            delegator_snapshots_fetch_failed: 0,
        }
    }

    fn record(&mut self, metric: Metric) {
        match metric {
            Metric::InsertedEvents(counts) => {
                for (event_type, (inserted, total)) in counts {
                    *self.inserted.entry(event_type).or_insert(0) += inserted;
                    *self.duplicates.entry(event_type).or_insert(0) +=
                        total.saturating_sub(inserted);
                }
            }
            Metric::BackfilledBlocks(count) => {
                self.backfilled_blocks_ok += count;
            }
            Metric::FailedToBackfill(count) => {
                self.backfilled_blocks_err += count;
            }
            Metric::FailedToInsert => {
                self.insert_events_err += 1;
            }
            Metric::InsertTimeout => {
                self.insert_timeout_err += 1;
            }
            Metric::DbConnected => {
                self.db_connections += 1;
            }
            Metric::RpcTimeout => {
                self.rpc_timeout_err += 1;
            }
            Metric::RpcConnRefused => {
                self.rpc_conn_refused_err += 1;
            }
            Metric::TransactionsFetched(count) => {
                self.transactions_fetched += count;
            }
            Metric::TransactionFetchFailed(count) => {
                self.transaction_fetch_failed += count;
            }
            Metric::TransactionGapsFound(count) => {
                self.transaction_gaps_found += count;
            }
            Metric::BackfilledBlockTips(count) => {
                self.block_tips_fetched += count;
            }
            Metric::BlockTipsFetchFailed(count) => {
                self.block_tips_fetch_failed += count;
            }
            Metric::BackfilledDelegatorSnapshots(count) => {
                self.delegator_snapshots_fetched += count;
            }
            Metric::DelegatorSnapshotFetchFailed(count) => {
                self.delegator_snapshots_fetch_failed += count;
            }
        }
    }
}

pub struct MetricsRequest {
    response_tx: tokio::sync::oneshot::Sender<MetricsState>,
}

pub async fn process_metrics(
    mut metrics_rx: mpsc::UnboundedReceiver<Metric>,
    mut request_rx: mpsc::UnboundedReceiver<MetricsRequest>,
) -> Result<()> {
    let mut state = MetricsState::new();

    loop {
        tokio::select! {
            Some(metric) = metrics_rx.recv() => {
                state.record(metric);
            }
            Some(request) = request_rx.recv() => {
                let _ = request.response_tx.send(state.clone());
            }
            else => break,
        }
    }
    Ok(())
}

async fn metrics_handler(
    axum::Extension(request_tx): axum::Extension<mpsc::UnboundedSender<MetricsRequest>>,
) -> impl axum::response::IntoResponse {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    let _ = request_tx.send(MetricsRequest { response_tx });

    let state = match response_rx.await {
        Ok(s) => s,
        Err(_) => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to get metrics".to_string(),
            )
                .into_response();
        }
    };

    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        state.to_string(),
    )
        .into_response()
}

pub async fn run_metrics_server(
    request_tx: mpsc::UnboundedSender<MetricsRequest>,
    bind_addr: String,
) -> Result<()> {
    use axum::{Router, routing::get};

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .layer(tower::ServiceBuilder::new().layer(axum::Extension(request_tx)));

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    info!("Metrics server listening on http://{}", bind_addr);

    axum::serve(listener, app).await?;
    Ok(())
}
