use crate::events::EventType;
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
}

#[derive(Debug, Clone)]
struct MetricsState {
    inserted: HashMap<EventType, u64>,
    duplicates: HashMap<EventType, u64>,
    insert_events_err: u64,
    insert_timeout_err: u64,
    backfilled_blocks_ok: u64,
    backfilled_blocks_err: u64,
    db_connections: u64,
    rpc_timeout_err: u64,
    rpc_conn_refused_err: u64,
    transactions_fetched: u64,
    transaction_fetch_failed: u64,
    transaction_gaps_found: u64,
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
        }
    }

    fn as_prometheus_metrics(&self) -> String {
        let mut output = String::new();

        output.push_str("# HELP staking_events_inserted_total Total number of staking events inserted into the database\n");
        output.push_str("# TYPE staking_events_inserted_total counter\n");
        for event_type in EventType::all_types() {
            let count = self.inserted.get(&event_type).unwrap_or(&0);
            output.push_str(&format!(
                "staking_events_inserted_total{{event_type=\"{}\"}} {}\n",
                event_type, count
            ));
        }

        output.push_str("# HELP staking_events_duplicates_total Total number of duplicate staking events detected\n");
        output.push_str("# TYPE staking_events_duplicates_total counter\n");
        for event_type in EventType::all_types() {
            let count = self.duplicates.get(&event_type).unwrap_or(&0);
            output.push_str(&format!(
                "staking_events_duplicates_total{{event_type=\"{}\"}} {}\n",
                event_type, count
            ));
        }

        output.push_str("# HELP staking_backfilled_blocks_ok Number of blocks backfilled\n");
        output.push_str("# TYPE staking_backfilled_blocks_ok counter\n");
        output.push_str(&format!(
            "staking_backfilled_blocks_ok {}\n",
            self.backfilled_blocks_ok
        ));

        output.push_str(
            "# HELP staking_backfilled_blocks_err Number of blocks that failed to backfill\n",
        );
        output.push_str("# TYPE staking_backfilled_blocks_err counter\n");
        output.push_str(&format!(
            "staking_backfilled_blocks_err {}\n",
            self.backfilled_blocks_err
        ));

        output.push_str(
            "# HELP staking_insert_events_err Number of events that failed to be inserted\n",
        );
        output.push_str("# TYPE staking_insert_events_err counter\n");
        output.push_str(&format!(
            "staking_insert_events_err {}\n",
            self.insert_events_err
        ));

        output.push_str(
            "# HELP staking_insert_timeout_err Number of insert operations that timed out\n",
        );
        output.push_str("# TYPE staking_insert_timeout_err counter\n");
        output.push_str(&format!(
            "staking_insert_timeout_err {}\n",
            self.insert_timeout_err
        ));

        output.push_str(
            "# HELP staking_db_connections_total Total number of database connections established\n",
        );
        output.push_str("# TYPE staking_db_connections_total counter\n");
        output.push_str(&format!(
            "staking_db_connections_total {}\n",
            self.db_connections
        ));

        output.push_str("# HELP staking_rpc_timeout_err Number of RPC timeout events\n");
        output.push_str("# TYPE staking_rpc_timeout_err counter\n");
        output.push_str(&format!(
            "staking_rpc_timeout_err {}\n",
            self.rpc_timeout_err
        ));

        output.push_str(
            "# HELP staking_rpc_conn_refused_err Number of RPC connection refused errors\n",
        );
        output.push_str("# TYPE staking_rpc_conn_refused_err counter\n");
        output.push_str(&format!(
            "staking_rpc_conn_refused_err {}\n",
            self.rpc_conn_refused_err
        ));

        output.push_str(
            "# HELP staking_transactions_fetched_total Total number of transactions fetched\n",
        );
        output.push_str("# TYPE staking_transactions_fetched_total counter\n");
        output.push_str(&format!(
            "staking_transactions_fetched_total {}\n",
            self.transactions_fetched
        ));

        output.push_str(
            "# HELP staking_transaction_fetch_failed_total Total number of failed transaction fetches\n",
        );
        output.push_str("# TYPE staking_transaction_fetch_failed_total counter\n");
        output.push_str(&format!(
            "staking_transaction_fetch_failed_total {}\n",
            self.transaction_fetch_failed
        ));

        output.push_str(
            "# HELP staking_transaction_gaps_found_total Total number of transaction gaps found\n",
        );
        output.push_str("# TYPE staking_transaction_gaps_found_total counter\n");
        output.push_str(&format!(
            "staking_transaction_gaps_found_total {}\n",
            self.transaction_gaps_found
        ));

        output
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
        state.as_prometheus_metrics(),
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
