use crate::events::StakingEventType;
use axum::response::IntoResponse;
use eyre::Result;
use log::info;
use std::collections::HashMap;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq)]
pub enum Metric {
    DuplicateEvent(StakingEventType),
    InsertedEvent(StakingEventType),
    BackfilledBlocks(u64),
    FailedToBackfill(u64),
    FailedToInsert,
}

#[derive(Debug, Clone)]
struct MetricsState {
    inserted: HashMap<StakingEventType, u64>,
    duplicates: HashMap<StakingEventType, u64>,
    insert_events_err: u64,
    backfilled_blocks_ok: u64,
    backfilled_blocks_err: u64,
}

impl MetricsState {
    fn new() -> Self {
        Self {
            inserted: HashMap::new(),
            duplicates: HashMap::new(),
            backfilled_blocks_ok: 0,
            backfilled_blocks_err: 0,
            insert_events_err: 0,
        }
    }

    fn record(&mut self, metric: Metric) {
        match metric {
            Metric::InsertedEvent(event_type) => {
                *self.inserted.entry(event_type).or_insert(0) += 1;
            }
            Metric::DuplicateEvent(event_type) => {
                *self.duplicates.entry(event_type).or_insert(0) += 1;
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
        }
    }

    fn as_prometheus_metrics(&self) -> String {
        let mut output = String::new();

        output.push_str("# HELP staking_events_inserted_total Total number of staking events inserted into the database\n");
        output.push_str("# TYPE staking_events_inserted_total counter\n");
        for event_type in StakingEventType::all_types() {
            let count = self.inserted.get(&event_type).unwrap_or(&0);
            output.push_str(&format!(
                "staking_events_inserted_total{{event_type=\"{}\"}} {}\n",
                event_type, count
            ));
        }

        output.push_str("# HELP staking_events_duplicates_total Total number of duplicate staking events detected\n");
        output.push_str("# TYPE staking_events_duplicates_total counter\n");
        for event_type in StakingEventType::all_types() {
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
        output.push_str("# TYPE staking_backfilled_blocks_err counter\n");
        output.push_str(&format!(
            "staking_insert_events_err {}\n",
            self.insert_events_err
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
