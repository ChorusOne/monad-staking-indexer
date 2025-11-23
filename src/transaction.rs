use crate::events;
use alloy::rpc::types::AccessList;

pub fn extract_access_list(tx: &alloy::rpc::types::Transaction) -> AccessList {
    use alloy::consensus::TxEnvelope;
    match &tx.inner {
        TxEnvelope::Eip1559(tx_1559) => tx_1559.tx().access_list.clone(),
        TxEnvelope::Eip2930(tx_2930) => tx_2930.tx().access_list.clone(),
        TxEnvelope::Eip4844(tx_4844) => tx_4844.tx().tx().access_list.clone(),
        _ => AccessList::default(),
    }
}

#[derive(Debug, Clone)]
pub struct EventTxData {
    pub transaction_hash: String,
    pub block_number: u64,
    pub event_type: events::StakingEventType,
    pub access_list: AccessList,
}
