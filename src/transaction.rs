use crate::events;
use alloy::consensus::Transaction;
use alloy::primitives::U256;
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

pub fn calculate_block_tips(block: &alloy::rpc::types::Block) -> eyre::Result<U256> {
    let base_fee_per_gas = block.header.base_fee_per_gas.unwrap_or(0) as u128;

    let transactions = block
        .transactions
        .as_transactions()
        .ok_or_else(|| eyre::eyre!("Block has no transaction data"))?;

    let total_priority_fees: U256 = transactions
        .iter()
        .map(|tx| {
            let gas_used = tx.gas_limit() as u128;
            let eff_gas_price = tx.effective_gas_price.unwrap_or(0);

            let tip_per_gas = eff_gas_price.saturating_sub(base_fee_per_gas);
            U256::from(tip_per_gas.saturating_mul(gas_used))
        })
        .sum();

    Ok(total_priority_fees)
}

#[derive(Debug, Clone)]
pub struct EventTxData {
    pub transaction_hash: String,
    pub block_number: u64,
    pub event_type: events::StakingEventType,
    pub access_list: AccessList,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::consensus::TxEnvelope;
    use alloy::primitives::{Address, Bytes, FixedBytes};
    use alloy::rpc::types::{BlockTransactions, Header};

    #[test]
    fn test_calculate_block_tips() {
        let base_fee = 10_u128;

        let tx1 = create_mock_transaction(21000, 15);
        let tx2 = create_mock_transaction(50000, 20);
        let tx3 = create_mock_transaction(100000, 12);

        let expected_tip =
            (15 - base_fee) * 21000 + (20 - base_fee) * 50000 + (12 - base_fee) * 100000;

        let block = create_mock_block(base_fee, vec![tx1, tx2, tx3]);

        let result = calculate_block_tips(&block).unwrap();
        assert_eq!(result, U256::from(expected_tip));
    }

    #[test]
    fn test_calculate_block_tips_with_zero_tips() {
        let base_fee = 10_u128;
        let effective_gas_price = 10_u128;

        let tx = create_mock_transaction(21000, effective_gas_price);
        let block = create_mock_block(base_fee, vec![tx]);

        let result = calculate_block_tips(&block).unwrap();
        assert_eq!(result, U256::ZERO);
    }

    #[test]
    fn test_calculate_block_tips_when_effective_price_below_base_fee() {
        let base_fee = 100_u128;
        let effective_gas_price = 50_u128;

        let tx = create_mock_transaction(21000, effective_gas_price);
        let block = create_mock_block(base_fee, vec![tx]);

        let result = calculate_block_tips(&block).unwrap();
        assert_eq!(result, U256::ZERO);
    }

    fn create_mock_transaction(
        gas_limit: u128,
        effective_gas_price: u128,
    ) -> alloy::rpc::types::Transaction {
        let legacy_tx = alloy::consensus::TxLegacy {
            chain_id: Some(1),
            nonce: 0,
            gas_price: effective_gas_price,
            gas_limit: gas_limit as u64,
            to: alloy::primitives::TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Bytes::new(),
        };

        alloy::rpc::types::Transaction {
            inner: TxEnvelope::Legacy(alloy::consensus::Signed::new_unchecked(
                legacy_tx,
                alloy::primitives::PrimitiveSignature::test_signature(),
                FixedBytes::ZERO,
            )),
            block_hash: Some(FixedBytes::ZERO),
            block_number: Some(100),
            transaction_index: Some(0),
            effective_gas_price: Some(effective_gas_price),
            from: Address::ZERO,
        }
    }

    fn create_mock_block(
        base_fee: u128,
        transactions: Vec<alloy::rpc::types::Transaction>,
    ) -> alloy::rpc::types::Block {
        let mut header = alloy::consensus::Header::default();
        header.base_fee_per_gas = Some(base_fee as u64);

        alloy::rpc::types::Block {
            header: Header {
                inner: header,
                ..Default::default()
            },
            transactions: BlockTransactions::Full(transactions),
            ..Default::default()
        }
    }
}
