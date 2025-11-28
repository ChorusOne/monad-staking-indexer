use std::time::Duration;

use monad_staking_indexer::{
    BackfillWork, BlockBatch, DbRequest, db,
    events::{self, BlockMeta, Event, EventType, StakingEvent, StakingEventType},
    metrics, pg_utils, test_utils,
};

#[test]
fn process_single_block() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let (tx, mut backfill_rx, mut metrics_rx) = test_utils::spawn_process_event_logs(&pool);

        let delegate = events::DelegateEvent {
            val_id: 1,
            delegator: "1234567890123456789012345678901234567890".to_string(),
            amount: 1000u64.into(),
            activation_epoch: 1,
            block_meta: events::BlockMeta {
                block_number: 100,
                block_hash: "0xabcdef".to_string(),
                block_timestamp: 1234567890,
            },
            tx_meta: events::TxMeta {
                transaction_hash: "0x123abc".to_string(),
                transaction_index: 0,
            },
        };

        let mut batch = BlockBatch::new();
        batch.add_block_meta(delegate.block_meta.clone());
        batch.add_event(Event::Staking(StakingEvent::Delegate(delegate)));
        tx.send(DbRequest::InsertCompleteBlocks(Box::new(batch)))
            .unwrap();

        let got = metrics_rx.recv().await.unwrap();

        if let metrics::Metric::InsertedEvents(hm) = got {
            assert_eq!(
                hm.get(&EventType::Staking(StakingEventType::Delegate)),
                Some(&(1, 1))
            );
        } else {
            panic!("unexpected");
        };

        tx.send(DbRequest::GetBlockGaps).unwrap();

        let result = backfill_rx.recv().await.unwrap();
        match result {
            BackfillWork::NoBlockGaps(range) => {
                assert_eq!(range.start, 0);
                assert_eq!(range.end, 100);
            }
            _ => panic!("Expected NoBlockGaps"),
        }

        drop(tx);
        assert_eq!(backfill_rx.recv().await, None);

        Ok(())
    })
    .unwrap();
}

#[test]
fn processes_non_consecutive_blocks() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let (tx, mut backfill_rx, mut metrics_rx) = test_utils::spawn_process_event_logs(&pool);

        let delegate = events::DelegateEvent {
            val_id: 1,
            delegator: "1234567890123456789012345678901234567890".to_string(),
            amount: 1000u64.into(),
            activation_epoch: 1,
            block_meta: events::BlockMeta {
                block_number: 100,
                block_hash: "0xabcdef".to_string(),
                block_timestamp: 1234567890,
            },
            tx_meta: events::TxMeta {
                transaction_hash: "0x123abc".to_string(),
                transaction_index: 0,
            },
        };

        let mut delegate2 = delegate.clone();
        delegate2.block_meta.block_number = 200;
        delegate2.block_meta.block_hash = "0xbbbbbb".to_string();

        let mut batch1 = BlockBatch::new();
        batch1.add_block_meta(delegate.block_meta.clone());
        batch1.add_event(Event::Staking(StakingEvent::Delegate(delegate)));

        let mut batch2 = BlockBatch::new();
        batch2.add_block_meta(delegate2.block_meta.clone());
        batch2.add_event(Event::Staking(StakingEvent::Delegate(delegate2)));

        tx.send(DbRequest::InsertCompleteBlocks(Box::new(batch1)))
            .unwrap();
        tx.send(DbRequest::InsertCompleteBlocks(Box::new(batch2)))
            .unwrap();

        tx.send(DbRequest::GetBlockGaps).unwrap();
        drop(tx);

        metrics_rx.recv().await.unwrap();
        metrics_rx.recv().await.unwrap();

        let gap = backfill_rx.recv().await.unwrap();
        match gap {
            BackfillWork::BlockGap(range) => {
                assert_eq!(range.start, 101);
                assert_eq!(range.end, 200);
            }
            _ => panic!("Expected BlockGap"),
        }

        assert!(backfill_rx.recv().await.is_none());

        Ok(())
    })
    .unwrap();
}

async fn insert_blockmeta(
    pool: &sqlx::PgPool,
    meta: &BlockMeta,
) -> Result<std::collections::HashMap<EventType, (u64, u64)>, db::repository::DbError> {
    let mut batch = BlockBatch::new();
    batch.add_block_meta(meta.clone());
    db::insert_blocks(pool, &batch, Duration::from_secs(1)).await
}

#[test]
fn test_block_gaps_with_consecutive_blocks() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let gaps = db::repository::get_block_gaps(&pool, 0).await?;
        assert_eq!(gaps.len(), 0);

        for i in 1..10 {
            let block_meta = events::BlockMeta {
                block_number: i,
                block_hash: format!("0xhash{}", i),
                block_timestamp: 1234567890 + i,
            };
            insert_blockmeta(&pool, &block_meta).await?;
        }

        let gaps = db::repository::get_block_gaps(&pool, 0).await?;
        assert_eq!(gaps.len(), 0);

        Ok(())
    })
    .unwrap();
}

#[test]
fn test_get_max_block_number() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let max_block = db::repository::get_max_block_number(&pool).await?;
        assert_eq!(max_block, None);

        let block_meta_1 = events::BlockMeta {
            block_number: 100,
            block_hash: "0xhash100".to_string(),
            block_timestamp: 1234567890,
        };
        insert_blockmeta(&pool, &block_meta_1).await?;

        let max_block = db::repository::get_max_block_number(&pool).await?;
        assert_eq!(max_block, Some(100));

        let block_meta_2 = events::BlockMeta {
            block_number: 50,
            block_hash: "0xhash50".to_string(),
            block_timestamp: 1234567850,
        };
        insert_blockmeta(&pool, &block_meta_2).await?;

        let block_meta_3 = events::BlockMeta {
            block_number: 200,
            block_hash: "0xhash200".to_string(),
            block_timestamp: 1234567900,
        };
        insert_blockmeta(&pool, &block_meta_3).await?;

        let max_block = db::repository::get_max_block_number(&pool).await?;
        assert_eq!(max_block, Some(200));

        Ok(())
    })
    .unwrap();
}

#[test]
fn test_get_block_gaps_with_multiple_gaps() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let gaps = db::repository::get_block_gaps(&pool, 0).await?;
        assert_eq!(gaps.len(), 0);

        let blocks_to_insert = vec![10, 15, 20, 25, 100, 105, 110, 500];
        for block_num in blocks_to_insert {
            let block_meta = events::BlockMeta {
                block_number: block_num,
                block_hash: format!("0xhash{}", block_num),
                block_timestamp: 1234567890 + block_num,
            };
            insert_blockmeta(&pool, &block_meta).await?;
        }

        let gaps = db::repository::get_block_gaps(&pool, 0).await?;
        assert_eq!(gaps.len(), 7);

        assert_eq!(gaps[0].start, 11);
        assert_eq!(gaps[0].end, 15);

        assert_eq!(gaps[1].start, 16);
        assert_eq!(gaps[1].end, 20);

        assert_eq!(gaps[2].start, 21);
        assert_eq!(gaps[2].end, 25);

        assert_eq!(gaps[3].start, 26);
        assert_eq!(gaps[3].end, 100);

        assert_eq!(gaps[4].start, 101);
        assert_eq!(gaps[4].end, 105);

        assert_eq!(gaps[5].start, 106);
        assert_eq!(gaps[5].end, 110);

        assert_eq!(gaps[6].start, 111);
        assert_eq!(gaps[6].end, 500);

        Ok(())
    })
    .unwrap();
}

#[test]
fn test_block_gaps_with_checkpoint() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let (tx, mut backfill_rx, mut metrics_rx) = test_utils::spawn_process_event_logs(&pool);

        for i in 1..=20 {
            let block_meta = events::BlockMeta {
                block_number: i,
                block_hash: format!("0xhash{}", i),
                block_timestamp: 1234567890 + i,
            };
            let mut batch = BlockBatch::new();
            batch.add_block_meta(block_meta);
            tx.send(DbRequest::InsertCompleteBlocks(Box::new(batch)))
                .unwrap();
        }

        for _ in 0..20 {
            metrics_rx.recv().await.unwrap();
        }

        tx.send(DbRequest::GetBlockGaps).unwrap();
        let gap = backfill_rx.recv().await.unwrap();
        match gap {
            BackfillWork::NoBlockGaps(range) => {
                assert_eq!(range.start, 0);
                assert_eq!(range.end, 20);
            }
            _ => panic!("Expected NoBlockGaps"),
        }

        let checkpoint = db::repository::get_block_sync_checkpoint(&pool).await?;
        assert_eq!(checkpoint, 20);

        let block_meta_30 = events::BlockMeta {
            block_number: 30,
            block_hash: "0xhash30".to_string(),
            block_timestamp: 1234567920,
        };
        let mut batch = BlockBatch::new();
        batch.add_block_meta(block_meta_30);
        tx.send(DbRequest::InsertCompleteBlocks(Box::new(batch)))
            .unwrap();

        metrics_rx.recv().await.unwrap();

        tx.send(DbRequest::GetBlockGaps).unwrap();

        let gap = backfill_rx.recv().await.unwrap();
        match gap {
            BackfillWork::BlockGap(range) => {
                assert_eq!(range.start, 21);
                assert_eq!(range.end, 30);
            }
            _ => panic!("Expected BlockGap"),
        }

        let checkpoint = db::repository::get_block_sync_checkpoint(&pool).await?;
        assert_eq!(checkpoint, 20);

        drop(tx);
        assert!(backfill_rx.recv().await.is_none());

        Ok(())
    })
    .unwrap();
}

#[test]
fn test_checkpoint_functions() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let checkpoint = db::repository::get_block_sync_checkpoint(&pool).await?;
        assert_eq!(checkpoint, 0);

        db::repository::update_block_sync_checkpoint(&pool, 100).await?;
        let checkpoint = db::repository::get_block_sync_checkpoint(&pool).await?;
        assert_eq!(checkpoint, 100);

        db::repository::update_block_sync_checkpoint(&pool, 500).await?;
        let checkpoint = db::repository::get_block_sync_checkpoint(&pool).await?;
        assert_eq!(checkpoint, 500);

        Ok(())
    })
    .unwrap();
}

#[test]
fn test_block_tips_gaps() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let block_meta = events::BlockMeta {
            block_number: 100,
            block_hash: "0xabcdef".to_string(),
            block_timestamp: 1234567890,
        };
        let validator_rewarded = events::ValidatorRewardedEvent {
            validator_id: 1,
            from: "1234567890123456789012345678901234567890".to_string(),
            amount: bigdecimal::BigDecimal::from(5000u64),
            epoch: 750, // > 747, where staking was enabled
            block_meta: block_meta.clone(),
            tx_meta: events::TxMeta {
                transaction_hash: "0x123abc".to_string(),
                transaction_index: 0,
            },
        };

        let mut batch = BlockBatch::new();
        batch.add_block_meta(block_meta);
        batch.add_event(Event::System(events::SystemEvent::ValidatorRewarded(
            validator_rewarded,
        )));

        db::insert_blocks(&pool, &batch, Duration::from_secs(1)).await?;

        let tips = db::repository::get_missing_block_tips(&pool, &[1]).await?;
        assert_eq!(
            tips.len(),
            1,
            "Should find 1 missing block tip for validator 1"
        );
        assert_eq!(tips[0], 100, "Missing tip should be for block 100");

        db::set_block_tip(&pool, 100, bigdecimal::BigDecimal::from(1000000u64)).await?;

        let tips_after = db::repository::get_missing_block_tips(&pool, &[1]).await?;
        assert_eq!(
            tips_after.len(),
            0,
            "Should have no missing tips after inserting"
        );

        Ok(())
    })
    .unwrap();
}
