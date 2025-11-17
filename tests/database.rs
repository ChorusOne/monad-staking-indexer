use monad_staking_indexer::{db, events, metrics, pg_utils, test_utils};

#[test]
fn process_single_block() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let (tx, mut metrics_rx) = test_utils::spawn_process_event_logs(&pool);

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

        tx.send(events::StakingEvent::Delegate(delegate)).unwrap();

        let got = metrics_rx.recv().await.unwrap();

        assert_eq!(
            got,
            metrics::Metric::InsertedEvent(events::StakingEventType::Delegate)
        );

        drop(tx);

        // we inserted exactly 1 event, there can't be gaps
        let gaps = db::repository::get_block_gaps(&pool).await.unwrap();
        assert_eq!(gaps.len(), 0);

        Ok(())
    })
    .unwrap();
}

#[test]
fn processes_non_consecutive_blocks() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let (tx, mut metrics_rx) = test_utils::spawn_process_event_logs(&pool);

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

        tx.send(events::StakingEvent::Delegate(delegate)).unwrap();
        tx.send(events::StakingEvent::Delegate(delegate2)).unwrap();

        drop(tx);

        metrics_rx.recv().await.unwrap();
        metrics_rx.recv().await.unwrap();
        // got metrics for both == they are stored in db

        // we inserted 2 events with height={100, 200}
        let gaps = db::repository::get_block_gaps(&pool).await.unwrap();
        assert_eq!(gaps.len(), 1);

        assert_eq!(gaps[0].start, 101);
        assert_eq!(gaps[0].end, 200);

        Ok(())
    })
    .unwrap();
}

#[test]
fn test_block_gaps_with_consecutive_blocks() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let gaps = db::repository::get_block_gaps(&pool).await?;
        assert_eq!(gaps.len(), 0);

        for i in 1..10 {
            let block_meta = events::BlockMeta {
                block_number: i,
                block_hash: format!("0xhash{}", i),
                block_timestamp: 1234567890 + i,
            };
            db::repository::ensure_block(&pool, &block_meta).await?;
        }

        let gaps = db::repository::get_block_gaps(&pool).await?;
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
        db::repository::ensure_block(&pool, &block_meta_1).await?;

        let max_block = db::repository::get_max_block_number(&pool).await?;
        assert_eq!(max_block, Some(100));

        let block_meta_2 = events::BlockMeta {
            block_number: 50,
            block_hash: "0xhash50".to_string(),
            block_timestamp: 1234567850,
        };
        db::repository::ensure_block(&pool, &block_meta_2).await?;

        let block_meta_3 = events::BlockMeta {
            block_number: 200,
            block_hash: "0xhash200".to_string(),
            block_timestamp: 1234567900,
        };
        db::repository::ensure_block(&pool, &block_meta_3).await?;

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

        let gaps = db::repository::get_block_gaps(&pool).await?;
        assert_eq!(gaps.len(), 0);

        let blocks_to_insert = vec![10, 15, 20, 25, 100, 105, 110, 500];
        for block_num in blocks_to_insert {
            let block_meta = events::BlockMeta {
                block_number: block_num,
                block_hash: format!("0xhash{}", block_num),
                block_timestamp: 1234567890 + block_num,
            };
            db::repository::ensure_block(&pool, &block_meta).await?;
        }

        let gaps = db::repository::get_block_gaps(&pool).await?;
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
