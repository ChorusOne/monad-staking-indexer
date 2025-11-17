use monad_staking_indexer::{db, events, pg_utils, test_utils};

#[test]
fn test_delegate_event_duplicates() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let event1 = events::StakingEvent::Delegate(events::DelegateEvent {
            val_id: 1,
            delegator: "1234567890123456789012345678901234567890".to_string(),
            amount: 1000u64.into(),
            activation_epoch: 1,
            block_meta: events::BlockMeta {
                block_number: 100,
                block_hash: "0xabc1".to_string(),
                block_timestamp: 1234567890,
            },
            tx_meta: events::TxMeta {
                transaction_hash: "0xtx1".to_string(),
                transaction_index: 0,
            },
        });

        let mut event2 = event1.clone();
        if let events::StakingEvent::Delegate(ref mut e) = event2 {
            e.val_id = 2;
        }

        db::repository::insert_staking_event(&pool, &event1).await?;
        db::repository::insert_staking_event(&pool, &event2).await?;

        let result = db::repository::insert_staking_event(&pool, &event1).await;
        assert!(matches!(
            result,
            Err(db::repository::DbError::DuplicateEvent { .. })
        ));

        Ok(())
    })
    .unwrap();
}

#[test]
fn test_undelegate_event_duplicates() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let event1 = events::StakingEvent::Undelegate(events::UndelegateEvent {
            val_id: 1,
            delegator: "1234567890123456789012345678901234567890".to_string(),
            withdrawal_id: 100,
            amount: 1000u64.into(),
            activation_epoch: 1,
            block_meta: events::BlockMeta {
                block_number: 100,
                block_hash: "0xabc1".to_string(),
                block_timestamp: 1234567890,
            },
            tx_meta: events::TxMeta {
                transaction_hash: "0xtx1".to_string(),
                transaction_index: 0,
            },
        });

        let mut event2 = event1.clone();
        if let events::StakingEvent::Undelegate(ref mut e) = event2 {
            e.val_id = 2;
        }

        db::repository::insert_staking_event(&pool, &event1).await?;
        db::repository::insert_staking_event(&pool, &event2).await?;

        let result = db::repository::insert_staking_event(&pool, &event1).await;
        assert!(matches!(
            result,
            Err(db::repository::DbError::DuplicateEvent { .. })
        ));

        Ok(())
    })
    .unwrap();
}

#[test]
fn test_withdraw_event_duplicates() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let event1 = events::StakingEvent::Withdraw(events::WithdrawEvent {
            val_id: 1,
            delegator: "1234567890123456789012345678901234567890".to_string(),
            withdrawal_id: 100,
            amount: 1000u64.into(),
            activation_epoch: 1,
            block_meta: events::BlockMeta {
                block_number: 100,
                block_hash: "0xabc1".to_string(),
                block_timestamp: 1234567890,
            },
            tx_meta: events::TxMeta {
                transaction_hash: "0xtx1".to_string(),
                transaction_index: 0,
            },
        });

        let mut event2 = event1.clone();
        if let events::StakingEvent::Withdraw(ref mut e) = event2 {
            e.val_id = 2;
        }

        db::repository::insert_staking_event(&pool, &event1).await?;
        db::repository::insert_staking_event(&pool, &event2).await?;

        let result = db::repository::insert_staking_event(&pool, &event1).await;
        assert!(matches!(
            result,
            Err(db::repository::DbError::DuplicateEvent { .. })
        ));

        Ok(())
    })
    .unwrap();
}

#[test]
fn test_claim_rewards_event_duplicates() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let event1 = events::StakingEvent::ClaimRewards(events::ClaimRewardsEvent {
            val_id: 1,
            delegator: "1234567890123456789012345678901234567890".to_string(),
            amount: 1000u64.into(),
            epoch: 10,
            block_meta: events::BlockMeta {
                block_number: 100,
                block_hash: "0xabc1".to_string(),
                block_timestamp: 1234567890,
            },
            tx_meta: events::TxMeta {
                transaction_hash: "0xtx1".to_string(),
                transaction_index: 0,
            },
        });

        let mut event2 = event1.clone();
        if let events::StakingEvent::ClaimRewards(ref mut e) = event2 {
            e.val_id = 2;
        }

        db::repository::insert_staking_event(&pool, &event1).await?;
        db::repository::insert_staking_event(&pool, &event2).await?;

        let result = db::repository::insert_staking_event(&pool, &event1).await;
        assert!(matches!(
            result,
            Err(db::repository::DbError::DuplicateEvent { .. })
        ));

        Ok(())
    })
    .unwrap();
}

#[test]
fn test_validator_status_changed_event_duplicates() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let event1 =
            events::StakingEvent::ValidatorStatusChanged(events::ValidatorStatusChangedEvent {
                validator_id: 1,
                flags: 1,
                block_meta: events::BlockMeta {
                    block_number: 100,
                    block_hash: "0xabc1".to_string(),
                    block_timestamp: 1234567890,
                },
                tx_meta: events::TxMeta {
                    transaction_hash: "0xtx1".to_string(),
                    transaction_index: 0,
                },
            });

        let mut event2 = event1.clone();
        if let events::StakingEvent::ValidatorStatusChanged(ref mut e) = event2 {
            e.validator_id = 2;
        }

        db::repository::insert_staking_event(&pool, &event1).await?;
        db::repository::insert_staking_event(&pool, &event2).await?;

        let result = db::repository::insert_staking_event(&pool, &event1).await;
        assert!(matches!(
            result,
            Err(db::repository::DbError::DuplicateEvent { .. })
        ));

        Ok(())
    })
    .unwrap();
}

#[test]
fn test_commission_changed_event_duplicates() {
    pg_utils::with_postgres_and_schema_async(|pool| async move {
        test_utils::init_test_logger();

        let event1 = events::StakingEvent::CommissionChanged(events::CommissionChangedEvent {
            validator_id: 1,
            old_commission: 100u64.into(),
            new_commission: 150u64.into(),
            block_meta: events::BlockMeta {
                block_number: 100,
                block_hash: "0xabc1".to_string(),
                block_timestamp: 1234567890,
            },
            tx_meta: events::TxMeta {
                transaction_hash: "0xtx1".to_string(),
                transaction_index: 0,
            },
        });

        let mut event2 = event1.clone();
        if let events::StakingEvent::CommissionChanged(ref mut e) = event2 {
            e.validator_id = 2;
        }

        db::repository::insert_staking_event(&pool, &event1).await?;
        db::repository::insert_staking_event(&pool, &event2).await?;

        let result = db::repository::insert_staking_event(&pool, &event1).await;
        assert!(matches!(
            result,
            Err(db::repository::DbError::DuplicateEvent { .. })
        ));

        Ok(())
    })
    .unwrap();
}
