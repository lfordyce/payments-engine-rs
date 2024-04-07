use payments_engine_rs::core::{Envelope, EventSourced, Persisted, Scenario};
use payments_engine_rs::domain::{Transaction, TransactionEvent, TransactionType};
use payments_engine_rs::runtime::Service;
use rust_decimal_macros::dec;

#[tokio::test]
async fn it_records_new_account_deposit() {
    Scenario
        .when(Envelope::from(Transaction {
            status: Default::default(),
            client_id: 1,
            tx_id: 1,
            transaction_type: TransactionType::Deposit,
            amount: Some(dec!(10.123)),
        }))
        .then(vec![Persisted {
            stream_id: 1,
            version: 1,
            event: Envelope::from(TransactionEvent::WasOpened {
                tx_id: 1,
                account_holder_id: 1,
                transaction: Transaction {
                    status: Default::default(),
                    client_id: 1,
                    tx_id: 1,
                    transaction_type: TransactionType::Deposit,
                    amount: Some(dec!(10.123)),
                },
            }),
        }])
        .assert_on(|even_store| Service::from(EventSourced::from(even_store)))
        .await;
}

#[tokio::test]
async fn it_updates_deposit_of_existing_account() {
    Scenario
        .given(vec![Persisted {
            stream_id: 1,
            version: 1,
            event: Envelope::from(TransactionEvent::WasOpened {
                tx_id: 1,
                account_holder_id: 1,
                transaction: Transaction {
                    status: Default::default(),
                    client_id: 1,
                    tx_id: 1,
                    transaction_type: TransactionType::Deposit,
                    amount: Some(dec!(10.123)),
                },
            }),
        }])
        .when(Envelope::from(Transaction {
            status: Default::default(),
            client_id: 1,
            tx_id: 2,
            transaction_type: TransactionType::Deposit,
            amount: Some(dec!(10.123)),
        }))
        .then(vec![Persisted {
            stream_id: 1,
            version: 2,
            event: Envelope::from(TransactionEvent::DepositWasRecorded {
                amount: dec!(10.123),
                transaction: Transaction {
                    status: Default::default(),
                    client_id: 1,
                    tx_id: 2,
                    transaction_type: TransactionType::Deposit,
                    amount: Some(dec!(10.123)),
                },
            }),
        }])
        .assert_on(|even_store| Service::from(EventSourced::from(even_store)))
        .await;
}
