use std::collections::hash_map::Entry;
use std::collections::HashMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::core::{Aggregate, Message, Root};

/// Transaction type enum
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Default, Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum Status {
    #[default]
    Ok,
    Disputed,
    Resolved,
    ChargedBack,
    Declined,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Eq)]
pub struct Transaction {
    /// Used internally
    #[serde(skip, default)]
    pub status: Status,
    /// Client ID
    #[serde(rename = "client")]
    pub client_id: u16,
    /// Transaction ID
    #[serde(rename = "tx")]
    pub tx_id: u32,
    /// Transaction type ( deposit, withdrawal, dispute, etc.)
    #[serde(rename = "type")]
    pub transaction_type: TransactionType,
    /// Transaction amount, if withdrawal or deposit type
    pub amount: Option<Decimal>,
}

impl Message for Transaction {
    fn name(&self) -> &'static str {
        "InboundTransaction"
    }
}

impl Transaction {
    pub fn can_be_disputed(&self) -> bool {
        self.status == Status::Ok
            && (self.transaction_type == TransactionType::Withdrawal
                || self.transaction_type == TransactionType::Deposit)
    }

    pub fn can_complete_dispute(&self) -> bool {
        self.status == Status::Disputed
            && (self.transaction_type == TransactionType::Withdrawal
                || self.transaction_type == TransactionType::Deposit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionEvent {
    WasOpened {
        tx_id: u32,
        account_holder_id: u16,
        transaction: Transaction,
    },
    DepositWasRecorded {
        amount: Decimal,
        transaction: Transaction,
    },
    WithdrawalWasRecorded {
        amount: Decimal,
        transaction: Transaction,
    },
    DisputeWasRecorded {
        tx_id: u32,
        amount: Decimal,
    },
    ResolveWasRecorded {
        tx_id: u32,
        amount: Decimal,
    },
    ChargebackWasRecorded {
        tx_id: u32,
        amount: Decimal,
    },
}

impl Message for TransactionEvent {
    fn name(&self) -> &'static str {
        match self {
            TransactionEvent::WasOpened { .. } => "Opened",
            TransactionEvent::DepositWasRecorded { .. } => "Deposit",
            TransactionEvent::WithdrawalWasRecorded { .. } => "Withdrawal",
            TransactionEvent::DisputeWasRecorded { .. } => "Dispute",
            TransactionEvent::ResolveWasRecorded { .. } => "Resolve",
            TransactionEvent::ChargebackWasRecorded { .. } => "Chargeback",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BankAccountError {
    #[error("Account has not been opened yet")]
    NotOpenedYet,
    #[error("Account has already been opened")]
    AlreadyOpened,
    #[error("Transaction with id {0} has negative amount")]
    NegativeTransactionAttempted(u32),
    #[error("No money to deposit has been specified")]
    NoMoneyDeposited,
    #[error("Insufficient available funds")]
    InsufficientFunds,
    #[error("Transfer transaction was destined to a different recipient: {0}")]
    WrongTransactionRecipient(u32),
    #[error("Duplicate transaction attempted: {0}")]
    DuplicateTransactionRecipient(u32),
    #[error("Invalid transaction dispute")]
    InvalidTransactionDispute,
    #[error("Invalid transaction chargeback")]
    InvalidTransactionChargeBack,
    #[error("Insufficient held funds")]
    InsufficientHeldFunds,
    #[error("Tried to apply transaction with id {tx} to a locked account {id}")]
    LockedAccount { id: u16, tx: u32 },
}

/// Balance for the account
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Balance {
    /// The total funds that are available. This should be equal to the total - held amounts
    pub available: Decimal,
    /// The total funds that are held for dispute. This should be equal to total - available amounts
    pub held: Decimal,
}

impl Balance {
    pub fn new(available: Decimal) -> Self {
        Balance {
            available,
            held: Decimal::default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct AccountSnapShot {
    client: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

#[derive(Debug, Clone)]
pub struct Account {
    id: u16,
    balance: Balance,
    pending_transactions: HashMap<u32, Transaction>,
    locked: bool,
}

impl Aggregate for Account {
    type Id = u16;
    type Event = TransactionEvent;
    type Error = BankAccountError;

    fn type_name() -> &'static str {
        "Account"
    }

    fn aggregate_id(&self) -> &Self::Id {
        &self.id
    }

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, Self::Error> {
        match state {
            None => match event {
                TransactionEvent::WasOpened {
                    transaction,
                    tx_id,
                    account_holder_id,
                } => {
                    let amount = transaction
                        .amount
                        .ok_or(BankAccountError::NoMoneyDeposited)?;
                    if amount < Decimal::ZERO {
                        return Err(BankAccountError::NegativeTransactionAttempted(tx_id));
                    }
                    Ok(Account {
                        id: account_holder_id,
                        balance: Balance::new(amount),
                        pending_transactions: HashMap::from([(tx_id, transaction)]),
                        locked: false,
                    })
                }
                _ => Err(BankAccountError::NotOpenedYet),
            },
            Some(mut account) => match event {
                TransactionEvent::WasOpened { .. } => Err(BankAccountError::AlreadyOpened),
                TransactionEvent::DepositWasRecorded {
                    amount,
                    transaction,
                } => {
                    account.balance.available += amount;
                    account
                        .pending_transactions
                        .insert(transaction.tx_id, transaction);
                    Ok(account)
                }
                TransactionEvent::WithdrawalWasRecorded {
                    amount,
                    transaction,
                } => {
                    account.balance.available -= amount;
                    account
                        .pending_transactions
                        .insert(transaction.tx_id, transaction);
                    Ok(account)
                }
                TransactionEvent::DisputeWasRecorded { tx_id, amount } => {
                    match account
                        .pending_transactions
                        .entry(tx_id)
                        .and_modify(|tx| tx.status = Status::Disputed)
                    {
                        Entry::Occupied(t) => match t.get().transaction_type {
                            TransactionType::Deposit => {
                                account.balance.available -= amount;
                                account.balance.held += amount;
                                Ok(account)
                            }
                            TransactionType::Withdrawal => {
                                if account.balance.available >= amount {
                                    account.balance.available -= amount;
                                }
                                account.balance.held += amount;
                                Ok(account)
                            }
                            _ => Err(BankAccountError::InvalidTransactionDispute),
                        },
                        Entry::Vacant(_) => {
                            unreachable!("this should never happen")
                        }
                    }
                }
                TransactionEvent::ResolveWasRecorded { tx_id, amount } => {
                    match account
                        .pending_transactions
                        .entry(tx_id)
                        .and_modify(|tx| tx.status = Status::Ok)
                    {
                        Entry::Occupied(t) => match t.get().transaction_type {
                            TransactionType::Deposit | TransactionType::Withdrawal
                                if account.balance.held >= amount =>
                            {
                                account.balance.held -= amount;
                                account.balance.available += amount;
                                Ok(account)
                            }
                            _ => Err(BankAccountError::InsufficientHeldFunds),
                        },
                        Entry::Vacant(_) => unreachable!(),
                    }
                }
                TransactionEvent::ChargebackWasRecorded { tx_id, amount } => {
                    match account
                        .pending_transactions
                        .entry(tx_id)
                        .and_modify(|tx| tx.status = Status::ChargedBack)
                    {
                        Entry::Occupied(t) => match t.get().transaction_type {
                            TransactionType::Deposit | TransactionType::Withdrawal
                                if account.balance.held >= amount =>
                            {
                                account.balance.held -= amount;
                                account.locked = true;
                                Ok(account)
                            }
                            _ => Err(BankAccountError::InvalidTransactionChargeBack),
                        },
                        Entry::Vacant(_) => {
                            unreachable!()
                        }
                    }
                }
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct BankAccountRoot(Root<Account>);
impl std::ops::Deref for BankAccountRoot {
    type Target = Root<Account>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for BankAccountRoot {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl From<Root<Account>> for BankAccountRoot {
    fn from(root: Root<Account>) -> Self {
        Self(root)
    }
}
impl From<BankAccountRoot> for Root<Account> {
    fn from(value: BankAccountRoot) -> Self {
        value.0
    }
}

impl BankAccountRoot {
    pub fn open(transaction: Transaction) -> Result<Self, BankAccountError> {
        Root::<Account>::record_new(
            TransactionEvent::WasOpened {
                account_holder_id: transaction.client_id,
                tx_id: transaction.tx_id,
                transaction,
            }
            .into(),
        )
        .map(Self)
    }

    pub fn deposit(&mut self, transaction: Transaction) -> Result<(), BankAccountError> {
        if self.locked {
            return Err(BankAccountError::LockedAccount {
                id: transaction.client_id,
                tx: transaction.tx_id,
            });
        }
        let amount = transaction
            .amount
            .ok_or(BankAccountError::NoMoneyDeposited)?;
        if amount < Decimal::ZERO {
            return Err(BankAccountError::NegativeTransactionAttempted(
                transaction.tx_id,
            ));
        }
        if self.pending_transactions.get(&transaction.tx_id).is_some() {
            return Err(BankAccountError::DuplicateTransactionRecipient(
                transaction.tx_id,
            ));
        }
        self.record_that(
            TransactionEvent::DepositWasRecorded {
                amount,
                transaction,
            }
            .into(),
        )
    }

    pub fn withdrawal(&mut self, transaction: Transaction) -> Result<(), BankAccountError> {
        if self.locked {
            return Err(BankAccountError::LockedAccount {
                id: transaction.client_id,
                tx: transaction.tx_id,
            });
        }
        let amount = transaction
            .amount
            .ok_or(BankAccountError::NoMoneyDeposited)?;
        if amount < Decimal::ZERO {
            return Err(BankAccountError::NegativeTransactionAttempted(
                transaction.tx_id,
            ));
        }

        if self.balance.available < amount {
            return Err(BankAccountError::InsufficientFunds);
        }

        if self.pending_transactions.get(&transaction.tx_id).is_some() {
            return Err(BankAccountError::DuplicateTransactionRecipient(
                transaction.tx_id,
            ));
        }

        self.record_that(
            TransactionEvent::WithdrawalWasRecorded {
                amount,
                transaction,
            }
            .into(),
        )
    }

    pub fn dispute(&mut self, transaction: Transaction) -> Result<(), BankAccountError> {
        if self.locked {
            return Err(BankAccountError::LockedAccount {
                id: transaction.client_id,
                tx: transaction.tx_id,
            });
        }
        match self.pending_transactions.get(&transaction.tx_id) {
            Some(disputed_tx) if disputed_tx.can_be_disputed() => {
                let disputed = disputed_tx.clone();
                self.record_that(
                    TransactionEvent::DisputeWasRecorded {
                        tx_id: disputed.tx_id,
                        amount: disputed.amount.ok_or(BankAccountError::NoMoneyDeposited)?,
                    }
                    .into(),
                )
            }
            _ => Err(BankAccountError::WrongTransactionRecipient(
                transaction.tx_id,
            )),
        }
    }

    pub fn resolve(&mut self, transaction: Transaction) -> Result<(), BankAccountError> {
        if self.locked {
            return Err(BankAccountError::LockedAccount {
                id: transaction.client_id,
                tx: transaction.tx_id,
            });
        }
        match self.pending_transactions.get(&transaction.tx_id) {
            Some(disputed_tx) if disputed_tx.can_complete_dispute() => {
                let disputed = disputed_tx.clone();
                self.record_that(
                    TransactionEvent::ResolveWasRecorded {
                        amount: disputed.amount.ok_or(BankAccountError::NoMoneyDeposited)?,
                        tx_id: disputed.tx_id,
                    }
                    .into(),
                )
            }
            _ => Err(BankAccountError::WrongTransactionRecipient(
                transaction.tx_id,
            )),
        }
    }

    pub fn chargeback(&mut self, transaction: Transaction) -> Result<(), BankAccountError> {
        if self.locked {
            return Err(BankAccountError::LockedAccount {
                id: transaction.client_id,
                tx: transaction.tx_id,
            });
        }
        match self.pending_transactions.get(&transaction.tx_id) {
            Some(disputed_tx) if disputed_tx.can_complete_dispute() => {
                let disputed = disputed_tx.clone();
                self.record_that(
                    TransactionEvent::ChargebackWasRecorded {
                        amount: disputed.amount.ok_or(BankAccountError::NoMoneyDeposited)?,
                        tx_id: disputed.tx_id,
                    }
                    .into(),
                )
            }
            _ => Err(BankAccountError::WrongTransactionRecipient(
                transaction.tx_id,
            )),
        }
    }

    pub fn snapshot(&self) -> AccountSnapShot {
        AccountSnapShot {
            client: self.id,
            available: self.balance.available.round_dp(4),
            held: self.balance.held.round_dp(4),
            total: (self.balance.available + self.balance.held).round_dp(4),
            locked: self.locked,
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use crate::core::repository::{Getter, Saver};
    use crate::core::{EventSourced, GetError, InMemory};

    use super::*;

    #[tokio::test]
    async fn repository_persists_new_aggregate_root() {
        let event_store = InMemory::<u16, TransactionEvent>::default();
        let account_repository = EventSourced::<Account, _>::from(event_store);

        let dpstt = Transaction {
            status: Default::default(),
            client_id: 1,
            tx_id: 1,
            amount: Some(dec!(10.89)),
            transaction_type: TransactionType::Deposit,
        };

        if dpstt.transaction_type == TransactionType::Deposit {
            match account_repository.get(&dpstt.client_id).await {
                Ok(account) => {
                    let mut root = BankAccountRoot::from(account);
                    root.deposit(dpstt.clone()).unwrap();
                    account_repository.save(&mut root).await.unwrap();
                }
                Err(_err) if matches!(GetError::NotFound, _err) => {
                    let mut root = BankAccountRoot::open(dpstt.clone()).expect("new account root");
                    account_repository
                        .save(&mut root)
                        .await
                        .expect("new account version should be saved successfully");
                }
                Err(_err) => {}
            };
        }

        let root = account_repository
            .get(&dpstt.client_id)
            .await
            .expect("account record should have saved");
        assert_eq!(1, root.id);
    }
}
