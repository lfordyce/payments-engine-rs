use crate::core::{Aggregate, Message, Root};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct TransactionEvent {
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

impl Message for TransactionEvent {
    fn name(&self) -> &'static str {
        match self.transaction_type {
            TransactionType::Deposit => "Deposit",
            TransactionType::Withdrawal => "Withdrawal",
            TransactionType::Dispute => "Dispute",
            TransactionType::Resolve => "Resolve",
            TransactionType::Chargeback => "Chargeback",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BankAccountError {
    #[error("bank account has not been opened yet")]
    NotOpenedYet,
    #[error("bank account has already been opened")]
    AlreadyOpened,
    #[error("empty id provided for the new bank account")]
    EmptyAccountId,
    #[error("empty account holder id provided for the new bank account")]
    EmptyAccountHolderId,
    #[error("a deposit was attempted with negative import")]
    NegativeDepositAttempted,
    #[error("no money to deposit has been specified")]
    NoMoneyDeposited,
    #[error("transfer could not be sent due to insufficient funds")]
    InsufficientFunds,
    #[error("transfer transaction was destined to a different recipient: {0}")]
    WrongTransactionRecipient(u16),
    #[error("the account is closed")]
    Closed,
    #[error("bank account has already been closed")]
    AlreadyClosed,
}

#[derive(Default, Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum TransactionStatus {
    #[default]
    Ok,
    Disputed,
    Chargedback,
    Declined,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct Transaction {
    #[serde(skip, default)]
    pub status: TransactionStatus,

    #[serde(rename = "type")]
    pub transaction_type: TransactionType,

    pub id: u32,
    pub beneficiary_account_id: u16,
    pub amount: Option<Decimal>,
}

impl Transaction {
    pub fn can_be_disputed(&self) -> bool {
        self.status == TransactionStatus::Ok
            && (self.transaction_type == TransactionType::Withdrawal
                || self.transaction_type == TransactionType::Deposit)
    }

    pub fn can_complete_dispute(&self) -> bool {
        self.status == TransactionStatus::Disputed
            && (self.transaction_type == TransactionType::Withdrawal
                || self.transaction_type == TransactionType::Deposit)
    }
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

#[derive(Debug, Clone)]
pub struct Account {
    id: u16,
    balance: Balance,
    pending_transactions: HashMap<u32, Transaction>,
    disputed_transactions: HashSet<u32>,
    previous_tx_id: u32,
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
            None => match event.transaction_type {
                TransactionType::Deposit => {
                    let mut account = Account {
                        id: event.client_id,
                        balance: Balance::new(event.amount.unwrap_or_default()),
                        pending_transactions: HashMap::new(),
                        disputed_transactions: HashSet::new(),
                        previous_tx_id: event.tx_id,
                        locked: false,
                    };
                    account.pending_transactions.insert(
                        event.tx_id,
                        Transaction {
                            status: TransactionStatus::Ok,
                            transaction_type: TransactionType::Deposit,
                            id: event.tx_id,
                            beneficiary_account_id: event.client_id,
                            amount: event.amount,
                        },
                    );
                    account.previous_tx_id = event.tx_id;
                    Ok(account)
                }
                _ => Err(BankAccountError::NotOpenedYet),
            },
            Some(mut account) => match event.transaction_type {
                TransactionType::Deposit => {
                    if let Some(amount) = event.amount {
                        if !account.locked {
                            account.balance.available += amount;
                        }
                    }
                    Ok(account)
                }
                TransactionType::Withdrawal => {
                    if !account.locked && account.balance.available >= event.amount.unwrap() {
                        account.balance.available -= event.amount.unwrap();
                    }
                    Ok(account)
                }
                TransactionType::Dispute => Err(BankAccountError::NotOpenedYet),
                TransactionType::Resolve => Err(BankAccountError::NotOpenedYet),
                TransactionType::Chargeback => Err(BankAccountError::NotOpenedYet),
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
    pub fn create(evt: TransactionEvent) -> Result<Self, BankAccountError> {
        Root::<Account>::record_new(evt.into()).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventSourced, GetError, InMemory};
    use rust_decimal_macros::dec;
    use crate::core::repository::{Getter, Saver};

    #[tokio::test]
    async fn repository_persists_new_aggregate_root() {
        let event_store = InMemory::<u16, TransactionEvent>::default();
        let account_repository = EventSourced::<Account, _>::from(event_store);

        let mut dpstt = TransactionEvent {
            client_id: 1,
            tx_id: 1,
            amount: Some(dec!(10.89)),
            transaction_type: TransactionType::Deposit,
        };

        match account_repository.get(&dpstt.client_id).await {
            Ok(account) => {}
            Err(err) => {
                if matches!(GetError::NotFound, err) {
                    let mut root = BankAccountRoot::create(dpstt.clone()).expect("new account root");
                    account_repository.save(&mut root).await.expect("new account version should be saved successfully");
                }
            }
        };

        let root = account_repository.get(&dpstt.client_id).await.expect("account record should have saved");
        assert_eq!(1, root.id);
    }
}
