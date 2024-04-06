use crate::core::repository::Repository;
use crate::core::{Envelope, GetError, Handler};
use crate::domain::{Account, BankAccountRoot, Transaction, TransactionType};
use crate::runtime::sealed::State;
use async_trait::async_trait;
use std::error::Error as StdError;
use std::fmt::Error;
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;

pub trait Read {
    type Request;
    fn recv(&mut self) -> Result<Self::Request, Box<dyn StdError + Send + Sync + 'static>>;
}

#[derive(Clone)]
pub struct Service {
    repository: Arc<dyn Repository<Account>>,
}

impl<R> From<R> for Service
where
    R: Repository<Account> + 'static,
{
    fn from(repository: R) -> Self {
        Self {
            repository: Arc::new(repository),
        }
    }
}

#[async_trait]
impl Handler<Transaction> for Service {
    type Error = anyhow::Error;

    async fn handle(&self, command: Envelope<Transaction>) -> Result<(), Self::Error> {
        let command = command.message;

        match command.transaction_type {
            TransactionType::Deposit => match self.repository.get(&command.client_id).await {
                Ok(account) => {
                    let mut root = BankAccountRoot::from(account);
                    root.deposit(command)?;
                    self.repository.save(&mut root).await?
                }
                Err(err) if matches!(GetError::NotFound, err) => {
                    let mut root =
                        BankAccountRoot::open(command.clone()).expect("new account root");
                    self.repository.save(&mut root).await?
                }
                Err(err) => (),
            },
            TransactionType::Withdrawal => {
                let mut root: BankAccountRoot =
                    self.repository.get(&command.client_id).await?.into();
                root.withdrawal(command)?;
                self.repository.save(&mut root).await?
            }
            TransactionType::Dispute => {
                let mut root: BankAccountRoot =
                    self.repository.get(&command.client_id).await?.into();
                root.dispute(command)?;
                self.repository.save(&mut root).await?
            }
            TransactionType::Resolve => {
                let mut root: BankAccountRoot =
                    self.repository.get(&command.client_id).await?.into();
                root.resolve(command)?;
                self.repository.save(&mut root).await?
            }
            TransactionType::Chargeback => {
                let mut root: BankAccountRoot =
                    self.repository.get(&command.client_id).await?.into();
                root.chargeback(command)?;
                self.repository.save(&mut root).await?
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("already exists")]
    Duplicated,
    #[error("closed")]
    Closed,
    #[error(transparent)]
    Other(#[from] Box<dyn StdError + Send + Sync + 'static>),
}

pub struct Runtime<E, S: State> {
    repository: Arc<dyn Repository<Account>>,
    executor: E,
    _state: PhantomData<S>,
}

impl<E> Runtime<E, Idle> {}

pub enum Idle {}
pub enum Dead {}

mod sealed {
    pub trait State {}
    impl State for super::Dead {}
    impl State for super::Idle {}
}
