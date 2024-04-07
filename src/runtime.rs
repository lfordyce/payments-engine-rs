use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::error::Error as StdError;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::core::repository::Repository;
use crate::core::{Envelope, GetError, Handler};
use crate::domain::{Account, BankAccountRoot, Transaction, TransactionType};
use crate::runtime::sealed::State;

pub trait Read {
    type Request;
    #[allow(clippy::type_complexity)]
    fn recv(
        &mut self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Self::Request, Box<dyn StdError + Send + Sync + 'static>>>
                + Send
                + '_,
        >,
    >;
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
                Err(_err) if matches!(GetError::NotFound, _err) => {
                    let mut root = BankAccountRoot::open(command.clone())?;
                    self.repository.save(&mut root).await?
                }
                Err(_err) => (),
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

/// An executor of futures.
pub trait Executor {
    /// Place the future into the executor to be run.
    fn execute<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
}

#[derive(Clone, Copy)]
pub struct TokioExecutor;

impl Executor for TokioExecutor {
    fn execute<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::task::spawn(future);
    }
}

pub struct Runtime<E, S: State> {
    svc: Service,
    connector: HashMap<String, Box<dyn Read<Request = Transaction> + Send>>,
    executor: E,
    account_ids: BTreeSet<u16>,
    _state: PhantomData<S>,
}

impl Runtime<TokioExecutor, Idle> {
    pub fn new(svc: Service) -> Self {
        Runtime {
            svc,
            connector: Default::default(),
            executor: TokioExecutor,
            account_ids: BTreeSet::new(),
            _state: PhantomData,
        }
    }
}

impl<E, S: State> Runtime<E, S> {
    #[inline]
    pub const fn account_ids(&self) -> &BTreeSet<u16> {
        &self.account_ids
    }
}

impl<E> Runtime<E, Idle> {
    pub fn with_connector(
        mut self,
        connector_id: impl Into<String>,
        connector: impl Read<Request = Transaction> + Send + 'static,
    ) -> Result<Self, ConnectorError> {
        let Entry::Vacant(entry) = self.connector.entry(connector_id.into()) else {
            return Err(ConnectorError::Duplicated)?;
        };

        entry.insert(Box::new(connector));
        Ok(self)
    }

    pub async fn run(mut self) -> anyhow::Result<Runtime<E, Dead>>
    where
        E: Executor,
    {
        let (tx, rx) = flume::bounded(8192);

        for (connector_name, mut connector) in self.connector.drain() {
            let tx = tx.clone();
            self.executor.execute(async move {
                while let Ok(request) = connector.recv().await {
                    if let Err(err) = tx.send_async(request).await {
                        eprintln!("connector `{connector_name}` failed: {err}");
                        break;
                    }
                }
            });
        }

        drop(tx);

        while let Ok(request) = rx.recv_async().await {
            self.account_ids.insert(request.client_id);
            if let Err(err) = self.svc.handle(request.into()).await {
                tracing::warn!(error=?err, "Error processing transaction:");
            }
        }

        let runtime = Runtime {
            svc: self.svc,
            connector: self.connector,
            executor: self.executor,
            account_ids: self.account_ids,
            _state: PhantomData,
        };

        Ok(runtime)
    }
}

pub enum Idle {}
pub enum Dead {}

mod sealed {
    pub trait State {}
    impl State for super::Dead {}
    impl State for super::Idle {}
}
