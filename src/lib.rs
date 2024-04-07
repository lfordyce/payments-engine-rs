use std::error::Error;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;

use clap::Parser;
use csv::Trim;
use either::Either;
use futures::TryFutureExt;
use tap::Pipe;

use crate::core::repository::Getter;
use crate::core::{EventSourced, InMemory};
use crate::domain::{Account, BankAccountRoot, Transaction, TransactionEvent};
use crate::runtime::{ConnectorError, Read, Runtime, Service};

pub mod core;
pub mod domain;
pub mod runtime;

#[derive(Parser, Clone, Debug)]
pub struct Args {
    pub input: InputType,
}

#[derive(Debug, Default, Clone)]
pub enum InputType {
    #[default]
    Stdin,
    File(PathBuf),
}

#[derive(Debug, thiserror::Error)]
enum ProcessingError {
    #[error(transparent)]
    ParseError(#[from] csv::Error),
    #[error(transparent)]
    DispatchError(#[from] flume::SendError<Transaction>),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl From<&str> for InputType {
    fn from(s: &str) -> Self {
        InputType::File(s.to_owned().into())
    }
}

struct InputProcessor {
    rx: flume::Receiver<Transaction>,
}

impl From<InputType> for InputProcessor {
    fn from(value: InputType) -> Self {
        let (tx, rx) = flume::bounded(128 * 1024);

        std::thread::spawn(move || -> Result<(), ProcessingError> {
            let mut rdr = match value {
                InputType::File(path) => Either::Left(std::fs::File::open(path)?),
                InputType::Stdin => Either::Right(io::stdin()),
            }
            .pipe(|either_reader| {
                csv::ReaderBuilder::new()
                    .trim(Trim::All)
                    .flexible(true)
                    .from_reader(either_reader)
            });
            for result in rdr.deserialize() {
                let record: Transaction = result?;
                tx.send(record)?
            }

            Ok(())
        });
        Self { rx }
    }
}

impl Read for InputProcessor {
    type Request = Transaction;

    fn recv(
        &mut self,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = anyhow::Result<Self::Request, Box<dyn Error + Send + Sync + 'static>>,
                > + Send
                + '_,
        >,
    > {
        let fut = self
            .rx
            .recv_async()
            .map_err(Box::<dyn Error + Send + Sync + 'static>::from)
            .map_err(ConnectorError::Other)
            .map_err(Into::into);

        Box::pin(fut)
    }
}

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    let tracer = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::ERROR)
        .finish();
    tracing::subscriber::set_global_default(tracer)?;

    let event_store = InMemory::<u16, TransactionEvent>::default();
    let account_repository = EventSourced::<Account, _>::from(event_store);
    let application_service = Service::from(account_repository.clone());

    let engine = Runtime::new(application_service)
        .with_connector("stdin_or_file", InputProcessor::from(args.input))?;

    let engine = engine.run().await?;

    let mut wtr = csv::Writer::from_writer(io::stdout());
    for id in engine.account_ids() {
        let root: BankAccountRoot = account_repository.get(id).await?.into();
        wtr.serialize(&root.snapshot())?;
    }
    wtr.flush()?;

    Ok(())
}
