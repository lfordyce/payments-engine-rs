use std::error::Error;
use std::future::Future;
use std::io;
use std::pin::Pin;

use clap::Parser;
use csv::Trim;
use either::Either;
use futures::TryFutureExt;
use tap::{Pipe, TapFallible as _};

use crate::cli::{Args, InputType, ProcessingError};
use crate::core::repository::Getter;
use crate::core::{EventSourced, InMemory};
use crate::domain::{Account, BankAccountRoot, Transaction, TransactionEvent};
use crate::runtime::{ConnectorError, Read, Runtime, Service};

mod cli;
pub mod core;
pub mod domain;
pub mod runtime;

struct InputProcessor {
    rx: flume::Receiver<Transaction>,
}

impl From<InputType> for InputProcessor {
    fn from(value: InputType) -> Self {
        let (tx, rx) = flume::bounded(128 * 1024);

        std::thread::spawn(move || -> Result<(), ProcessingError> {
            let rdr = match value {
                InputType::File(path) => Either::Left(std::fs::File::open(path)?),
                InputType::Stdin => Either::Right(io::stdin()),
            }
            .pipe(|either_reader| {
                csv::ReaderBuilder::new()
                    .trim(Trim::All)
                    .flexible(true)
                    .from_reader(either_reader)
            });

            for record in rdr
                .into_deserialize::<Transaction>()
                .map(|record| {
                    record
                        .tap_err(|err| tracing::error!(error=?err, "Error parsing CSV records"))
                        .map_err(ProcessingError::from)
                })
                .map_while(Result::ok)
            {
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

    args.instrumentation.setup()?;

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
