mod core;
mod domain;
mod runtime;

use std::collections::HashSet;
use std::io;
use std::path::PathBuf;

use crate::core::repository::{Getter, Saver};
use crate::core::{EventSourced, GetError, Handler, InMemory};
use crate::domain::{Account, BankAccountRoot, Transaction, TransactionEvent, TransactionType};
use crate::runtime::Service;
use clap::Parser;
use csv::Trim;
use either::Either;
use tap::Pipe;

#[derive(Parser, Clone, Debug)]
pub struct Args {
    // #[arg(short, long, value_name = "TRANSACTION FILE SOURCE")]
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
    DispatchError(#[from] std::sync::mpsc::SendError<Transaction>),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl From<&str> for InputType {
    fn from(s: &str) -> Self {
        InputType::File(s.to_owned().into())
    }
}

struct InputProcessor {
    rx: std::sync::mpsc::Receiver<Transaction>,
}

impl From<InputType> for InputProcessor {
    fn from(value: InputType) -> Self {
        let (tx, rx) = std::sync::mpsc::channel();

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

pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    let processor = InputProcessor::from(args.input);
    let event_store = InMemory::<u16, TransactionEvent>::default();
    let account_repository = EventSourced::<Account, _>::from(event_store);
    let application_service = Service::from(account_repository.clone());

    let mut account_ids = HashSet::new();
    while let Ok(tx) = processor.rx.recv() {
        account_ids.insert(tx.client_id);
        if let Err(err) = application_service.handle(tx.into()).await {
            println!("transaction error {:?}", err)
        }
        // match tx.transaction_type {
        //     TransactionType::Deposit => {
        //         match account_repository.get(&tx.client_id).await {
        //             Ok(account) => {
        //                 let mut root = BankAccountRoot::from(account);
        //                 root.deposit(tx).unwrap();
        //                 account_repository.save(&mut root).await.unwrap();
        //             }
        //             Err(err) if matches!(GetError::NotFound, err) => {
        //                 let mut root = BankAccountRoot::open(tx.clone()).expect("new account root");
        //                 account_repository
        //                     .save(&mut root)
        //                     .await
        //                     .expect("new account version should be saved successfully");
        //             }
        //             Err(err) => {
        //                 panic!("{}", err);
        //             }
        //         };
        //     }
        //     TransactionType::Withdrawal => {
        //         // println!("withdrawal for client_id {:?} with amount {:?}", tx.client_id, tx.amount);
        //         let mut root: BankAccountRoot =
        //             account_repository.get(&tx.client_id).await.unwrap().into();
        //         if let Ok(_) = root.withdrawal(tx) {
        //             account_repository.save(&mut root).await.unwrap()
        //         }
        //     }
        //     TransactionType::Dispute => {
        //         let mut root: BankAccountRoot =
        //             account_repository.get(&tx.client_id).await.unwrap().into();
        //         root.dispute(tx).unwrap();
        //         account_repository.save(&mut root).await.unwrap()
        //     }
        //     TransactionType::Resolve => {
        //         let mut root: BankAccountRoot =
        //             account_repository.get(&tx.client_id).await.unwrap().into();
        //         root.resolve(tx).unwrap();
        //         account_repository.save(&mut root).await.unwrap()
        //     }
        //     TransactionType::Chargeback => {
        //         let mut root: BankAccountRoot =
        //             account_repository.get(&tx.client_id).await.unwrap().into();
        //         root.chargeback(tx).unwrap();
        //         account_repository.save(&mut root).await.unwrap()
        //     }
        // }
    }

    let mut wtr = csv::Writer::from_writer(io::stdout());
    for id in account_ids {
        let mut root: BankAccountRoot = account_repository.get(&id).await.unwrap().into();
        wtr.serialize(&root.snapshot())?;
    }
    wtr.flush()?;

    Ok(())
}
