mod domain;
mod core;

use std::io;
use std::path::PathBuf;

use crate::domain::{TransactionEvent};
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
    DispatchError(#[from] std::sync::mpsc::SendError<TransactionEvent>),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl From<&str> for InputType {
    fn from(s: &str) -> Self {
        InputType::File(s.to_owned().into())
    }
}

struct InputProcessor {
    rx: std::sync::mpsc::Receiver<TransactionEvent>,
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
                let record: TransactionEvent = result?;
                tx.send(record)?
            }

            Ok(())
        });
        Self { rx }
    }
}

pub fn run() -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let args = Args::parse();

    let processor = InputProcessor::from(args.input);

    while let Ok(r) = processor.rx.recv() {
        println!("{:?}", r)
    }

    Ok(())
}