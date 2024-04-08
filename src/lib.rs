use std::error::Error;
use std::future::Future;
use std::io;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::pin::Pin;

use clap::Parser;
use csv::Trim;
use either::Either;
use futures::TryFutureExt;
use tap::Pipe;
use tracing::Subscriber;
use tracing_subscriber::{
    filter::Directive,
    layer::{Layer, SubscriberExt},
    registry::LookupSpan,
    util::SubscriberInitExt,
    EnvFilter,
};

use crate::core::repository::Getter;
use crate::core::{EventSourced, InMemory};
use crate::domain::{Account, BankAccountRoot, Transaction, TransactionEvent};
use crate::runtime::{ConnectorError, Read, Runtime, Service};

pub mod core;
pub mod domain;
pub mod runtime;

#[derive(Parser, Debug)]
pub struct Args {
    pub input: InputType,

    #[clap(flatten)]
    pub(crate) instrumentation: Instrumentation,
}

#[derive(Clone, Default, Debug, clap::ValueEnum)]
pub(crate) enum Logger {
    #[default]
    Compact,
    Full,
    Pretty,
    Json,
}

impl std::fmt::Display for Logger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let logger = match self {
            Logger::Compact => "compact",
            Logger::Full => "full",
            Logger::Pretty => "pretty",
            Logger::Json => "json",
        };
        write!(f, "{}", logger)
    }
}

#[derive(clap::Args, Debug, Default)]
pub(crate) struct Instrumentation {
    /// Enable debug logs, -vvv for trace
    #[arg(short = 'v', env = "DEMO_VERBOSITY", long, action = clap::ArgAction::Count, global = true)]
    pub verbose: u8,
    /// Which logger to use
    #[arg(long, env = "DEMO_LOGGER", default_value_t = Default::default(), global = true)]
    pub(crate) logger: Logger,
    /// Tracing directives
    ///
    /// See https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives
    #[arg(long = "log-directive", global = true, env = "DEMO_LOG_DIRECTIVES", value_delimiter = ',', num_args = 0..)]
    pub(crate) log_directives: Vec<Directive>,
}

impl Instrumentation {
    pub(crate) fn log_level(&self) -> String {
        match self.verbose {
            0 => "error",
            1 => "info",
            2 => "debug",
            _ => "trace",
        }
        .to_string()
    }
    // (continued below)
    pub(crate) fn setup(&self) -> anyhow::Result<()> {
        let filter_layer = self.filter_layer()?;

        let registry = tracing_subscriber::registry()
            .with(filter_layer)
            .with(tracing_error::ErrorLayer::default());

        // `try_init` called inside `match` since `with` changes the type
        match self.logger {
            Logger::Compact => registry.with(self.fmt_layer_compact()).try_init()?,
            Logger::Full => registry.with(self.fmt_layer_full()).try_init()?,
            Logger::Pretty => registry.with(self.fmt_layer_pretty()).try_init()?,
            Logger::Json => registry.with(self.fmt_layer_json()).try_init()?,
        }

        Ok(())
    }

    pub(crate) fn filter_layer(&self) -> anyhow::Result<EnvFilter> {
        let mut filter_layer = match EnvFilter::try_from_default_env() {
            Ok(layer) => layer,
            Err(e) => {
                // Catch a parse error and report it, ignore a missing env
                if let Some(source) = e.source() {
                    match source.downcast_ref::<std::env::VarError>() {
                        Some(std::env::VarError::NotPresent) => (),
                        _ => return Err(e.into()),
                    }
                }
                // If the `--log-directive` is specified, don't set a default
                if self.log_directives.is_empty() {
                    EnvFilter::try_new(format!(
                        "{}={}",
                        env!("CARGO_PKG_NAME").replace('-', "_"),
                        self.log_level()
                    ))?
                } else {
                    EnvFilter::try_new("")?
                }
            }
        };

        for directive in &self.log_directives {
            let directive_clone = directive.clone();
            filter_layer = filter_layer.add_directive(directive_clone);
        }

        Ok(filter_layer)
    }

    pub(crate) fn fmt_layer_full<S>(&self) -> impl Layer<S>
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        tracing_subscriber::fmt::Layer::new()
            .with_ansi(std::io::stderr().is_terminal())
            .with_writer(std::io::stderr)
    }

    pub(crate) fn fmt_layer_pretty<S>(&self) -> impl Layer<S>
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        tracing_subscriber::fmt::Layer::new()
            .with_ansi(std::io::stderr().is_terminal())
            .with_writer(std::io::stderr)
            .pretty()
    }

    pub(crate) fn fmt_layer_json<S>(&self) -> impl Layer<S>
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        tracing_subscriber::fmt::Layer::new()
            .with_ansi(std::io::stderr().is_terminal())
            .with_writer(std::io::stderr)
            .json()
    }

    pub(crate) fn fmt_layer_compact<S>(&self) -> impl Layer<S>
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        tracing_subscriber::fmt::Layer::new()
            .with_ansi(std::io::stderr().is_terminal())
            .with_writer(std::io::stderr)
            .compact()
            .without_time()
            .with_target(false)
            .with_thread_ids(false)
            .with_thread_names(false)
            .with_file(false)
            .with_line_number(false)
    }
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

    args.instrumentation.setup()?;

    // let trace_layer = TraceLayer::new_for_http()
    //     .make_span_with(trace_layer::trace_layer_make_span_with)
    //     .on_request(trace_layer::trace_layer_on_request)
    //     .on_response(trace_layer::trace_layer_on_response);

    tracing::info!("testing logger");

    // let tracer = tracing_subscriber::FmtSubscriber::builder()
    //     .with_max_level(tracing::Level::ERROR)
    //     .finish();
    // tracing::subscriber::set_global_default(tracer)?;

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
