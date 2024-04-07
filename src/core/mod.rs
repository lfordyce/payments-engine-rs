mod aggregate;
mod command;
pub(crate) mod repository;
mod store;

pub use aggregate::{Aggregate, Envelope, Message, Root};
pub use command::Handler;
pub use repository::{EventSourced, GetError};
pub use store::InMemory;
pub use store::Persisted;

#[cfg(any(test, feature = "test"))]
pub use command::__scenario::{Scenario, ScenarioGiven, ScenarioThen, ScenarioWhen};

#[cfg(any(test, feature = "test"))]
pub use store::__tracking::{EventStoreExt, Tracking};
