mod aggregate;
pub(crate) mod repository;
mod store;

pub use aggregate::{Aggregate, Message, Root};
pub use repository::{EventSourced, GetError};
pub use store::InMemory;
