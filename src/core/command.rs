use std::future::Future;

use crate::core::aggregate::Envelope;
use crate::core::Message;
use async_trait::async_trait;

/// A software component that is able to handle [Command]s of a certain type,
/// and mutate the state as a result of the command handling, or fail.
///
/// In an event-sourced system, the [Command] Handler
/// should use an [Aggregate][crate::aggregate::Aggregate] to evaluate
/// a [Command] to ensure business invariants are respected.
#[async_trait]
pub trait Handler<T>: Send + Sync
where
    T: Message,
{
    /// The error type returned by the Handler while handling a [Command].
    type Error: Send + Sync;

    /// Handles a [Command] and returns an error if the handling has failed.
    ///
    /// Since [Command]s are solely modifying the state of the system,
    /// they do not return anything to the caller but the result of the operation
    /// (expressed by a [Result] type).
    async fn handle(&self, command: Envelope<T>) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T, Err, F, Fut> Handler<T> for F
where
    T: Message + Send + Sync + 'static,
    Err: Send + Sync,
    F: Send + Sync + Fn(Envelope<T>) -> Fut,
    Fut: Send + Sync + Future<Output = Result<(), Err>>,
{
    type Error = Err;

    async fn handle(&self, command: Envelope<T>) -> Result<(), Self::Error> {
        self(command).await
    }
}
