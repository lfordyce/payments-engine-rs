use crate::core::aggregate::{Envelope, Message};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use futures::stream::{iter, StreamExt};

pub type Version = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("conflict error detected, expected version was: {expected}, found: {actual}")]
pub struct ConflictError {
    /// The [Version] value that was expected when calling the function that failed.
    pub expected: Version,

    /// The actual [Version] value, which mismatch caused this error.
    pub actual: Version,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Check {
    /// Disables any kind of optimistic locking check, allowing
    /// for any [Version] to be used compared to the new one.
    Any,
    /// Expects that the previous [Version] used for the operation
    /// must have the value specified.
    MustBe(Version),
}

/// An [Event] that has been persisted to the Event [Store].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Persisted<Id, Evt>
where
    Evt: Message,
{
    /// The id of the Event Stream the persisted Event belongs to.
    pub stream_id: Id,

    /// The version of the Event Stream when this Event has been recorded.
    ///
    /// This value is used for optimistic concurrency checks, to avoid
    /// data races in parallel command evaluations.
    ///
    /// Check the [Version][version::Version] type and module documentation for more info.
    pub version: Version,

    /// The actual Domain Event carried by this envelope.
    pub event: Envelope<Evt>,
}

/// Specifies the slice of the Event Stream to select when calling [`Store::stream`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionSelect {
    /// Selects all [Event][Envelope]s in the Event [Stream].
    All,

    /// Selects all [Event][Envelope]s in the Event [Stream] starting from the [Event]
    /// with the specified [Version][version::Version].
    From(Version),
}

/// Stream is a stream of [Persisted] Domain Events.
pub type Stream<'a, Id, Evt, Err> = BoxStream<'a, Result<Persisted<Id, Evt>, Err>>;

/// Interface used to stream [Persisted][event::Persisted] Domain Events
/// from an Event Store to an application.
pub trait Streamer<StreamId, Event>: Send + Sync
where
    StreamId: Send + Sync,
    Event: Message + Send + Sync,
{
    /// The error type returned by the Store during a [`stream`] call.
    type Error: Send + Sync;

    /// Opens an Event Stream, effectively streaming all Domain Events
    /// of an Event Stream back in the application.
    fn stream(&self, id: &StreamId, select: VersionSelect) -> Stream<StreamId, Event, Self::Error>;
}

/// All possible error types returned by [`Appender::append`].
#[derive(Debug, thiserror::Error)]
pub enum AppendError {
    /// Error returned when [Appender::append] encounters a conflict error
    /// while appending the new Domain Events.
    #[error("failed to append new domain events: {0}")]
    Conflict(#[from] ConflictError),
    /// Error returned when the [Appender] implementation has encountered an error.
    #[error("failed to append new domain events, an error occurred: {0}")]
    Internal(#[from] anyhow::Error),
}

#[async_trait]
/// Interface used to append new Domain Events in an Event Store.
pub trait Appender<StreamId, Event>: Send + Sync
where
    StreamId: Send + Sync,
    Event: Message + Send + Sync,
{
    /// Appens new Domain Events to the specified Event Stream.
    ///
    /// The result of this operation is the new [Version][version::Version]
    /// of the Event Stream with the specified Domain Events added to it.
    async fn append(
        &self,
        id: StreamId,
        version_check: Check,
        events: Vec<Envelope<Event>>,
    ) -> Result<Version, AppendError>;
}

/// An [Event][event::Envelope] Store, used to store Domain Events in Event Streams -- a stream
/// of Domain Events -- and retrieve them.
///
/// Each Event Stream is represented by a unique Stream identifier.
pub trait Store<StreamId, Event>:
    Streamer<StreamId, Event> + Appender<StreamId, Event> + Send + Sync
where
    StreamId: Send + Sync,
    Event: Message + Send + Sync,
{
}

impl<T, StreamId, Event> Store<StreamId, Event> for T
where
    T: Streamer<StreamId, Event> + Appender<StreamId, Event> + Send + Sync,
    StreamId: Send + Sync,
    Event: Message + Send + Sync,
{
}

#[derive(Debug)]
struct InMemoryBackend<Id, Evt>
where
    Evt: Message,
{
    event_streams: HashMap<Id, Vec<Persisted<Id, Evt>>>,
}

impl<Id, Evt> Default for InMemoryBackend<Id, Evt>
where
    Evt: Message,
{
    fn default() -> Self {
        Self {
            event_streams: HashMap::default(),
        }
    }
}

/// In-memory implementation of [`event::Store`] trait,
/// backed by a thread-safe [`std::collections::HashMap`].
#[derive(Debug, Clone)]
pub struct InMemory<Id, Evt>
where
    Evt: Message,
{
    backend: Arc<RwLock<InMemoryBackend<Id, Evt>>>,
}

impl<Id, Evt> Default for InMemory<Id, Evt>
where
    Evt: Message,
{
    fn default() -> Self {
        Self {
            backend: Arc::default(),
        }
    }
}

impl<Id, Evt> Streamer<Id, Evt> for InMemory<Id, Evt>
where
    Id: Clone + Eq + Hash + Send + Sync,
    Evt: Message + Clone + Send + Sync,
{
    type Error = Infallible;

    fn stream(&self, id: &Id, select: VersionSelect) -> Stream<Id, Evt, Self::Error> {
        let backend = self
            .backend
            .read()
            .expect("acquire read lock on event store backend");

        let events = backend
            .event_streams
            .get(id)
            .cloned()
            .unwrap_or_default() // NOTE: the new Vec is empty, so there will be no memory allocation!
            .into_iter()
            .filter(move |evt| match select {
                VersionSelect::All => true,
                VersionSelect::From(v) => evt.version >= v,
            });

        iter(events).map(Ok).boxed()
    }
}

#[async_trait]
impl<Id, Evt> Appender<Id, Evt> for InMemory<Id, Evt>
where
    Id: Clone + Eq + Hash + Send + Sync,
    Evt: Message + Clone + Send + Sync,
{
    async fn append(
        &self,
        id: Id,
        version_check: Check,
        events: Vec<Envelope<Evt>>,
    ) -> Result<Version, AppendError> {
        let mut backend = self
            .backend
            .write()
            .expect("acquire write lock on event store backend");

        let last_event_stream_version = backend
            .event_streams
            .get(&id)
            .and_then(|events| events.last())
            .map(|event| event.version)
            .unwrap_or_default();

        if let Check::MustBe(expected) = version_check {
            if last_event_stream_version != expected {
                return Err(AppendError::Conflict(ConflictError {
                    expected,
                    actual: last_event_stream_version,
                }));
            }
        }

        let mut persisted_events: Vec<Persisted<Id, Evt>> = events
            .into_iter()
            .enumerate()
            .map(|(i, event)| Persisted {
                stream_id: id.clone(),
                version: last_event_stream_version + (i as u64) + 1,
                event,
            })
            .collect();

        let new_last_event_stream_version = persisted_events
            .last()
            .map(|evt| evt.version)
            .unwrap_or_default();

        backend
            .event_streams
            .entry(id)
            .and_modify(|events| events.append(&mut persisted_events))
            .or_insert_with(|| persisted_events);

        Ok(new_last_event_stream_version)
    }
}

#[cfg(test)]
mod test {
    use futures::TryStreamExt;
    use lazy_static::lazy_static;

    use super::*;

    const STREAM_ID: &str = "stream:test";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct StringMessage(pub(crate) &'static str);

    impl Message for StringMessage {
        fn name(&self) -> &'static str {
            "string_payload"
        }
    }

    lazy_static! {
        static ref EVENTS: Vec<Envelope<StringMessage>> = vec![
            Envelope::from(StringMessage("event-1")),
            Envelope::from(StringMessage("event-2")),
            Envelope::from(StringMessage("event-3")),
        ];
    }

    #[tokio::test]
    async fn it_works() {
        let event_store = InMemory::<&'static str, StringMessage>::default();

        let new_event_stream_version = event_store
            .append(STREAM_ID, Check::MustBe(0), EVENTS.clone())
            .await
            .expect("append should not fail");

        let expected_version = EVENTS.len() as Version;
        assert_eq!(expected_version, new_event_stream_version);

        let expected_events = EVENTS
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, event)| Persisted {
                stream_id: STREAM_ID,
                version: (i as Version) + 1,
                event,
            })
            .collect::<Vec<_>>();

        let event_stream: Vec<_> = event_store
            .stream(&STREAM_ID, VersionSelect::All)
            .try_collect()
            .await
            .expect("opening an event stream should not fail");

        assert_eq!(expected_events, event_stream);
    }
}
