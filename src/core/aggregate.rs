use crate::core::store::Version;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

pub trait Aggregate: Sized + Send + Sync + Clone {
    /// The type used to uniquely identify the Aggregate.
    type Id: Send + Sync;

    /// The type of Domain Events that interest this Aggregate.
    /// Usually, this type should be an `enum`.
    type Event: Message + Send + Sync + Clone;

    /// The error type that can be returned by [`Aggregate::apply`] when
    /// mutating the Aggregate state.
    type Error: Send + Sync;

    /// A unique name identifier for this Aggregate type.
    fn type_name() -> &'static str;

    /// Returns the unique identifier for the Aggregate instance.
    fn aggregate_id(&self) -> &Self::Id;

    /// Mutates the state of an Aggregate through a Domain Event.
    ///
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, Self::Error>;
}

/// Represents a piece of domain data that occurs in the system.
///
/// Each Message has a specific name to it, which should ideally be
/// unique within the domain you're operating in. Example: a Domain Event
/// that represents when an Order was created can have a `name()`: `"OrderWasCreated"`.
pub trait Message {
    /// Returns the domain name of the [Message].
    fn name(&self) -> &'static str;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<T>
where
    T: Message,
{
    /// The message payload.
    pub message: T,
}

impl<T> From<T> for Envelope<T>
where
    T: Message,
{
    fn from(message: T) -> Self {
        Envelope { message }
    }
}

impl<T> PartialEq for Envelope<T>
where
    T: Message + PartialEq,
{
    fn eq(&self, other: &Envelope<T>) -> bool {
        self.message == other.message
    }
}

/// An Aggregate Root represents the Domain Entity object used to
/// load and save an [Aggregate] from and to a [Repository], and
/// to perform actions that may result in new Domain Events
/// to change the state of the Aggregate.
///
/// The Aggregate state and list of Domain Events recorded
/// are handled by the [Root] object itself.
#[derive(Debug, Clone, PartialEq)]
#[must_use]
pub struct Root<T>
where
    T: Aggregate,
{
    aggregate: T,
    version: Version,
    recorded_events: Vec<Envelope<T::Event>>,
}

impl<T> std::ops::Deref for Root<T>
where
    T: Aggregate,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.aggregate
    }
}

impl<T> Root<T>
where
    T: Aggregate,
{
    /// Returns the current version for the [Aggregate].
    pub fn version(&self) -> Version {
        self.version
    }

    /// Returns the unique identifier of the [Aggregate].
    pub fn aggregate_id(&self) -> &T::Id {
        self.aggregate.aggregate_id()
    }

    /// Returns the list of uncommitted, recorded Domain [Event]s from the [Root]
    /// and resets the internal list to its default value.
    #[doc(hidden)]
    pub fn take_uncommitted_events(&mut self) -> Vec<Envelope<T::Event>> {
        std::mem::take(&mut self.recorded_events)
    }

    /// Creates a new [Aggregate] [Root] instance by applying the specified
    /// Domain Event.
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    pub fn record_new(event: Envelope<T::Event>) -> Result<Self, T::Error> {
        Ok(Root {
            version: 1,
            aggregate: T::apply(None, event.message.clone())?,
            recorded_events: vec![event],
        })
    }

    /// Records a change to the [Aggregate] [Root], expressed by the specified
    /// Domain Event.
    ///
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    pub fn record_that(&mut self, event: Envelope<T::Event>) -> Result<(), T::Error> {
        self.aggregate = T::apply(Some(self.aggregate.clone()), event.message.clone())?;
        self.recorded_events.push(event);
        self.version += 1;

        Ok(())
    }
}

/// List of possible errors that can be returned by [`Root::rehydrate_async`].
#[derive(Debug, thiserror::Error)]
pub enum RehydrateError<T, I> {
    /// Error returned during rehydration when the [Aggregate Root][Root]
    /// is applying a Domain Event using [Aggregate::apply].
    ///
    /// This usually implies the Event Stream for the [Aggregate]
    /// contains corrupted or unexpected data.
    #[error("failed to apply domain event while rehydrating aggregate: {0}")]
    Domain(#[source] T),

    /// This error is returned by [Root::rehydrate_async] when the underlying
    /// [futures::TryStream] has returned an error.
    #[error("failed to rehydrate aggregate from event stream: {0}")]
    Inner(#[source] I),
}

impl<T> Root<T>
where
    T: Aggregate,
{
    /// Rehydrates an [Aggregate] Root from its state and version.
    /// Useful for [Repository] implementations outside the [EventSourcedRepository] one.
    #[doc(hidden)]
    pub fn rehydrate_from_state(version: Version, aggregate: T) -> Root<T> {
        Root {
            version,
            aggregate,
            recorded_events: Vec::default(),
        }
    }

    /// Rehydrates an [Aggregate Root][Root] from a stream of Domain Events.
    #[doc(hidden)]
    pub(crate) fn rehydrate(
        mut stream: impl Iterator<Item = Envelope<T::Event>>,
    ) -> Result<Option<Root<T>>, T::Error> {
        stream.try_fold(None, |ctx: Option<Root<T>>, event| {
            let new_ctx_result = match ctx {
                None => Root::<T>::rehydrate_from(event),
                Some(ctx) => ctx.apply_rehydrated_event(event),
            };

            Ok(Some(new_ctx_result?))
        })
    }

    /// Rehydrates an [Aggregate Root][Root] from a stream of Domain Events.
    #[doc(hidden)]
    pub(crate) async fn rehydrate_async<Err>(
        stream: impl futures::TryStream<Ok = Envelope<T::Event>, Error = Err>,
    ) -> Result<Option<Root<T>>, RehydrateError<T::Error, Err>> {
        stream
            .map_err(RehydrateError::Inner)
            .try_fold(None, |ctx: Option<Root<T>>, event| async {
                let new_ctx_result = match ctx {
                    None => Root::<T>::rehydrate_from(event),
                    Some(ctx) => ctx.apply_rehydrated_event(event),
                };

                Ok(Some(new_ctx_result.map_err(RehydrateError::Domain)?))
            })
            .await
    }

    /// Creates a new [Root] instance from a Domain [Event]
    /// while rehydrating an [Aggregate].
    ///
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    #[doc(hidden)]
    pub(crate) fn rehydrate_from(event: Envelope<T::Event>) -> Result<Root<T>, T::Error> {
        Ok(Root {
            version: 1,
            aggregate: T::apply(None, event.message)?,
            recorded_events: Vec::default(),
        })
    }

    /// Applies a new Domain [Event] to the [Root] while rehydrating
    /// an [Aggregate].
    ///
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    #[doc(hidden)]
    pub(crate) fn apply_rehydrated_event(
        mut self,
        event: Envelope<T::Event>,
    ) -> Result<Root<T>, T::Error> {
        self.aggregate = T::apply(Some(self.aggregate), event.message)?;
        self.version += 1;

        Ok(self)
    }
}
