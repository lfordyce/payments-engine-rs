use std::fmt::Debug;
use std::marker::PhantomData;

use crate::core::store::{
    AppendError, Check, ConflictError, Store, Streamer, Version, VersionSelect,
};
use crate::core::{Aggregate, Root};
use async_trait::async_trait;
use futures::TryStreamExt;

/// All possible errors returned by [`Getter::get`].
#[derive(Debug, thiserror::Error)]
pub enum GetError {
    /// Error returned when the [Aggregate Root][aggregate::Root] could not be found in the data store.
    #[error("failed to get aggregate root: not found")]
    NotFound,
    /// Error returned when the [Getter] implementation has encountered an error.
    #[error("failed to get aggregate root, an error occurred: {0}")]
    Internal(#[from] anyhow::Error),
}

/// Trait used to implement read access to a data store from which
/// to load an [aggregate::Root] instance, given its id.
#[async_trait]
pub trait Getter<T>: Send + Sync
where
    T: Aggregate,
{
    /// Loads an [aggregate::Root] instance from the data store,
    /// referenced by its unique identifier.
    async fn get(&self, id: &T::Id) -> Result<Root<T>, GetError>;
}

/// All possible errors returned by [`Saver::save`].
#[derive(Debug, thiserror::Error)]
pub enum SaveError {
    /// Error returned when [Saver::save] encounters a conflict error while saving the new Aggregate Root.
    #[error("failed to save aggregate root: {0}")]
    Conflict(#[from] ConflictError),
    /// Error returned when the [Saver] implementation has encountered an error.
    #[error("failed to save aggregate root, an error occurred: {0}")]
    Internal(#[from] anyhow::Error),
}

/// Trait used to implement write access to a data store, which can be used
/// to save the latest state of an [aggregate::Root] instance.
#[async_trait]
pub trait Saver<T>: Send + Sync
where
    T: Aggregate,
{
    /// Saves a new version of an [aggregate::Root] instance to the data store.
    async fn save(&self, root: &mut Root<T>) -> Result<(), SaveError>;
}

/// A Repository is an object that allows to load and save
/// an [Aggregate Root][aggregate::Root] from and to a persistent data store.
pub trait Repository<T>: Getter<T> + Saver<T> + Send + Sync
where
    T: Aggregate,
{
}

impl<T, R> Repository<T> for R
where
    T: Aggregate,
    R: Getter<T> + Saver<T> + Send + Sync,
{
}

/// An Event-sourced implementation of the [Repository] interface.
///
/// It uses an [Event Store][crate::core::store::Store] instance to stream Domain Events
/// for a particular Aggregate, and append uncommitted Domain Events
/// recorded by an Aggregate Root.
#[derive(Debug, Clone)]
pub struct EventSourced<T, S>
where
    T: Aggregate,
    S: Store<T::Id, T::Event>,
{
    store: S,
    aggregate: PhantomData<T>,
}

impl<T, S> From<S> for EventSourced<T, S>
where
    T: Aggregate,
    S: Store<T::Id, T::Event>,
{
    fn from(store: S) -> Self {
        Self {
            store,
            aggregate: PhantomData,
        }
    }
}

#[async_trait]
impl<T, S> Getter<T> for EventSourced<T, S>
where
    T: Aggregate,
    T::Id: Clone,
    T::Error: std::error::Error + Send + Sync + 'static,
    S: Store<T::Id, T::Event>,
    <S as Streamer<T::Id, T::Event>>::Error: std::error::Error + Send + Sync + 'static,
{
    async fn get(&self, id: &T::Id) -> Result<Root<T>, GetError> {
        let stream = self
            .store
            .stream(id, VersionSelect::All)
            .map_ok(|persisted| persisted.event);

        let ctx = Root::<T>::rehydrate_async(stream)
            .await
            .map_err(anyhow::Error::from)
            .map_err(GetError::Internal)?;

        ctx.ok_or(GetError::NotFound)
    }
}

#[async_trait]
impl<T, S> Saver<T> for EventSourced<T, S>
where
    T: Aggregate,
    T::Id: Clone,
    S: Store<T::Id, T::Event>,
{
    async fn save(&self, root: &mut Root<T>) -> Result<(), SaveError> {
        let events_to_commit = root.take_uncommitted_events();
        let aggregate_id = root.aggregate_id();

        if events_to_commit.is_empty() {
            return Ok(());
        }

        let current_event_stream_version = root.version() - (events_to_commit.len() as Version);

        self.store
            .append(
                aggregate_id.clone(),
                Check::MustBe(current_event_stream_version),
                events_to_commit,
            )
            .await
            .map_err(|err| match err {
                AppendError::Internal(err) => SaveError::Internal(err),
                AppendError::Conflict(err) => SaveError::Conflict(err),
            })?;

        Ok(())
    }
}
