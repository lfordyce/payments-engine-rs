use std::future::Future;

use crate::core::aggregate::Envelope;
use crate::core::Message;
use async_trait::async_trait;

/// A component that is able to handle [Command]s of a certain type,
/// and mutate the state as a result of the command handling, or fail.
///
/// In an event-sourced system, the [Command] Handler
/// should use an [Aggregate][crate::core::aggregate::Aggregate] to evaluate
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

#[cfg(any(test, feature = "test"))]
#[doc(hidden)]
pub mod __scenario {
    use crate::core::store::{Appender, Check, Persisted};
    use crate::core::{Envelope, EventStoreExt, Handler, InMemory, Message, Tracking};
    use std::fmt::Debug;
    use std::hash::Hash;

    /// A test scenario that can be used to test a [Command][command::Envelope] [Handler][command::Handler]
    /// using a [given-then-when canvas](https://www.agilealliance.org/glossary/gwt/) approach.
    pub struct Scenario;

    impl Scenario {
        /// Sets the precondition state of the system for the [Scenario], which
        /// is expressed by a list of Domain [Event][event::Envelope]s in an Event-sourced system.
        #[must_use]
        pub fn given<Id, Evt>(self, events: Vec<Persisted<Id, Evt>>) -> ScenarioGiven<Id, Evt>
        where
            Evt: Message,
        {
            ScenarioGiven { given: events }
        }

        /// Specifies the [Command][command::Envelope] to test in the [Scenario], in the peculiar case
        /// of having a clean system.
        #[must_use]
        pub fn when<Id, Evt, Cmd>(self, command: Envelope<Cmd>) -> ScenarioWhen<Id, Evt, Cmd>
        where
            Evt: Message,
            Cmd: Message,
        {
            ScenarioWhen {
                given: Vec::default(),
                when: command,
            }
        }
    }

    #[doc(hidden)]
    pub struct ScenarioGiven<Id, Evt>
    where
        Evt: Message,
    {
        given: Vec<Persisted<Id, Evt>>,
    }

    impl<Id, Evt> ScenarioGiven<Id, Evt>
    where
        Evt: Message,
    {
        /// Specifies the [Command][command::Envelope] to test in the [Scenario].
        #[must_use]
        pub fn when<Cmd>(self, command: Envelope<Cmd>) -> ScenarioWhen<Id, Evt, Cmd>
        where
            Cmd: Message,
        {
            ScenarioWhen {
                given: self.given,
                when: command,
            }
        }
    }

    #[doc(hidden)]
    pub struct ScenarioWhen<Id, Evt, Cmd>
    where
        Evt: Message,
        Cmd: Message,
    {
        given: Vec<Persisted<Id, Evt>>,
        when: Envelope<Cmd>,
    }

    impl<Id, Evt, Cmd> ScenarioWhen<Id, Evt, Cmd>
    where
        Evt: Message,
        Cmd: Message,
    {
        /// Sets the expectation on the result of the [Scenario] to be positive
        /// and produce a specified list of Domain [Event]s.
        #[must_use]
        pub fn then(self, events: Vec<Persisted<Id, Evt>>) -> ScenarioThen<Id, Evt, Cmd> {
            ScenarioThen {
                given: self.given,
                when: self.when,
                case: ScenarioThenCase::Produces(events),
            }
        }

        /// Sets the expectation on the result of the [Scenario] to return an error.
        #[must_use]
        pub fn then_fails(self) -> ScenarioThen<Id, Evt, Cmd> {
            ScenarioThen {
                given: self.given,
                when: self.when,
                case: ScenarioThenCase::Fails,
            }
        }
    }

    enum ScenarioThenCase<Id, Evt>
    where
        Evt: Message,
    {
        Produces(Vec<Persisted<Id, Evt>>),
        Fails,
    }

    #[doc(hidden)]
    pub struct ScenarioThen<Id, Evt, Cmd>
    where
        Evt: Message,
        Cmd: Message,
    {
        given: Vec<Persisted<Id, Evt>>,
        when: Envelope<Cmd>,
        case: ScenarioThenCase<Id, Evt>,
    }

    impl<Id, Evt, Cmd> ScenarioThen<Id, Evt, Cmd>
    where
        Id: Clone + Eq + Hash + Send + Sync + Debug,
        Evt: Message + Clone + PartialEq + Send + Sync + Debug,
        Cmd: Message,
    {
        /// Executes the whole [Scenario] by constructing a Command [Handler][command::Handler]
        /// with the provided closure function and running the specified assertions.
        ///
        /// # Panics
        ///
        /// The method panics if the assertion fails.
        pub async fn assert_on<F, H>(self, handler_factory: F)
        where
            F: Fn(Tracking<InMemory<Id, Evt>, Id, Evt>) -> H,
            H: Handler<Cmd>,
        {
            let event_store = InMemory::<Id, Evt>::default();
            let tracking_event_store = event_store.clone().with_recorded_events_tracking();

            for event in self.given {
                event_store
                    .append(
                        event.stream_id,
                        Check::MustBe(event.version - 1),
                        vec![event.event],
                    )
                    .await
                    .expect("domain event in 'given' should be inserted in the event store");
            }

            let handler = handler_factory(tracking_event_store.clone());
            let result = handler.handle(self.when).await;

            match self.case {
                ScenarioThenCase::Produces(events) => {
                    let recorded_events = tracking_event_store.recorded_events();
                    assert_eq!(events, recorded_events);
                }
                ScenarioThenCase::Fails => assert!(result.is_err()),
            };
        }
    }
}
