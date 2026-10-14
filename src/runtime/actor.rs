//! User actor contracts and the private typed-to-erased adapter.
//! The scheduler never downcasts domain state itself.
use crate::{ActorError, Decision, OutboxEffect, decode, encode};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    any::{Any, TypeId},
    marker::PhantomData,
    panic::{AssertUnwindSafe, catch_unwind},
};

pub(super) type Erased = Box<dyn Any + Send>;

/// Transactional virtual object. `decide` must not mutate `state` through shared interior state.
/// No `.await`: effects are durable intents committed with state (not executed inline).
pub trait DurableActor: Send + Sync + 'static {
    const KIND: &'static str;
    type State: Clone + Serialize + DeserializeOwned + Send + 'static;
    type Command: Serialize + Send + 'static;
    type Reply: Serialize + DeserializeOwned + Send + 'static;

    fn initial(key: &str) -> Self::State;
    fn decide(
        state: &Self::State,
        command: Self::Command,
    ) -> Result<Decision<Self::State, Self::Reply>, ActorError>;
}
/// In-memory virtual object. State is *not* durable; idle eviction or crashes reset it.
/// A returned Reply may itself be a domain-specific Result type.
pub trait SessionActor: Send + Sync + 'static {
    const KIND: &'static str;
    type State: Clone + Send + 'static;
    type Command: Send + 'static;
    type Reply: Send + 'static;

    fn initial(key: &str) -> Self::State;
    fn handle(state: &mut Self::State, command: Self::Command) -> Self::Reply;
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Durable,
    Session,
}

pub(super) enum Executed {
    Durable {
        next: Erased,
        reply: Erased,
        state_bytes: Vec<u8>,
        reply_bytes: Vec<u8>,
        effects: Vec<OutboxEffect>,
    },
    Session {
        reply: Erased,
    },
}

fn user_call<T>(f: impl FnOnce() -> T) -> Result<T, ActorError> {
    catch_unwind(AssertUnwindSafe(f)).map_err(|_| ActorError::HandlerPanicked)
}

pub(super) trait ErasedKind: Send + Sync {
    fn mode(&self) -> Mode;
    fn actor_type_id(&self) -> TypeId;
    fn initial(&self, key: &str) -> Result<Erased, ActorError>;
    fn decode_state(&self, bytes: &[u8]) -> Result<Erased, ActorError>;
    fn clone_state(&self, state: &Erased) -> Result<Erased, ActorError>;
    fn execute(&self, state: &mut Erased, command: Erased) -> Result<Executed, ActorError>;
    fn decode_reply(&self, bytes: &[u8]) -> Result<Erased, ActorError>;
}

pub(super) struct DurableKind<A>(pub(super) PhantomData<A>);
impl<A: DurableActor> ErasedKind for DurableKind<A> {
    fn mode(&self) -> Mode {
        Mode::Durable
    }
    fn actor_type_id(&self) -> TypeId {
        TypeId::of::<A>()
    }
    fn initial(&self, key: &str) -> Result<Erased, ActorError> {
        Ok(Box::new(user_call(|| A::initial(key))?))
    }
    fn decode_state(&self, bytes: &[u8]) -> Result<Erased, ActorError> {
        Ok(Box::new(decode::<A::State>(bytes)?))
    }
    fn clone_state(&self, state: &Erased) -> Result<Erased, ActorError> {
        let state = state
            .downcast_ref::<A::State>()
            .ok_or(ActorError::TypeMismatch)?;
        Ok(Box::new(user_call(|| state.clone())?))
    }
    fn execute(&self, state: &mut Erased, command: Erased) -> Result<Executed, ActorError> {
        let state = state
            .downcast_ref::<A::State>()
            .ok_or(ActorError::TypeMismatch)?;
        let command = *command
            .downcast::<A::Command>()
            .map_err(|_| ActorError::TypeMismatch)?;
        let Decision {
            next_state,
            reply,
            effects,
        } = user_call(|| A::decide(state, command))??;
        let state_bytes = encode(&next_state)?;
        let reply_bytes = encode(&reply)?;
        Ok(Executed::Durable {
            next: Box::new(next_state),
            reply: Box::new(reply),
            state_bytes,
            reply_bytes,
            effects,
        })
    }
    fn decode_reply(&self, bytes: &[u8]) -> Result<Erased, ActorError> {
        Ok(Box::new(decode::<A::Reply>(bytes)?))
    }
}

pub(super) struct SessionKind<A>(pub(super) PhantomData<A>);
impl<A: SessionActor> ErasedKind for SessionKind<A> {
    fn mode(&self) -> Mode {
        Mode::Session
    }
    fn actor_type_id(&self) -> TypeId {
        TypeId::of::<A>()
    }
    fn initial(&self, key: &str) -> Result<Erased, ActorError> {
        Ok(Box::new(user_call(|| A::initial(key))?))
    }
    fn decode_state(&self, _bytes: &[u8]) -> Result<Erased, ActorError> {
        Err(ActorError::TypeMismatch)
    }
    fn clone_state(&self, state: &Erased) -> Result<Erased, ActorError> {
        let state = state
            .downcast_ref::<A::State>()
            .ok_or(ActorError::TypeMismatch)?;
        Ok(Box::new(user_call(|| state.clone())?))
    }
    fn execute(&self, state: &mut Erased, command: Erased) -> Result<Executed, ActorError> {
        let state = state
            .downcast_mut::<A::State>()
            .ok_or(ActorError::TypeMismatch)?;
        let command = *command
            .downcast::<A::Command>()
            .map_err(|_| ActorError::TypeMismatch)?;
        let reply = user_call(|| A::handle(state, command))?;
        Ok(Executed::Session {
            reply: Box::new(reply),
        })
    }
    fn decode_reply(&self, _bytes: &[u8]) -> Result<Erased, ActorError> {
        Err(ActorError::TypeMismatch)
    }
}
