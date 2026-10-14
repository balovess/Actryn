//! Embedded actor runtime: typed front-end, erasure boundary, scheduler.
mod actor;
mod api;
mod lease;
mod metrics;
mod scheduler;

pub use actor::{DurableActor, SessionActor};
pub use api::{ActorRuntime, ActorRuntimeBuilder, DurableRef, SessionRef};
pub use metrics::RuntimeMetrics;
