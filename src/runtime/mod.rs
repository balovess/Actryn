//! Embedded actor runtime: typed front-end, erasure boundary, scheduler.
mod actor;
mod api;
mod lease;
mod metrics;
mod scheduler;
mod timing;

pub use actor::{DurableActor, SessionActor};
pub use api::{ActorRuntime, ActorRuntimeBuilder, DurableRef, SessionRef};
pub use metrics::RuntimeMetrics;
pub use timing::{TimingObserver, TimingStage};
