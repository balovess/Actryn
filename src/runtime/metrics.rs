//! Cheap, per-shard telemetry with no global metrics backend dependency.
//! Counters are observational; they do not participate in correctness.
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Process-local runtime counters. Values are snapshots, not a transactionally
/// consistent view across shards. `live_activations` counts resident slots.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeMetrics {
    pub accepted_requests: u64,
    pub rejected_requests: u64,
    pub commands_executed: u64,
    pub durable_commits: u64,
    pub duplicate_replies: u64,
    pub handler_panics: u64,
    pub activations_created: u64,
    pub activations_evicted: u64,
    pub live_activations: usize,
}

#[derive(Default)]
pub(super) struct ShardMetrics {
    pub accepted_requests: AtomicU64,
    pub rejected_requests: AtomicU64,
    pub commands_executed: AtomicU64,
    pub durable_commits: AtomicU64,
    pub duplicate_replies: AtomicU64,
    pub handler_panics: AtomicU64,
    pub activations_created: AtomicU64,
    pub activations_evicted: AtomicU64,
    pub live_activations: AtomicUsize,
}

impl ShardMetrics {
    pub fn snapshot(&self) -> RuntimeMetrics {
        use Ordering::Relaxed;
        RuntimeMetrics {
            accepted_requests: self.accepted_requests.load(Relaxed),
            rejected_requests: self.rejected_requests.load(Relaxed),
            commands_executed: self.commands_executed.load(Relaxed),
            durable_commits: self.durable_commits.load(Relaxed),
            duplicate_replies: self.duplicate_replies.load(Relaxed),
            handler_panics: self.handler_panics.load(Relaxed),
            activations_created: self.activations_created.load(Relaxed),
            activations_evicted: self.activations_evicted.load(Relaxed),
            live_activations: self.live_activations.load(Relaxed),
        }
    }
    pub fn activated(&self) {
        self.activations_created.fetch_add(1, Ordering::Relaxed);
        self.live_activations.fetch_add(1, Ordering::Relaxed);
    }
    pub fn evicted(&self) {
        self.activations_evicted.fetch_add(1, Ordering::Relaxed);
        self.live_activations.fetch_sub(1, Ordering::Relaxed);
    }
}

impl RuntimeMetrics {
    pub(super) fn add(&mut self, part: Self) {
        self.accepted_requests += part.accepted_requests;
        self.rejected_requests += part.rejected_requests;
        self.commands_executed += part.commands_executed;
        self.durable_commits += part.durable_commits;
        self.duplicate_replies += part.duplicate_replies;
        self.handler_panics += part.handler_panics;
        self.activations_created += part.activations_created;
        self.activations_evicted += part.activations_evicted;
        self.live_activations += part.live_activations;
    }
}
