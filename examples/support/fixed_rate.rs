use actryn::{ActorError, StorageError};
use futures::Future;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{runtime::Handle, sync::mpsc};

const HISTOGRAM_BUCKETS: usize = 64 * 8;

pub struct FixedRateReport {
    target_rate: u64,
    offered: usize,
    admitted: usize,
    rejected_at_capacity: usize,
    completed: usize,
    failed: usize,
    peak_inflight: usize,
    duration: Duration,
    scheduled_to_response: LatencyHistogram,
    driver_lag: LatencyHistogram,
    failure_kinds: BTreeMap<String, usize>,
}

impl FixedRateReport {
    pub fn print(&self, label: &str) {
        println!(
            "load={label} target_ops_s={} offered={} admitted={} capacity_rejected={} completed={} failed={} peak_inflight={} duration={:?} completed_ops_s={:.0}",
            self.target_rate,
            self.offered,
            self.admitted,
            self.rejected_at_capacity,
            self.completed,
            self.failed,
            self.peak_inflight,
            self.duration,
            self.completed as f64 / self.duration.as_secs_f64()
        );
        println!(
            "arrival_generator=dedicated-thread; scheduled_to_response p50~={:?} p95~={:?} p99~={:?}; driver_lag p50~={:?} p95~={:?} p99~={:?}; percentiles=approximate_logarithmic_buckets",
            self.scheduled_to_response.percentile(50),
            self.scheduled_to_response.percentile(95),
            self.scheduled_to_response.percentile(99),
            self.driver_lag.percentile(50),
            self.driver_lag.percentile(95),
            self.driver_lag.percentile(99),
        );
        if !self.failure_kinds.is_empty() {
            let summary = self
                .failure_kinds
                .iter()
                .map(|(kind, count)| format!("{kind}:{count}"))
                .collect::<Vec<_>>()
                .join(",");
            println!("failure_kinds={summary}");
        }
    }
}

#[derive(Clone)]
struct LatencyHistogram {
    counts: [u64; HISTOGRAM_BUCKETS],
    total: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            counts: [0; HISTOGRAM_BUCKETS],
            total: 0,
        }
    }
}

impl LatencyHistogram {
    fn record(&mut self, elapsed: Duration) {
        self.counts[bucket_index(elapsed)] += 1;
        self.total += 1;
    }

    fn percentile(&self, percentile: u64) -> Duration {
        let target = self.total.saturating_mul(percentile).div_ceil(100).max(1);
        let mut seen = 0u64;
        for (bucket, count) in self.counts.iter().enumerate() {
            seen += count;
            if seen >= target {
                return Duration::from_nanos(bucket_upper_bound(bucket));
            }
        }
        Duration::ZERO
    }
}

struct CompletionSample {
    failure_kind: Option<String>,
    scheduled_to_response: Duration,
}

struct ProducerSummary {
    admitted: usize,
    rejected_at_capacity: usize,
    peak_inflight: usize,
    driver_lag: LatencyHistogram,
}

/// Issue requests at absolute planned times from a dedicated producer thread.
/// Outstanding work is bounded, and latency starts at the planned arrival time so
/// scheduler delay and late dispatch are not hidden by coordinated omission.
pub async fn run_fixed_rate<F, Fut>(
    target_rate: u64,
    duration: Duration,
    max_inflight: usize,
    issue: F,
) -> FixedRateReport
where
    F: Fn(usize) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<(), ActorError>> + Send + 'static,
{
    assert!(target_rate > 0 && !duration.is_zero() && max_inflight > 0);
    let offered = (u128::from(target_rate) * duration.as_nanos() / 1_000_000_000)
        .min(usize::MAX as u128) as usize;
    let outstanding = Arc::new(AtomicUsize::new(0));
    let (completed_tx, mut completed_rx) = mpsc::channel(max_inflight);
    let handle = Handle::current();
    let producer_outstanding = outstanding.clone();

    let producer = tokio::task::spawn_blocking(move || {
        std::thread::sleep(Duration::from_millis(250));
        let planned_start = Instant::now() + Duration::from_millis(5);
        let measurement_end = planned_start + duration;
        let mut summary = ProducerSummary {
            admitted: 0,
            rejected_at_capacity: 0,
            peak_inflight: 0,
            driver_lag: LatencyHistogram::default(),
        };

        for index in 0..offered {
            let offset_nanos = index as u128 * 1_000_000_000 / u128::from(target_rate);
            let scheduled = planned_start + Duration::from_nanos(offset_nanos as u64);
            wait_until(scheduled, target_rate);
            summary
                .driver_lag
                .record(Instant::now().saturating_duration_since(scheduled));

            let Some(inflight) = try_reserve(&producer_outstanding, max_inflight) else {
                summary.rejected_at_capacity += 1;
                continue;
            };
            summary.admitted += 1;
            summary.peak_inflight = summary.peak_inflight.max(inflight);
            let issue = issue.clone();
            let completed_tx = completed_tx.clone();
            let outstanding = producer_outstanding.clone();
            handle.spawn(async move {
                let failure_kind = issue(index).await.err().map(error_kind);
                let sample = CompletionSample {
                    failure_kind,
                    scheduled_to_response: Instant::now().saturating_duration_since(scheduled),
                };
                let _ = completed_tx.send(sample).await;
                outstanding.fetch_sub(1, Ordering::Release);
            });
        }
        wait_until(measurement_end, target_rate);
        summary
    });

    let mut report = FixedRateReport {
        target_rate,
        offered,
        admitted: 0,
        rejected_at_capacity: 0,
        completed: 0,
        failed: 0,
        peak_inflight: 0,
        duration,
        scheduled_to_response: LatencyHistogram::default(),
        driver_lag: LatencyHistogram::default(),
        failure_kinds: BTreeMap::new(),
    };
    while let Some(sample) = completed_rx.recv().await {
        report
            .scheduled_to_response
            .record(sample.scheduled_to_response);
        if let Some(kind) = sample.failure_kind {
            report.failed += 1;
            *report.failure_kinds.entry(kind).or_default() += 1;
        } else {
            report.completed += 1;
        }
    }
    let producer = producer
        .await
        .expect("fixed-rate arrival producer panicked");
    report.admitted = producer.admitted;
    report.rejected_at_capacity = producer.rejected_at_capacity;
    report.peak_inflight = producer.peak_inflight;
    report.driver_lag = producer.driver_lag;
    report
}

fn error_kind(error: ActorError) -> String {
    match error {
        ActorError::Rejected(_) => "rejected".into(),
        ActorError::Storage(detail) => storage_error_kind(&detail),
        ActorError::UnknownOutcome => "unknown_outcome".into(),
        ActorError::Codec(_) => "codec".into(),
        ActorError::RequestIdConflict => "request_id_conflict".into(),
        ActorError::VersionConflict => "version_conflict".into(),
        ActorError::OwnershipLost => "ownership_lost".into(),
        ActorError::MailboxFull => "mailbox_full".into(),
        ActorError::CapacityExceeded => "capacity_exceeded".into(),
        ActorError::ShuttingDown => "shutting_down".into(),
        ActorError::InvalidKey => "invalid_key".into(),
        ActorError::UnregisteredKind => "unregistered_kind".into(),
        ActorError::DuplicateKind => "duplicate_kind".into(),
        ActorError::MissingStorage => "missing_storage".into(),
        ActorError::InvalidConfiguration => "invalid_configuration".into(),
        ActorError::TypeMismatch => "type_mismatch".into(),
        ActorError::HandlerPanicked => "handler_panicked".into(),
        ActorError::NoOwner => "no_owner".into(),
        ActorError::ClusterProtocol(_) => "cluster_protocol".into(),
    }
}

fn storage_error_kind(detail: &StorageError) -> String {
    match &detail.code {
        Some(code) => format!("storage_{}_{}", detail.provider, code),
        None => format!("storage_{}", detail.provider),
    }
}

fn try_reserve(outstanding: &AtomicUsize, limit: usize) -> Option<usize> {
    let mut current = outstanding.load(Ordering::Acquire);
    loop {
        if current >= limit {
            return None;
        }
        match outstanding.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Some(current + 1),
            Err(actual) => current = actual,
        }
    }
}

fn wait_until(deadline: Instant, target_rate: u64) {
    if target_rate >= 1_000 {
        while Instant::now() < deadline {
            std::hint::spin_loop();
        }
    } else {
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining > Duration::from_millis(2) {
                std::thread::sleep(remaining - Duration::from_millis(1));
            } else {
                std::hint::spin_loop();
            }
        }
    }
}

fn bucket_index(elapsed: Duration) -> usize {
    let nanos = elapsed.as_nanos().max(1).min(u64::MAX as u128) as u64;
    let exponent = (u64::BITS - nanos.leading_zeros() - 1) as usize;
    let base = 1u64 << exponent;
    let offset = nanos - base;
    let subdivision = if exponent >= 3 {
        (offset >> (exponent - 3)) as usize
    } else {
        ((offset << 3) / base) as usize
    };
    exponent * 8 + subdivision.min(7)
}

fn bucket_upper_bound(bucket: usize) -> u64 {
    let exponent = bucket / 8;
    let subdivision = bucket % 8 + 1;
    let base = 1u128 << exponent;
    let upper = base + (base * subdivision as u128).div_ceil(8) - 1;
    upper.min(u64::MAX as u128) as u64
}

pub fn rates_from_saturation(saturation_ops_per_second: u64) -> [(u64, u64); 4] {
    [40u64, 60, 80, 95].map(|percent| {
        (
            percent,
            (u128::from(saturation_ops_per_second) * u128::from(percent) / 100) as u64,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{LatencyHistogram, bucket_index, bucket_upper_bound};
    use std::time::Duration;

    #[test]
    fn histogram_reports_upper_bucket_percentiles() {
        let mut histogram = LatencyHistogram::default();
        histogram.record(Duration::from_nanos(2));
        histogram.record(Duration::from_nanos(4));
        histogram.record(Duration::from_nanos(8));

        assert_eq!(histogram.percentile(50), Duration::from_nanos(4));
        assert_eq!(histogram.percentile(95), Duration::from_nanos(8));
        assert_eq!(LatencyHistogram::default().percentile(99), Duration::ZERO);
    }

    #[test]
    fn histogram_bucketing_handles_small_and_large_durations() {
        assert_eq!(bucket_index(Duration::ZERO), 0);
        assert_eq!(bucket_index(Duration::from_nanos(2)), 8);
        assert_eq!(bucket_index(Duration::from_nanos(3)), 12);
        assert_eq!(bucket_index(Duration::from_nanos(4)), 16);
        assert_eq!(bucket_index(Duration::from_nanos(8)), 24);

        let largest = Duration::from_nanos(u64::MAX);
        assert_eq!(bucket_index(largest), 64 * 8 - 1);
        assert_eq!(bucket_upper_bound(64 * 8 - 1), u64::MAX);
    }
}
