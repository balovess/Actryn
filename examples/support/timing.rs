use actryn::{TimingObserver, TimingStage};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const STAGE_COUNT: usize = 13;
const BUCKETS: usize = 64 * 8;

struct Histogram {
    count: AtomicU64,
    total_ns: AtomicU64,
    max_ns: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

pub struct TimingRecorder {
    stages: [Histogram; STAGE_COUNT],
}

impl Default for TimingRecorder {
    fn default() -> Self {
        Self {
            stages: std::array::from_fn(|_| Histogram::default()),
        }
    }
}

impl TimingObserver for TimingRecorder {
    fn observe(&self, stage: TimingStage, elapsed: Duration) {
        let Some(index) = stage_index(stage) else {
            return;
        };
        let nanos = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        let histogram = &self.stages[index];
        histogram.count.fetch_add(1, Ordering::Relaxed);
        histogram.total_ns.fetch_add(nanos, Ordering::Relaxed);
        histogram.max_ns.fetch_max(nanos, Ordering::Relaxed);
        histogram.buckets[bucket_index(nanos)].fetch_add(1, Ordering::Relaxed);
    }
}

impl TimingRecorder {
    pub fn reset(&self) {
        for histogram in &self.stages {
            histogram.count.store(0, Ordering::Relaxed);
            histogram.total_ns.store(0, Ordering::Relaxed);
            histogram.max_ns.store(0, Ordering::Relaxed);
            for bucket in &histogram.buckets {
                bucket.store(0, Ordering::Relaxed);
            }
        }
    }

    pub fn print(&self) {
        for stage in [
            TimingStage::SchedulerQueue,
            TimingStage::IngressQueue,
            TimingStage::SchedulerCompletion,
            TimingStage::ClientResponse,
            TimingStage::ActorExecution,
            TimingStage::Serialization,
            TimingStage::StoragePoolAcquire,
            TimingStage::StorageTransactionBegin,
            TimingStage::StorageFence,
            TimingStage::StorageReceiptLookup,
            TimingStage::StorageWrites,
            TimingStage::StorageTransactionCommit,
            TimingStage::StorageCommitTotal,
        ] {
            let Some(index) = stage_index(stage) else {
                continue;
            };
            let histogram = &self.stages[index];
            let count = histogram.count.load(Ordering::Relaxed);
            if count == 0 {
                continue;
            }
            let total = histogram.total_ns.load(Ordering::Relaxed);
            let maximum = histogram.max_ns.load(Ordering::Relaxed);
            println!(
                "timing_stage={stage:?} samples={count} avg={:?} p50~={:?} p95~={:?} p99~={:?} max={:?}",
                Duration::from_nanos(total / count),
                self.percentile(index, count, 50),
                self.percentile(index, count, 95),
                self.percentile(index, count, 99),
                Duration::from_nanos(maximum),
            );
        }
        println!("timing_percentiles=approximate_logarithmic_buckets");
    }

    fn percentile(&self, index: usize, count: u64, percentile: u64) -> Duration {
        let target = count.saturating_mul(percentile).div_ceil(100).max(1);
        let mut seen = 0u64;
        for (bucket, observations) in self.stages[index].buckets.iter().enumerate() {
            seen += observations.load(Ordering::Relaxed);
            if seen >= target {
                return Duration::from_nanos(bucket_upper_bound(bucket));
            }
        }
        Duration::ZERO
    }
}

fn stage_index(stage: TimingStage) -> Option<usize> {
    Some(match stage {
        TimingStage::SchedulerQueue => 0,
        TimingStage::IngressQueue => 1,
        TimingStage::SchedulerCompletion => 2,
        TimingStage::ClientResponse => 3,
        TimingStage::ActorExecution => 4,
        TimingStage::Serialization => 5,
        TimingStage::StoragePoolAcquire => 6,
        TimingStage::StorageTransactionBegin => 7,
        TimingStage::StorageFence => 8,
        TimingStage::StorageReceiptLookup => 9,
        TimingStage::StorageWrites => 10,
        TimingStage::StorageTransactionCommit => 11,
        TimingStage::StorageCommitTotal => 12,
        _ => return None,
    })
}

fn bucket_index(nanos: u64) -> usize {
    let nanos = nanos.max(1);
    let exponent = (u64::BITS - nanos.leading_zeros() - 1) as usize;
    let base = 1u64 << exponent;
    let subdivision = ((u128::from(nanos - base) * 8) / u128::from(base)) as usize;
    exponent * 8 + subdivision.min(7)
}

fn bucket_upper_bound(bucket: usize) -> u64 {
    let exponent = bucket / 8;
    let subdivision = bucket % 8 + 1;
    let base = 1u128 << exponent;
    let upper = base + (base * subdivision as u128).div_ceil(8) - 1;
    upper.min(u64::MAX as u128) as u64
}
