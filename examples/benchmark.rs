//! In-process SessionActor workload (not a database or external RPC benchmark).
//! Arguments: actors commands concurrency physical_shards
//! cargo run --release --example benchmark -- 1000 20000 256 4
//! Fixed-rate mode: --open-loop saturation_ops_s duration_s actors shards [max_inflight] [--timings]
mod support;
use actryn::{ActorRuntime, SessionActor};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{
    env,
    time::{Duration, Instant},
};
use support::{
    TimingRecorder,
    fixed_rate::{rates_from_saturation, run_fixed_rate},
};

struct Counter;
#[derive(Clone)]
struct State {
    value: u64,
}
struct Increment;
impl SessionActor for Counter {
    const KIND: &'static str = "bench.counter.v1";
    type State = State;
    type Command = Increment;
    type Reply = u64;
    fn initial(_: &str) -> State {
        State { value: 0 }
    }
    fn handle(state: &mut State, _: Increment) -> u64 {
        state.value += 1;
        state.value
    }
}

fn percentile(sorted: &[Duration], percent: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((sorted.len() - 1) * percent / 100).min(sorted.len() - 1);
    sorted[rank]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let first = args.next();
    if first.as_deref() == Some("--open-loop") {
        let saturation: u64 = args.next().ok_or("missing saturation_ops_s")?.parse()?;
        let duration_secs: u64 = args.next().ok_or("missing duration_s")?.parse()?;
        let actors: usize = args.next().ok_or("missing actors")?.parse()?;
        let shards: usize = args.next().ok_or("missing shards")?.parse()?;
        let next = args.next();
        let (max_inflight, timing_already_set) = match next.as_deref() {
            None => (32_768, false),
            Some("--timings") => (32_768, true),
            Some(value) => (value.parse()?, false),
        };
        let timing_enabled = timing_already_set
            || match args.next() {
                None => false,
                Some(flag) if flag == "--timings" => true,
                Some(_) => return Err("unexpected trailing argument".into()),
            };
        if saturation < 3 || duration_secs == 0 || actors == 0 || max_inflight == 0 {
            return Err(
                "saturation must be >= 3; duration, actors, and max_inflight must be positive"
                    .into(),
            );
        }
        run_open_loop(
            saturation,
            Duration::from_secs(duration_secs),
            actors,
            shards,
            max_inflight,
            timing_enabled,
        )
        .await?;
        return Ok(());
    }

    let actors: usize = first.unwrap_or_else(|| "1000".into()).parse()?;
    let commands: usize = args.next().unwrap_or_else(|| "20000".into()).parse()?;
    let concurrency: usize = args.next().unwrap_or_else(|| "256".into()).parse()?;
    let shards: usize = args.next().unwrap_or_else(|| "4".into()).parse()?;
    if actors == 0
        || commands == 0
        || concurrency == 0
        || shards == 0
        || shards > actryn::LOGICAL_SHARDS
        || !actryn::LOGICAL_SHARDS.is_multiple_of(shards)
    {
        return Err(
            "actors, commands, and concurrency must be positive; shards must divide LOGICAL_SHARDS"
                .into(),
        );
    }
    let rt = ActorRuntime::builder()
        .shards(shards)
        .activation_limit(actors)
        .register_session::<Counter>()?
        .build()?;
    let mut actor_load = vec![0; shards];
    let refs: Vec<_> = (0..actors)
        .map(|i| {
            let key = format!("actor:{i}");
            let shard = actryn::storage::shard_for(Counter::KIND, &key) % shards;
            actor_load[shard] += 1;
            rt.session::<Counter>(key)
        })
        .collect();
    // Warm-up is excluded; this benchmark measures in-memory turn round trips.
    for actor in &refs {
        actor.command(Increment).await?;
    }
    let start = Instant::now();
    let mut work = FuturesUnordered::new();
    let mut latency = Vec::with_capacity(commands);
    for i in 0..commands {
        let actor = refs[i % actors].clone();
        work.push(async move {
            let began = Instant::now();
            actor.command(Increment).await.map(|_| began.elapsed())
        });
        if work.len() >= concurrency {
            latency.push(work.next().await.expect("outstanding operation")?);
        }
    }
    while let Some(result) = work.next().await {
        latency.push(result?);
    }
    let elapsed = start.elapsed();
    latency.sort_unstable();
    let metrics = rt.metrics();
    let mean_latency_secs =
        latency.iter().map(Duration::as_secs_f64).sum::<f64>() / latency.len() as f64;
    let operations_per_second = commands as f64 / elapsed.as_secs_f64();
    let estimated_inflight = mean_latency_secs * operations_per_second;
    println!(
        "actors={actors} commands={commands} concurrency={concurrency} shards={shards} duration={elapsed:?}"
    );
    println!(
        "ops/s={operations_per_second:.0} mean={:?} p50={:?} p95={:?} p99={:?} little_law_inflight~{estimated_inflight:.1}",
        Duration::from_secs_f64(mean_latency_secs),
        percentile(&latency, 50),
        percentile(&latency, 95),
        percentile(&latency, 99)
    );
    println!(
        "live_activations={} created={} evicted={} handled={}",
        metrics.live_activations,
        metrics.activations_created,
        metrics.activations_evicted,
        metrics.commands_executed
    );
    println!(
        "actor_load_min={} actor_load_max={}",
        actor_load.iter().min().copied().unwrap_or_default(),
        actor_load.iter().max().copied().unwrap_or_default()
    );
    rt.shutdown().await?;
    Ok(())
}

async fn run_open_loop(
    saturation: u64,
    duration: Duration,
    actors: usize,
    shards: usize,
    max_inflight: usize,
    timing_enabled: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if shards == 0
        || shards > actryn::LOGICAL_SHARDS
        || !actryn::LOGICAL_SHARDS.is_multiple_of(shards)
    {
        return Err("shards must divide LOGICAL_SHARDS".into());
    }
    let recorder = timing_enabled.then(|| std::sync::Arc::new(TimingRecorder::default()));
    let mut builder = ActorRuntime::builder()
        .shards(shards)
        .activation_limit(actors);
    if let Some(observer) = recorder.as_ref() {
        builder = builder.timing_observer(observer.clone());
    }
    let runtime = builder.register_session::<Counter>()?.build()?;
    let refs = std::sync::Arc::new(
        (0..actors)
            .map(|i| runtime.session::<Counter>(format!("open-loop:{i}")))
            .collect::<Vec<_>>(),
    );
    for actor in refs.iter() {
        actor.command(Increment).await?;
    }
    let rates = rates_from_saturation(saturation);
    println!("precondition=1s at each offered rate; measurements follow in ascending order");
    for &(_, rate) in &rates {
        let actors = refs.clone();
        let issue = move |index: usize| {
            let actor = actors[index % actors.len()].clone();
            async move { actor.command(Increment).await.map(|_| ()) }
        };
        let _ = run_fixed_rate(rate, Duration::from_secs(1), max_inflight, issue).await;
    }
    for (percent, rate) in rates {
        if let Some(recorder) = recorder.as_ref() {
            recorder.reset();
        }
        let actors = refs.clone();
        let issue = move |index: usize| {
            let actor = actors[index % actors.len()].clone();
            async move { actor.command(Increment).await.map(|_| ()) }
        };
        let report = run_fixed_rate(rate, duration, max_inflight, issue).await;
        report.print(&format!("{percent}%"));
        if let Some(recorder) = recorder.as_ref() {
            recorder.print();
        }
    }
    runtime.shutdown().await?;
    Ok(())
}
