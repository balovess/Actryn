//! Reproducible, in-process SessionActor workload (not an external RPC benchmark).
//! cargo run --release --example benchmark -- 1000 20000 256
use actryn::{ActorRuntime, SessionActor};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{
    env,
    time::{Duration, Instant},
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
    let actors: usize = args.next().unwrap_or_else(|| "1000".into()).parse()?;
    let commands: usize = args.next().unwrap_or_else(|| "20000".into()).parse()?;
    let concurrency: usize = args.next().unwrap_or_else(|| "256".into()).parse()?;
    assert!(actors > 0 && concurrency > 0);
    let rt = ActorRuntime::builder()
        .shards(4)
        .activation_limit(actors.max(1))
        .register_session::<Counter>()?
        .build()?;
    let refs: Vec<_> = (0..actors)
        .map(|i| rt.session::<Counter>(format!("actor:{i}")))
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
    println!("actors={actors} commands={commands} concurrency={concurrency} duration={elapsed:?}");
    println!(
        "ops/s={:.0} p50={:?} p95={:?} p99={:?}",
        commands as f64 / elapsed.as_secs_f64(),
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
    rt.shutdown().await?;
    Ok(())
}
