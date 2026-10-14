//! Durable actor workload for measuring adapter commit throughput and latency.
//!
//! Arguments: provider actors commands concurrency physical_shards pool_size state_bytes effects effect_bytes
//! SQLite: cargo run --release --example durable_benchmark -- sqlite 100 10000 64 4 64 1024 0 0
//! Fixed-rate: --open-loop provider saturation_ops_s duration_s actors shards pool_size state_bytes effects effect_bytes [max_inflight] [--timings]
//! Add `--timings` after the arguments to collect approximate phase distributions.
//! PostgreSQL: enable the `postgres` feature and set ACTOR_BENCH_DATABASE_URL to a dedicated
//! disposable database before selecting `postgres` as the provider.
use actryn::{
    ActorError, ActorRuntime, Decision, DurableActor, LOGICAL_SHARDS, OutboxEffect, SqliteStorage,
};
use futures::{StreamExt, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use std::{
    env,
    error::Error,
    io::{Error as IoError, ErrorKind},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
mod support;
use support::{
    TimingRecorder,
    fixed_rate::{rates_from_saturation, run_fixed_rate},
};

struct Counter;

#[derive(Clone, Serialize, Deserialize)]
struct State {
    value: u64,
    payload: Vec<u8>,
}

#[derive(Serialize)]
struct Increment {
    state_bytes: usize,
    effects: usize,
    effect_bytes: usize,
}

struct BenchmarkConfig {
    provider: String,
    run_id: String,
    actor_keys: Vec<String>,
    logical_shards: Vec<usize>,
    commands: usize,
    concurrency: usize,
    physical_shards: usize,
    pool_size: usize,
    state_bytes: usize,
    effects: usize,
    effect_bytes: usize,
    timing_observer: Option<std::sync::Arc<TimingRecorder>>,
    open_loop: Option<OpenLoopConfig>,
}

struct OpenLoopConfig {
    saturation_ops_per_second: u64,
    duration: Duration,
    max_inflight: usize,
}

impl DurableActor for Counter {
    const KIND: &'static str = "bench.durable-counter.v1";
    type State = State;
    type Command = Increment;
    type Reply = u64;

    fn initial(_: &str) -> Self::State {
        State {
            value: 0,
            payload: Vec::new(),
        }
    }

    fn decide(state: &State, command: Increment) -> Result<Decision<State, u64>, ActorError> {
        let value = state
            .value
            .checked_add(1)
            .ok_or_else(|| ActorError::Rejected("counter overflow".into()))?;
        let payload = if state.payload.len() == command.state_bytes {
            state.payload.clone()
        } else {
            vec![0; command.state_bytes]
        };
        let mut decision = Decision::new(State { value, payload }, value);
        for ordinal in 0..command.effects {
            decision = decision.with_effect(OutboxEffect::new(
                "bench.destination.v1",
                format!("{}:{value}:{ordinal}", state.value),
                vec![0; command.effect_bytes],
            ));
        }
        Ok(decision)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let first = args.next();
    let open_loop_mode = first.as_deref() == Some("--open-loop");
    let provider = if open_loop_mode {
        args.next().unwrap_or_else(|| "sqlite".into())
    } else {
        first.unwrap_or_else(|| "sqlite".into())
    };
    let mut timing_flag_from_open_loop = false;
    let (
        actors,
        commands,
        concurrency,
        physical_shards,
        pool_size,
        state_bytes,
        effects,
        effect_bytes,
        open_loop,
    ) = if open_loop_mode {
        let saturation_ops_per_second: u64 = args
            .next()
            .ok_or("missing saturation_ops_per_second")?
            .parse()?;
        let duration_secs: u64 = args.next().ok_or("missing duration_secs")?.parse()?;
        let actors: usize = args.next().ok_or("missing actors")?.parse()?;
        let physical_shards: usize = args.next().ok_or("missing physical_shards")?.parse()?;
        let pool_size: usize = args.next().ok_or("missing pool_size")?.parse()?;
        let state_bytes: usize = args.next().unwrap_or_else(|| "0".into()).parse()?;
        let effects: usize = args.next().unwrap_or_else(|| "0".into()).parse()?;
        let effect_bytes: usize = args.next().unwrap_or_else(|| "0".into()).parse()?;
        let max_inflight: usize = match args.next().as_deref() {
            None => 32_768,
            Some("--timings") => {
                timing_flag_from_open_loop = true;
                32_768
            }
            Some(value) => value.parse()?,
        };
        if saturation_ops_per_second < 3 || duration_secs == 0 || max_inflight == 0 {
            return Err(
                "saturation must be >= 3; duration and max_inflight must be positive".into(),
            );
        }
        (
            actors,
            1,
            max_inflight,
            physical_shards,
            pool_size,
            state_bytes,
            effects,
            effect_bytes,
            Some(OpenLoopConfig {
                saturation_ops_per_second,
                duration: Duration::from_secs(duration_secs),
                max_inflight,
            }),
        )
    } else {
        let actors: usize = args.next().unwrap_or_else(|| "100".into()).parse()?;
        let commands: usize = args.next().unwrap_or_else(|| "10000".into()).parse()?;
        let concurrency: usize = args.next().unwrap_or_else(|| "64".into()).parse()?;
        let physical_shards: usize = args.next().unwrap_or_else(|| "4".into()).parse()?;
        let pool_size: usize = args
            .next()
            .unwrap_or_else(|| concurrency.min(64).to_string())
            .parse()?;
        let state_bytes: usize = args.next().unwrap_or_else(|| "0".into()).parse()?;
        let effects: usize = args.next().unwrap_or_else(|| "0".into()).parse()?;
        let effect_bytes: usize = args.next().unwrap_or_else(|| "0".into()).parse()?;
        (
            actors,
            commands,
            concurrency,
            physical_shards,
            pool_size,
            state_bytes,
            effects,
            effect_bytes,
            None,
        )
    };
    let is_open_loop = open_loop.is_some();
    let timing_enabled = timing_flag_from_open_loop
        || match args.next() {
            None => false,
            Some(flag) if flag == "--timings" => true,
            Some(_) => {
                return Err(
                    IoError::new(ErrorKind::InvalidInput, "unexpected trailing argument").into(),
                );
            }
        };
    let timing_observer = timing_enabled.then(|| std::sync::Arc::new(TimingRecorder::default()));
    if actors == 0 || commands == 0 || concurrency == 0 || physical_shards == 0 || pool_size == 0 {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "actors, commands, concurrency, physical_shards, and pool_size must be positive",
        )
        .into());
    }

    let run_id = format!(
        "{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let actor_keys = (0..actors)
        .map(|index| format!("durable-bench-{run_id}:{index}"))
        .collect::<Vec<_>>();
    let mut used_shards = vec![false; LOGICAL_SHARDS];
    for key in &actor_keys {
        used_shards[actryn::storage::shard_for(Counter::KIND, key)] = true;
    }
    let logical_shards = used_shards
        .into_iter()
        .enumerate()
        .filter_map(|(shard, used)| used.then_some(shard))
        .collect::<Vec<_>>();

    match provider.as_str() {
        "sqlite" => {
            let temp_dir = tempfile::tempdir()?;
            let storage = SqliteStorage::open(temp_dir.path().join("actryn-durable-bench.sqlite"))?;
            run(
                storage,
                BenchmarkConfig {
                    provider: provider.clone(),
                    run_id,
                    actor_keys,
                    logical_shards,
                    commands,
                    concurrency,
                    physical_shards,
                    pool_size,
                    state_bytes,
                    effects,
                    effect_bytes,
                    timing_observer: timing_observer.clone(),
                    open_loop,
                },
            )
            .await?;
        }
        "postgres" => {
            #[cfg(feature = "postgres")]
            {
                let url = env::var("ACTOR_BENCH_DATABASE_URL").map_err(|_| {
                    IoError::new(
                        ErrorKind::NotFound,
                        "ACTOR_BENCH_DATABASE_URL must target a dedicated disposable database",
                    )
                })?;
                let config = url.parse::<tokio_postgres::Config>()?;
                let manager = deadpool_postgres::Manager::new(config, tokio_postgres::NoTls);
                let pool = deadpool_postgres::Pool::builder(manager)
                    .max_size(pool_size)
                    .build()?;
                let mut storage = actryn::PostgresStorage::new(pool);
                if let Some(observer) = timing_observer.as_ref() {
                    storage = storage.with_timing_observer(observer.clone());
                }
                storage.initialize_schema().await?;
                run(
                    storage,
                    BenchmarkConfig {
                        provider: provider.clone(),
                        run_id,
                        actor_keys,
                        logical_shards,
                        commands,
                        concurrency,
                        physical_shards,
                        pool_size,
                        state_bytes,
                        effects,
                        effect_bytes,
                        timing_observer: timing_observer.clone(),
                        open_loop,
                    },
                )
                .await?;
            }
            #[cfg(not(feature = "postgres"))]
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "PostgreSQL support is optional; enable the `postgres` feature",
            )
            .into());
        }
        _ => {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "provider must be `sqlite` or `postgres`",
            )
            .into());
        }
    }
    if !is_open_loop && let Some(observer) = timing_observer {
        observer.print();
    }
    Ok(())
}

async fn run<S: actryn::StorageAdapter>(
    storage: S,
    config: BenchmarkConfig,
) -> Result<(), Box<dyn Error>> {
    let BenchmarkConfig {
        provider,
        run_id,
        actor_keys,
        logical_shards,
        commands,
        concurrency,
        physical_shards,
        pool_size,
        state_bytes,
        effects,
        effect_bytes,
        timing_observer,
        open_loop,
    } = config;
    let mut builder = ActorRuntime::builder()
        .storage(storage)
        .node_id(format!("durable-bench-{run_id}"))
        .claim_shards(
            "durable-benchmark",
            logical_shards.clone(),
            Duration::from_secs(120),
        )
        .shards(physical_shards)
        .activation_limit(actor_keys.len());
    if let Some(observer) = timing_observer.as_ref() {
        builder = builder.timing_observer(observer.clone());
    }
    let runtime = builder.register_durable::<Counter>()?.build_async().await?;
    let refs = actor_keys
        .iter()
        .map(|key| runtime.durable::<Counter>(key.clone()))
        .collect::<Vec<_>>();
    let actor_distribution = if refs.len() == 1 {
        "single-hot-actor"
    } else {
        "round-robin"
    };

    let benchmark = async {
        for (index, actor) in refs.iter().enumerate() {
            actor
                .command(
                    format!("warm-{run_id}-{index}"),
                    Increment {
                        state_bytes,
                        effects,
                        effect_bytes,
                    },
                )
                .await?;
        }

        if let Some(load) = open_loop {
            println!(
                "provider={provider} workload=fixed-rate actors={} distribution={actor_distribution} physical_shards={physical_shards} logical_shards={} pool_size={pool_size} state_bytes={state_bytes} effects_per_command={effects} effect_bytes={effect_bytes} saturation_ops_s={} duration={:?}",
                refs.len(),
                logical_shards.len(),
                load.saturation_ops_per_second,
                load.duration
            );
            let actors = std::sync::Arc::new(refs.clone());
            let rates = rates_from_saturation(load.saturation_ops_per_second);
            println!("precondition=1s at each offered rate; measurements follow in ascending order");
            for &(percent, rate) in &rates {
                let actors = actors.clone();
                let phase_id = format!("warm-{run_id}-{percent}");
                let issue = move |index: usize| {
                    let actor = actors[index % actors.len()].clone();
                    let request_id = format!("load-{phase_id}-{index}");
                    async move {
                        actor
                            .command(
                                request_id,
                                Increment {
                                    state_bytes,
                                    effects,
                                    effect_bytes,
                                },
                            )
                            .await
                            .map(|_| ())
                    }
                };
                let _ = run_fixed_rate(
                    rate,
                    Duration::from_secs(1),
                    load.max_inflight,
                    issue,
                )
                .await;
            }
            for (percent, rate) in rates {
                if let Some(observer) = timing_observer.as_ref() {
                    observer.reset();
                }
                let actors = actors.clone();
                let phase_id = format!("measure-{run_id}-{percent}");
                let issue = move |index: usize| {
                    let actor = actors[index % actors.len()].clone();
                    let request_id = format!("load-{phase_id}-{index}");
                    async move {
                        actor
                            .command(
                                request_id,
                                Increment {
                                    state_bytes,
                                    effects,
                                    effect_bytes,
                                },
                            )
                            .await
                            .map(|_| ())
                    }
                };
                let report = run_fixed_rate(rate, load.duration, load.max_inflight, issue).await;
                report.print(&format!("{percent}%"));
                if let Some(observer) = timing_observer.as_ref() {
                    observer.print();
                }
            }
        } else {
            let start = Instant::now();
            let mut work = FuturesUnordered::new();
            let mut latency = Vec::with_capacity(commands);
            for index in 0..commands {
                let actor = refs[index % refs.len()].clone();
                let request_id = format!("commit-{run_id}-{index}");
                work.push(async move {
                    let began = Instant::now();
                    actor
                        .command(
                            request_id,
                            Increment {
                                state_bytes,
                                effects,
                                effect_bytes,
                            },
                        )
                        .await
                        .map(|_| began.elapsed())
                });
                if work.len() >= concurrency {
                    latency.push(work.next().await.expect("non-empty work queue")?);
                }
            }
            while let Some(result) = work.next().await {
                latency.push(result?);
            }
            let elapsed = start.elapsed();
            latency.sort_unstable();
            let metrics = runtime.metrics();
            println!(
                "provider={provider} actors={} distribution={actor_distribution} commands={commands} concurrency={concurrency} physical_shards={physical_shards} logical_shards={} pool_size={pool_size} state_bytes={state_bytes} effects_per_command={effects} effect_bytes={effect_bytes} duration={elapsed:?}",
                refs.len(),
                logical_shards.len()
            );
            println!(
                "commits/s={:.0} p50={:?} p95={:?} p99={:?}",
                commands as f64 / elapsed.as_secs_f64(),
                percentile(&latency, 50),
                percentile(&latency, 95),
                percentile(&latency, 99)
            );
            println!(
                "live_activations={} created={} evicted={} handled_including_warmup={}",
                metrics.live_activations,
                metrics.activations_created,
                metrics.activations_evicted,
                metrics.commands_executed
            );
        }
        Ok::<(), Box<dyn Error>>(())
    }
    .await;

    let shutdown = runtime.shutdown().await;
    benchmark?;
    shutdown?;
    Ok(())
}

fn percentile(sorted: &[Duration], percent: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((sorted.len() - 1) * percent / 100).min(sorted.len() - 1);
    sorted[rank]
}
