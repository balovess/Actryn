//! Linux RSS probe for activated SessionActors. Allocator and page-cache effects
//! make this an engineering estimate, not a byte-accurate object measurement.
//! cargo run --release --example memory_density -- 50000
use actryn::{ActorRuntime, SessionActor};
use std::{env, time::Duration};

struct EmptyActor;
#[derive(Clone)]
struct State(u64);
impl SessionActor for EmptyActor {
    const KIND: &'static str = "bench.empty.v1";
    type State = State;
    type Command = ();
    type Reply = ();
    fn initial(_: &str) -> State {
        State(0)
    }
    fn handle(state: &mut State, _: ()) {
        state.0 += 1;
    }
}

#[cfg(target_os = "linux")]
fn resident_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    kb.checked_mul(1024)
}

#[cfg(not(target_os = "linux"))]
fn resident_bytes() -> Option<u64> {
    None
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let actors: usize = env::args()
        .nth(1)
        .unwrap_or_else(|| "50000".into())
        .parse()?;
    assert!(actors > 0);
    let runtime = ActorRuntime::builder()
        .shards(4)
        .activation_limit(actors)
        .idle_timeout(Duration::from_secs(5))
        .eviction_interval(Duration::from_millis(50))
        .register_session::<EmptyActor>()?
        .build()?;
    let baseline = resident_bytes();
    for i in 0..actors {
        runtime
            .session::<EmptyActor>(format!("session:{i}"))
            .command(())
            .await?;
    }
    let full = resident_bytes();
    let active = runtime.metrics().live_activations;
    println!("resident_activations={active} rss_before={baseline:?} rss_after={full:?}");
    if active != actors {
        eprintln!(
            "warning: some actors evicted during warm-up; increase idle_timeout for density measurement"
        );
    }
    if let (Some(before), Some(after)) = (baseline, full)
        && active == actors
    {
        println!(
            "rss_delta_per_resident_actor_bytes={:.1}",
            after.saturating_sub(before) as f64 / active as f64
        );
    }
    tokio::time::sleep(Duration::from_secs(6)).await;
    println!(
        "after_idle_activations={} rss={:?}",
        runtime.metrics().live_activations,
        resident_bytes()
    );
    runtime.shutdown().await?;
    Ok(())
}
