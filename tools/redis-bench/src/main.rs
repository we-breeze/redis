//! redis-bench — a load-test harness for the redis SDK.
//!
//! Runs a fixed number of operations across `--concurrency` async workers and
//! reports throughput plus latency percentiles (p50/p95/p99). It drives the
//! SDK's real RESP/multiplexing stack:
//!
//! - **mesh mode** (default, `--namespace`): through the SDK's [`Client`],
//!   measuring the full pool/multiplexing/routing/HA stack against the breeze
//!   mesh.
//! - **direct mode** (`--direct host:port`): through the harness's
//!   [`DirectClient`], a thin pool of the SDK's `MultiplexedConnection`s
//!   straight to a raw redis-server. Use this to iterate locally without a
//!   mesh, e.g. against the `redis:7` image.
//!
//! Usage:
//!   redis-bench --namespace my_ns --concurrency 64 --ops 100000 get
//!   redis-bench --direct 127.0.0.1:6379 --concurrency 64 --ops 100000 set
//!
//! # Exit code
//!
//! Non-zero if the error rate exceeds `--max-error-rate`.

mod direct;
mod driver;
mod stats;

use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use direct::DirectClient;
use driver::{Workload, WorkloadKind};
use redis::connection::ConnectionLike;
use redis::{Client, MeshConfig, Transport};
use stats::{MemoryWindow, OpBudget, Summary, WorkerStats};

// Install mimalloc (with per-request heap accounting under the `memory-stats`
// feature) as the process-wide allocator. The SDK itself stays
// allocator-agnostic; only this binary pins one.
brz_mem::install_global_allocator!();

/// Arguments for the redis load-test harness.
#[derive(Parser, Debug)]
#[command(name = "redis-bench", version, about = "Load-test the redis SDK")]
struct Args {
    /// Mesh resource namespace to connect through.
    #[arg(long, env = "BREEZE_REDIS_NS")]
    namespace: Option<String>,

    /// Connect directly to a raw `host:port` redis-server (or `unix:/path`)
    /// instead of through the mesh (bypasses sock-file discovery).
    #[arg(long)]
    direct: Option<String>,

    /// Use a unix socket for the mesh transport (ignored with --direct).
    #[arg(long)]
    unix: bool,

    /// Deployment group segment of the mesh sock-file name.
    #[arg(long, default_value = "default")]
    group: String,

    /// Directory the mesh publishes sock files into.
    #[arg(long, default_value = "/tmp/breeze/socks")]
    socket_dir: String,

    /// Number of multiplexed connections in the pool (mesh client or direct).
    #[arg(long, default_value_t = 8)]
    pool_size: usize,

    /// Per-connection in-flight request budget.
    #[arg(long, default_value_t = 4096)]
    max_inflight: usize,

    /// Per-command operation timeout, in milliseconds.
    #[arg(long, default_value_t = 1000)]
    op_timeout_ms: u64,

    /// Concurrent async workers issuing operations.
    #[arg(short = 'c', long, default_value_t = 32)]
    concurrency: usize,

    /// Total number of logical operations to issue (0 = run for --duration).
    #[arg(short = 'n', long, default_value_t = 0)]
    ops: u64,

    /// Run for this many seconds when --ops is 0.
    #[arg(short = 'd', long, default_value_t = 30)]
    duration: u64,

    /// Number of distinct keys in the pre-generated pool.
    #[arg(long, default_value_t = 10000)]
    keys: usize,

    /// Fixed length of each key, in bytes.
    #[arg(long, default_value_t = 32)]
    key_len: usize,

    /// Maximum value size, in bytes; the SET workload cycles through
    /// `1..=max`. Ignored by the GET workload.
    #[arg(long, default_value_t = 1024)]
    val_size: usize,

    /// Warm-up operations per worker before measurement begins (the `getset`
    /// legacy path). For `get`/`set` the keys are pre-seeded instead.
    #[arg(long, default_value_t = 50)]
    warmup: usize,

    /// Fail the run (non-zero exit) if the error rate exceeds this fraction.
    #[arg(long, default_value_t = 0.01)]
    max_error_rate: f64,

    /// Workload to run.
    #[arg(value_enum, default_value_t = WorkloadKindArg::Get)]
    workload: WorkloadKindArg,
}
/// CLI-facing mirror of [`WorkloadKind`] (clap needs its own ValueEnum here).
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum WorkloadKindArg {
    /// One GET per op — pure single-round-trip read (ping/pong).
    Get,
    /// One SET per op — pure single-round-trip write (ping/pong).
    Set,
}

impl From<WorkloadKindArg> for WorkloadKind {
    fn from(a: WorkloadKindArg) -> Self {
        match a {
            WorkloadKindArg::Get => WorkloadKind::Get,
            WorkloadKindArg::Set => WorkloadKind::Set,
        }
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "redis=warn,redis_bench=info".into()),
        )
        .init();

    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let code = rt.block_on(run(args));
    std::process::exit(code);
}

async fn run(args: Args) -> i32 {
    let workload: WorkloadKind = args.workload.into();

    let client: Arc<dyn ConnectionLike> = match build_client(&args).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: failed to connect: {e}");
            return 2;
        }
    };

    // Pre-generate the key/value pool once: keys are a fixed length, values
    // cycle through `1..=val_size`. Workers index into this with a plain
    // integer, so the hot path has no `format!()` allocations — the per-op
    // allocation count then reflects the SDK alone.
    let pool = Arc::new(driver::Pool::new(args.keys, args.key_len, args.val_size));
    eprintln!(
        "redis-bench: workload={:?} keys={} key_len={} val_size={} concurrency={} pool_size={} max_inflight={}",
        workload,
        args.keys,
        args.key_len,
        args.val_size,
        args.concurrency,
        args.pool_size,
        args.max_inflight
    );

    // Seed the keyspace: for GET, the keys must exist first; for SET we
    // pre-fill so the workload is pure overwrite (no `setnx` growth effects).
    eprintln!("seeding {} keys...", args.keys);
    if let Err(e) = seed_keys(&client, &pool).await {
        eprintln!("error: failed to seed keys: {e}");
        return 2;
    }

    let runner = Arc::from(workload.runner(pool));
    eprintln!("warming up ({} ops/worker)...", args.warmup);
    warmup(&client, &runner, args.concurrency, args.warmup).await;

    let mode = if args.ops > 0 {
        RunMode::Count(args.ops)
    } else {
        RunMode::Timed(Duration::from_secs(args.duration))
    };

    eprintln!("running...");
    let mem_before = brz_mem::heap();
    let (summary, elapsed) = measured_run(&client, &runner, args.concurrency, mode).await;
    let mem_window = MemoryWindow::between(mem_before, brz_mem::heap(), summary.total());
    report(&summary, elapsed, &mem_window);

    let error_rate = if summary.total() == 0 {
        0.0
    } else {
        summary.errors as f64 / summary.total() as f64
    };
    if error_rate > args.max_error_rate {
        eprintln!(
            "error rate {:.2}% exceeds threshold {:.2}% — failing",
            error_rate * 100.0,
            args.max_error_rate * 100.0
        );
        return 1;
    }
    0
}

/// Whether a run is bounded by a total op count or a wall-clock duration.
enum RunMode {
    Count(u64),
    Timed(Duration),
}

/// Build the connection the harness will drive. Returns it as a trait object
/// so the rest of the harness is agnostic to mesh-vs-direct.
async fn build_client(args: &Args) -> Result<Arc<dyn ConnectionLike>, String> {
    if let Some(addr) = &args.direct {
        let dc = DirectClient::connect(
            addr,
            args.pool_size,
            args.max_inflight,
            Duration::from_millis(args.op_timeout_ms),
        )
        .await
        .map_err(|e| e.to_string())?;
        return Ok(Arc::new(dc));
    }

    let ns = args
        .namespace
        .clone()
        .ok_or_else(|| "either --namespace or --direct is required".to_string())?;

    let mut cfg = MeshConfig::new(ns)
        .with_group(&args.group)
        .with_socket_dir(&args.socket_dir)
        .with_pool_size(args.pool_size)
        .with_max_inflight(args.max_inflight);
    if args.unix {
        cfg = cfg.with_transport(Transport::Unix);
    }
    cfg.op_timeout = Duration::from_millis(args.op_timeout_ms);

    let client = Client::from_config(cfg).await.map_err(|e| e.to_string())?;
    Ok(Arc::new(client))
}

/// Issue throwaway operations to prime the pool and let the server/mesh warm
/// up; not measured.
async fn warmup(
    client: &Arc<dyn ConnectionLike>,
    runner: &Arc<dyn Workload>,
    concurrency: usize,
    per_worker: usize,
) {
    let mut handles = Vec::with_capacity(concurrency);
    for w in 0..concurrency {
        let client = client.clone();
        let runner = runner.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..per_worker {
                let _ = runner.run(&*client, (w as u64) * 1000 + i as u64).await;
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

/// Pre-populate Redis with every key in the pool, writing a value whose size
/// cycles through `1..=max_value_size`. This makes the GET workload hit
/// existing keys and the SET workload pure-overwrite. Seeding is not the
/// measured phase, so it runs concurrently across the harness pool to keep it
/// fast for large key counts.
async fn seed_keys(client: &Arc<dyn ConnectionLike>, pool: &driver::Pool) -> Result<(), String> {
    use redis::Commands;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Snapshot the keys/values into owned Arcs so spawned tasks are 'static.
    let keys: Arc<[Vec<u8>]> = Arc::from(pool.keys());
    let values: Arc<[Vec<u8>]> = Arc::from(pool.values());
    let next = Arc::new(AtomicUsize::new(0));
    let total = keys.len();
    let workers = 8.min(total.max(1));
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let client = client.clone();
        let next = next.clone();
        let keys = keys.clone();
        let values = values.clone();
        handles.push(tokio::spawn(async move {
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= total {
                    break;
                }
                let key: &[u8] = &keys[i % keys.len()];
                let value: &[u8] = &values[i % values.len()];
                if let Err(e) = client.set::<()>(key, value).await {
                    eprintln!("seed SET failed at key index {i}: {e}");
                    return false;
                }
            }
            true
        }));
    }
    for h in handles {
        match h.await {
            Ok(true) => {}
            Ok(false) => return Err("a seed SET failed".into()),
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

/// Run the measured phase. Workers claim from a shared op budget (count mode)
/// or race a deadline (timed mode); each records latency into a local
/// histogram, merged at the end.
async fn measured_run(
    client: &Arc<dyn ConnectionLike>,
    runner: &Arc<dyn Workload>,
    concurrency: usize,
    mode: RunMode,
) -> (Summary, Duration) {
    let budget = Arc::new(OpBudget::new(match &mode {
        RunMode::Count(n) => *n,
        RunMode::Timed(_) => u64::MAX,
    }));
    let deadline = match &mode {
        RunMode::Timed(d) => Some(Instant::now() + *d),
        RunMode::Count(_) => None,
    };
    // Whether workers stop when the op budget is exhausted (count mode).
    let bounded = matches!(mode, RunMode::Count(_));
    let start = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for w in 0..concurrency {
        let client = client.clone();
        let budget = budget.clone();
        let runner = runner.clone();
        handles.push(tokio::spawn(async move {
            let mut local = WorkerStats::new();
            let mut op = (w as u64) * 1_000_000;
            loop {
                if let Some(dl) = deadline
                    && Instant::now() >= dl
                {
                    break;
                }
                if bounded && !budget.try_claim() {
                    break;
                }
                let t = Instant::now();
                let ok = runner.run(&*client, op).await;
                let elapsed = t.elapsed();
                if ok {
                    local.record(elapsed);
                } else {
                    local.record_error();
                }
                op += 1;
            }
            local
        }));
    }

    let mut summary = Summary::new();
    for h in handles {
        if let Ok(local) = h.await {
            local.add_to(&mut summary);
        }
    }
    (summary, start.elapsed())
}

/// Print the result table.
fn report(summary: &Summary, elapsed: Duration, memory: &MemoryWindow) {
    let total = summary.total();
    let secs = elapsed.as_secs_f64().max(1e-9);
    let rps = total as f64 / secs;
    let error_pct = if total == 0 {
        0.0
    } else {
        summary.errors as f64 / (total + summary.errors) as f64 * 100.0
    };

    println!();
    println!("======== redis-bench ========");
    println!("elapsed:        {:.3} s", secs);
    println!("ops:            {}", total);
    println!("errors:         {} ({:.2}%)", summary.errors, error_pct);
    println!("throughput:     {:.0} ops/s", rps);
    if total > 0 {
        println!(
            "latency (us):   min={}  mean={:.1}  p50={}  p95={}  p99={}  max={}",
            summary.min_us(),
            summary.mean_us(),
            summary.percentile_us(50.0),
            summary.percentile_us(95.0),
            summary.percentile_us(99.0),
            summary.max_us(),
        );
    }
    memory.print();
    println!("=============================");
}
