//! redis-bench — a load-test harness for the redis SDK.
//!
//! Runs a fixed number of operations across `--concurrency` async workers and
//! reports throughput plus latency percentiles (p50/p95/p99). It drives the
//! SDK's three client modes:
//!
//! - **sidecar mode** (default, `--namespace`): through
//!   [`SidecarClient`](redis::sidecar::SidecarClient), measuring the full
//!   pool/multiplexing/routing/HA stack against the breeze mesh.
//! - **direct mode** (`--direct host:port[:db]`): through
//!   [`DirectClient`](redis::direct::DirectClient), the SDK's direct-backend
//!   stack (AUTH/SELECT handshake, circuit breaker, retries, DNS watcher)
//!   straight to a raw redis-server.
//! - **replay mode** (`--replay host:port`, feature `replay`): through
//!   [`redis::replay`], the single-persistent-connection replay/comparison
//!   client. HGET-only; each worker owns its own connection (the replay
//!   client is `&mut` single-stream by design).
//!
//! Usage:
//!   redis-bench --namespace my_ns --concurrency 64 --ops 100000 get
//!   redis-bench --direct 127.0.0.1:6379 --concurrency 64 --ops 100000 set
//!   redis-bench --replay 127.0.0.1:6379 --concurrency 64 --ops 100000
//!
//! # Exit code
//!
//! Non-zero if the error rate exceeds `--max-error-rate`.

mod driver;
mod fault;
mod stats;

use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use driver::{Workload, WorkloadKind};
use fault::FaultInjector;
use redis::connection::ConnectionLike;
use redis::sidecar::{MeshConfig, SidecarClient, Transport};
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

    /// Direct-backend mode: connect to a raw `host:port[:db]` redis-server
    /// through the SDK's `redis::direct::DirectClient` (no mesh).
    #[arg(long)]
    direct: Option<String>,

    /// Replay mode: connect to a raw `host:port` redis-server through
    /// `redis::replay` (single persistent connection per worker, HGET-only).
    #[arg(long)]
    replay: Option<String>,

    /// Shards mode: comma-separated direct backends
    /// (`host:port[:db],host:port[:db],...`), driven through the SDK's
    /// `direct::Shards` client-side router.
    #[arg(long, value_delimiter = ',')]
    shards: Option<Vec<String>>,

    /// Hash algorithm for --shards (breeze mesh names, e.g. crc32).
    #[arg(long, default_value = "crc32")]
    hash: String,

    /// Distribution for --shards (e.g. modula, range-256, ketama).
    #[arg(long, default_value = "modula")]
    distribution: String,

    /// With fault injection in --shards mode: only proxy (delay) this shard
    /// index. Defaults to shard 0.
    #[arg(long)]
    fault_shard: Option<usize>,

    /// Use a unix socket for the mesh transport (ignored with --direct).
    #[arg(long)]
    unix: bool,

    /// Deployment group segment of the mesh sock-file name.
    #[arg(long, default_value = "default")]
    group: String,

    /// Directory the mesh publishes sock files into.
    #[arg(long, default_value = "/tmp/breeze/socks")]
    socket_dir: String,

    /// Minimum live pooled connections kept warm (0 = fully lazy start).
    #[arg(long, default_value_t = 2)]
    min_conns: usize,

    /// Maximum live pooled connections (mesh client or direct).
    #[arg(long, default_value_t = 15)]
    max_conns: usize,

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
    #[arg(long, default_value_t = 15)]
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

    /// Fraction of operations delayed by --slow-ms (simulated slow requests).
    #[arg(long, default_value_t = 0.0)]
    slow_rate: f64,

    /// Delay injected for slow operations, in milliseconds.
    #[arg(long, default_value_t = 200)]
    slow_ms: u64,

    /// Fraction of operations delayed past the client timeout (simulated
    /// timeouts; the SDK's op_timeout/retry path kicks in).
    #[arg(long, default_value_t = 0.0)]
    timeout_rate: f64,

    /// Delay injected for timed-out operations, in milliseconds. Should
    /// exceed --op-timeout-ms.
    #[arg(long, default_value_t = 5000)]
    timeout_ms: u64,

    /// Workload to run.
    #[arg(value_enum, default_value_t = WorkloadKindArg::Hget)]
    workload: WorkloadKindArg,
}
/// CLI-facing mirror of [`WorkloadKind`] (clap needs its own ValueEnum here).
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum WorkloadKindArg {
    /// One HGET per op — pure single-round-trip read (ping/pong).
    Hget,
    /// One HMGET (4 fields) per op — multi-field read.
    Hmget,
}

impl From<WorkloadKindArg> for WorkloadKind {
    fn from(a: WorkloadKindArg) -> Self {
        match a {
            WorkloadKindArg::Hget => WorkloadKind::Hget,
            WorkloadKindArg::Hmget => WorkloadKind::Hmget,
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

async fn run(mut args: Args) -> i32 {
    // Optional fault injection: insert a delaying TCP proxy between the
    // bench and the target (mesh endpoint or raw redis) so slow/timeout
    // faults exercise the SDK's timeout/breaker/retry machinery.
    let injector = FaultInjector::new(
        args.slow_rate,
        args.slow_ms,
        args.timeout_rate,
        args.timeout_ms,
    )
    .map(Arc::new);
    if let Some(injector) = &injector {
        if let Err(e) = inject_faults(&mut args, injector.clone()).await {
            eprintln!("error: fault injection setup failed: {e}");
            return 2;
        }
        eprintln!(
            "fault injection: slow={}*{}ms timeout={}*{}ms",
            args.slow_rate, args.slow_ms, args.timeout_rate, args.timeout_ms
        );
    }

    if args.replay.is_some() {
        return run_replay(args, injector).await;
    }

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
        "redis-bench: workload={:?} keys={} key_len={} val_size={} concurrency={} min_conns={} max_conns={} max_inflight={}",
        workload,
        args.keys,
        args.key_len,
        args.val_size,
        args.concurrency,
        args.min_conns,
        args.max_conns,
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
    if let Some(injector) = &injector {
        let (slow, timeout) = injector.counts();
        eprintln!("fault injection: slow={slow} timeout={timeout} injected");
    }
    finish(&summary, elapsed, mem_before, &args)
}

/// Report the run and compute the exit code from the error rate.
fn finish(
    summary: &Summary,
    elapsed: Duration,
    mem_before: Option<brz_mem::HeapStats>,
    args: &Args,
) -> i32 {
    let mem_window = MemoryWindow::between(mem_before, brz_mem::heap(), summary.total());
    report(summary, elapsed, &mem_window);
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

/// Insert the fault-injecting proxy between the bench and the configured
/// target, rewriting `args` to point at the proxy. Sidecar mode re-publishes
/// a bench-owned sock file (proxy port) in a fresh socket dir.
async fn inject_faults(args: &mut Args, injector: Arc<FaultInjector>) -> Result<(), String> {
    if let Some(shards) = args.shards.clone() {
        // Only the selected shard goes through the fault proxy; the others
        // connect directly — modeling "one slow shard among many backends".
        let fault_shard = args.fault_shard.unwrap_or(0);
        if fault_shard >= shards.len() {
            return Err(format!(
                "--fault-shard {fault_shard} out of range ({} shards)",
                shards.len()
            ));
        }
        let mut rewritten = shards;
        let addr = &rewritten[fault_shard];
        let parts: Vec<&str> = addr.split(':').collect();
        let (host_port, db) = match parts.len() {
            2 => (addr.clone(), String::new()),
            3 => (
                format!("{}:{}", parts[0], parts[1]),
                format!(":{}", parts[2]),
            ),
            _ => return Err(format!("invalid shard address '{addr}'")),
        };
        let target = resolve(&host_port).await?;
        let proxy = fault::start_proxy(target, injector).await?;
        eprintln!("fault proxy: {proxy} -> {target} (shard {fault_shard} only)");
        rewritten[fault_shard] = format!("{proxy}{db}");
        args.shards = Some(rewritten);
        return Ok(());
    }
    if let Some(addr) = args.direct.clone() {
        // host:port[:db] — proxy the host:port part, keep the db suffix.
        let parts: Vec<&str> = addr.split(':').collect();
        let (host_port, db) = match parts.len() {
            2 => (addr.clone(), String::new()),
            3 => (
                format!("{}:{}", parts[0], parts[1]),
                format!(":{}", parts[2]),
            ),
            _ => return Err(format!("invalid --direct address '{addr}'")),
        };
        let target = resolve(&host_port).await?;
        let proxy = fault::start_proxy(target, injector).await?;
        eprintln!("fault proxy: {proxy} -> {target}");
        args.direct = Some(format!("{proxy}{db}"));
        return Ok(());
    }
    if let Some(addr) = args.replay.clone() {
        let target = resolve(&addr).await?;
        let proxy = fault::start_proxy(target, injector).await?;
        eprintln!("fault proxy: {proxy} -> {target}");
        args.replay = Some(proxy.to_string());
        return Ok(());
    }
    if let Some(ns) = args.namespace.clone() {
        let endpoints = redis::sidecar::discovery::scan_endpoints(
            std::path::Path::new(&args.socket_dir),
            &args.group,
            &ns,
            args.unix,
        );
        let target = endpoints
            .into_iter()
            .find_map(|e| match e {
                redis::sidecar::Endpoint::Tcp(addr) => Some(addr),
                redis::sidecar::Endpoint::Unix(_) => None,
            })
            .ok_or("no TCP mesh endpoint to proxy (unix endpoints unsupported)")?;
        let proxy = fault::start_proxy(target, injector).await?;
        let dir = std::env::temp_dir().join(format!("redis-bench-fault-{}", std::process::id()));
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let sock = dir.join(format!(
            "static.config.api.example.com+3+config+cloud+redis+{}+{}@redis:{}@rs",
            args.group,
            ns,
            proxy.port()
        ));
        std::fs::File::create(&sock).map_err(|e| e.to_string())?;
        eprintln!("fault proxy: {proxy} -> {target} (sock {})", sock.display());
        args.socket_dir = dir.to_string_lossy().into_owned();
        args.unix = false;
        return Ok(());
    }
    Err("fault injection requires one of --namespace/--direct/--replay".to_string())
}

async fn resolve(host_port: &str) -> Result<std::net::SocketAddr, String> {
    tokio::net::lookup_host(host_port)
        .await
        .map_err(|e| format!("resolve {host_port}: {e}"))?
        .next()
        .ok_or_else(|| format!("resolve {host_port}: no addresses"))
}

/// Build the connection the harness will drive. Returns it as a trait object
/// so the rest of the harness is agnostic to mesh-vs-direct.
async fn build_client(args: &Args) -> Result<Arc<dyn ConnectionLike>, String> {
    if let Some(shards) = &args.shards {
        let mut clients = Vec::with_capacity(shards.len());
        for addr in shards {
            let mut cfg = redis::direct::ServerConfig::new(addr)
                .map_err(|e| e.to_string())?
                .with_min_connections(args.min_conns)
                .with_max_connections(args.max_conns);
            cfg.max_inflight = args.max_inflight;
            cfg.op_timeout = Duration::from_millis(args.op_timeout_ms);
            clients.push(
                redis::direct::DirectClient::connect(cfg)
                    .await
                    .map_err(|e| format!("shard {addr}: {e}"))?,
            );
        }
        let router =
            redis::direct::Shards::new(&args.hash, &args.distribution, shards.clone(), clients);
        return Ok(Arc::new(router));
    }
    if let Some(addr) = &args.direct {
        let mut cfg = redis::direct::ServerConfig::new(addr)
            .map_err(|e| e.to_string())?
            .with_min_connections(args.min_conns)
            .with_max_connections(args.max_conns);
        cfg.max_inflight = args.max_inflight;
        cfg.op_timeout = Duration::from_millis(args.op_timeout_ms);
        let dc = redis::direct::DirectClient::connect(cfg)
            .await
            .map_err(|e| e.to_string())?;
        return Ok(Arc::new(dc));
    }

    let ns = args
        .namespace
        .clone()
        .ok_or_else(|| "one of --namespace, --direct, or --replay is required".to_string())?;

    let mut cfg = MeshConfig::new(ns)
        .with_group(&args.group)
        .with_socket_dir(&args.socket_dir)
        .with_min_connections(args.min_conns)
        .with_max_connections(args.max_conns)
        .with_max_inflight(args.max_inflight);
    if args.unix {
        cfg = cfg.with_transport(Transport::Unix);
    }
    cfg.op_timeout = Duration::from_millis(args.op_timeout_ms);

    let client = SidecarClient::from_config(cfg)
        .await
        .map_err(|e| e.to_string())?;
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
                // One HSET per key, all workload fields at once.
                let mut cmd = redis::cmd("HSET");
                cmd.arg(key);
                for field in driver::FIELDS {
                    cmd.arg(field).arg(value);
                }
                // Tolerate transient faults (fault injection may hang a
                // connection; writes don't auto-retry by design).
                let mut ok = false;
                for attempt in 0..5 {
                    match cmd.exec_async(&*client).await {
                        Ok(()) => {
                            ok = true;
                            break;
                        }
                        Err(e) if attempt == 4 => {
                            eprintln!("seed HSET failed at key index {i}: {e}");
                        }
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
                if !ok {
                    return false;
                }
            }
            true
        }));
    }
    for h in handles {
        match h.await {
            Ok(true) => {}
            Ok(false) => return Err("a seed HSET failed".into()),
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

/// Replay mode (feature `replay`): each worker owns one
/// [`redis::replay::RedisConnection`] (the client is a `&mut` single-stream
/// connection by design, mirroring the replay proxy's lane model) and issues
/// sequential HGETs. Seeding goes through the SDK's direct client, since the
/// replay client is read-only.
#[cfg(feature = "replay")]
async fn run_replay(args: Args, injector: Option<Arc<FaultInjector>>) -> i32 {
    use redis::replay::RedisConnection;

    let addr = args.replay.clone().unwrap();
    let Some((host, port)) = addr.rsplit_once(':') else {
        eprintln!("error: --replay expects host:port, got '{addr}'");
        return 2;
    };
    let Ok(port) = port.parse::<u16>() else {
        eprintln!("error: invalid port in --replay '{addr}'");
        return 2;
    };
    let host = host.to_string();
    const FIELD: &str = "f";

    // Textual hash keys (the replay client takes `&str`).
    let keys: Arc<Vec<String>> = Arc::new((0..args.keys).map(|i| format!("h:bench:{i}")).collect());

    eprintln!(
        "redis-bench: replay={}:{} keys={} concurrency={}",
        host,
        port,
        keys.len(),
        args.concurrency
    );

    // Seed the hashes through the SDK direct client (concurrently).
    eprintln!("seeding {} hash keys...", keys.len());
    {
        let cfg = match redis::direct::ServerConfig::new(&addr) {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("error: {e}");
                return 2;
            }
        };
        let seeder = match redis::direct::DirectClient::connect(cfg).await {
            Ok(client) => client,
            Err(e) => {
                eprintln!("error: failed to connect for seeding: {e}");
                return 2;
            }
        };
        let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut seed_handles = Vec::new();
        for _ in 0..8.min(keys.len().max(1)) {
            let seeder = seeder.clone();
            let keys = keys.clone();
            let next = next.clone();
            seed_handles.push(tokio::spawn(async move {
                use redis::Commands as _;
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= keys.len() {
                        return true;
                    }
                    let mut ok = false;
                    for _ in 0..5 {
                        if seeder.hset::<i64>(&keys[i], FIELD, i as i64).await.is_ok() {
                            ok = true;
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    if !ok {
                        return false;
                    }
                }
            }));
        }
        for h in seed_handles {
            match h.await {
                Ok(true) => {}
                Ok(false) => {
                    eprintln!("error: a seed HSET failed");
                    return 2;
                }
                Err(e) => {
                    eprintln!("error: seed task failed: {e}");
                    return 2;
                }
            }
        }
    }

    let mode = if args.ops > 0 {
        RunMode::Count(args.ops)
    } else {
        RunMode::Timed(Duration::from_secs(args.duration))
    };
    let budget = Arc::new(OpBudget::new(match &mode {
        RunMode::Count(n) => *n,
        RunMode::Timed(_) => u64::MAX,
    }));
    let deadline = match &mode {
        RunMode::Timed(d) => Some(Instant::now() + *d),
        RunMode::Count(_) => None,
    };
    let bounded = matches!(mode, RunMode::Count(_));

    eprintln!("running (replay, hget)...");
    let mem_before = brz_mem::heap();
    let start = Instant::now();

    let mut handles = Vec::with_capacity(args.concurrency);
    for w in 0..args.concurrency {
        let host = host.clone();
        let keys = keys.clone();
        let budget = budget.clone();
        let warmup = args.warmup;
        let injector = injector.clone();
        handles.push(tokio::spawn(async move {
            let mut local = WorkerStats::new();
            let mut conn = match RedisConnection::connect(&host, port).await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("worker {w}: connect failed: {e}");
                    local.record_error();
                    return local;
                }
            };
            let mut op = (w as u64) * 1_000_000;
            // Warm-up ops prime the connection and are not recorded, nor do
            // they consume the op budget.
            for i in 0..warmup {
                let key = &keys[(w * warmup + i) % keys.len()];
                let _ = conn.hget(key, FIELD).await;
            }
            loop {
                if let Some(dl) = deadline
                    && Instant::now() >= dl
                {
                    break;
                }
                if bounded && !budget.try_claim() {
                    break;
                }
                let key = &keys[(op as usize) % keys.len()];
                if let Some(injector) = &injector {
                    injector.maybe_delay().await;
                }
                let t = Instant::now();
                let ok = conn.hget(key, FIELD).await.is_ok();
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
    if let Some(injector) = &injector {
        let (slow, timeout) = injector.counts();
        eprintln!("fault injection: slow={slow} timeout={timeout} injected");
    }
    finish(&summary, start.elapsed(), mem_before, &args)
}

/// Stub when the `replay` feature is off.
#[cfg(not(feature = "replay"))]
async fn run_replay(_args: Args, _injector: Option<Arc<FaultInjector>>) -> i32 {
    eprintln!("error: --replay requires the `replay` feature");
    eprintln!("hint:  cargo run -p redis-bench --features replay -- --replay host:port");
    2
}
