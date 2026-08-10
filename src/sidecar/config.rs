//! Client configuration for connecting to the breeze mesh.
//!
//! The client talks to a single local mesh agent that proxies to the real
//! Redis backends. Connection is by resource **namespace**: the mesh exposes a
//! per-namespace endpoint (a unix socket or a `127.0.0.1` port) which we
//! discover from the sock-file directory (see [`crate::sidecar::discovery`]).

use std::path::PathBuf;
use std::time::Duration;

/// Default sock-file directory the mesh agent publishes endpoints into.
pub const DEFAULT_SOCKET_DIR: &str = "/tmp/breeze/socks";
/// Default registry domain segment used in sock-file names.
pub const DEFAULT_DOMAIN: &str = "static.config.api.example.com";
/// Default resource type code (see the Java `ResourceTypeEnum`).
pub const DEFAULT_RESOURCE: &str = "redis";

/// Which transport the mesh endpoint uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    /// A `127.0.0.1:<port>` TCP endpoint (the Java default).
    Tcp,
    /// A unix domain socket at `<socket_dir>/U_<namespace>.sock`.
    Unix,
}

/// How to reach and pool a mesh-proxied Redis resource.
#[derive(Clone, Debug)]
pub struct MeshConfig {
    /// Resource namespace — identifies which backend the mesh routes to.
    pub namespace: String,
    /// Deployment group (part of the sock-file name).
    pub group: String,
    /// Registry domain (leading segment of the sock-file name).
    pub domain: String,
    /// Resource type code (`redis`, `counterservice`, `pika`, ...).
    pub resource: String,
    /// Transport to the mesh endpoint.
    pub transport: Transport,
    /// Directory the mesh publishes sock files / listens in.
    pub socket_dir: PathBuf,
    /// Maximum live connections in the pool. The pool starts at one
    /// connection and grows on demand (when every live connection is above
    /// half its in-flight budget), so this is a cap, not a fixed size —
    /// important when one process serves ~1000 namespaces.
    pub pool_size: usize,
    /// Per-command operation timeout.
    pub op_timeout: Duration,
    /// How long to wait for the mesh endpoint to become connectable.
    pub connect_wait: Duration,
    /// Commands slower than this are counted and slow-logged.
    pub slow_time_threshold: Duration,
    /// Retry attempts for read commands.
    pub max_try_time: u32,
    /// Retry attempts for write commands.
    pub write_retry: u32,
    /// Maintenance patrol cadence while the pool is healthy (dead-connection
    /// sweep, DNS refresh gate, min-idle top-up). Default 30s, in the spirit of
    /// clientBalancer's `poolWatchInterval`.
    pub healthy_patrol_interval: Duration,
    /// Recovery probe cadence while the breaker is open. Default 1s,
    /// matching clientBalancer's `poolHealthyInterval` — keep this small:
    /// it is the only recovery path while requests fast-fail.
    pub unhealthy_probe_interval: Duration,
    /// Maximum number of in-flight requests per connection. When the budget
    /// is exhausted new requests fail fast with
    /// [`ErrorKind::Overloaded`](crate::ErrorKind::Overloaded) instead of
    /// queueing, so one stalled backend cannot grow memory unboundedly in a
    /// process serving many namespaces.
    pub max_inflight: usize,
}

impl MeshConfig {
    /// A config for `namespace` with mesh-appropriate defaults.
    pub fn new(namespace: impl Into<String>) -> Self {
        MeshConfig {
            namespace: namespace.into(),
            group: "default".to_string(),
            domain: DEFAULT_DOMAIN.to_string(),
            resource: DEFAULT_RESOURCE.to_string(),
            transport: Transport::Tcp,
            socket_dir: PathBuf::from(DEFAULT_SOCKET_DIR),
            pool_size: 4,
            op_timeout: Duration::from_millis(1000),
            connect_wait: Duration::from_secs(10),
            slow_time_threshold: Duration::from_millis(50),
            max_try_time: 2,
            write_retry: 1,
            max_inflight: 4096,
            healthy_patrol_interval: Duration::from_secs(30),
            unhealthy_probe_interval: Duration::from_secs(1),
        }
    }

    /// Set the deployment group.
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = group.into();
        self
    }

    /// Use a unix domain socket instead of TCP.
    pub fn with_transport(mut self, transport: Transport) -> Self {
        self.transport = transport;
        self
    }

    /// Override the sock-file directory (useful for tests).
    pub fn with_socket_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.socket_dir = dir.into();
        self
    }

    /// Override the maximum number of live pooled connections.
    pub fn with_pool_size(mut self, size: usize) -> Self {
        self.pool_size = size.max(1);
        self
    }

    /// Override the per-connection in-flight request budget.
    pub fn with_max_inflight(mut self, max_inflight: usize) -> Self {
        self.max_inflight = max_inflight.max(1);
        self
    }

    /// Override the healthy-pool patrol cadence.
    pub fn with_healthy_patrol_interval(mut self, interval: Duration) -> Self {
        self.healthy_patrol_interval = interval;
        self
    }

    /// Override the unhealthy-pool recovery probe cadence.
    pub fn with_unhealthy_probe_interval(mut self, interval: Duration) -> Self {
        self.unhealthy_probe_interval = interval;
        self
    }
}
