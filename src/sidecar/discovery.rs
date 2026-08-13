//! Redis TCP endpoint discovery through the shared Breeze registry parser.
//!
//! Redis advertisements encode coordinates as
//! `...+<group>+<namespace>@redis:<port>@<backend>`. The shared discovery crate
//! owns parsing, exact matching, `/data1/breeze/socks`, and
//! `MESH_CONNECT_HOST`; this module owns Redis's bounded readiness wait and
//! connectability probes.

use std::path::Path;
use std::time::{Duration, Instant};

use brz_discovery::{CoordinateLayout, Registry};

pub use brz_discovery::Endpoint;

use super::config::MeshConfig;
use crate::error::{ErrorKind, RedisError, RedisResult};

const REDIS_PROTOCOL: &str = "redis";
const REDIS_LAYOUT: CoordinateLayout = CoordinateLayout::GroupNamespace;

/// Discover the mesh endpoint for `cfg` and wait until it accepts connections.
///
/// Re-scans the socks directory on a 100 ms cadence up to
/// `cfg.connect_wait`, tolerating publication shortly after client startup.
pub async fn discover(cfg: &MeshConfig) -> RedisResult<Endpoint> {
    let deadline = Instant::now() + cfg.connect_wait;
    loop {
        if let Some(endpoint) = scan_current(cfg).await {
            return Ok(endpoint);
        }
        if Instant::now() >= deadline {
            return Err(RedisError::from_kind(
                ErrorKind::NoConnection,
                "mesh TCP endpoint not published or not listening yet",
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Scan once and return the first currently connectable endpoint.
///
/// Every candidate is probed, so a stale advertisement cannot mask a live
/// endpoint for the same exact `(protocol, group, namespace)` coordinate.
pub async fn scan_current(cfg: &MeshConfig) -> Option<Endpoint> {
    for endpoint in scan_endpoints(&cfg.socket_dir, &cfg.group, &cfg.namespace) {
        if connectable(&endpoint).await {
            return Some(endpoint);
        }
    }
    None
}

/// Return every exact matching TCP endpoint.
///
/// A missing or transiently unreadable registry yields no candidates; the
/// bounded readiness/maintenance caller decides whether to retry.
pub fn scan_endpoints(dir: &Path, group: &str, namespace: &str) -> Vec<Endpoint> {
    Registry::new(dir)
        .discover(REDIS_PROTOCOL, REDIS_LAYOUT, group, namespace)
        .unwrap_or_default()
}

/// Return the first exact matching TCP endpoint.
pub fn scan_endpoint(dir: &Path, group: &str, namespace: &str) -> Option<Endpoint> {
    scan_endpoints(dir, group, namespace).into_iter().next()
}

async fn connectable(endpoint: &Endpoint) -> bool {
    tokio::net::TcpStream::connect((endpoint.host.as_str(), endpoint.port))
        .await
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_only_exact_tcp_coordinate() {
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "static.config.api.example.com+3+config+cloud+redis+feed+ns@redis:9470@rs",
            "static.config.api.example.com+3+config+cloud+redis+feed+ns@redis:U_ns@rs",
            "static.config.api.example.com+3+config+cloud+redis+other+ns@redis:9471@rs",
            "static.config.api.example.com+3+config+cloud+redis+feed+ns@mc:9472@cs",
        ] {
            std::fs::write(dir.path().join(name), []).unwrap();
        }

        assert_eq!(
            scan_endpoints(dir.path(), "feed", "ns"),
            vec![Endpoint {
                host: "127.0.0.1".into(),
                port: 9470
            }]
        );
        assert!(scan_endpoints(dir.path(), "missing", "ns").is_empty());
    }
}
