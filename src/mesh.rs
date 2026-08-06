//! Mesh endpoint discovery.
//!
//! The breeze mesh agent publishes one endpoint per resource namespace. In the
//! PaaS deployment model the mesh (not the SDK) creates the sock file, so the
//! client's job is to *discover* the already-published endpoint and wait until
//! it is connectable:
//!
//! - **Unix**: `<socket_dir>/U_<namespace>.sock`.
//! - **TCP**: a `127.0.0.1:<port>` port parsed from the sock-file name
//!   `<domain>+3+config+cloud+<type>+<group>+<namespace>@<sockType>:<port>@<proto>`.
//!
//! We do not write sock files, register backends, or resolve DNS — there is a
//! single local mesh server.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::config::{MeshConfig, Transport};
use crate::error::{ErrorKind, RedisError, RedisResult};

/// A resolved, connectable mesh endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Endpoint {
    /// A `127.0.0.1:<port>` TCP endpoint.
    Tcp(SocketAddr),
    /// A unix domain socket path.
    Unix(PathBuf),
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Endpoint::Tcp(addr) => write!(f, "tcp://{addr}"),
            Endpoint::Unix(path) => write!(f, "unix://{}", path.display()),
        }
    }
}

/// Discover the mesh endpoint for `cfg` and wait until it accepts connections.
///
/// Polls up to `cfg.connect_wait` (100 ms cadence). For TCP it re-scans the
/// sock directory each round, tolerating the mesh publishing the file slightly
/// later than the client starts.
pub async fn discover(cfg: &MeshConfig) -> RedisResult<Endpoint> {
    let deadline = Instant::now() + cfg.connect_wait;
    loop {
        let attempt = match cfg.transport {
            Transport::Unix => discover_unix(cfg).await,
            Transport::Tcp => discover_tcp(cfg).await,
        };
        match attempt {
            Ok(endpoint) => return Ok(endpoint),
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(err);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn discover_unix(cfg: &MeshConfig) -> RedisResult<Endpoint> {
    let path = cfg.socket_dir.join(format!("U_{}.sock", cfg.namespace));
    if !path.exists() {
        return Err(not_ready("unix sock file not published yet"));
    }
    match tokio::net::UnixStream::connect(&path).await {
        Ok(_) => Ok(Endpoint::Unix(path)),
        Err(_) => Err(not_ready("unix endpoint not listening yet")),
    }
}

async fn discover_tcp(cfg: &MeshConfig) -> RedisResult<Endpoint> {
    let port = scan_tcp_port(&cfg.socket_dir, &cfg.group, &cfg.namespace)
        .ok_or_else(|| not_ready("no sock file for namespace yet"))?;
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    match tokio::net::TcpStream::connect(addr).await {
        Ok(_) => Ok(Endpoint::Tcp(addr)),
        Err(_) => Err(not_ready("tcp endpoint not listening yet")),
    }
}

fn not_ready(what: &'static str) -> RedisError {
    RedisError::from_kind(ErrorKind::NoConnection, what)
}

/// Scan `dir` for the sock file of `(group, namespace)` and return its port.
///
/// Sock-file names look like
/// `domain+3+config+cloud+redis+group+namespace@redis:PORT@rs`; the port sits
/// between the two `@` markers. Prefers a `+group+namespace` match but falls
/// back to matching the namespace alone.
pub fn scan_tcp_port(dir: &Path, group: &str, namespace: &str) -> Option<u16> {
    let entries = std::fs::read_dir(dir).ok()?;
    let group_marker = format!("+{group}+{namespace}");
    let ns_marker = format!("+{namespace}");
    let mut fallback: Option<u16> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(port) = parse_port(&name) else {
            continue;
        };
        let prefix = name.split('@').next().unwrap_or("");
        if prefix.ends_with(&group_marker) {
            return Some(port);
        }
        if prefix.ends_with(&ns_marker) {
            fallback = Some(port);
        }
    }
    fallback
}

/// Extract the port from `...@<sockType>:<port>@<proto>`.
fn parse_port(name: &str) -> Option<u16> {
    let mut parts = name.split('@');
    let _prefix = parts.next()?;
    let middle = parts.next()?; // "<sockType>:<port>"
    let port_str = middle.rsplit(':').next()?;
    port_str.parse::<u16>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn parses_port_from_sockname() {
        assert_eq!(
            parse_port("static.config.api.example.com+3+config+cloud+redis+g1+ns1@redis:9300@rs"),
            Some(9300)
        );
        assert_eq!(parse_port("no-at-marker"), None);
    }

    #[test]
    fn scans_by_group_namespace() {
        let dir = std::env::temp_dir().join(format!("mesh_scan_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        File::create(dir.join("dom+3+config+cloud+redis+g1+nsA@redis:9310@rs")).unwrap();
        File::create(dir.join("dom+3+config+cloud+redis+g2+nsB@redis:9320@rs")).unwrap();

        assert_eq!(scan_tcp_port(&dir, "g1", "nsA"), Some(9310));
        assert_eq!(scan_tcp_port(&dir, "g2", "nsB"), Some(9320));
        // Namespace-only fallback when the group does not match.
        assert_eq!(scan_tcp_port(&dir, "other", "nsA"), Some(9310));
        assert_eq!(scan_tcp_port(&dir, "g1", "missing"), None);

        std::fs::remove_dir_all(&dir).ok();
    }
}
