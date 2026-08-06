//! Mesh endpoint discovery.
//!
//! The breeze mesh agent publishes one sock **config file** per resource in the
//! socket directory (default `/tmp/breeze/socks`). The SDK discovers the
//! already-published endpoint by parsing those files directly — there is no
//! fetch/pull step, and we never write sock files or register backends.
//!
//! A config file name has exactly three `@`-separated fields, mirroring the
//! mesh's own `Quadruple::parse`:
//!
//! ```text
//! <service>@<protocol>:<slot>@<backend>
//! e.g. static.config.api.example.com+3+config+cloud+redis+feed+auto_translate_llm@redis:9470@rs
//! ```
//!
//! - `<service>` is the registry-prefixed path with `/` replaced by `+`; its
//!   tail is `...+<group>+<namespace>`.
//! - `<protocol>:<slot>` — if `<slot>` is a port number the endpoint is TCP on
//!   `127.0.0.1:<slot>`; otherwise it is a unix socket at `<dir>/<slot>.sock`.
//! - Files ending in `.sock` are live listener sockets, not config files, and
//!   are skipped.

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

/// The transport slot parsed out of a sock config file's middle field.
enum SockKind {
    Tcp(u16),
    Unix(String),
}

/// Discover the mesh endpoint for `cfg` and wait until it accepts connections.
///
/// Re-scans the sock directory on a 100 ms cadence up to `cfg.connect_wait`,
/// tolerating the mesh publishing the file slightly after the client starts.
pub async fn discover(cfg: &MeshConfig) -> RedisResult<Endpoint> {
    let deadline = Instant::now() + cfg.connect_wait;
    let prefer_unix = matches!(cfg.transport, Transport::Unix);
    loop {
        if let Some(endpoint) =
            scan_endpoint(&cfg.socket_dir, &cfg.group, &cfg.namespace, prefer_unix)
            && connectable(&endpoint).await
        {
            return Ok(endpoint);
        }
        if Instant::now() >= deadline {
            return Err(not_ready(
                "mesh endpoint not published or not listening yet",
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn connectable(endpoint: &Endpoint) -> bool {
    match endpoint {
        Endpoint::Tcp(addr) => tokio::net::TcpStream::connect(addr).await.is_ok(),
        Endpoint::Unix(path) => tokio::net::UnixStream::connect(path).await.is_ok(),
    }
}

fn not_ready(what: &'static str) -> RedisError {
    RedisError::from_kind(ErrorKind::NoConnection, what)
}

/// Scan `dir` for the sock config file matching `(group, namespace)` and build
/// its [`Endpoint`].
///
/// When several files match, the best is chosen by a score preferring the
/// requested transport family and then a specific `+group+namespace` match over
/// a namespace-only match (so an unrelated resource sharing the namespace tail
/// never wins over the exact one).
pub fn scan_endpoint(
    dir: &Path,
    group: &str,
    namespace: &str,
    prefer_unix: bool,
) -> Option<Endpoint> {
    let entries = std::fs::read_dir(dir).ok()?;
    let group_marker = format!("+{group}+{namespace}");
    let ns_marker = format!("+{namespace}");
    let mut best: Option<(u8, Endpoint)> = None;
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        let Some((service, kind)) = parse_sock(&name) else {
            continue;
        };
        let is_group = service.ends_with(&group_marker);
        if !is_group && !service.ends_with(&ns_marker) {
            continue;
        }
        let endpoint = match kind {
            SockKind::Tcp(port) => {
                Endpoint::Tcp(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)))
            }
            SockKind::Unix(slot) => Endpoint::Unix(dir.join(format!("{slot}.sock"))),
        };
        let family_match = matches!(endpoint, Endpoint::Unix(_)) == prefer_unix;
        let score = (family_match as u8) * 2 + (is_group as u8);
        if best
            .as_ref()
            .is_none_or(|(best_score, _)| score > *best_score)
        {
            best = Some((score, endpoint));
        }
    }
    best.map(|(_, endpoint)| endpoint)
}

/// Parse a sock config file name into `(service, transport)`, mirroring the
/// mesh's `Quadruple::parse`. Returns `None` for `.sock` files and names that
/// are not exactly three `@`-separated fields.
fn parse_sock(name: &str) -> Option<(&str, SockKind)> {
    if name.ends_with(".sock") {
        return None;
    }
    let fields: Vec<&str> = name.split('@').collect();
    if fields.len() != 3 {
        return None;
    }
    let service = fields[0];
    let mut protocol_fields = fields[1].split(':');
    let _protocol = protocol_fields.next()?;
    let kind = match protocol_fields.next() {
        Some(slot) => match slot.parse::<u16>() {
            Ok(port) => SockKind::Tcp(port),
            Err(_) => SockKind::Unix(slot.to_string()),
        },
        // No slot: unix socket named after the service.
        None => SockKind::Unix(service.to_string()),
    };
    Some((service, kind))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn parses_tcp_sockname() {
        let name = "static.config.api.example.com+3+config+cloud+redis+feed+auto_translate_llm@redis:9470@rs";
        let (service, kind) = parse_sock(name).unwrap();
        assert!(service.ends_with("+feed+auto_translate_llm"));
        assert!(matches!(kind, SockKind::Tcp(9470)));
    }

    #[test]
    fn parses_unix_sockname() {
        // A non-numeric slot denotes a unix socket at <dir>/<slot>.sock.
        let name = "dom+3+config+cloud+redis+feed+auto_translate_llm@redis:U_auto_translate_llm@rs";
        let (_, kind) = parse_sock(name).unwrap();
        match kind {
            SockKind::Unix(slot) => assert_eq!(slot, "U_auto_translate_llm"),
            _ => panic!("expected unix"),
        }
    }

    #[test]
    fn rejects_dot_sock_and_malformed() {
        assert!(parse_sock("something.sock").is_none());
        assert!(parse_sock("no-at-markers").is_none());
        assert!(parse_sock("only@two").is_none());
    }

    #[test]
    fn scans_tcp_and_unix_by_preference() {
        let dir = std::env::temp_dir().join(format!("mesh_scan_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        File::create(dir.join(
            "static.config.api.example.com+3+config+cloud+redis+feed+auto_translate_llm@redis:9470@rs",
        ))
        .unwrap();
        File::create(dir.join(
            "dom+3+config+cloud+redis+feed+auto_translate_llm@redis:U_auto_translate_llm@rs",
        ))
        .unwrap();
        File::create(dir.join("dom+3+config+cloud+redis+g2+other@redis:9320@rs")).unwrap();

        // Prefer TCP.
        let tcp = scan_endpoint(&dir, "feed", "auto_translate_llm", false).unwrap();
        assert_eq!(
            tcp,
            Endpoint::Tcp(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9470)))
        );

        // Prefer unix.
        let unix = scan_endpoint(&dir, "feed", "auto_translate_llm", true).unwrap();
        assert_eq!(unix, Endpoint::Unix(dir.join("U_auto_translate_llm.sock")));

        // Namespace-only fallback still resolves when the group differs.
        let other = scan_endpoint(&dir, "wrong", "other", false).unwrap();
        assert_eq!(
            other,
            Endpoint::Tcp(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9320)))
        );

        assert!(scan_endpoint(&dir, "feed", "missing", false).is_none());

        std::fs::remove_dir_all(&dir).ok();
    }
}
