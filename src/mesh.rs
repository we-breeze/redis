//! One-shot discovery of a Redis TCP endpoint published by Breeze mesh.

use std::path::{Path, PathBuf};

use brz_discovery::{CoordinateLayout, Endpoint, Registry};

use crate::{ErrorKind, RedisError, RedisResult};

/// Default directory where Breeze publishes TCP endpoint advertisements.
pub use brz_discovery::DEFAULT_SOCKS_DIR;
/// Environment variable used to override the local mesh TCP host.
pub use brz_discovery::MESH_CONNECT_HOST_ENV;

const REDIS_PROTOCOL: &str = "redis";
const REDIS_LAYOUT: CoordinateLayout = CoordinateLayout::GroupNamespace;

/// Coordinates used by [`crate::RedisService::mesh`].
///
/// Discovery is intentionally one-shot. Once resolved, `RedisService` treats
/// the result exactly like [`crate::RedisService::single`]; changing registry
/// files does not replace the service topology.
#[derive(Clone, Debug)]
pub struct MeshConfig {
    group: String,
    namespace: String,
    socket_dir: PathBuf,
}

impl MeshConfig {
    /// Creates an exact Redis `(group, namespace)` coordinate.
    pub fn new(group: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            group: group.into(),
            namespace: namespace.into(),
            socket_dir: PathBuf::from(DEFAULT_SOCKS_DIR),
        }
    }

    /// Overrides the Breeze registry directory, primarily for composition and tests.
    #[must_use]
    pub fn with_socket_dir(mut self, directory: impl Into<PathBuf>) -> Self {
        self.socket_dir = directory.into();
        self
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn socket_dir(&self) -> &Path {
        &self.socket_dir
    }

    pub(crate) fn resolve(&self) -> RedisResult<Endpoint> {
        Registry::new(&self.socket_dir)
            .discover(REDIS_PROTOCOL, REDIS_LAYOUT, &self.group, &self.namespace)
            .map_err(|error| {
                RedisError::new(
                    ErrorKind::ClientError,
                    format!(
                        "cannot read Breeze registry {}: {error}",
                        self.socket_dir.display()
                    ),
                )
            })?
            .into_iter()
            .next()
            .ok_or_else(|| {
                RedisError::new(
                    ErrorKind::NoConnection,
                    format!(
                        "no Redis TCP endpoint for group={} namespace={} in {}",
                        self.group,
                        self.namespace,
                        self.socket_dir.display()
                    ),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_only_the_exact_redis_coordinate() {
        let directory = tempfile::tempdir().unwrap();
        for name in [
            "static.config.api.example.com+3+config+cloud+redis+feed+profiles@redis:9470@rs",
            "static.config.api.example.com+3+config+cloud+redis+other+profiles@redis:9471@rs",
            "static.config.api.example.com+3+config+cloud+redis+feed+profiles@mc:9472@cs",
        ] {
            std::fs::write(directory.path().join(name), []).unwrap();
        }

        let endpoint = MeshConfig::new("feed", "profiles")
            .with_socket_dir(directory.path())
            .resolve()
            .unwrap();
        assert_eq!(endpoint.host, "127.0.0.1");
        assert_eq!(endpoint.port, 9470);

        assert!(
            MeshConfig::new("missing", "profiles")
                .with_socket_dir(directory.path())
                .resolve()
                .is_err()
        );
    }
}
