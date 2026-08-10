//! The mode-agnostic [`Client`] — a unified proxy over the two access modes.
//!
//! `Client` is an enum over [`crate::sidecar::SidecarClient`]
//! (mesh access) and [`crate::direct::DirectClient`] (direct
//! backend access). It implements [`ConnectionLike`] by delegating to the
//! wrapped client, so the full [`Commands`](crate::Commands) surface works
//! uniformly regardless of which mode a resource uses — useful for code
//! paths that serve resources of mixed modes.

use crate::cmd::Cmd;
use crate::connection::{ConnectionLike, RedisFuture};
use crate::direct::DirectClient;
use crate::pipeline::Pipeline;
use crate::sidecar::SidecarClient;
use crate::types::Value;

/// A client proxy over either access mode.
#[derive(Clone)]
pub enum Client {
    /// Mesh access mode (see [`crate::sidecar`]).
    Sidecar(SidecarClient),
    /// Direct backend access mode (see [`crate::direct`]).
    Direct(DirectClient),
}

impl Client {
    /// The wrapped sidecar client, if in mesh mode.
    pub fn as_sidecar(&self) -> Option<&SidecarClient> {
        match self {
            Client::Sidecar(client) => Some(client),
            Client::Direct(_) => None,
        }
    }

    /// The wrapped direct client, if in direct mode.
    pub fn as_direct(&self) -> Option<&DirectClient> {
        match self {
            Client::Direct(client) => Some(client),
            Client::Sidecar(_) => None,
        }
    }

    /// Whether the underlying pool(s) currently serve requests.
    pub fn is_available(&self) -> bool {
        match self {
            Client::Sidecar(client) => client.is_available(),
            Client::Direct(client) => client.is_available(),
        }
    }
}

impl From<SidecarClient> for Client {
    fn from(client: SidecarClient) -> Self {
        Client::Sidecar(client)
    }
}

impl From<DirectClient> for Client {
    fn from(client: DirectClient) -> Self {
        Client::Direct(client)
    }
}

impl ConnectionLike for Client {
    fn req_command<'a>(&'a self, command: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move {
            match self {
                Client::Sidecar(client) => client.req_command(command).await,
                Client::Direct(client) => client.req_command(command).await,
            }
        })
    }

    fn req_pipeline<'a>(
        &'a self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            match self {
                Client::Sidecar(client) => client.req_pipeline(pipeline, offset, count).await,
                Client::Direct(client) => client.req_pipeline(pipeline, offset, count).await,
            }
        })
    }
}
