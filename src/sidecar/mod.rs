//! Mesh access mode: reach Redis **through the local breeze mesh agent**.
//!
//! The mesh publishes one sock config file per resource in a local directory;
//! [`discovery`] parses the file name to resolve the namespace's local
//! endpoint (no fetch/register step), and the client speaks RESP over it —
//! sharding and backend failover are the mesh's job.
//!
//! Entry points:
//!
//! - [`SidecarClient`] — the pooled, retrying client
//!   ([`SidecarClient::connect("namespace")`](SidecarClient::connect));
//! - [`MeshConfig`] — namespace/group/transport/pool/timeout settings;
//! - [`MeshRouting`] — `with_hashkey` / `broadcast` / `at_master` routing
//!   preambles understood by the mesh.
//!
//! For direct backend access (no mesh), see [`crate::direct`].

pub mod client;
pub mod config;
pub mod discovery;
pub mod routing;

pub use client::SidecarClient;
pub use config::{MeshConfig, Transport};
pub use discovery::Endpoint;
pub use routing::{MeshRouting, Prefixed};
