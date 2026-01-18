//! # Upstream Interface
//!
//! This module defines the contract for finding a backend server. By abstracting
//! "where to go", we can easily swap between Static IPs, DNS Discovery, or
//! complex Load Balancers without changing the core Gateway logic.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `Upstream` Trait**:
//!     * Must inherit `Sync + Send` to be safe for global sharing.
//!
//! 2.  **Define `select_peer` Method**:
//!     * Signature: `async fn select_peer(&self, session: &mut Session, ctx: &mut GatewayContext) -> Result<Box<HttpPeer>>`
//!     * **Input**: Access to the session (for headers/path) and Context (for sticky session keys).
//!     * **Output**: A `pingora::upstreams::peer::HttpPeer` struct, configured with the target IP, SNI, and TLS settings.
//!     * **Error**: Returns our `Error` type (e.g., `GatewayError::UpstreamUnavailable`).
use async_trait::async_trait;
use pingora::upstreams::peer::HttpPeer;
use pingora::proxy::Session;

use crate::context::GatewayContext;

#[async_trait]
pub trait Upstream: Send + Sync {
    async fn select_peer(
        &self,
        session: &mut Session,
        ctx: &mut GatewayContext,
    ) -> pingora::Result<Box<HttpPeer>>;
}