//! # Static Upstream Implementation
//!
//! A simple implementation of the `Upstream` trait that routes all traffic to a
//! single, hardcoded IP address and port.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `StaticUpstream` Struct**:
//!     * Fields:
//!         * `addr`: `(String, u16)` - The destination IP and port.
//!         * `tls`: `bool` - Whether to use HTTPS.
//!         * `sni`: `String` - The SNI hostname to present during handshake.
//!
//! 2.  **Implement `Upstream` Trait**:
//!     * **`select_peer`**:
//!         * Construct a `Box<HttpPeer>` using the stored configuration.
//!         * This is a "dumb" connector; it does no load balancing or health checking.
//!         * Useful for admin APIs or simple sidecars.
use async_trait::async_trait;
use pingora::prelude::Session;
use pingora::upstreams::peer::HttpPeer;
use crate::context::GatewayContext;
use crate::upstream::Upstream;
use crate::error::Result;


pub struct StaticUpstream {
    addr: (String, u16),
    tls: bool,
    sni: String,
}

impl StaticUpstream {
    pub fn new(addr: (String, u16), tls: bool, sni: String) -> Self {
        StaticUpstream { addr, tls, sni }
    }
}

#[async_trait]
impl Upstream for StaticUpstream {
    async fn select_peer(&self, _session: &mut Session, _ctx: &mut GatewayContext) -> Result<Box<HttpPeer>> {
        Ok(Box::new(HttpPeer::new(
            self.addr.clone(),
            self.tls,
            self.sni.clone(),
        )))
    }
}

