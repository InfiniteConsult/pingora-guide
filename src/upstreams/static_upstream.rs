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
//!         * `client_cert`: `Option<Arc<CertKey>>` - Mutual TLS if enabled
//!         * `options`: `ClusterOptions` - Options for the HTTP Peers
//!
//! 2.  **Implement `Upstream` Trait**:
//!     * **`select_peer`**:
//!         * Construct a `Box<HttpPeer>` using the stored configuration.
//!         * This is a "dumb" connector; it does no load balancing or health checking.
//!         * Useful for admin APIs or simple sidecars.
use std::sync::Arc;
use async_trait::async_trait;
use pingora::prelude::Session;
use pingora::upstreams::peer::HttpPeer;
use pingora::utils::tls::CertKey;
use pingora::upstreams::peer::ALPN;
use crate::context::GatewayContext;
use crate::upstream::{Upstream, ClusterOptions};
use crate::error::Result;


pub struct StaticUpstream {
    addr: (String, u16),
    tls: bool,
    sni: String,
    client_cert: Option<Arc<CertKey>>,
    options: ClusterOptions,
}

impl StaticUpstream {
    pub fn new(
        addr: (String, u16),
        tls: bool,
        sni: String,
        client_cert: Option<Arc<CertKey>>,
        options: Option<ClusterOptions>
    ) -> Self {
        StaticUpstream { addr, tls, sni, client_cert, options: options.unwrap_or_default() }
    }
}

#[async_trait]
impl Upstream for StaticUpstream {
    async fn select_peer(&self, _session: &mut Session, _ctx: &mut GatewayContext) -> Result<Box<HttpPeer>> {
        let mut peer = HttpPeer::new(
            self.addr.clone(),
            self.tls,
            self.sni.clone(),
        );

        peer.options.connection_timeout = Some(self.options.connect_timeout);
        peer.options.read_timeout = Some(self.options.read_timeout);
        peer.options.write_timeout = Some(self.options.write_timeout);
        peer.options.idle_timeout = self.options.idle_timeout;

        if self.options.enable_h2 {
            peer.options.alpn = ALPN::H2H1;
        } else {
            peer.options.alpn = ALPN::H1;
        }

        peer.options.verify_hostname = self.options.verify_hostname;

        if let Some(cert) = &self.client_cert {
            peer.client_cert_key = Some(cert.clone());
        }
        Ok(Box::new(peer))
    }
}

