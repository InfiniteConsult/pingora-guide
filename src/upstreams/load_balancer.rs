//! # Load Balanced Cluster
//!
//! A robust implementation of the `Upstream` trait that wraps Pingora's native
//! `LoadBalancer` struct. This component handles health-aware traffic distribution.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `LoadBalancedCluster` Struct**:
//!     * Fields:
//!         * `lb`: `Arc<LoadBalancer<RoundRobin>>` - The core balancing logic engine.
//!         * `tls`: `bool` - Global setting for this cluster (e.g., "all backends are HTTPS").
//!         * `sni`: `String` - Global SNI setting.
//!         * `client_cert`: `Option<Arc<CertKey>>` - Mutual TLS if enabled
//!         * `options`: `ClusterOptions` - Options for the HTTP Peers
//!
//! 2.  **Implement `Upstream` Trait**:
//!     * **`select_peer`**:
//!         * Call `self.lb.select(b"", 256)`.
//!         * **Error Handling**: If `select()` returns `None` (empty or unhealthy pool),
//!           return `Err(Error::Gateway(GatewayError::UpstreamUnavailable))`.
//!         * **Success**: Convert the selected `Backend` into a `Box<HttpPeer>`.
//!
//! 3.  **Note**: This struct assumes the `LoadBalancer` is already initialized and populated.
//!     It relies on external `BackgroundService`s (Lesson 30) to update the health status.
use async_trait::async_trait;
use std::sync::Arc;
use pingora::lb::LoadBalancer;
use pingora::lb::selection::{BackendIter, BackendSelection};
use pingora::prelude::{HttpPeer, Session};
use pingora::utils::tls::CertKey;
use pingora::upstreams::peer::ALPN;
use crate::context::{GatewayContext, RequestMeta};
use crate::error::{Result, GatewayError, PingoraGuideError};
use crate::config::{ClusterOptions, HashSource};
use crate::upstream::Upstream;

pub struct LoadBalancerCluster<S> {
    lb: Arc<LoadBalancer<S>>,
    tls: bool,
    sni: String,
    client_cert: Option<Arc<CertKey>>,
    options: ClusterOptions,
    hash_source: HashSource,
}

impl<S> LoadBalancerCluster<S> {
    pub fn new(
        lb: Arc<LoadBalancer<S>>,
        tls: bool,
        sni: String,
        client_cert: Option<Arc<CertKey>>,
        options: Option<ClusterOptions>,
        hash_source: Option<HashSource>,
    ) -> Self {
        let hash_source = match hash_source {
            Some(h) => match h {
                HashSource::Cookie { name } => HashSource::Cookie { name: name  + "=" },
                _ => h
            },
            None => HashSource::None
        };

        Self {
            lb,
            tls,
            sni,
            client_cert,
            options: options.unwrap_or_default(),
            hash_source
        }
    }
}

#[async_trait]
impl<S> Upstream for LoadBalancerCluster<S>
where
    S: BackendSelection + Send + Sync + 'static,
    S::Iter: BackendIter
{
    async fn select_peer(&self, session: &mut Session, ctx: &mut GatewayContext) -> Result<Box<HttpPeer>> {
        let key_str = match &self.hash_source {
            HashSource::None => String::new(),
            HashSource::ClientIp => {
                let client_ip = match session.client_addr() {
                    Some(ip) => {
                        if let Some(socket_ip) = ip.as_inet() {
                            socket_ip.ip().to_string()
                        } else {
                            uuid::Uuid::new_v4().to_string()
                        }
                    },
                    None => String::new(),
                };
                client_ip
            },
            HashSource::Cookie { name } => {
                let cookie_key = name;
                let mut session_id = String::new();
                if let Some(header_val) = session.req_header().headers.get("Cookie") {
                    if let Ok(cookie_str) = header_val.to_str() {
                        for part in cookie_str.split(";") {
                            let part = part.trim();
                            if part.starts_with(cookie_key) {
                                session_id = part[cookie_key.len()..].to_string();
                                break;
                            }
                        }
                    }
                }
                session_id
            },
            HashSource::Uri => {
                session.req_header().uri.path().to_string()

            }
            HashSource::Header { name } => {
                let header_key = name;
                let mut header_val_str = String::new();
                if let Some(header_val) = session.req_header().headers.get(header_key) {
                    if let Ok(header_str) = header_val.to_str() {
                        header_val_str = header_str.to_string();
                    }
                }
                header_val_str
            }
        };
        let key = key_str.as_bytes();

        // probably set configurations for max_iterations somewhere.
        let upstream = self.lb.select(key, 256).ok_or_else(|| PingoraGuideError::Gateway(GatewayError::UpstreamUnavailable))?;

        if let Some(meta) = ctx.get_mut::<RequestMeta>() {
            meta.peer_addr = Some(upstream.addr.clone());
            meta.sni = Some(self.sni.clone());
        }

        let mut peer = Box::new(HttpPeer::new(
            upstream,
            self.tls,
            self.sni.clone()
        ));

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

        Ok(peer)
    }
}
