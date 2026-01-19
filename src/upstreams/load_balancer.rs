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
use crate::context::GatewayContext;
use crate::error::{Result, GatewayError};
use crate::upstream::{ClusterOptions, HashSource};
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
                HashSource::Cookie(s) => HashSource::Cookie(s + "="),
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
    S: BackendSelection + Send + Sync,
    S::Iter: BackendIter
{
    async fn select_peer(&self, session: &mut Session, ctx: &mut GatewayContext) -> Result<Box<HttpPeer>> {
        let key_str = match &self.hash_source {
            HashSource::None => String::new(),
            HashSource::ClientIp => {
                let client_ip = match session.client_addr() {
                    Some(ip) => {
                        let inet_ip = ip.as_inet().unwrap();
                        inet_ip.to_string()
                    },
                    None => String::new(),
                };
                client_ip
            },
            HashSource::Cookie(cookie_key) => {
                let mut session_id = String::new();
                if let Some(header_val) = session.req_header().headers.get("Cookie") {
                    if let Ok(cookie_str) = header_val.to_str() {
                        for part in cookie_str.split(";") {
                            let part = part.trim();
                            if part.starts_with(cookie_key) {
                                 session_id = part[cookie_key.len()..].to_string()
                            }
                        }
                    }
                }
                session_id
            },
            _ => String::new(),
        };
        todo!()
    }
}
