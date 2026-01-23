use std::sync::Arc;
use std::time::Duration;
use std::collections::{BTreeSet, HashMap};
use std::net::ToSocketAddrs;
use std::path::PathBuf;

use async_trait::async_trait;
use http::Extensions;
use log::{error, info};
use tokio::net::lookup_host;

use pingora::prelude::*;

use pingora::lb::{LoadBalancer, Backends, Backend};
use pingora::lb::selection::{RoundRobin, BackendSelection, BackendIter};
use pingora::lb::selection::consistent::KetamaHashing;
use pingora::lb::discovery::ServiceDiscovery;
use pingora::lb::health_check::{TcpHealthCheck, HealthCheck};
use pingora::protocols::l4::socket::SocketAddr;
use pingora::services::listening::Service;
use pingora::services::background::BackgroundService;

use crate::config::{UpstreamConf, UpstreamSource, LoadBalancerSelection, HealthCheckConf};
use crate::upstream::Upstream;
use crate::upstreams::load_balancer::LoadBalancerCluster;
use crate::upstreams::static_upstream::StaticUpstream;

pub struct DnsDiscovery {
    pub hostname: String,
}

#[async_trait]
impl ServiceDiscovery for DnsDiscovery {
    async fn discover(&self) -> Result<(BTreeSet<Backend>, HashMap<u64, bool>)> {

        let mut addrs = lookup_host(&self.hostname).await
            .map_err(|_e| pingora::Error::new(ErrorType::Custom("DNSResolutionFailed")))?;


        let mut upstreams: BTreeSet<Backend> = BTreeSet::new();

        for addr in addrs {
            let backend = Backend {
                addr: SocketAddr::Inet(addr),
                weight: 1,
                ext: Extensions::new(),
            };
            upstreams.insert(backend);
        }

        Ok((upstreams, HashMap::new()))
    }
}

pub struct FileDiscovery {
    pub path: PathBuf,
}

#[async_trait]
impl ServiceDiscovery for FileDiscovery {
    async fn discover(&self) -> Result<(BTreeSet<Backend>, HashMap<u64, bool>)> {
        let contents = match tokio::fs::read_to_string(&self.path).await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to read upstream file: {}", e);
                return Err(Error::explain(ErrorType::InternalError, e.to_string()))
            },
        };

        let mut upstreams = BTreeSet::new();
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with("#") {
                continue;
            }
            match lookup_host(line).await {
                Ok(mut addrs) => {
                    if let Some(addr) = addrs.next() {
                        let backend = Backend {
                            addr: SocketAddr::Inet(addr),
                            weight: 1,
                            ext: Extensions::new()
                        };
                        upstreams.insert(backend);
                    }
                },
                Err(e) => error!("Failed to parse address '{}': {}", line, e),
            }
        }
        Ok((upstreams, HashMap::new()))
    }
}