use std::sync::Arc;
use std::time::Duration;
use std::collections::{BTreeSet, HashMap};
use std::net::ToSocketAddrs;
use std::path::PathBuf;

use async_trait::async_trait;
use http::Extensions;
use log::{error, info};
use tokio::net::lookup_host;

// ErrorType variants exposed directly in prelude. IDE gives redundant prefix warnings
use pingora::prelude::*;

use pingora::lb::{LoadBalancer, Backends, Backend};
use pingora::lb::selection::{RoundRobin, BackendSelection, BackendIter};
use pingora::lb::selection::consistent::KetamaHashing;
use pingora::lb::discovery::ServiceDiscovery;
use pingora::lb::health_check::{TcpHealthCheck, HealthCheck};
use pingora::protocols::l4::socket::SocketAddr;
use pingora::services::listening::Service;
use pingora::services::background::BackgroundService;

use crate::config::{UpstreamConf, UpstreamSource, LoadBalancerSelection, HealthCheckConf, FileFormat, BackendConf};
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
            .map_err(|_e| pingora::Error::new(Custom("DNSResolutionFailed")))?;


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
    pub format: FileFormat,
}

#[async_trait]
impl ServiceDiscovery for FileDiscovery {
    async fn discover(&self) -> Result<(BTreeSet<Backend>, HashMap<u64, bool>)> {
        let content = tokio::fs::read_to_string(&self.path).await
            .map_err(|e| Error::explain(InternalError, e.to_string()))?;

        let backend_confs: Vec<BackendConf> = match self.format {
            FileFormat::Yaml => serde_yaml::from_str(&content)
                .map_err(|e| Error::explain(InternalError, e.to_string()))?,
            FileFormat::Json => serde_json::from_str(&content)
                .map_err(|e| Error::explain(InternalError, e.to_string()))?,
        };

        let mut upstreams = BTreeSet::new();
        for b_conf in backend_confs {
            let socket_addrs = b_conf.address.to_socket_addrs()
                .map_err(|e| Error::explain(InternalError, e.to_string()))?;

            for addr in socket_addrs {
                let  backend = Backend {
                    addr: SocketAddr::Inet(addr),
                    weight: b_conf.weight as usize,
                    ext: Extensions::new(),
                };
                upstreams.insert(backend);
            }
        }
        Ok((upstreams, HashMap::new()))
    }
}

pub struct CommandHealthCheck {
    pub command: String,
    pub timeout: Duration,
    pub args: Vec<String>,
    failure_threshold: usize,
    success_threshold: usize,
}

impl CommandHealthCheck {
    fn new(
        command: String,
        timeout: Duration,
        args: Vec<String>,
        failure_threshold: usize,
        success_threshold: usize
    ) -> Self {
        Self {
            command,
            timeout,
            args,
            failure_threshold,
            success_threshold
        }
    }
}

#[async_trait]
impl HealthCheck for CommandHealthCheck {
    async fn check(&self, target: &Backend) -> Result<()> {
        let cmd_future = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&self.command)
            .env("TARGET_IP", target.addr.to_string())
            .output();

        let output = match tokio::time::timeout(self.timeout, cmd_future).await {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => return Err(Error::explain(Custom("CommandFailed"), e.to_string())),
            Err(_) => return Err(Error::explain(ReadTimedout, "Health check timed out")),
        };

        if output.status.success() {
            Ok(())
        } else {
            Err(Error::explain(Custom("HealthCheckFailed"), "Non-zero exit code"))
        }
    }

    fn health_threshold(&self, success: bool) -> usize {
        if success {
            self.success_threshold
        } else {
            self.failure_threshold
        }
    }

}