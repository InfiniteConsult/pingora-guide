use std::sync::Arc;
use std::time::Duration;
use std::collections::{BTreeSet, HashMap};
use std::net::ToSocketAddrs;
use std::path::PathBuf;

use async_trait::async_trait;
use http::Extensions;
use tokio::net::lookup_host;

use pingora::prelude::*;

use pingora::lb::{LoadBalancer, Backends, Backend};
use pingora::lb::selection::{RoundRobin, BackendSelection, BackendIter, Random};
use pingora::lb::selection::consistent::KetamaHashing;
use pingora::lb::discovery::{ServiceDiscovery, Static};
use pingora::lb::health_check::{TcpHealthCheck, HealthCheck};
use pingora::protocols::l4::socket::SocketAddr;
use pingora::services::Service as ServiceTrait;
use pingora_load_balancing::health_check::HttpHealthCheck;
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

fn finalize_lb<S>(
    mut lb: LoadBalancer<S>,
    conf: &UpstreamConf
) -> (Arc<LoadBalancer<S>>, Vec<Box<dyn ServiceTrait>>)
where
    S: BackendSelection + 'static + Send + Sync,
    S::Iter: BackendIter,
{
    if let Some(hc_conf) = &conf.health_check {
        match hc_conf {
            HealthCheckConf::Tcp(common) => {
                let mut hc = TcpHealthCheck::new();
                hc.peer_template.options.connection_timeout = Some(common.timeout);
                hc.consecutive_success = common.consecutive_success;
                hc.consecutive_failure = common.consecutive_failure;

                lb.set_health_check(hc);
                lb.health_check_frequency = Some(common.interval);
            },
            HealthCheckConf::Http { common, path, expected_status: _ } => {
                let host = match &conf.source {
                    UpstreamSource::Dns { hostname, .. } => hostname.clone(),
                    _ => "localhost".to_string(),
                };

                let mut hc = HttpHealthCheck::new(&host, conf.options.tls);
                hc.peer_template.options.connection_timeout = Some(common.timeout);
                hc.consecutive_success = common.consecutive_success;
                hc.consecutive_failure = common.consecutive_failure;

                if let Ok(uri) = path.parse::<http::Uri>() {
                    hc.req.set_uri(uri);
                }

                lb.set_health_check(Box::new(hc));
                lb.health_check_frequency = Some(common.interval);
            },
            HealthCheckConf::Custom { common, command } => {
                let hc = CommandHealthCheck::new(
                    command.clone(),
                    common.timeout,
                    vec![],
                    common.consecutive_failure,
                    common.consecutive_success
                );
                lb.set_health_check(Box::new(hc));
                lb.health_check_frequency = Some(common.interval);
            }
        }
    }

    lb.parallel_health_check = true;

    let background = background_service(format!("lb-{}", conf.id).as_str(), lb);

    let lb_arc = background.task();

    (lb_arc, vec![Box::new(background)])
}

fn make_lb_instance<S>(
    conf: &UpstreamConf,
) -> Result<(Box<dyn Upstream>, Vec<Box<dyn ServiceTrait>>)>
where
    S: BackendSelection + 'static + Send + Sync,
    S::Iter: BackendIter,
{
    let mut lb: LoadBalancer<S> = match &conf.source {
        UpstreamSource::Static { backends } => {
            let mut upstreams = BTreeSet::new();
            for b_conf in backends {
                let addrs = b_conf.address.to_socket_addrs()
                    .map_err(|e| Error::explain(ErrorType::InternalError, e.to_string()))?;

                for addr in addrs {
                    upstreams.insert(Backend {
                        addr: SocketAddr::Inet(addr),
                        weight: b_conf.weight as usize,
                        ext: Extensions::new(),
                    });
                }
            }
            let discovery = Static::new(upstreams);
            let backends = Backends::new(discovery);
            LoadBalancer::from_backends(backends)
        },
        UpstreamSource::Dns { hostname, refresh_interval } => {
            let discovery = DnsDiscovery {
                hostname: hostname.clone(),
            };
            let backends = Backends::new(Box::new(discovery));
            let mut lb = LoadBalancer::from_backends(backends);
            lb.update_frequency = Some(*refresh_interval);
            lb
        },
        UpstreamSource::File { path, format, refresh_interval } => {
            let discovery = FileDiscovery {
                path: PathBuf::from(path),
                format: format.clone(),
            };
            let backends = Backends::new(Box::new(discovery));
            let mut lb = LoadBalancer::from_backends(backends);
            lb.update_frequency = Some(*refresh_interval);
            lb
        },
        UpstreamSource::Uds { .. } => {
            return Err(Error::explain(
                ErrorType::Custom("InvalidConfiguration"),
                "Unix Domain Sockets cannot be used with Load Balancer Selection algorithms."
            ));
        }
    };

    let (shared_lb, services) = finalize_lb(lb, conf);

    let cluster = LoadBalancerCluster::new(
        shared_lb,
        conf.options.tls,
        conf.options.sni.clone().unwrap_or_default(),
        None,
        Some(conf.options.clone()),
        Some(conf.hash_source.clone())
    );

    Ok((Box::new(cluster), services))
}

pub fn make_upstream(
    conf: &UpstreamConf
) -> Result<(Box<dyn Upstream>, Vec<Box<dyn ServiceTrait>>)> {
    if let UpstreamSource::Uds { path } = &conf.source {
        let upstream = StaticUpstream::new_uds(path.clone(), conf.options.clone());
        return Ok((Box::new(upstream), vec![]));
    }

    match conf.selection {
        LoadBalancerSelection::RoundRobin => make_lb_instance::<RoundRobin>(conf),
        LoadBalancerSelection::Consistent => make_lb_instance::<KetamaHashing>(conf),
        LoadBalancerSelection::Random => {
            make_lb_instance::<Random>(conf)
        }
    }
}