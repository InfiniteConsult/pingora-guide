use async_trait::async_trait;
use log::{info, error};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::background_service;

use pingora_load_balancing::{Backends, LoadBalancer, Backend};
use pingora_load_balancing::discovery::ServiceDiscovery;
use pingora_load_balancing::selection::RoundRobin;
use pingora::protocols::l4::socket::SocketAddr;

use std::sync::Arc;
use std::time::Duration;
use std::path::PathBuf;
use std::collections::{BTreeSet, HashMap};
use std::net::ToSocketAddrs;
use http::Extensions;

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
            match line.to_socket_addrs() {
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

pub struct LB(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        info!("Routed to upstream: {:?}", upstream);

        let peer = Box::new(HttpPeer::new(
            upstream,
            false,
            "file-discovery.cluster".to_string()
        ));

        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "file-discovery-cluster")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let discovery = FileDiscovery {
        path: PathBuf::from("conf/upstreams.txt"),
    };

    let backends = Backends::new(Box::new(discovery));
    let mut upstreams = LoadBalancer::from_backends(backends);
    upstreams.update_frequency = Some(Duration::from_secs(1));

    let background = background_service("file_discovery", upstreams);
    let lb_ref = background.task();

    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6178");

    info!("File-Based Discovery LB running on 0.0.0.0:6178");
    info!("Watching file: conf/upstreams.txt");

    my_server.add_service(background);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}