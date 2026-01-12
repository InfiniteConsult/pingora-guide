use async_trait::async_trait;
use log::{info, error};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::server::ShutdownWatch;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::{background_service, BackgroundService};

use pingora_load_balancing::{Backends, LoadBalancer, Backend};
use pingora_load_balancing::discovery::ServiceDiscovery;
use pingora_load_balancing::selection::RoundRobin;
use pingora::protocols::l4::socket::SocketAddr;

use std::sync::Arc;
use std::time::Duration;
use std::collections::{BTreeSet, HashMap};
use std::net::ToSocketAddrs;
use tokio::sync::RwLock;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use http::Extensions;

#[derive(Clone)]
pub struct UpstreamState {
    pub upstreams: Arc<RwLock<BTreeSet<Backend>>>,
}

pub struct InMemoryDiscovery {
    pub state: UpstreamState,
}

#[async_trait]
impl ServiceDiscovery for InMemoryDiscovery {
    async fn discover(&self) -> Result<(BTreeSet<Backend>, HashMap<u64, bool>)> {
        let guard = self.state.upstreams.read().await;
        Ok((guard.clone(), HashMap::new()))
    }
}

pub struct AdminApiService {
    pub state: UpstreamState,
}

#[async_trait]
impl BackgroundService for AdminApiService {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let listener = TcpListener::bind("0.0.0.0:9090").await.expect("Failed to bind Admin Port 9090");
        info!("Admin API listening on 0.0.0.0:9090");

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    info!("Admin API shutting down...");
                    return;
                }
                res = listener.accept() => {
                    match res {
                        Ok((mut socket, addr)) => {
                            info!("Admin connection from {}", addr);
                            let state = self.state.clone();

                            tokio::spawn(async move {
                                let mut buf = [0u8; 1024];
                                let n = match socket.read(&mut buf).await {
                                    Ok(n) if n > 0 => n,
                                    _ => return,
                                };

                                let req_str = String::from_utf8_lossy(&buf[..n]);
                                let body = if let Some(idx) = req_str.find("\r\n\r\n") {
                                    req_str[idx+4..].trim()
                                } else {
                                    req_str.trim()
                                };

                                if !body.is_empty() {
                                    if let Ok(mut addrs) = body.to_socket_addrs() {
                                        if let Some(new_addr) = addrs.next() {
                                            let mut guard = state.upstreams.write().await;
                                            guard.clear();
                                            guard.insert(Backend {
                                                addr: SocketAddr::Inet(new_addr),
                                                weight: 1,
                                                ext: Extensions::new(),
                                            });
                                            info!("Admin: Hot-swapped upstream to {}", new_addr);
                                            let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK").await;
                                        } else {
                                            let _ = socket.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\nInvalid IP").await;
                                        }
                                    } else {
                                        error!("Admin: Failed to parse IP: {}", body);
                                        let _ = socket.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\nParse Error").await;
                                    }
                                }
                            });
                        }
                        Err(e) => error!("Admin accept error: {}", e),
                    }
                }
            }
        }
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
            "hot-swap.cluster".to_string()
        ));

        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "hot-swap-cluster")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let initial_addr = "172.28.0.20:8080".to_socket_addrs()?.next().unwrap();
    let mut initial_set = BTreeSet::new();
    initial_set.insert(Backend {
        addr: SocketAddr::Inet(initial_addr),
        weight: 1,
        ext: Extensions::new(),
    });

    let state = UpstreamState {
        upstreams: Arc::new(RwLock::new(initial_set)),
    };

    let discovery = InMemoryDiscovery { state: state.clone() };
    let backends = Backends::new(Box::new(discovery));
    let mut upstreams = LoadBalancer::from_backends(backends);
    upstreams.update_frequency = Some(Duration::from_millis(500));

    let lb_service = background_service("lb_updater", upstreams);
    let lb_ref = lb_service.task();

    let admin_task = AdminApiService { state: state.clone() };
    let admin_service = background_service("admin_api", admin_task);

    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6179");

    info!("Hot-Swap Proxy running on 0.0.0.0:6179");
    info!("Admin API running on 0.0.0.0:9090");

    my_server.add_service(lb_service);
    my_server.add_service(admin_service);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}