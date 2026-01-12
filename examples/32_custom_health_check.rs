use async_trait::async_trait;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::background_service;

use pingora_load_balancing::{LoadBalancer, Backend};
use pingora_load_balancing::selection::RoundRobin;
use pingora_load_balancing::health_check::HealthCheck;
use pingora::protocols::l4::socket::SocketAddr;

use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct BodyMatchCheck {
    pub host: String,
    pub expected_string: String,
}

#[async_trait]
impl HealthCheck for BodyMatchCheck {
    async fn check(&self, target: &Backend) -> Result<()> {
        let addr = match &target.addr {
            SocketAddr::Inet(addr) => *addr,
            SocketAddr::Unix(_) => {
                return Err(Error::explain(ErrorType::InternalError, "Custom check only supports TCP backends"));
            }
        };

        let mut stream = match tokio::time::timeout(
            Duration::from_secs(1),
            TcpStream::connect(addr),
        ).await {
            Ok(Ok(s)) => s,
            _ => return Err(Error::explain(ErrorType::ConnectTimedout, "Connection timed out")),
        };

        let request = format!(
            "GET / HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            self.host
        );
        if let Err(e) = stream.write_all(request.as_bytes()).await {
            return Err(Error::explain(ErrorType::WriteError, e.to_string()));
        }

        let mut buffer = [0u8; 1024];
        let n = match tokio::time::timeout(
            Duration::from_secs(1),
            stream.read(&mut buffer),
        ).await {
            Ok(Ok(n)) => n,
            _ => return Err(Error::explain(ErrorType::ReadTimedout, "Read timed out")),
        };

        let response_text = String::from_utf8_lossy(&buffer[..n]);
        if response_text.contains(&self.expected_string) {
            Ok(())
        } else {
            warn!("Custom Check Failed for {:?}: Body did not contain '{}'", addr, self.expected_string);
            Err(Error::explain(ErrorType::Custom("InvalidBody"), "Body validation failed"))
        }
    }

    fn health_threshold(&self, _success: bool) -> usize {
        1
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
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "All upstreams are down"))?;
        info!("Routed to upstream: {:?}", upstream);
        let peer = Box::new(HttpPeer::new(
            upstream,
            false,
            "custom-check.cluster.local".to_string(),
        ));
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX
    ) -> Result<()> {
        upstream_request.insert_header("Host", "custom-check-cluster")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080",
        "172.28.0.21:8080",
    ])?;

    let hc = BodyMatchCheck {
        host: "localhost".to_string(),
        expected_string: "BLUE".to_string(),
    };

    upstreams.set_health_check(Box::new(hc));
    upstreams.health_check_frequency = Some(Duration::from_secs(1));
    upstreams.parallel_health_check = true;

    let background = background_service("custom_health_check", upstreams);
    let lb_ref = background.task();

    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6176");

    info!("Custom Health Check LB running on 0.0.0.0:6176");

    my_server.add_service(background);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}
