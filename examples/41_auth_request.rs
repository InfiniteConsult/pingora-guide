use async_trait::async_trait;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;

use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use std::sync::Arc;

const SECRET_TOKEN: &[u8] = b"Bearer super-secret-token";

pub struct AuthProxy(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for AuthProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        let auth_header = session.req_header().headers.get("Authorization");

        match auth_header {
            None => {
                warn!("Auth Failed: Missing Authorization header");
                session.respond_error(401).await?;
                return Ok(true);
            }
            Some(value) => {
                if value.as_bytes() != SECRET_TOKEN {
                    warn!("Auth Failed: Invalid Token");
                    session.respond_error(403).await?;
                    return Ok(true);
                }
            }
        }

        info!("Auth Success: Valid Token");
        Ok(false)
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        let peer = Box::new(HttpPeer::new(upstream, false, "auth-protected.cluster".to_string()));
        Ok(peer)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080",
        "172.28.0.21:8080",
    ])?;

    let mut my_proxy = http_proxy_service(&my_server.configuration, AuthProxy(Arc::new(upstreams)));
    my_proxy.add_tcp("0.0.0.0:6185");

    info!("Auth Proxy running on 0.0.0.0:6185");
    info!("Required Token: Bearer super-secret-token");

    my_server.add_service(my_proxy);
    my_server.run_forever();
}