use async_trait::async_trait;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;
use pingora::http::ResponseHeader;

use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use std::sync::Arc;

const EXPECTED_AUTH: &[u8] = b"Basic YWRtaW46cGFzc3dvcmQ=";

pub struct BasicAuthProxy(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for BasicAuthProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        let auth_header = session.req_header().headers.get("Authorization");

        match auth_header {
            None => {
                warn!("Auth Failed: Missing Header. Sending Challenge");
                let mut header = ResponseHeader::build(401, Some(3)).unwrap();
                header.insert_header("WWW-Authenticate", "Basic realm=\"PingoraProxy\"").unwrap();
                header.insert_header("Content-Length", "0").unwrap();
                session.write_response_header(Box::new(header), true).await?;
                return Ok(true)
            }
            Some(value) => {
                if value.as_bytes() != EXPECTED_AUTH {
                    warn!("Auth Failed: Wrong Credentials");
                    session.respond_error(403).await?;
                    return Ok(true);
                }
            }
        }
        info!("Auth Success: admin:password verified");
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

        let peer = Box::new(HttpPeer::new(upstream, false, "basic-auth.cluster".to_string()));
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

    let mut my_proxy = http_proxy_service(&my_server.configuration, BasicAuthProxy(Arc::new(upstreams)));
    my_proxy.add_tcp("0.0.0.0:6186");

    info!("Basic Auth Proxy running on 0.0.0.0:6186");
    info!("Credentials: admin / password");

    my_server.add_service(my_proxy);
    my_server.run_forever();
}