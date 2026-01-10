use async_trait::async_trait;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use std::time::Duration;
use pingora::proxy::FailToProxy;

pub struct TimeoutProxy;

#[async_trait]
impl ProxyHttp for TimeoutProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let path = session.req_header().uri.path();

        let (addr, sni, timeout) = if path == "/timeout" {
            info!("Routing to Blackhole IP (192.0.2.1) to force timeout...");
            (("192.0.2.1", 80), "blackhole.local", Duration::from_millis(100))
        } else {
            // Case B: The Happy Path
            info!("Routing to Blue (Standard Timeout)");
            (("172.28.0.20", 8080), "blue.pingora.local", Duration::from_secs(2))
        };

        let mut peer = Box::new(HttpPeer::new(addr, false, sni.to_string()));

        peer.options.connection_timeout = Some(timeout);
        peer.options.read_timeout = Some(Duration::from_secs(2));

        Ok(peer)
    }

    async fn fail_to_proxy(
        &self,
        session: &mut Session,
        e: &Error,
        _ctx: &mut Self::CTX
    ) -> FailToProxy
    where
        Self::CTX: Send + Sync,
    {
        match e.etype {
            ErrorType::ConnectTimedout | ErrorType::ReadTimedout => {
                warn!("Custom Error Handler: Connection Timed Out!");
                let _ = session.respond_error(504).await;
                FailToProxy {
                    error_code: 504,
                    can_reuse_downstream: false,
                }
            }
            _ => {
                warn!("Fail to proxy: {:?}", e);
                let _ = session.respond_error(502).await;
                FailToProxy{
                    error_code: 502,
                    can_reuse_downstream: false,
                }
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, TimeoutProxy);
    my_proxy.add_tcp("0.0.0.0:6163");

    info!("Timeout Config Proxy running on 0.0.0.0:6163");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}