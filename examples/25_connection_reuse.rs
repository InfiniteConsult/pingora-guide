use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::protocols::Digest;
use std::time::Duration;

pub struct ReusingProxy;

pub struct ReusingCtx {
    pub is_reused: bool,
}

#[async_trait]
impl ProxyHttp for ReusingProxy {
    type CTX = ReusingCtx;

    fn new_ctx(&self) -> Self::CTX {
        ReusingCtx { is_reused: false }
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let addr = ("172.28.0.20", 8080);
        let mut peer = Box::new(HttpPeer::new(addr, false, "blue.pingora.local".to_string()));

        peer.options.idle_timeout = Some(Duration::from_secs(30));
        peer.options.connection_timeout = Some(Duration::from_secs(1));

        Ok(peer)
    }

    async fn connected_to_upstream(
        &self,
        _session: &mut Session,
        reused: bool,
        _peer: &HttpPeer,
        #[cfg(unix)] _fd: std::os::unix::io::RawFd,
        #[cfg(windows)] _sock: std::os::windows::io::RawSocket,
        _digest: Option<&Digest>,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.is_reused = reused;
        Ok(())
    }

    async fn logging(
        &self,
        _session: &mut Session,
        _e: Option<&pingora::Error>,
        ctx: &mut Self::CTX,
    ) {
        info!("Request Complete. Reused Connection: {}", ctx.is_reused);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, ReusingProxy);
    my_proxy.add_tcp("0.0.0.0:6169");

    info!("Connection Reuse Demo running on 0.0.0.0:6169");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}