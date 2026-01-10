use async_trait::async_trait;
use bytes::Bytes;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

pub struct BodyInspector;

pub struct BodyCtx {
    buffer: Vec<u8>,
}

#[async_trait]
impl ProxyHttp for BodyInspector {
    type CTX =BodyCtx;

    fn new_ctx(&self) -> Self::CTX {
        BodyCtx { buffer: Vec::new() }
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let peer = Box::new(HttpPeer::new(
            ("172.28.0.20", 8080),
            false,
            "blue.pingora.local".to_string(),
        ));
        Ok(peer)
    }

    async fn request_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        _end_of_stream: bool,
        ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        if let Some(bytes) = body {
            ctx.buffer.extend_from_slice(bytes);
            let content = String::from_utf8_lossy(&ctx.buffer);
            if content.contains("rogue") {
                warn!("Security Alert: Forbidden content 'rogue' detected in body!");
                return Err(pingora::Error::new(ErrorType::Custom("SecurityPolicyViolation")));
            }
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, BodyInspector);
    my_proxy.add_tcp("0.0.0.0:6152");

    info!("Body Inspector running on 0.0.0.0:6152");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}