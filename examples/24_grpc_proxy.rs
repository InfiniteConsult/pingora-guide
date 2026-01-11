use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::{HttpPeer, ALPN};


pub struct GrpcProxy;

#[async_trait]
impl ProxyHttp for GrpcProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let addr = ("172.28.0.23", 9001);
        let sni = "grpc.pingora.local";

        let mut peer = Box::new(HttpPeer::new(addr, true, sni.to_string()));
        peer.options.alpn = ALPN::H2;

        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        if let Some(ctype) = upstream_request.headers.get("content-type") {
            if ctype.to_str().unwrap_or("").starts_with("application/grpc") {
                info!("Proxying gRPC request...");
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

    let mut my_proxy = http_proxy_service(&my_server.configuration, GrpcProxy);

    my_proxy.add_tls(
        "0.0.0.0:6168",
        "conf/keys/server.crt",
        "conf/keys/server.key"
    ).expect("Failed to add TLS listener");

    info!("gRPC Proxy running on 0.0.0.0:6168");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}