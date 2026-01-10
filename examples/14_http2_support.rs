use async_trait::async_trait;
use log::info;
use pingora::listeners::tls::TlsSettings;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::{ALPN, HttpPeer};
use std::path::Path;

pub struct Http2Proxy;

#[async_trait]
impl ProxyHttp for Http2Proxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let addr = ("172.28.0.22", 443);
        let mut peer = Box::new(HttpPeer::new(
            addr,
            true,
            "advanced.pingora.local".to_string(),
        ));
        peer.options.alpn = ALPN::H2H1;

        info!("Forwarding to Upstream Advanced via HTTPS (ALPN: H2/H1)");
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        upstream_request.insert_header("Host", "advanced.pingora.local")?;
        Ok(())
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, Http2Proxy);

    let cert_path = "/keys/server.crt";
    let key_path = "/keys/server.key";

    if !Path::new(cert_path).exists() {
        return Err(format!("Missing keys at {}", cert_path).into());
    }

    let mut tls_settings = TlsSettings::intermediate(cert_path, key_path)?;
    tls_settings.enable_h2();
    my_proxy.add_tls_with_settings("0.0.0.0:6154", None, tls_settings);

    info!("HTTP/2 Proxy running on 0.0.0.0:6154");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}