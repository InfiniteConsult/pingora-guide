use async_trait::async_trait;
use log::{error, info};
use pingora::listeners::tls::TlsSettings;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use std::path::Path;

pub struct TlsProxy;

#[async_trait]
impl ProxyHttp for TlsProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> pingora::Result<Box<HttpPeer>> {
        let addr = ("172.28.0.20", 8080);
        info!("Forwarding HTTPS request to Upstream Blue ({:?})", addr);
        let peer = Box::new(HttpPeer::new(
            addr,
            false,
            "blue.pingora.local".to_string()
        ));
        Ok(peer)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(
        &my_server.configuration,
        TlsProxy,
    );

    let cert_path = "/keys/server.crt";
    let key_path = "/keys/server.key";

    if !Path::new(cert_path).exists() || !Path::new(key_path).exists() {
        error!("Certificates not found! Make sure you ran scripts/00-setup-certs.sh");
        return Err(format!("Missing keys at {}", cert_path).into());
    }

    let mut tls_settings = TlsSettings::intermediate(cert_path, key_path)?;
    tls_settings.enable_h2();

    my_proxy.add_tls_with_settings("0.0.0.0:6147", None, tls_settings);

    info!("HTTPS Proxy running on 0.0.0.0:6147 -> Forwarding to Upstream Blue");
    my_server.add_service(my_proxy);;
    my_server.run_forever();
}