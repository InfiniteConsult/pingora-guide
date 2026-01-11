use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::utils::tls::CertKey;
use pingora::tls::x509::X509;
use pingora::tls::pkey::PKey;
use std::fs;
use std::sync::Arc;

pub struct MtlsProxy {
    client_cert: Arc<CertKey>
}

#[async_trait]
impl ProxyHttp for MtlsProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let path = session.req_header().uri.path();
        let addr = ("172.28.0.22", 8443);
        let sni = "advanced.pingora.local";

        let mut peer = Box::new(HttpPeer::new(addr, true, sni.to_string()));

        if path == "/auth" {
            info!("Attaching Client Certificate for /auth request...");
            peer.client_cert_key = Some(self.client_cert.clone());
        } else {
            info!("Connecting anonymously (no cert) for {}...", path);
        }
        Ok(peer)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let cert_path = "/keys/client.crt";
    let key_path = "/keys/client.key";

    if !std::path::Path::new(cert_path).exists() {
        return Err(format!("Client keys missing at {}. Run 00-setup-certs.sh", cert_path).into());
    }

    let cert_bytes = fs::read(cert_path)?;
    let key_bytes = fs::read(key_path)?;

    let x509 = X509::from_pem(&cert_bytes[..])
        .map_err(|e| format!("Failed to parse certificate: {}", e))?;
    let key = PKey::private_key_from_pem(&key_bytes)
        .map_err(|e| format!("Failed to parse private key: {}", e))?;

    let cert_key = CertKey::new(vec![x509], key);
    let client_cert = Arc::new(cert_key);

    let my_proxy = MtlsProxy { client_cert };

    let mut my_service = http_proxy_service(&my_server.configuration, my_proxy);
    my_service.add_tcp("0.0.0.0:6165");

    info!("mTLS Proxy running on 0.0.0.0:6165");
    my_server.add_service(my_service);
    my_server.run_forever();
}