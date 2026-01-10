use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

pub struct UdsProxy;

#[async_trait]
impl ProxyHttp for UdsProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let peer = Box::new(HttpPeer::new_uds(
            "/tmp/upstream.sock",
            false,
            "uds.local".to_string(),
        )?);

        info!("Forwarding to UDS: /tmp/upstream.sock");
        Ok(peer)
    }
}

async fn run_mock_uds_server(path: &'static str) {
    let _ = std::fs::remove_file(path);

    let listener = UnixListener::bind(path).expect("Failed to bind UDS");
    info!("Mock Upstream running at {}", path);
    tokio::spawn(async move {
        loop {
            if let Ok((mut stream, _addr)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf).await;
                    let response = "HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nHello via UDS";
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        }
    });
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_mock_uds_server("/tmp/upstream.sock"));
        std::thread::park();
    });

    let mut my_proxy = http_proxy_service(&my_server.configuration, UdsProxy);
    my_proxy.add_tcp("0.0.0.0:6162");

    info!("UDS Proxy running on 0.0.0.0:6162");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}