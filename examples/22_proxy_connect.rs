use async_trait::async_trait;
use log::{error, info};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::connectors::L4Connect;
// Fix type ambiguity by aliasing the concrete struct
use pingora::protocols::l4::stream::Stream as L4Stream;
use pingora::protocols::l4::socket::SocketAddr as PingoraSocketAddr;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug)]
struct ProxyTunnelConnector {
    proxy_addr: SocketAddr,
    remote_host: String,
    remote_port: u16,
}

#[async_trait]
impl L4Connect for ProxyTunnelConnector {
    async fn connect(&self, _addr: &PingoraSocketAddr) -> Result<L4Stream> {
        info!("Connector: Dialing Proxy at {:?}...", self.proxy_addr);

        let mut socket = TcpStream::connect(self.proxy_addr).await.map_err(|e| {
            Error::explain(ErrorType::ConnectError, format!("Failed to connect to proxy: {}", e))
        })?;

        let connect_req = format!(
            "CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\n\r\n",
            self.remote_host, self.remote_port, self.remote_host, self.remote_port
        );

        info!("Connector: Sending CONNECT request for {}:{}", self.remote_host, self.remote_port);
        socket.write_all(connect_req.as_bytes()).await.map_err(|e| {
            Error::explain(ErrorType::WriteError, format!("Failed to write CONNECT req: {}", e))
        })?;

        let mut buf = [0u8; 4096];
        let n = socket.read(&mut buf).await.map_err(|e| {
            Error::explain(ErrorType::ReadError, format!("Failed to read proxy resp: {}", e))
        })?;

        let response = String::from_utf8_lossy(&buf[..n]);
        if !response.contains(" 200 ") {
            return Err(Error::explain(
                ErrorType::ConnectProxyFailure,
                format!("Proxy refused tunnel: {}", response.lines().next().unwrap_or("Unknown"))
            ));
        }

        info!("Connector: Tunnel established! Handing socket to Pingora core.");

        Ok(L4Stream::from(socket))
    }
}

pub struct TunnelProxy;

#[async_trait]
impl ProxyHttp for TunnelProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let upstream_host = "advanced.pingora.local";
        let upstream_port = 443;

        let proxy_addr_str = "127.0.0.1:3128";
        let proxy_socket_addr: SocketAddr = proxy_addr_str.parse().unwrap();

        let mut peer = Box::new(HttpPeer::new(
            proxy_socket_addr,
            true,
            upstream_host.to_string()
        ));

        let connector = ProxyTunnelConnector {
            proxy_addr: proxy_socket_addr,
            remote_host: upstream_host.to_string(),
            remote_port: upstream_port,
        };

        peer.options.custom_l4 = Some(Arc::new(connector));
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "advanced.pingora.local")?;
        Ok(())
    }
}

async fn run_mock_forward_proxy() {
    let addr = "0.0.0.0:3128";
    let listener = TcpListener::bind(addr).await.expect("Failed to bind Mock Proxy");
    info!("Mock Proxy listening on {}", addr);

    loop {
        if let Ok((mut client_socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let n = client_socket.read(&mut buf).await.unwrap_or(0);
                if n == 0 { return; }

                let req = String::from_utf8_lossy(&buf[..n]);
                if req.starts_with("CONNECT") {
                    info!("Mock Proxy: Connecting to upstream_advanced...");
                    if let Ok(mut upstream) = TcpStream::connect("172.28.0.22:443").await {
                        let _ = client_socket.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n").await;
                        let _ = tokio::io::copy_bidirectional(&mut client_socket, &mut upstream).await;
                    }
                }
            });
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_mock_forward_proxy());
    });

    let mut my_proxy = http_proxy_service(&my_server.configuration, TunnelProxy);
    my_proxy.add_tcp("0.0.0.0:6166");

    info!("Pingora Tunnel Service running on 0.0.0.0:6166");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}