use async_trait::async_trait;
use log::{info};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::protocol::Message};

pub struct WebSocketProxy;

#[async_trait]
impl ProxyHttp for WebSocketProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let addr = ("127.0.0.1", 9091);
        let mut peer = Box::new(HttpPeer::new(addr, false, "".to_string()));

        peer.options.read_timeout = None;
        peer.options.write_timeout = None;
        peer.options.connection_timeout = Some(Duration::from_secs(5));

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
        if let Some(upgrade) = upstream_request.headers.get("Upgrade") {
            info!("Proxy: Detected Upgrade Header: {:?}", upgrade);
        }
        Ok(())
    }
}

async fn run_echo_server() {
    let addr = "127.0.0.1:9091";
    let listener = TcpListener::bind(addr).await.expect("Failed to bind Echo Server.");
    info!("Echo Server listening on {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(async move {
            let mut ws_stream = accept_async(stream).await.expect("Failed to accept WS");
            while let Some(msg) = ws_stream.next().await {
                let msg = msg.expect("Error reading message.");
                if msg.is_text() {
                    let text = msg.to_text().unwrap();
                    let response_text = format!("Echo [{}]: {}", chrono::Local::now().format("%H:%M:%S"), text);

                    info!("Server: Received '{}', Replying...", text);
                    ws_stream.send(Message::Text(response_text.into())).await.unwrap();
                }
            }
        });
    }
}

async fn run_test_client() {
    sleep(Duration::from_secs(2)).await;

    let url = "ws://127.0.0.1:6167";
    info!("Client: Connection to Proxy at {}", url);

    let(mut ws_stream, _) = connect_async(url).await.expect("Failed to connect to proxy.");
    info!("Client: Connected! Starting message loop...");

    for i in 1..=3 {
        let msg = format!("Ping #{}", i);
        ws_stream.send(Message::Text(msg.into())).await.expect("Failed to send");

        if let Some(resp) = ws_stream.next().await {
            let resp_msg = resp.expect("Failed to read response");
            info!("Client: Received '{}'", resp_msg.to_text().unwrap());
        }
        sleep(Duration::from_secs(1)).await;
    }

    info!("Client: Test Complete. Closing connection.");
    ws_stream.close(None).await.unwrap();
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let _server_handle = std::thread::spawn(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_echo_server());
    });

    let _client_handle = std::thread::spawn(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_test_client());
    });

    let mut my_proxy = http_proxy_service(&my_server.configuration, WebSocketProxy);
    my_proxy.add_tcp("0.0.0.0:6167");

    info!("WebSocket Proxy running on 0.0.0.0:6167");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}