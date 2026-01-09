// examples/00_basic_server.rs
use async_trait::async_trait;
use log::{error, info};
use pingora::prelude::*;
use pingora::protocols::{Stream, GetSocketDigest};
use pingora::server::configuration::Opt;
use pingora::server::{Server, ShutdownWatch};
use pingora::services::listening::Service;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};


#[derive(Clone)]
pub struct EchoApp;

#[async_trait]
impl pingora::apps::ServerApp for EchoApp {
    async fn process_new(
        self: &Arc<Self>,
        mut stream: Stream,
        shutdown: &ShutdownWatch,
    ) -> Option<Stream> {
        if let Some(digest) = stream.get_socket_digest() {
            if let Some(peer_addr) = digest.peer_addr() {
                info!("New connection from: {:?}", peer_addr);
            }
        }

        let mut buf = [0u8; 1024];

        loop {
            if *shutdown.borrow() {
                info!("Server shutting down, closing connection");
                return None
            }

            let read_result = stream.read(&mut buf).await;
            match read_result {
                Ok(0) => {
                    info!("Client closed connection");
                    return None;
                },
                Ok(n) => {
                    if let Err(e) = stream.write_all(&buf[0..n]).await {
                        error!("Failed to write to stream: {}", e);
                        return None;
                    }
                    if let Err(e) = stream.flush().await {
                        error!("Failed to flush stream: {}", e);
                        return None;
                    }
                },
                Err(e) => {
                    error!("Stream read error: {}", e);
                    return None;
                }
            }
        }
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opt: Opt = Opt::parse_args();
    let mut my_server: Server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let echo_logic: EchoApp = EchoApp;
    let mut service = Service::new("Echo Service".to_string(), echo_logic);
    service.add_tcp("0.0.0.0:6142");
    my_server.add_service(service);

    info!("Starting server on 0.0.0.0:6142");
    my_server.run_forever();
}
