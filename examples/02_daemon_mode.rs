use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::{Server, ShutdownWatch};
use pingora::services::background::BackgroundService;
use std::time::Duration;
use tokio::time::interval;

pub struct HeartbeatService;

#[async_trait]
impl BackgroundService for HeartbeatService {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let mut period = interval(Duration::from_secs(1));
        info!("Heartbeat service started. PID: {}", std::process::id());

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    info!("Shutdown signal received. Stopping heartbeat.");
                    break;
                }
                _ = period.tick() => {
                    info!("Beep... (PID: {})", std::process::id());
                }
            }
        }
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    if my_server.configuration.daemon {
        println!("Preparing to daemonize. Logs will be redirected to: {:?}", my_server.configuration.error_log);
        println!("Check the PID file at: {}", my_server.configuration.pid_file);
    } else {
        println!("Running in foreground mode. Pass '-d' or use config file to daemonize.");
    }

    let heartbeat = HeartbeatService;
    let service = background_service("Heartbeat", heartbeat);

    my_server.add_service(service);
    my_server.run_forever();
}