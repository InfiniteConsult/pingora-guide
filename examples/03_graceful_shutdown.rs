use async_trait::async_trait;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::{Server, ShutdownWatch};
use std::sync::Arc;
use std::time::Duration;
use pingora::services::background::BackgroundService;
use tokio::time::sleep;

pub struct BatchJobService;

#[async_trait]
impl BackgroundService for BatchJobService {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        info!("BatchJob Service started. Waiting for jobs");
        let mut job_id = 0;

        loop {
            if *shutdown.borrow() {
                info!("Shutdown requested. No new jobs will be started.");
                break;
            }

            job_id += 1;
            info!("Starting Job #{} (simulated 20s duration)...", job_id);

            let job_duration = Duration::from_secs(20);
            tokio::select! {
                _ = sleep(job_duration) => {
                    info!("Job #{} completed successfully.", job_id);
                }

                _ = shutdown.changed() => {
                    warn!("Shutdown signal received while Job #{} is running!", job_id);
                    warn!("Finishing Job #{} before exiting...", job_id);
                    // Simulate waiting.
                    sleep(Duration::from_secs(10)).await;
                    info!("Job #{} completed gracefully during shutdown.", job_id);
                    break;

                }
            }

            sleep(Duration::from_secs(1)).await;
        }
        info!("BatchJob Service has stopped cleanly.");
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;

    if let Some(conf) = Arc::get_mut(&mut my_server.configuration) {
        conf.grace_period_seconds = Some(10);
    }
    my_server.bootstrap();

    let service = background_service("BatchJobService", BatchJobService);
    my_server.add_service(service);

    info!("Server running. Send SIGTERM to trigger graceful shutdown (e.g. 'pkill -TERM -f 03_graceful_shutdown').");
    my_server.run_forever();
}