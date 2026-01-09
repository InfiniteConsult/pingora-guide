use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::{Server, ShutdownWatch};
use pingora::services::background::BackgroundService;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::time::interval;

pub struct ThreadReporterService;

#[async_trait]
impl BackgroundService for ThreadReporterService {
    async fn start(&self, shutdown: ShutdownWatch) {
        let mut period = interval(Duration::from_secs(1));
        info!("ThreadReporter started.");

        loop {
            if *shutdown.borrow() {
                break;
            }

            let thread_id = thread::current().id();
            let thread_name = thread::current().name().unwrap_or("unnamed").to_string();
            info!("I am running on thread: {:?} ({})", thread_id, thread_name);
            period.tick().await;
        }
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;

    if let Some(conf) = Arc::get_mut(&mut my_server.configuration) {
        conf.threads = 2;
        conf.work_stealing = false;
    }

    my_server.bootstrap();

    let reporter_a = background_service("Reporter-A", ThreadReporterService);
    let reporter_b = background_service("Reporter-B", ThreadReporterService);

    my_server.add_service(reporter_a);
    my_server.add_service(reporter_b);

    info!("Server starting with work_stealing = False.");
    my_server.run_forever();
}