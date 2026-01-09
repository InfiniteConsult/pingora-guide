use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::{Server, ShutdownWatch};
use pingora::services::background::BackgroundService;
use pingora::services::listening::Service;
use pingora::protocols::Stream;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::time::interval;

struct AppState {
    connection_count: AtomicUsize
}

#[derive(Clone)]
pub struct CounterApp {
    state: Arc<AppState>,
}

#[async_trait]
impl pingora::apps::ServerApp for CounterApp {
    async fn process_new(
        self: &Arc<Self>,
        mut stream: Stream,
        _shutdown: &ShutdownWatch
    ) -> Option<Stream> {
        let count = self.state.connection_count.fetch_add(1, Ordering::Relaxed) + 1;
        info!("Traffic: New connection handled. Count is now {}", count);
        let response = format!("Hello! You are visitor #{}\n", count);
        let _ = stream.write_all(response.as_bytes()).await;
        None
    }
}

pub struct MetricExporter {
    state: Arc<AppState>,
}

#[async_trait]
impl BackgroundService for MetricExporter {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let mut period = interval(Duration::from_secs(2));
        info!("Exporter: Service started.");

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    info!("Exporter: Shutdown requested.");
                    break;
                }
                _ = period.tick() => {
                    let count = self.state.connection_count.load(Ordering::Relaxed);
                    info!("Exporter: Current Total Connections: {}", count);
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

    let state = Arc::new(AppState {
        connection_count: AtomicUsize::new(0),
    });

    let traffic_logic = CounterApp { state: state.clone() };
    let mut traffic_service = Service::new("Traffic".to_string(), traffic_logic);
    traffic_service.add_tcp("0.0.0.0:6145");

    let exporter_logic = MetricExporter { state: state.clone() };
    let background_service = background_service("MetricExporter", exporter_logic);

    my_server.add_service(traffic_service);
    my_server.add_service(background_service);

    info!("Server started. Traffic on port 6145. Metrics in logs.");
    my_server.run_forever();
}