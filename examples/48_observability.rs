use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;
use pingora::services::listening::Service;

use prometheus::{register_histogram, register_int_counter, Histogram, IntCounter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static REQUEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub struct MetricsContext {
    start_time: Instant,
}

pub struct ObservabilityProxy {
    req_counter: IntCounter,
    req_histogram: Histogram,
}

#[async_trait]
impl ProxyHttp for ObservabilityProxy {
    type CTX = MetricsContext;

    fn new_ctx(&self) -> Self::CTX {
        MetricsContext { start_time: Instant::now() }
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let count = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let (addr, port, label) = if count % 2 == 0 {
            ("127.0.0.1", 6201, "Blue")
        } else {
            ("127.0.0.1", 6202, "Green")
        };

        info!("Forwarding request #{} to {}", count, label);
        let peer = Box::new(HttpPeer::new((addr,port), false, "metrics.local".to_string()));
        Ok(peer)
    }

    async fn logging(&self, session: &mut Session, _e: Option<&Error>, ctx: &mut Self::CTX)
    where
        Self::CTX: Send + Sync,
    {
        let duration = ctx.start_time.elapsed();
        let duration_secs = duration.as_secs_f64();

        self.req_counter.inc();
        self.req_histogram.observe(duration_secs);

        info!(
            "Request finished. Path: {}, Latency: {:.4}s, Total Request: {}",
            session.req_header().uri.path(),
            duration_secs,
            self.req_counter.get()
        );
    }
}

async fn run_mock_upstream(port: u16, name: &str, delay_ms: u64) {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await.unwrap();
    info!("{} Upstream started on port {}", name, port);

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            let name = name.to_string();
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;

                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;

                let body = format!("Response from {}", name);
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                    Content-Length: {}\r\n\
                    Connection: close\r\n\
                    \r\n\
                    {}\n",
                    body.len() + 1,
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    std::thread::spawn(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            tokio::join!(
                run_mock_upstream(6201, "Blue", 10),
                run_mock_upstream(6202, "Green", 100),
            )
        });
    });

    let proxy = ObservabilityProxy {
        req_counter: register_int_counter!(
            "http_requests_total",
            "Total number of HTTP requests processed."
        ).unwrap(),
        req_histogram: register_histogram!(
            "http_request_duration_seconds",
            "The HTTP request latency in seconds."
        ).unwrap(),
    };

    let mut proxy_service = http_proxy_service(&my_server.configuration, proxy);
    proxy_service.add_tcp("0.0.0.0:6199");
    my_server.add_service(proxy_service);

    let mut prometheus_service = Service::prometheus_http_service();
    prometheus_service.add_tcp("0.0.0.0:6200");
    my_server.add_service(prometheus_service);

    info!("Proxy running on 6199, Metrics on 6200");
    my_server.run_forever();
}