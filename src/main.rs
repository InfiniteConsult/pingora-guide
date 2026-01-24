use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use clap::Parser;
use log::{info, error};

use pingora::server::{Server, ShutdownWatch, ListenFds};
use pingora::server::configuration::Opt;

// Service struct
use pingora::services::listening::Service;

// The actual trait
use pingora::services::Service as ServiceTrait;
use pingora::proxy::http_proxy_service;

// Local modules

use pingora_guide::config::GatewayConf;
use pingora_guide::gateway::Gateway;
use pingora_guide::middleware::Middleware;
use pingora_guide::middlewares::metrics::MetricsMiddleware;
use pingora_guide::upstream::Upstream;
use pingora_guide::upstreams::factory;
use pingora_guide::upstreams::router::Router;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value = "gateway_full.yaml")]
    config: String,

    #[clap(short, long)]
    daemon: bool,
}

pub struct DynamicService(pub Box<dyn ServiceTrait>);

#[async_trait]
impl ServiceTrait for DynamicService {
    async fn start_service(
        &mut self,
        fds: Option<ListenFds>,
        shutdown: ShutdownWatch,
        listeners_per_fd: usize,
    ) {
        self.0.start_service(#[cfg(unix)] fds, shutdown, listeners_per_fd).await
    }

    fn name(&self) -> &str {
        self.0.name()
    }

    fn threads(&self) -> Option<usize> {
        self.0.threads()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args = Args::parse();
    info!("Starting Pingora Gateway...");
    info!("Loading configuration from: {}", args.config);

    let conf = match GatewayConf::load_from_yaml(&args.config) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            return Err(e);
        }
    };

    let mut opt = Opt::default();
    if args.daemon || conf.server.daemon {
        opt.daemon = true;
    }

    let mut my_server = Server::new(Some(opt))?;

    if let Some(server_conf) = Arc::get_mut(&mut my_server.configuration) {
        if let Some(pid_file) = &conf.server.pid_file {
            server_conf.pid_file = pid_file.clone();
        }

        // Apply low-level tuning
        server_conf.threads = conf.server.worker_threads.unwrap_or_else(num_cpus::get);
        server_conf.grace_period_seconds = Some(conf.server.graceful_shutdown_timeout
            .map(|d| d.as_secs())
            .unwrap_or(30));
    }

    my_server.bootstrap();

    let mut upstream_map: HashMap<String, Box<dyn Upstream>> = HashMap::new();
    let mut background_services = Vec::new();

    for u_conf in &conf.upstreams {
        info!("Initializing Upstream ID: {}", u_conf.id);
        match factory::make_upstream(u_conf) {
            Ok((upstream, mut services)) => {
                upstream_map.insert(u_conf.id.clone(), upstream);
                background_services.append(&mut services);
            },
            Err(e) => {
                error!("Failed to initialize upstream '{}': {}", u_conf.id, e);
                return Err(Box::new(e));
            }
        }
    }

    let router = Router::new(&conf);

    let mut middlewares: Vec<Box<dyn Middleware>> = Vec::new();

    // 1. Metrics (Start Timer)
    middlewares.push(Box::new(MetricsMiddleware::new()));

    // --- FUTURE MIDDLEWARES ---
    // middlewares.push(Box::new(IpRestrictionMiddleware::new(&conf.security)));
    // middlewares.push(Box::new(RequestSizeMiddleware::new(&conf.server)));
    // middlewares.push(Box::new(AuthMiddleware::new()));
    // middlewares.push(Box::new(RateLimitMiddleware::new()));
    // middlewares.push(Box::new(ConcurrencyMiddleware::new()));
    // middlewares.push(Box::new(CacheMiddleware::new()));
    // --------------------------

    let gateway = Gateway {
        conf: conf.clone(),
        router: Arc::new(router),
        upstreams: Arc::new(upstream_map),
        middlewares,
    };

    let mut proxy_service = http_proxy_service(&my_server.configuration, gateway);

    for listener in &conf.server.listeners {
        if let Some(tls) = &listener.tls {
            info!("Adding TLS Listener: {}", listener.address);
            proxy_service.add_tls(&listener.address, &tls.cert_path, &tls.key_path)?;
        } else {
            info!("Adding TCP Listener: {}", listener.address);
            proxy_service.add_tcp(&listener.address);
        }
    }

    my_server.add_service(proxy_service);

    for bg_service in background_services {
        my_server.add_service(DynamicService(bg_service));
    }

    if let Some(obs) = &conf.observability {
        if let Some(addr) = &obs.prometheus_addr {
            info!("Exposing Prometheus metrics at: {}", addr);
            let mut prom_service = Service::prometheus_http_service();
            prom_service.add_tcp(addr);
            my_server.add_service(prom_service);
        }
    }

    info!("Server setup complete. Running...");
    my_server.run_forever();
}


