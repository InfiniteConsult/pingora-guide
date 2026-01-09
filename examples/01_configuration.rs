use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::{Server, ShutdownWatch};
use pingora::services::listening::Service;
use std::sync::Arc;
use pingora::protocols::Stream;

#[derive(Clone)]
pub struct ConfigDemoApp;


#[async_trait]
impl pingora::apps::ServerApp for ConfigDemoApp {
    async fn process_new(
        self: &Arc<Self>,
        _stream: Stream,
        _shutdown: &ShutdownWatch
    ) -> Option<Stream> {
        None
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    let conf = &my_server.configuration;


    info!("--- Configuration Loaded ---");
    info!("  version: {}", conf.version);
    info!("  daemon: {}", conf.daemon);
    info!("  error_log: {:?}", conf.error_log);
    info!("  pid_file: {}", conf.pid_file);
    info!("  upgrade_sock: {}", conf.upgrade_sock);
    info!("  user: {:?}", conf.user);
    info!("  group: {:?}", conf.group);
    info!("  threads: {}", conf.threads);
    info!("  listener_tasks_per_fd: {}", conf.listener_tasks_per_fd);
    info!("  work_stealing: {}", conf.work_stealing);
    info!("  ca_file: {:?}", conf.ca_file);
    info!("  grace_period_seconds: {:?}", conf.grace_period_seconds);
    info!("  graceful_shutdown_timeout_seconds: {:?}", conf.graceful_shutdown_timeout_seconds);

    info!("  client_bind_to_ipv4: {:?}", conf.client_bind_to_ipv4);
    info!("  client_bind_to_ipv6: {:?}", conf.client_bind_to_ipv6);
    info!("  upstream_keepalive_pool_size: {}", conf.upstream_keepalive_pool_size);
    info!("  upstream_connect_offload_threadpools: {:?}", conf.upstream_connect_offload_threadpools);
    info!("  upstream_connect_offload_thread_per_pool: {:?}", conf.upstream_connect_offload_thread_per_pool);
    info!("  upstream_debug_ssl_keylog: {}", conf.upstream_debug_ssl_keylog);
    info!("  max_retries: {}", conf.max_retries);
    info!("----------------------------");

    my_server.bootstrap();

    let mut service = Service::new("ConfigDemo".to_string(), ConfigDemoApp);
    service.add_tcp("0.0.0.0:6143");
    my_server.add_service(service);

    info!("Starting server. Verify the thread count in the logs above matches your YAML.");
    my_server.run_forever();
}