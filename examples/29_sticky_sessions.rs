use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::background_service;
use pingora::http::ResponseHeader;

use pingora_load_balancing::LoadBalancer;
use pingora_load_balancing::selection::consistent::KetamaHashing;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

static SESSION_COUNTER: AtomicUsize = AtomicUsize::new(100);

pub struct StickyCtx {
    pub new_session_id: Option<String>,
}

pub struct LB(Arc<LoadBalancer<KetamaHashing>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = StickyCtx;
    fn new_ctx(&self) -> Self::CTX {
        StickyCtx { new_session_id: None }
    }

    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let mut session_id = String::new();
        if let Some(cookie_val) = session.req_header().headers.get("Cookie") {
            let cookie_str = cookie_val.to_str().unwrap_or("");
            for part in cookie_str.split(';') {
                let part = part.trim();
                if part.starts_with("session-id=") {
                    session_id = part.trim_start_matches("session-id=").to_string();
                    info!("Found existing session cookie: {}", session_id);
                    break;
                }
            }
        }

        if session_id.is_empty() {
            let new_id = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
            session_id = format!("user-{}", new_id);
            info!("No cookie found. Generated new session id: {}", session_id);
            ctx.new_session_id = Some(session_id.clone());
        }

        let key = session_id.as_bytes();
        let upstream = self.0
            .select(key, 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        info!("Session '{}' stuck to upstream: {:?}", session_id, upstream);

        let peer = Box::new(HttpPeer::new(
            upstream,
            false,
            "sticky.cluster".to_string(),
        ));
        Ok(peer)
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        if let Some(new_id) = &ctx.new_session_id {
            let cookie_value = format!("session-id={}; Path=/", new_id);
            upstream_response.insert_header("Set-Cookie", cookie_value)?;
            info!("Injected Set-Cookie header for {}", new_id);
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080",
        "172.28.0.21:8080",
    ])?;

    let background = background_service("sticky_lb", upstreams);
    let lb_ref = background.task();

    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6173");

    info!("Sticky Session LB running on 0.0.0.0:6173");

    my_server.add_service(background);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}