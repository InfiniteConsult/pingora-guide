use async_trait::async_trait;
use log::info;
use once_cell::sync::Lazy;
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;
use pingora::http::ResponseHeader;

use pingora_cache::{MemCache, CacheKey, CacheMetaDefaults, RespCacheable, CachePhase};
use pingora_cache::cache_control::CacheControl;
use pingora_cache::lock::CacheLock;
use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static MEM_CACHE: Lazy<MemCache> = Lazy::new(MemCache::new);
static CACHE_LOCK: Lazy<CacheLock> = Lazy::new(|| CacheLock::new(Duration::from_secs(5)));
static CONTENT_VERSION: AtomicUsize = AtomicUsize::new(0);

pub struct SWRProxy;

#[async_trait]
impl ProxyHttp for SWRProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(&self, _session: &mut Session, _ctx: &mut Self::CTX) -> Result<Box<HttpPeer>> {
        let peer = Box::new(HttpPeer::new(("127.0.0.1", 6199), false, "swr.local".to_string()));
        Ok(peer)
    }

    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        let key = CacheKey::new("", session.req_header().uri.path(), "");
        session.cache.enable(
            &*MEM_CACHE,
            None,
            None,
            Some(&*CACHE_LOCK),
            None
        );
        session.cache.set_cache_key(key);
        Ok(())
    }

    fn response_cache_filter(
        &self,
        _session: &Session,
        resp: &ResponseHeader,
        _ctx: &mut Self::CTX
    ) -> Result<RespCacheable> {
        let cc = CacheControl::from_resp_headers(resp);
        let defaults = CacheMetaDefaults::new(
            |status| if status.as_u16() == 200 { Some(Duration::from_secs(5)) } else { None },
            10,
            0
        );
        Ok(pingora_cache::filters::resp_cacheable(
            cc.as_ref(),
            resp.clone(),
            false,
            &defaults
        ))
    }

    async fn logging(&self, session: &mut Session, _e: Option<&Error>, _ctx: &mut Self::CTX)
    where
        Self::CTX: Send + Sync,
    {
        let phase = session.cache.phase();
        match phase {
            CachePhase::Hit => info!("Status: HIT (Fresh) - Instant Response"),
            CachePhase::Miss => info!("Status: MISS (Fetching) - Slow Response"),
            CachePhase::Stale | CachePhase::StaleUpdating => {
                info!("Status: SWR ACTIVATED (Serving Stale while Updating)");
            }
            CachePhase::Expired => info!("Status: EXPIRED (Too Old) - Blocking Fetch"),
            _ => info!("Status: {:?}", phase),
        }
    }

    fn should_serve_stale(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
        error: Option<&Error>,
    ) -> bool {
        match error {
            None => true,
            Some(_) => false,
        }
    }
}

async fn run_slow_upstream() {
    let listener = TcpListener::bind("127.0.0.1:6199").await.unwrap();
    info!("Slow Upstream running on 127.0.0.1:6199");

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;

                // 2s latency
                tokio::time::sleep(Duration::from_secs(2)).await;

                let version = CONTENT_VERSION.fetch_add(1, Ordering::SeqCst) + 1;
                let body = format!("Content Version {}", version);

                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                    Content-Length: {}\r\n\
                    Cache-Control: public, max-age=5, stale-while-revalidate=10\r\n\
                    Connection: close\r\n\
                    \r\n\
                    {}",
                    body.len(),
                    body
                );

                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    pingora_cache::set_compression_dict_content(Cow::Borrowed(b""));

    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    std::thread::spawn(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_slow_upstream());
    });

    let mut my_proxy = http_proxy_service(&my_server.configuration, SWRProxy);
    my_proxy.add_tcp("0.0.0.0:6198");

    info!("SWR Proxy running on 0.0.0.0:6198");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}