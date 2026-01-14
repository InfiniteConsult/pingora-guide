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
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static MEM_CACHE: Lazy<MemCache> = Lazy::new(MemCache::new);
static CACHE_LOCK: Lazy<CacheLock> = Lazy::new(|| CacheLock::new(Duration::from_secs(5)));

pub struct CacheLockProxy;

#[async_trait]
impl ProxyHttp for CacheLockProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

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

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let peer = Box::new(HttpPeer::new(("127.0.0.1", 6195), false, "heavy.local".to_string()));
        Ok(peer)
    }

    fn response_cache_filter(
        &self,
        _session: &Session,
        resp: &ResponseHeader,
        _ctx: &mut Self::CTX
    ) -> Result<RespCacheable> {
        let cc = CacheControl::from_resp_headers(resp);
        let defaults = CacheMetaDefaults::new(|_| None, 0, 0);
        Ok(pingora_cache::filters::resp_cacheable(cc.as_ref(), resp.clone(), false, &defaults))
    }

    async fn logging(&self, session: &mut Session, _e: Option<&Error>, _ctx: &mut Self::CTX)
    where
        Self::CTX: Send + Sync,
    {
        let status = match session.cache.phase() {
            CachePhase::Hit => "HIT (READER)",
            CachePhase::Miss => "MISS (WRITER)",
            _ => "OTHER"
        };

        let wait_time = session.cache.lock_duration().unwrap_or(Duration::ZERO);
        info!(
            "Client Finished. Status: {}. Waited for lock: {:?}",
            status,
            wait_time
        )
    }
}

async fn run_slow_upstream() {
    let listener = TcpListener::bind("127.0.0.1:6195").await.unwrap();
    info!("Slow Upstream running on 127.0.0.1:6195");

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                info!(">> Processing expensive request (2s delay)...");
                tokio::time::sleep(Duration::from_secs(2)).await;

                let body = "Expensive Content Generated";
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                    Content-Length: {}\r\n\
                    Cache-Control: public, max-age=60\r\n\
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

    let mut my_proxy = http_proxy_service(&my_server.configuration, CacheLockProxy);
    my_proxy.add_tcp("0.0.0.0:6194");

    info!("Cache Lock Proxy running on 0.0.0.0:6194");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}