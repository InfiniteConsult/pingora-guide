use async_trait::async_trait;
use log::info;
use once_cell::sync::Lazy;
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;
use pingora::http::ResponseHeader;

use pingora_cache::{MemCache, CacheKey, CacheMetaDefaults, RespCacheable, CachePhase};
use pingora_cache::cache_control::CacheControl;
use std::borrow::Cow;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static MEM_CACHE: Lazy<MemCache> = Lazy::new(MemCache::new);

pub struct CacheControlProxy;

#[async_trait]
impl ProxyHttp for CacheControlProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        let key = CacheKey::new("", session.req_header().uri.path(), "");
        session.cache.enable(&*MEM_CACHE, None, None, None, None);
        session.cache.set_cache_key(key);
        Ok(())
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        // Connect to our dynamic mock upstream
        let peer = Box::new(HttpPeer::new(("127.0.0.1", 6193), false, "header.test.local".to_string()));
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
        let path = session.req_header().uri.path();
        match session.cache.phase() {
            CachePhase::Hit => info!("Path {}, Status: HIT", path),
            CachePhase::Miss => info!("Path {}, Status: MISS", path),
            CachePhase::Expired => info!("Path {}, Status: EXPIRED", path),
            CachePhase::Disabled(_) => info!("Path {}, Status: SKIP (Uncacheable)", path),
            _ => {}
        }
    }
}

async fn run_dynamic_upstream() {
    let listener = TcpListener::bind("127.0.0.1:6193").await.unwrap();
    info!("Dynamic Upstream running on 127.0.0.1:6193");

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);

                // Determine Header based on Path
                let cc_header = if req.contains("GET /short") {
                    "max-age=5"
                } else if req.contains("GET /long") {
                    "max-age=60"
                } else if req.contains("GET /no_store") {
                    "no-store"
                } else {
                    "max-age=10"
                };

                let body = format!("Content for {}", cc_header);
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                    Content-Length: {}\r\n\
                    Cache-Control: {}\r\n\
                    Connection: close\r\n\
                    \r\n\
                    {}\n",
                    body.len() + 1,
                    cc_header,
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
        rt.block_on(run_dynamic_upstream());
    });

    let mut my_proxy = http_proxy_service(&my_server.configuration, CacheControlProxy);
    my_proxy.add_tcp("0.0.0.0:6192");

    info!("Cache Control Proxy running on 0.0.0.0:6192");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}