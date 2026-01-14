use async_trait::async_trait;
use log::info;
use once_cell::sync::Lazy;
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;
use pingora::http::ResponseHeader;

// Imports
use pingora_cache::{MemCache, CacheKey, CacheMetaDefaults, RespCacheable, CachePhase};
use pingora_cache::cache_control::CacheControl;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::borrow::Cow;

static MEM_CACHE: Lazy<MemCache> = Lazy::new(MemCache::new);

pub struct CacheProxy;

#[async_trait]
impl ProxyHttp for CacheProxy {
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
        let peer = Box::new(HttpPeer::new(("127.0.0.1", 6191), false, "cache.local".to_string()));
        Ok(peer)
    }

    fn response_cache_filter(
        &self,
        _session: &Session,
        resp: &ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<RespCacheable> {
        let cc = CacheControl::from_resp_headers(resp);

        let defaults = CacheMetaDefaults::new(
            |status| if status.as_u16() == 200 { Some(Duration::from_secs(60)) } else { None },
            0,
            0
        );

        Ok(pingora_cache::filters::resp_cacheable(
            cc.as_ref(),
            resp.clone(),
            false,
            &defaults,
        ))
    }

    async fn logging(
        &self,
        session: &mut Session,
        _e: Option<&Error>,
        _ctx: &mut Self::CTX,
    ) {
        match session.cache.phase() {
            CachePhase::Hit => info!("Cache Status: HIT (Served from Memory)"),
            CachePhase::Miss => info!("Cache Status: MISS (Fetched from Upstream)"),
            CachePhase::Expired => info!("Cache Status: EXPIRED (Revalidating)"),
            _ => {}
        }
    }
}

async fn run_mock_upstream() {
    let listener = TcpListener::bind("127.0.0.1:6191").await.unwrap();
    info!("Mock Upstream started on 127.0.0.1:6191");

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;

                let body = "Response from Local Mock";
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
        rt.block_on(run_mock_upstream());
    });

    let mut my_proxy = http_proxy_service(&my_server.configuration, CacheProxy);
    my_proxy.add_tcp("0.0.0.0:6190");

    info!("Memory Cache Proxy running on 0.0.0.0:6190");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}