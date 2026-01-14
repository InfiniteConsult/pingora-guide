use async_trait::async_trait;
use log::info;
use once_cell::sync::Lazy;
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;
use pingora::http::ResponseHeader;

use pingora_cache::{MemCache, CacheKey, CacheMetaDefaults, RespCacheable};
use pingora_cache::cache_control::CacheControl;
use std::borrow::Cow;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static MEM_CACHE: Lazy<MemCache> = Lazy::new(MemCache::new);

pub struct PurgeProxy;

#[async_trait]
impl ProxyHttp for PurgeProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        let path = session.req_header().uri.path();
        info!("(1) Setup: Configuring CacheKey for path: '{}'", path);

        let key = CacheKey::new("", path, "");
        session.cache.enable(&*MEM_CACHE, None, None, None, None);
        session.cache.set_cache_key(key);
        Ok(())
    }

    fn is_purge(&self, session: &Session, _ctx: &Self::CTX) -> bool {
        let method = &session.req_header().method;
        if method.as_str() == "PURGE" {
            info!("(2) Decision: Method is PURGE. Hijacking request to delete item.");
            return true;
        }
        info!("(2) Decision: Method is {}. Proceeding to Standard Cache Flow.", method);
        false
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        info!("(3) Cache Miss: Connecting to Upstream to fetch fresh content...");
        let peer = Box::new(HttpPeer::new(("127.0.0.1", 6197), false, "purge.local".to_string()));
        Ok(peer)
    }

    fn response_cache_filter(
        &self,
        _session: &Session,
        resp: &ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<RespCacheable> {
        info!("(4) Validation: Examining Upstream Headers for Cache-Control...");
        let cc = CacheControl::from_resp_headers(resp);
        let defaults = CacheMetaDefaults::new(|_| None, 0, 0);
        Ok(pingora_cache::filters::resp_cacheable(cc.as_ref(), resp.clone(), false, &defaults))
    }
}

async fn run_upstream() {
    let listener = TcpListener::bind("127.0.0.1:6197").await.unwrap();
    info!("Upstream running on 127.0.0.1:6197");

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;

                let body = "Cached Content (TTL 300s)";
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                    Content-Length: {}\r\n\
                    Cache-Control: public, max-age=300\r\n\
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
        rt.block_on(run_upstream());
    });

    let mut my_proxy = http_proxy_service(&my_server.configuration, PurgeProxy);
    my_proxy.add_tcp("0.0.0.0:6196");

    info!("Purge Proxy running on 0.0.0.0:6196");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}