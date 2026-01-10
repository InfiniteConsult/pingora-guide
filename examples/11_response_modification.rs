use async_trait::async_trait;
use log::info;
use pingora::http::ResponseHeader;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

pub struct ResponseModProxy;

#[async_trait]
impl ProxyHttp for ResponseModProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let peer = Box::new(HttpPeer::new(
            ("172.28.0.20", 8080),
            false,
            "blue.pingora.local".to_string(),
        ));
        Ok(peer)
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        _ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        let _ = upstream_response.remove_header("X-App-Version");
        upstream_response.insert_header("X-Content-Type-Options", "nosniff")?;

        if let Some(date_val) = upstream_response.headers.get("Date") {
            let val_bytes = date_val.as_bytes().to_vec();
            upstream_response.insert_header("X-Legacy-Date", val_bytes)?;
        }

        info!("Response filtered. Stripped Version. Added Security Headers.");
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, ResponseModProxy);
    my_proxy.add_tcp("0.0.0.0:6151");

    info!("Response Mod Proxy running on 0.0.0.0:6151");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}