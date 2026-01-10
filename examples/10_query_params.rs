use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use http::uri::Uri;

pub struct QueryModeProxy;

#[async_trait]
impl ProxyHttp for QueryModeProxy {
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

    async fn upstream_request_filter(
        &self, _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        let uri = &upstream_request.uri;
        let path = uri.path();
        let query = uri.query().unwrap_or("");

        info!("Original Query: '{}'", query);

        let mut params: Vec<&str> = query.split("&")
            .filter(|part| !part.is_empty() && !part.starts_with("debug="))
            .collect();
        params.push("ref=pingora");
        let new_query = params.join("&");

        let new_uri_string = format!("{}?{}", path, new_query);
        let new_uri: Uri = new_uri_string.parse().expect("Failed to parse new URI");

        info!("Rewritten URI: {}", new_uri);

        upstream_request.set_uri(new_uri);
        upstream_request.insert_header("Host", "blue.pingora.local")?;

        Ok(())
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, QueryModeProxy);
    my_proxy.add_tcp("0.0.0.0:6150");

    info!("Query Param Proxy running on 0.0.0.0:6150");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}