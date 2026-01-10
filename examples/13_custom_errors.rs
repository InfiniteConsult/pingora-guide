use async_trait::async_trait;
use log::{error, info};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::http::ResponseHeader;
use pingora::proxy::FailToProxy;

pub struct CustomErrorProxy;

#[async_trait]
impl ProxyHttp for CustomErrorProxy {
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

    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        if session.req_header().uri.path() == "/oops" {
            return Err(pingora::Error::new(ErrorType::Custom("SimulatedFailure")));
        }
        Ok(false)
    }

    async fn fail_to_proxy(
        &self,
        session: &mut Session,
        e: &pingora::Error,
        _ctx: &mut Self::CTX
    ) -> FailToProxy
    where
        Self::CTX: Send + Sync,
    {
        error!("Entered fail_to_proxy with error: {:?}", e);
        let code = if let ErrorType::Custom("SimulatedFailure") = e.etype {
            400
        } else {
            500
        };

        let body = format!(
            r#"{{"status": "error", "code": {}, "message": "We caught a custom error!"}}"#,
            code
        );
        let content_length = body.len();

        let mut header = ResponseHeader::build(code, Some(3)).unwrap();
        header.insert_header("Content-Type", "application/json").unwrap();
        header.insert_header("Content-Length", content_length.to_string()).unwrap();

        let _ = session.write_response_header(Box::new(header), false).await;
        let _ = session.write_response_body(Some(body.into()), true).await;

        FailToProxy {
            error_code: code,
            can_reuse_downstream: false,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, CustomErrorProxy);
    my_proxy.add_tcp("0.0.0.0:6153");

    info!("Custom Error Proxy running on 0.0.0.0:6153");
    info!("Try: curl http://127.0.0.1:6153/oops");

    my_server.add_service(my_proxy);
    my_server.run_forever();
}