//! # Upstream Interface
//!
//! This module defines the contract for finding a backend server. By abstracting
//! "where to go", we can easily swap between Static IPs, DNS Discovery, or
//! complex Load Balancers without changing the core Gateway logic.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `Upstream` Trait**:
//!     * Must inherit `Sync + Send` to be safe for global sharing.
//!
//! 2.  **Define `select_peer` Method**:
//!     * Signature: `async fn select_peer(&self, session: &mut Session, ctx: &mut GatewayContext) -> Result<Box<HttpPeer>>`
//!     * **Input**: Access to the session (for headers/path) and Context (for sticky session keys).
//!     * **Output**: A `pingora::upstreams::peer::HttpPeer` struct, configured with the target IP, SNI, and TLS settings.
//!     * **Error**: Returns our `Error` type (e.g., `GatewayError::UpstreamUnavailable`).
use std::time::Duration;

use async_trait::async_trait;
use pingora::upstreams::peer::HttpPeer;
use pingora::proxy::Session;

use crate::error::Result;
use crate::context::GatewayContext;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct ClusterOptions {
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,

    #[serde(with = "humantime_serde")]
    pub read_timeout: Duration,

    #[serde(with = "humantime_serde")]
    pub write_timeout: Duration,

    #[serde(default="default_timeout", with = "option_humantime")]
    pub idle_timeout: Option<Duration>,
    pub enable_h2: bool,
    pub verify_hostname: bool,
}

fn default_timeout() -> Option<Duration> { Some(Duration::from_secs(60)) }

impl ClusterOptions {
    pub fn new(
        connect_timeout: Duration,
        read_timeout: Duration,
        write_timeout: Duration,
        idle_timeout: Option<Duration>,
        enable_h2: bool,
        verify_hostname: bool,
    ) -> Self {
        Self {
            connect_timeout,
            read_timeout,
            write_timeout,
            idle_timeout,
            enable_h2,
            verify_hostname
        }
    }
}

impl Default for ClusterOptions {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(5),
            Duration::from_secs(60),
            Duration::from_secs(60),
            Some(Duration::from_secs(60)),
            false,
            true,
        )
    }
}


#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum HashSource {
    None,
    ClientIp,
    Uri,
    Header(String),
    Cookie(String),

}

#[async_trait]
pub trait Upstream: Send + Sync {
    async fn select_peer(
        &self,
        session: &mut Session,
        ctx: &mut GatewayContext,
    ) -> Result<Box<HttpPeer>>;
}

mod option_humantime {
    use std::time::Duration;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S> (val: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match val {
            Some(d) => humantime_serde::serialize(d, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D> (deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Val {
            None,
            Some(#[serde(with = "humantime_serde")] Duration),
        }

        let v = Val::deserialize(deserializer)?;
        Ok(match v {
            Val::None => None,
            Val::Some(d) => Some(d),
        })
    }
}