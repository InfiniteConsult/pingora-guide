//! # IP Restriction Middleware
//!
//! This module implements Access Control Lists (ACLs) based on IP addresses.
//! It is designed to be the *first* middleware in the chain to reject malicious
//! traffic as early as possible.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `IpStrategy` Enum**:
//!     * `Allowlist(Vec<IpNetwork>)`: Only IPs in these ranges pass.
//!     * `Blocklist(Vec<IpNetwork>)`: IPs in these ranges are rejected.
//!     * We rely on the `ipnet` crate for CIDR parsing (e.g., "192.168.0.0/24").
//!
//! 2.  **Define `IpRestrictionMiddleware` Struct**:
//!     * Field: `strategy: IpStrategy`.
//!
//! 3.  **Implement `Middleware` Trait**:
//!     * **`handle_request`**:
//!         * Extract the client IP using `session.client_addr()`.
//!         * Convert the Pingora `SocketAddr` to a standard `std::net::IpAddr`.
//!         * Check if the IP matches the strategy.
//!         * **On Match (Allow) / No Match (Block)**: Return `Ok(MiddlewareDecision::Continue)`.
//!         * **On Mismatch (Allow) / Match (Block)**:
//!             * Log a warning.
//!             * Call `session.respond_error(403)`.
//!             * Return `Ok(MiddlewareDecision::Stop)`.

use std::str::FromStr;
use async_trait::async_trait;
use ipnet::IpNet;
use pingora::prelude::Session;

use crate::upstreams::router::RouteEntry;
use crate::context::GatewayContext;
use crate::error::{GatewayError, PingoraGuideError, Result};
use crate::middleware::{Middleware, MiddlewareDecision};

pub struct IpRestrictionMiddleware;

#[async_trait]
impl Middleware for IpRestrictionMiddleware {
    fn name(&self) -> &str {
        "ip_restriction"
    }

    async fn handle_request(
        &self, session: &mut Session,
        ctx: &mut GatewayContext
    ) -> Result<MiddlewareDecision> {
        let route = match ctx.get::<RouteEntry>() {
            Some(r) => r,
            None => return Ok(MiddlewareDecision::Continue)
        };


        let acl = match &route.access_control {
            Some(acl) => acl,
            None => return Ok(MiddlewareDecision::Continue)
        };

        let sock_addr = match session.client_addr() {
            Some(ip) => match ip.as_inet() {
                Some(inet_ip) => inet_ip,
                None => return Ok(MiddlewareDecision::Continue)
            },
            None => return Ok(MiddlewareDecision::Continue)
        };
        let ip = sock_addr.ip();

        for deny_block in &acl.deny {
             if deny_block.contains(&ip) {
                session.respond_error(403)
                    .await
                    .map_err(|e| {
                        PingoraGuideError::Gateway(GatewayError::AclError(e.to_string()))
                    })?;
                return Ok(MiddlewareDecision::Stop);
            }
        }

        for allow_block in &acl.allow {
            if allow_block.contains(&ip) {
                return Ok(MiddlewareDecision::Continue);
            }
        }

        if !acl.allow.is_empty() {
            session.respond_error(403)
                .await
                .map_err(|e| {
                    PingoraGuideError::Gateway(GatewayError::AclError(e.to_string()))
                })?;
            Ok(MiddlewareDecision::Stop)
        } else {
            Ok(MiddlewareDecision::Continue)
        }
    }
}