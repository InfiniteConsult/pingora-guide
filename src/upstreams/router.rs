//! # Request Router
//!
//! A dispatching implementation of the `Upstream` trait. It doesn't connect to
//! servers itself; it delegates to *other* `Upstream` instances based on the request path.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `Router` Struct**:
//!     * Fields:
//!         * `routes`: `matchit::Router<Box<dyn Upstream>>`.
//!         * We use the `matchit` crate (or a simple `HashMap` for this guide) to store
//!           path prefixes (e.g., `/api`) mapping to specific Upstreams.
//!
//! 2.  **Implement `Upstream` Trait**:
//!     * **`select_peer`**:
//!         * Extract the path from `session.req_header().uri.path()`.
//!         * Look up the best match in the routing table.
//!         * If found: Call `select_peer()` on the matched child upstream.
//!         * If not found: Return `Err(Error::Gateway(GatewayError::NoRouteMatched))`
//!           (which typically translates to a 404).
use std::str::FromStr;
use std::sync::Arc;
use std::collections::HashMap;
use regex::Regex;
use ipnet::IpNet;
use crate::config::{RouteConf, GatewayConf, PathType, AccessControlConf};
use crate::error::{GatewayError, PingoraGuideError, Result};
use crate::middleware::MiddlewareDecision;

#[derive(Debug, Clone)]
pub struct CompiledAccessControl {
    pub allow: Vec<IpNet>,
    pub deny: Vec<IpNet>,
}

#[derive(Debug, Clone)]
pub struct RouteEntry {
    pub conf: Arc<RouteConf>,
    pub access_control: Option<Arc<CompiledAccessControl>>
}

#[derive(Debug)]
pub struct Router {
    pub regex_routes: Vec<(Regex, usize)>,
    pub matchit_router: matchit::Router<usize>,
    pub route_registry: HashMap<usize, RouteEntry>
}

impl Router {
    pub fn new(conf: &GatewayConf) -> Result<Self> {
        let mut regex_routes: Vec<(Regex, usize)> = Vec::new();
        let mut matchit_router: matchit::Router<usize> = matchit::Router::new();
        let mut route_registry: HashMap<usize, RouteEntry> = HashMap::new();

        for (idx, route) in conf.routes.iter().enumerate() {
            match route.path_type {
                PathType::Regex => {
                    let re = match Regex::new(&route.path) {
                        Ok(re) => re,
                        Err(e) => {
                            return Err(PingoraGuideError::Gateway(GatewayError::RouteError(format!(
                                "Invalid Regex in configuration '{}': {}",
                                route.path,
                                e
                            ))));
                        }
                    };
                    regex_routes.push((re, idx));
                },
                PathType::Prefix | PathType::Exact => {
                    if !route.path.starts_with('/') {
                        return Err(PingoraGuideError::Gateway(GatewayError::RouteError(format!(
                            "Warning: Prefix route '{}' does not start with '/'",
                            route.path
                        ))));

                    }

                    match matchit_router.insert(&route.path, idx) {
                        Ok(_) => {},
                        Err(e) => {
                            return Err(PingoraGuideError::Gateway(GatewayError::RouteError(format!(
                                "Failed to insert route '{}': {}",
                                route.path,
                                e
                            ))));
                        }
                    }
                }
            }

            let mut acl_option: Option<Arc<CompiledAccessControl>> = None;
            if route.access_control.is_some() {
                let mut compiled_acl = CompiledAccessControl {
                    allow: Vec::new(),
                    deny: Vec::new(),
                };

                let acl = route.access_control.as_ref().unwrap();
                for deny_block in &acl.deny {
                    let deny_range = match IpNet::from_str(deny_block) {
                        Ok(ip_range) => ip_range,
                        Err(e) => return Err(PingoraGuideError::Gateway(GatewayError::AclError(e.to_string())))
                    };
                    compiled_acl.deny.push(deny_range);
                }

                for allow_block in &acl.allow {
                    let allow_range = match IpNet::from_str(allow_block) {
                        Ok(ip_range) => ip_range,
                        Err(e) => return Err(PingoraGuideError::Gateway(GatewayError::AclError(e.to_string())))
                    };
                    compiled_acl.allow.push(allow_range);
                }

                if !compiled_acl.allow.is_empty() || !compiled_acl.deny.is_empty() {
                    acl_option = Some(Arc::new(compiled_acl));
                }
            }

            let route_entry = RouteEntry {
                conf: Arc::new(route.clone()),
                access_control: acl_option
            };

            route_registry.insert(idx, route_entry);
        }

        Ok(Self {
            regex_routes,
            matchit_router,
            route_registry
        })
    }

    pub fn match_request(&self, path: &str, host: Option<&str>) -> Option<&RouteEntry> {

        if let Ok(match_result)  = self.matchit_router.at(path) {
            let id = match_result.value;
            let route = &self.route_registry[id];

            if matches!(route.conf.path_type, PathType::Exact) && path != route.conf.path {
                return None;
            }

            if self.validate_host(route.conf.as_ref(), host) {
                return Some(route)
            }
        }

        for (re, id) in &self.regex_routes {
            if re.is_match(path) {
                let route = &self.route_registry[id];
                if self.validate_host(route.conf.as_ref(), host) {
                    return Some(route)
                }
            }
        }
        
        None
    }

    fn validate_host(&self, route: &RouteConf, host: Option<&str>) -> bool {
        match &route.hostnames {
            Some(allowed_hosts) => {
                match host {
                    Some(h) => allowed_hosts.iter().any(|allowed| allowed == h),
                    None => false
                }
            },
            None => true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RouteConf, PathType};

    // Helper to create the standard "Kitchen Sink" test configuration
    fn make_test_router() -> Router {
        let routes = vec![
            // [Route 0]: Regex Promiscuous (Matches ^/api/v\d+)
            RouteConf {
                path: r"^/api/v\d+".to_string(),
                path_type: PathType::Regex,
                upstream_id: "api-cluster".to_string(),
                hostnames: None,
                ..RouteConf::default()
            },
            // [Route 1]: Regex Strict (Matches ^/admin, Host: admin.local)
            RouteConf {
                path: r"^/admin".to_string(),
                path_type: PathType::Regex,
                upstream_id: "admin-cluster".to_string(),
                hostnames: Some(vec!["admin.local".to_string()]),
                ..RouteConf::default()
            },
            // [Route 2]: Prefix Standard (Matches /static/*)
            RouteConf {
                path: "/static/{*path}".to_string(),
                path_type: PathType::Prefix,
                upstream_id: "static-cluster".to_string(),
                hostnames: None,
                ..RouteConf::default()
            },
            // [Route 3]: Exact Match (Matches /login)
            RouteConf {
                path: "/login".to_string(),
                path_type: PathType::Exact,
                upstream_id: "auth-cluster".to_string(),
                hostnames: None,
                ..RouteConf::default()
            },
            // [Route 4]: Broken Route (No leading slash)
            RouteConf {
                path: "invalid/path".to_string(),
                path_type: PathType::Prefix,
                upstream_id: "garbage".to_string(),
                hostnames: None,
                ..RouteConf::default()
            },
        ];

        let conf = GatewayConf {
            routes,
            ..GatewayConf::default()
        };

        Router::new(&conf).unwrap()
    }

    // --- Regex Tests ---
    #[test]
    fn regex_promiscuous_match() {
        // TC-01: Matches Regex, ignores Host
        let router = make_test_router();
        let result = router.match_request("/api/v1/user", Some("any.com"));

        assert!(result.is_some());
        assert_eq!(result.unwrap().conf.upstream_id, "api-cluster");
    }

    #[test]
    fn regex_strict_match() {
        // TC-02: Matches Regex AND Host
        let router = make_test_router();
        let result = router.match_request("/admin/settings", Some("admin.local"));

        assert!(result.is_some());
        assert_eq!(result.unwrap().conf.upstream_id, "admin-cluster");
    }

    #[test]
    fn regex_strict_mismatch_host_continues() {
        // TC-03: Matches path, but Host fails. Should NOT return a result (and shouldn't panic).
        // Since there is no fallback route in our config that matches /admin, it returns None.
        let router = make_test_router();
        let result = router.match_request("/admin/settings", Some("public.com"));

        assert!(result.is_none());
    }

    // --- Prefix & Exact Tests ---

    #[test]
    fn prefix_standard_match() {
        // TC-04: Matchit finds prefix
        let router = make_test_router();
        let result = router.match_request("/static/css/style.css", Some("foo.com"));

        assert!(result.is_some());
        assert_eq!(result.unwrap().conf.upstream_id, "static-cluster");
    }

    #[test]
    fn exact_match_success() {
        // TC-05: Exact match
        let router = make_test_router();
        let result = router.match_request("/login", Some("bar.com"));

        assert!(result.is_some());
        assert_eq!(result.unwrap().conf.upstream_id, "auth-cluster");
    }

    #[test]
    fn exact_match_enforcement_fail() {
        // TC-06: Prefix matches, but 'Exact' type rejects sub-paths
        let router = make_test_router();
        let result = router.match_request("/login/attempt", Some("bar.com"));

        assert!(result.is_none());
    }

    // --- Edge Case & Security Tests ---

    #[test]
    fn invalid_route_skipped() {
        // TC-07: The 'invalid/path' route should simply not exist in the router.
        let router = make_test_router();
        let result = router.match_request("invalid/path", None);
        // Note: Pingora usually normalizes paths to start with /, so checking "invalid/path"
        // directly tests if it ended up in matchit (which it shouldn't have).
        assert!(result.is_none());
    }

    #[test]
    fn global_fallback_none() {
        // TC-08: Path matches nothing
        let router = make_test_router();
        let result = router.match_request("/unknown", Some("any.com"));

        assert!(result.is_none());
    }

    #[test]
    fn missing_host_header_security() {
        // TC-09: Route requires host, request has None.
        let router = make_test_router();
        let result = router.match_request("/admin", None);

        assert!(result.is_none());
    }
}
