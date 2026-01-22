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
use std::collections::HashMap;
use regex::Regex;
use crate::config::{RouteConf, GatewayConf, PathType};

#[derive(Debug)]
pub struct Router {
    pub regex_routes: Vec<(Regex, usize)>,
    pub matchit_router: matchit::Router<usize>,
    pub route_registry: HashMap<usize, RouteConf>
}

impl Router {
    pub fn new(conf: &GatewayConf) -> Self {
        let mut regex_routes: Vec<(Regex, usize)> = Vec::new();
        let mut matchit_router: matchit::Router<usize> = matchit::Router::new();
        let mut route_registry: HashMap<usize, RouteConf> = HashMap::new();

        for (idx, route) in conf.routes.iter().enumerate() {
            route_registry.insert(idx, route.clone());
            match route.path_type {
                PathType::Regex => {
                    let re = Regex::new(&route.path).expect("Invalid Regex in configuration");
                    regex_routes.push((re, idx));
                },
                PathType::Prefix | PathType::Exact => {
                    if !route.path.starts_with('/') {
                        eprintln!("Warning: Prefix route '{}' does not start with '/'", route.path);
                        continue;
                    }

                    match matchit_router.insert(&route.path, idx) {
                        Ok(_) => {},
                        Err(e) => { eprintln!("Failed to insert route '{}': {}", route.path, e); }
                    }
                }
            }
        }

        Self {
            regex_routes,
            matchit_router,
            route_registry
        }
    }

    pub fn match_request(&self, path: &str, host: Option<&str>) -> Option<&RouteConf> {
        for (re, id) in &self.regex_routes {
            if re.is_match(path) {
                let route = &self.route_registry[id];
                if self.validate_host(route, host) {
                    return Some(route)
                }
            }
        }

        if let Ok(match_result)  = self.matchit_router.at(path) {
            let id = match_result.value;
            let route = &self.route_registry[id];

            if matches!(route.path_type, PathType::Exact) && path != route.path {
                return None;
            }

            if self.validate_host(route, host) {
                return Some(route)
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
