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