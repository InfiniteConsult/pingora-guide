//! # Gateway Orchestrator
//!
//! The core engine of the library. This struct implements Pingora's native
//! `ProxyHttp` trait and wires together our custom `Middleware` pipeline with
//! our `Upstream` routing logic.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `PingoraGateway` Struct**:
//!     * Fields:
//!         * `upstream`: `Box<dyn Upstream>` (Usually the Router).
//!         * `middlewares`: `Vec<Box<dyn Middleware>>` (The plugin chain).
//!
//! 2.  **Define Associated Type**:
//!     * `type CTX = GatewayContext`.
//!
//! 3.  **Implement `ProxyHttp` Trait**:
//!     * **`new_ctx`**: Initialize an empty `GatewayContext`.
//!     * **`request_filter`**:
//!         * Iterate through `self.middlewares`.
//!         * Call `mw.handle_request(session, ctx)`.
//!         * If any returns `MiddlewareDecision::Stop`, return `Ok(true)` immediately.
//!     * **`upstream_peer`**:
//!         * Call `self.upstream.select_peer(session, ctx)`.
//!         * Map our custom error to Pingora's expected error type.
//!     * **`response_filter`**:
//!         * Iterate through middlewares (order: typically same as request, or reversed).
//!         * Call `mw.handle_response(session, response_header, ctx)`.
//!     * **`logging`**:
//!         * Iterate through middlewares.
//!         * Call `mw.handle_logging(session, error, ctx)`.