//! # In-Flight Concurrency Middleware
//!
//! This module limits the number of *simultaneous* connections to the backend.
//! Unlike rate limiting (which measures speed), this measures capacity/load.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `ConnectionGuard` Wrapper**:
//!     * A struct that holds `Option<pingora_limits::inflight::Guard>`.
//!     * This wrapper will be stored in the `GatewayContext`.
//!     * **Why?** The `Guard` must live as long as the request. When the request
//!       ends, the Context is dropped, the Guard is dropped, and the slot is freed.
//!
//! 2.  **Define `ConcurrencyMiddleware` Struct**:
//!     * Field: `inflight`: `Arc<pingora_limits::inflight::Inflight>`.
//!     * Field: `max_concurrency`: `isize`.
//!
//! 3.  **Implement `Middleware` Trait**:
//!     * **`handle_request`**:
//!         * Call `self.inflight.incr("global_limit", 1)`.
//!         * **If count > max**:
//!             * Log warning.
//!             * Send `429 Too Many Requests`.
//!             * Return `Stop`. (Crucial: The guard returned by `incr` must be dropped here).
//!         * **If count <= max**:
//!             * Wrap the `Guard` in `ConnectionGuard`.
//!             * Insert it into `ctx`.
//!             * Return `Continue`.