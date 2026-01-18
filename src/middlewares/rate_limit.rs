//! # Rate Limiting Middleware
//!
//! This module implements a sliding window rate limiter to protect upstream services
//! from abusive traffic spikes. It supports dynamic key extraction (e.g., by IP or User ID).
//!
//! ## Implementation Plan
//!
//! 1.  **Define `RateLimitKey` Enum**:
//!     * `ClientIp`: Use the socket address.
//!     * `UserIdentity`: Use the `sub` field from the Auth middleware's `UserIdentity` (if present).
//!
//! 2.  **Define `RateLimitMiddleware` Struct**:
//!     * Fields:
//!         * `limiter`: `Arc<pingora_limits::rate::Rate>` - The shared counter state.
//!         * `max_req_per_sec`: `isize` - The threshold.
//!         * `key_strategy`: `RateLimitKey` - How to identify the caller.
//!
//! 3.  **Implement `Middleware` Trait**:
//!     * **`handle_request`**:
//!         * **Step 1: Extract Key**:
//!             * If strategy is `ClientIp`: Use `session.client_addr()`.
//!             * If strategy is `UserIdentity`: Look up `UserIdentity` in `GatewayContext`.
//!               If missing, fallback to IP or skip (configurable).
//!         * **Step 2: Observe**:
//!             * Call `self.limiter.observe(&key, 1)`.
//!         * **Step 3: Check**:
//!             * Call `self.limiter.rate(&key)`.
//!             * If `rate > max_req_per_sec`:
//!                 * Log warning.
//!                 * Send `429 Too Many Requests`.
//!                 * Return `Stop`.
//!             * Else: Return `Continue`.