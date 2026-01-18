//! # Request Size Middleware
//!
//! This module protects the backend from DoS attacks involving large payloads.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `RequestSizeMiddleware` Struct**:
//!     * Field: `max_bytes: usize`.
//!
//! 2.  **Implement `Middleware` Trait**:
//!     * **`handle_request`**:
//!         * Check the `Content-Length` header.
//!         * If `Content-Length > max_bytes`:
//!             * Log "Request payload too large".
//!             * Send `413 Payload Too Large`.
//!             * Return `Stop`.
//!         * If `Content-Length` is missing/chunked:
//!             * (Note: Strict streaming enforcement would require a `handle_body` hook
//!               in the trait. For this "Easy" implementation, we will log a warning
//!               that chunked encoding bypasses this check, or optionally block
//!               chunked requests entirely if strict security is required).
//!             * Return `Continue`.