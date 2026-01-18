//! # Static Upstream Implementation
//!
//! A simple implementation of the `Upstream` trait that routes all traffic to a
//! single, hardcoded IP address and port.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `StaticUpstream` Struct**:
//!     * Fields:
//!         * `addr`: `(String, u16)` - The destination IP and port.
//!         * `tls`: `bool` - Whether to use HTTPS.
//!         * `sni`: `String` - The SNI hostname to present during handshake.
//!
//! 2.  **Implement `Upstream` Trait**:
//!     * **`select_peer`**:
//!         * Construct a `Box<HttpPeer>` using the stored configuration.
//!         * This is a "dumb" connector; it does no load balancing or health checking.
//!         * Useful for admin APIs or simple sidecars.