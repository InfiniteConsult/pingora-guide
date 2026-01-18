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