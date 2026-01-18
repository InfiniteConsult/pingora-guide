//! # Configuration Module
//!
//! This module defines the configuration schema for the Gateway. It uses `serde`
//! to deserialize a YAML file into strongly-typed structs that control the
//! behavior of the middlewares and upstreams.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `GatewayConf` Struct**:
//!     * The top-level container.
//!     * Fields:
//!         * `http_port`: `Option<u16>` (Default: 8080).
//!         * `admin_port`: `Option<u16>` (Default: 9090).
//!         * `upstreams`: `Vec<UpstreamConf>`.
//!         * `security`: `SecurityConf`.
//!
//! 2.  **Define `UpstreamConf` Struct**:
//!     * Represents a single backend cluster.
//!     * Fields:
//!         * `name`: `String` (e.g., "primary", "auth-service").
//!         * `addrs`: `Vec<String>` (IP:Port list).
//!         * `path_prefix`: `String` (For routing, e.g., "/api/v1").
//!         * `tls`: `bool` (Whether to use HTTPS to upstream).
//!         * `sni`: `Option<String>`.
//!
//! 3.  **Define `SecurityConf` Struct**:
//!     * Represents global security settings.
//!     * Fields:
//!         * `rate_limit`: `Option<i32>` (Req/sec per user).
//!         * `ip_allowlist`: `Option<Vec<String>>` (CIDR blocks).
//!         * `auth_token`: `Option<String>` (Static Bearer token for demo).
//!
//! 4.  **Helper Method**:
//!     * `load_from_yaml(path: &str) -> Result<Self>`:
//!         * specific function to read the file and run `serde_yaml::from_str`.