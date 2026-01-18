//! # Capstone: Production API Gateway
//!
//! This binary is the culmination of the entire guide. It wires together all
//! the modules into a runnable server.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --release --bin api_gateway -- -c conf/gateway.yaml
//! ```
//!
//! ## Implementation Plan
//!
//! 1.  **Bootstrap**:
//!     * Initialize `Server`.
//!     * Initialize `env_logger`.
//!
//! 2.  **Initialize Shared State**:
//!     * Create `Arc<MemCache>` (shared by Cache and Purge logic).
//!     * Create `Arc<CacheLock>`.
//!     * Create `Arc<Rate>` (Global Rate Limiter).
//!
//! 3.  **Construct the Pipeline**:
//!     * Create `PingoraGateway` (the orchestrator).
//!
//!     * **Layer 1: Observability**:
//!         * Add `MetricsMiddleware`.
//!
//!     * **Layer 2: Security**:
//!         * Add `IpRestrictionMiddleware` (Allow `127.0.0.1/8` for admin).
//!         * Add `AuthMiddleware` (Bearer token).
//!         * Add `RateLimitMiddleware` (100 req/s).
//!
//!     * **Layer 3: Caching**:
//!         * Add `PurgeMiddleware`.
//!         * Add `CacheMiddleware`.
//!
//! 4.  **Construct the Router**:
//!     * Create `LoadBalancerUpstream` for the "Traffic" path.
//!     * Create `StaticUpstream` for the "Admin" path.
//!     * Wrap them in `Router`.
//!
//! 5.  **Launch**:
//!     * Register the `PingoraGateway` service on port 8080.
//!     * Register the `Prometheus` service on port 9091.
//!     * `server.run_forever()`.

fn main() {}