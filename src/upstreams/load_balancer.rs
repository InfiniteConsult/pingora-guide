//! # Load Balanced Cluster
//!
//! A robust implementation of the `Upstream` trait that wraps Pingora's native
//! `LoadBalancer` struct. This component handles health-aware traffic distribution.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `LoadBalancedCluster` Struct**:
//!     * Fields:
//!         * `lb`: `Arc<LoadBalancer<RoundRobin>>` - The core balancing logic engine.
//!         * `tls`: `bool` - Global setting for this cluster (e.g., "all backends are HTTPS").
//!         * `sni`: `String` - Global SNI setting.
//!
//! 2.  **Implement `Upstream` Trait**:
//!     * **`select_peer`**:
//!         * Call `self.lb.select(b"", 256)`.
//!         * **Error Handling**: If `select()` returns `None` (empty or unhealthy pool),
//!           return `Err(Error::Gateway(GatewayError::UpstreamUnavailable))`.
//!         * **Success**: Convert the selected `Backend` into a `Box<HttpPeer>`.
//!
//! 3.  **Note**: This struct assumes the `LoadBalancer` is already initialized and populated.
//!     It relies on external `BackgroundService`s (Lesson 30) to update the health status.