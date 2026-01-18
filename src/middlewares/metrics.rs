//! # Prometheus Metrics Middleware
//!
//! This module provides observability. It measures request latency and counts,
//! tagged by status code and path. It demonstrates how to persist state (timers)
//! from the start of a request to the end.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `RequestTimer` Struct**:
//!     * Field: `start: Instant`.
//!     * Inserted into `GatewayContext` at the start of a request.
//!
//! 2.  **Define `MetricsMiddleware` Struct**:
//!     * Fields:
//!         * `req_counter`: `IntCounterVec` (Labels: method, status).
//!         * `req_histogram`: `HistogramVec` (Labels: method, status).
//!     * The struct should initialize these metrics in its constructor using the
//!       `prometheus` crate macros.
//!
//! 3.  **Implement `Middleware` Trait**:
//!     * **`handle_request`**:
//!         * Create `RequestTimer { start: Instant::now() }`.
//!         * Insert into `ctx`.
//!         * Return `Continue`.
//!     * **`handle_logging`**:
//!         * Retrieve `RequestTimer` from `ctx`.
//!         * Calculate `elapsed()`.
//!         * Extract status code from `session` or `error`.
//!         * Extract HTTP method.
//!         * **Record**:
//!             * `req_counter.with_label_values(&[method, status]).inc()`
//!             * `req_histogram.with_label_values(&[method, status]).observe(duration)`