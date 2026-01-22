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

use std::time::Instant;
use prometheus::{
    register_histogram_vec, register_int_counter_vec, HistogramVec, IntCounterVec, Opts,
    HistogramOpts
};
use async_trait::async_trait;
use pingora::Error;
use pingora::prelude::Session;
use crate::context::{GatewayContext, RequestMeta};
use crate::middleware::{Middleware, MiddlewareDecision};
use crate::error::Result;

pub struct MetricsMiddleware {
    pub req_metric: IntCounterVec,
    pub latency_metric: HistogramVec,
}

impl MetricsMiddleware {
    pub fn new() -> Self {
        Self {
            req_metric: register_int_counter_vec!(
                Opts::new("http_requests_total", "Total number of HTTP requests"),
                &["method", "path", "status", "upstream"]
            ).unwrap(),
            latency_metric: register_histogram_vec!(
                HistogramOpts::new("http_request_duration_seconds", "The HTTP request latency in seconds."),
                &["method", "path", "status", "upstream"]
            ).unwrap()
        }
    }
}

#[async_trait]
impl Middleware for MetricsMiddleware {
    fn name(&self) -> &str {
        "metrics"
    }

    async fn handle_request(
        &self,
        _session: &mut Session,
        ctx: &mut GatewayContext
    ) -> Result<MiddlewareDecision> {
        let req_meta = ctx.get_mut::<RequestMeta>();
        match req_meta {
            Some(meta) => { meta.start_time = Instant::now() },
            None => { ctx.insert(RequestMeta::default()); }
        }
        Ok(MiddlewareDecision::Continue)
    }

    async fn handle_logging(
        &self,
        session: &mut Session,
        _e: Option<&Error>,
        ctx: &mut GatewayContext
    ) {
        // Use 'get' instead of 'get_mut' since we are only reading values
        let req_meta = ctx.get::<RequestMeta>();

        match req_meta {
            Some(meta) => {
                let method = session.req_header().method.to_string();

                let path = match &meta.matched_route_id {
                    Some(route_id) => route_id.to_owned(),
                    None => "unknown".to_string(),
                };

                let upstream_id = match &meta.upstream_id {
                    Some(id) => id.to_owned(),
                    None => "unknown".to_string(),
                };

                let status = match session.response_written() {
                    Some(resp) => resp.status.as_str(),
                    None => "0",
                };

                let labels = &[
                    method.as_str(),
                    path.as_str(),
                    status,
                    upstream_id.as_str()
                ];

                self.req_metric.with_label_values(labels).inc();

                self.latency_metric
                    .with_label_values(labels)
                    .observe(meta.start_time.elapsed().as_secs_f64());
            }
            None => {
                eprintln!("Error: RequestMeta missing during logging phase.");
            }
        }
    }
}