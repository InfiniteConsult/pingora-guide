//! # Middleware Interface
//!
//! This module defines the plugin system for the Gateway. It allows logic to be
//! injected at various stages of the request lifecycle: Request, Response, and Logging.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `MiddlewareDecision` Enum**:
//!     * Variants:
//!         * `Continue`: Proceed to the next middleware/stage.
//!         * `Stop`: Halt processing immediately (e.g., if a 403 was sent).
//!
//! 2.  **Define `Middleware` Trait**:
//!     * Must inherit `Sync + Send`.
//!     * **`handle_request`**:
//!         * Runs *before* upstream connection.
//!         * Can modify headers, check security, or return `Stop`.
//!     * **`handle_response`**:
//!         * Runs *after* headers are received from upstream.
//!         * Can modify response headers (e.g. `HSTS`) or decide cacheability.
//!     * **`handle_logging`**:
//!         * Runs *after* the session is finished.
//!         * Used for metrics and observability.
//!
//! 3.  **Default Implementations**:
//!     * Provide default "no-op" implementations for all methods so implementors
//!         only need to define the hooks they care about.