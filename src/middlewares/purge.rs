//! # Cache Purging Middleware
//!
//! This module provides an administrative backdoor to remove items from the cache
//! immediately.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `PurgeMiddleware` Struct**:
//!     * Fields:
//!         * `storage`: `Arc<MemCache>` - Must share the *same* instance as `CacheMiddleware`.
//!
//! 2.  **Implement `Middleware` Trait**:
//!     * **`handle_request`**:
//!         * Check method: `if session.req_header().method.as_str() == "PURGE"`.
//!         * If Match:
//!             * Reconstruct `CacheKey` (using logic identical to `CacheMiddleware`).
//!             * Call `self.storage.purge(key)`.
//!             * Return `200 OK`.
//!             * Return `Stop` (Do not forward PURGE requests to upstream).
//!         * If No Match:
//!             * Return `Continue`.