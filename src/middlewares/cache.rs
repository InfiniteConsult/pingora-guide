//! # Caching Middleware
//!
//! This module implements a high-performance HTTP caching layer. It handles
//! storage, expiration policies, and advanced optimization techniques like
//! Request Coalescing and Stale-While-Revalidate (SWR).
//!
//! ## Implementation Plan
//!
//! 1.  **Define `CacheMiddleware` Struct**:
//!     * Fields:
//!         * `storage`: `Arc<MemCache>` - The backend storage (RAM).
//!         * `lock`: `Arc<CacheLock>` - To prevent Thundering Herds (Lesson 45).
//!         * `defaults`: `CacheMetaDefaults` - Fallback policies (e.g. 60s for 200 OK).
//!
//! 2.  **Implement `Middleware` Trait**:
//!     * **`handle_request`**:
//!         * Generate `CacheKey` from URL path.
//!         * Call `session.cache.enable()` with the storage and the lock.
//!         * Insert a "CacheEnabled" flag into `ctx` (useful for logging).
//!         * Return `Continue`.
//!     * **`handle_response`**:
//!         * Use `pingora_cache::filters::resp_cacheable`.
//!         * This parses `Cache-Control` headers (RFC compliant) to decide if
//!           the response should be stored.
//!         * Return `Ok`.
//!     * **`should_serve_stale` (New Hook)**:
//!         * If the middleware trait supports this (it should, as per Part 4 spec),
//!           implement SWR logic.
//!         * Return `true` if `error` is `None` (meaning expired but clean).
//!         * This allows serve-old-while-fetching-new behavior.