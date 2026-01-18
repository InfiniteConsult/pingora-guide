//! # Authentication Middleware
//!
//! This module handles identity verification. It does *not* just block requests;
//! it actively populates the `GatewayContext` with a `UserIdentity`, allowing
//! subsequent layers (like Rate Limiting) to apply per-user policies.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `UserIdentity` Struct**:
//!     * Fields: `sub: String` (Subject/ID), `roles: Vec<String>`.
//!     * This struct will be inserted into the `GatewayContext`.
//!
//! 2.  **Define `AuthStrategy` Enum**:
//!     * `Bearer(String)`: Hardcoded token (simple equality check).
//!     * `Basic(String, String)`: Username and Password.
//!     * `Header(String)`: Custom API Key header name.
//!
//! 3.  **Define `AuthMiddleware` Struct**:
//!     * Field: `strategy: AuthStrategy`.
//!
//! 4.  **Implement `Middleware` Trait**:
//!     * **`handle_request`**:
//!         * Inspect `session.req_header()`.
//!         * **Case 1: Missing Credentials**:
//!             * If Basic Auth is configured, send `WWW-Authenticate` header + 401.
//!             * Otherwise, send generic 401.
//!             * Return `Stop`.
//!         * **Case 2: Invalid Credentials**:
//!             * Send 403 Forbidden.
//!             * Return `Stop`.
//!         * **Case 3: Success**:
//!             * Create a `UserIdentity`.
//!             * Insert it into `ctx`.
//!             * Return `Continue`.