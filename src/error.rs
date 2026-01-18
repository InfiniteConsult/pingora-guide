//! # Error Handling Module
//!
//! This module provides a unified error type for the Gateway library, wrapping both
//! internal logic errors and external Pingora errors.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `GatewayError` Enum**:
//!     * This enum captures semantic errors specific to our domain.
//!     * Variants:
//!         * `UpstreamUnavailable`: When the load balancer has no healthy backends.
//!         * `AuthFailure`: When a token is missing or invalid.
//!         * `RateLimited`: When a user exceeds their quota.
//!         * `InvalidRequest(String)`: For bad headers, size limits, etc.
//!         * `InternalError(String)`: For unexpected states.
//!
//! 2.  **Define `Error` Enum**:
//!     * The top-level error container.
//!     * Variants:
//!         * `Gateway(GatewayError)`: Our custom errors.
//!         * `Pingora(pingora::Error)`: Errors propagated from the core framework.
//!
//! 3.  **Implement Traits**:
//!     * `std::fmt::Display`: For nice logging.
//!     * `std::error::Error`: Standard trait implementation.
//!     * `From<pingora::Error>` for `Error`: To easily use `?` on Pingora calls.
//!     * `From<Error>` for `Box<pingora::Error>`: CRITICAL. This allows us to return
//!         our custom error type from `ProxyHttp` trait methods, which expect
//!         Pingora's boxed error type.
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq)]
pub enum GatewayError {
    UpstreamUnavailable,
    AuthFailure,
    RateLimited,
    InvalidRequest(String),
    InternalError(String),
}

impl fmt::Display for GatewayError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for GatewayError {}

#[derive(Debug)]
pub enum PingoraGuideError {
    Gateway(GatewayError),
    Pingora(pingora::Error),
}

impl std::error::Error for PingoraGuideError {}

impl fmt::Display for PingoraGuideError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PingoraGuideError::Gateway(e) => write!(f, "Gateway Error {}", e),
            PingoraGuideError::Pingora(e) => write!(f, "Pingora Error {}", e),
        }
    }
}

impl From<pingora::Error> for PingoraGuideError {
    fn from(value: pingora::Error) -> Self {
        PingoraGuideError::Pingora(value)
    }
}

impl From<PingoraGuideError> for Box<pingora::Error> {
    fn from(value: PingoraGuideError) -> Self {
        match value {
            PingoraGuideError::Pingora(error) => Box::new(error),
            PingoraGuideError::Gateway(error) => {
                pingora::Error::explain(
                    pingora::ErrorType::Custom("GatewayError"),
                    error.to_string()
                )
            },
        }
    }
}

pub type Result<T> = std::result::Result<T, PingoraGuideError>;