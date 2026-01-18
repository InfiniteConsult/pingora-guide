// src/lib.rs
// This library module will hold shared logic for our examples,
// such as common error types or mock upstream server helpers.

// Core Architecture Modules
pub mod config;
pub mod context;
pub mod error;
pub mod gateway;

// Interface Definitions
pub mod middleware;
pub mod upstream;

// Implementation Directories
pub mod middlewares;
pub mod upstreams;