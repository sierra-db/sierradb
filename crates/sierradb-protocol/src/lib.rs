//! # SierraDB Protocol
//! 
//! Shared protocol types and utilities for SierraDB client and server communication.
//! This crate defines the common types used across the SierraDB ecosystem without
//! any external dependencies.

pub mod error;

pub use error::{ErrorCode, ParsedError};