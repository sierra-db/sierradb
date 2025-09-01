//! # SierraDB Protocol
//!
//! Shared protocol types and utilities for SierraDB client and server
//! communication. This crate defines the common types used across the SierraDB
//! ecosystem without any external dependencies.

pub mod error;

use std::{cmp, fmt, num::ParseIntError, ops, str};

pub use error::ErrorCode;
use serde::{Deserialize, Serialize};

/// The expected version **before** the event is inserted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpectedVersion {
    /// Accept any version, whether the stream/partition exists or not.
    #[default]
    Any,
    /// The stream/partition must exist (have at least one event).
    Exists,
    /// The stream/partition must be empty (have no events yet).
    Empty,
    /// The stream/partition must be exactly at this version.
    Exact(u64),
}

impl ExpectedVersion {
    pub fn from_next_version(version: u64) -> Self {
        if version == 0 {
            ExpectedVersion::Empty
        } else {
            ExpectedVersion::Exact(version - 1)
        }
    }

    pub fn into_next_version(self) -> Option<u64> {
        match self {
            ExpectedVersion::Empty => Some(0),
            ExpectedVersion::Exact(version) => version.checked_add(1),
            _ => panic!("expected no stream or exact version"),
        }
    }

    /// Calculate the gap between expected and current version.
    /// Returns VersionGap::None if the expectation is satisfied.
    pub fn gap_from(self, current: CurrentVersion) -> VersionGap {
        match (self, current) {
            // Any version is acceptable
            (ExpectedVersion::Any, _) => VersionGap::None,

            // Must exist - check if stream has events
            (ExpectedVersion::Exists, CurrentVersion::Empty) => VersionGap::Incompatible,
            (ExpectedVersion::Exists, CurrentVersion::Current(_)) => VersionGap::None,

            // Must be empty - check if stream is empty
            (ExpectedVersion::Empty, CurrentVersion::Empty) => VersionGap::None,
            (ExpectedVersion::Empty, CurrentVersion::Current(n)) => VersionGap::Ahead(n + 1),

            // Must be at exact version
            (ExpectedVersion::Exact(expected), CurrentVersion::Empty) => {
                VersionGap::Behind(expected + 1)
            }
            (ExpectedVersion::Exact(expected), CurrentVersion::Current(current)) => {
                match expected.cmp(&current) {
                    cmp::Ordering::Equal => VersionGap::None,
                    cmp::Ordering::Greater => VersionGap::Behind(expected - current),
                    cmp::Ordering::Less => VersionGap::Ahead(current - expected),
                }
            }
        }
    }

    /// Check if the current version satisfies the expectation
    pub fn is_satisfied_by(self, current: CurrentVersion) -> bool {
        matches!(self.gap_from(current), VersionGap::None)
    }
}

impl fmt::Display for ExpectedVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpectedVersion::Any => write!(f, "any"),
            ExpectedVersion::Exists => write!(f, "exists"),
            ExpectedVersion::Empty => write!(f, "empty"),
            ExpectedVersion::Exact(version) => version.fmt(f),
        }
    }
}

impl str::FromStr for ExpectedVersion {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "empty" => Ok(ExpectedVersion::Empty),
            "any" => Ok(ExpectedVersion::Any),
            "exists" => Ok(ExpectedVersion::Exists),
            num_str => {
                let num = num_str.parse::<u64>()?;
                Ok(ExpectedVersion::Exact(num))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Actual position of a stream.
pub enum CurrentVersion {
    /// The stream/partition doesn't exist.
    Empty,
    /// The last stream version/partition sequence.
    Current(u64),
}

impl CurrentVersion {
    pub fn next(&self) -> u64 {
        match self {
            CurrentVersion::Current(version) => version + 1,
            CurrentVersion::Empty => 0,
        }
    }

    pub fn as_expected_version(&self) -> ExpectedVersion {
        match self {
            CurrentVersion::Current(version) => ExpectedVersion::Exact(*version),
            CurrentVersion::Empty => ExpectedVersion::Empty,
        }
    }
}

impl fmt::Display for CurrentVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CurrentVersion::Current(version) => version.fmt(f),
            CurrentVersion::Empty => write!(f, "empty"),
        }
    }
}

impl str::FromStr for CurrentVersion {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "empty" => Ok(CurrentVersion::Empty),
            num_str => {
                let num = num_str.parse::<u64>()?;
                Ok(CurrentVersion::Current(num))
            }
        }
    }
}

impl ops::AddAssign<u64> for CurrentVersion {
    fn add_assign(&mut self, rhs: u64) {
        match self {
            CurrentVersion::Current(current) => *current += rhs,
            CurrentVersion::Empty => {
                if rhs > 0 {
                    *self = CurrentVersion::Current(rhs - 1)
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VersionGap {
    /// No gap - expectation is satisfied
    None,
    /// Stream is ahead by this many versions
    Ahead(u64),
    /// Stream is behind by this many versions  
    Behind(u64),
    /// Incompatible expectation (e.g., expecting exists but stream is empty)
    Incompatible,
}
