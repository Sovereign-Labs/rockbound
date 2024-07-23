//! All structs related to caching layer of Rockbound.

use crate::Schema;

pub mod delta_reader;

/// Response for a paginated query which also includes the "next" key to pass.
#[derive(Debug)]
pub struct PaginatedResponse<S: Schema> {
    /// A vector of storage keys and their values
    pub key_value: Vec<(S::Key, S::Value)>,
    /// Key indicating the first key after the final pair from key_value.
    /// Meant to be passed in subsequent queries
    pub next: Option<S::Key>,
}
