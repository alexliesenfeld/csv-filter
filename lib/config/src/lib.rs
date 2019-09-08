extern crate hashbrown;
extern crate serde_json;

use hashbrown::HashSet;
use serde::Deserialize;

/// Represents detailed column configuration of a filter configuration.
#[derive(Deserialize, Debug)]
pub struct ColumnFilter {
    pub column: String,
    pub include: bool,
    pub values: Option<HashSet<String>>,
    pub min: Option<String>,
    pub max: Option<String>,
}

/// Contains all data of one filter configuration item from a configuration file.
#[derive(Deserialize, Debug)]
pub struct FilterConfig {
    pub filters: Vec<ColumnFilter>,
    pub output: String,
    pub sort_columns: Option<Vec<String>>,
}

/// Deserializes the JSON configuration file and returns a list of [`FilterConfig`].
///
/// # Arguments
/// * `json` - The full configuration content as a JSON string.
///
/// # Panics
/// This function will panic on any error.
pub fn deserialize(json: &str) -> Vec<FilterConfig> {
    serde_json::from_str(json.trim()).expect("Cannot deserialize JSON config")
}
