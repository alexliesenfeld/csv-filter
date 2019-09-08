extern crate csv_filter_config as config;
extern crate csv_filter_filter as filter;
extern crate csv_filter_sort as sort;

use config::FilterConfig;
use core::cmp;
use std::fs;
use std::sync::Arc;

/// # Arguments
/// * `csv_file_path` - Path to the CSV file that should be processed
/// * `config_file_path` - Path to the configuration file
/// * `output_dir_path` - Path to the directory that data should be written to
/// * `no_sort` - If sorting output files should be disabled
/// * `filter_parallelism` - Number of threads to use in the filtering stage.
/// * `sort_parallelism` -  Number of threads to use in the sorting stage (this implicitly sets
///                         the amount of files that can be sorted at a time)
///
/// # Panics
/// This function will panic on any error.
pub fn process(
    csv_file_path: &str,
    config_file_path: &str,
    output_dir_path: &str,
    no_sort: bool,
    filter_parallelism: usize,
    sort_parallelism: usize,
) {
    let filter_max_threads = cmp::max(1, filter_parallelism);
    println!(
        "Using up to {} threads for the filter stage",
        filter_max_threads
    );

    let all_filter_configs = read_filter_configs(config_file_path);

    filter::filter(
        csv_file_path,
        &all_filter_configs,
        output_dir_path,
        filter_max_threads,
    );

    if !no_sort {
        let sort_max_threads = cmp::max(1, sort_parallelism);
        println!(
            "Using up to {} threads for the sort stage",
            sort_max_threads
        );

        sort::sort_output_files(&all_filter_configs, output_dir_path, sort_max_threads);
    }
}

/// Reads all filter configurations from a config file. Returns a list of [`FilterConfig`] with
/// the contents from the config file.
///
/// # Arguments
/// * `file_path` - Path of the JSON configuration file
fn read_filter_configs(file_path: &str) -> Vec<Arc<FilterConfig>> {
    let json = fs::read_to_string(file_path).expect("Cannot read config file");
    let mut read_configs = config::deserialize(&json);

    for config in &read_configs {
        validate_config(&config).expect("Invalid configuration");
    }

    let mut filters: Vec<Arc<FilterConfig>> = Vec::new();
    while let Some(fc) = read_configs.pop() {
        filters.push(Arc::new(fc))
    }

    filters
}

/// Validates a [`FilterConfig`].
///
/// # Arguments
/// * `config` - The config to validate.
fn validate_config(config: &FilterConfig) -> Result<(), String> {
    // Makes sure there is at least one column that will be included per output file
    if config.filters.iter().all(|f| f.include == false) {
        return Err(format!(
            "Config for output file '{}' does not contain any output columns",
            &config.output
        ));
    }

    // Makes sure no config uses "min"/"max" values along with explicitly defined "values".
    for cf in &config.filters {
        if cf.values.is_some() && (cf.max.is_some() || cf.min.is_some()) {
            return Err(format!(
                "Config for output file '{}' defines values and a range (min/max)",
                &config.output
            ));
        }
    }

    // Makes sure all configs only use sort columns that do exist in the corresponding output file
    if let Some(sort_columns) = &config.sort_columns {
        let included_columns: Vec<String> = config
            .filters
            .iter()
            .filter(|f| f.include)
            .map(|f| f.column.to_string())
            .collect();

        for column in sort_columns {
            if !included_columns.contains(column) {
                return Err(format!(
                    "Config for output file '{}' contains sort column '{}' which is not part of the output file",
                    &config.output,
                    column
                ));
            }
        }
    }

    Ok(())
}
