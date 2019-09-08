//! The `csv_filter` crate provides a CSV file processor that is able to read
//! from an input CSV file and distribute the contents to a number of output
//! files according to a user provided configuration file.

extern crate crossbeam;
extern crate csv;
extern crate csv_filter_config;
extern crate csv_filter_util as util;
extern crate hashbrown;

use core::sync::atomic::{AtomicUsize, Ordering};
use csv_filter_config::FilterConfig;
use std::fs;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use crossbeam::channel::bounded as bounded_channel;
use csv::{Reader, ReaderBuilder, StringRecord};
use hashbrown::HashMap;

// These type definitions are only here for abbreviation
type OutputFileMap = Arc<HashMap<String, Mutex<csv::Writer<File>>>>;
type HeadersMap = Arc<HashMap<String, usize>>;

/// Processes a CSV file according to the provided configuration.
///
/// # Arguments
/// * `csv_file_path` - Path to the CSV file that should be processed
/// * `all_filter_configs` - A vector containing all configuration items
/// * `output_dir_path` - Path to the directory that data should be written to
/// * `max_threads` - The maximum number of threads to use
///
/// # Panics
/// This function will panic on any error.
pub fn filter(
    csv_file_path: &str,
    all_filter_configs: &Vec<Arc<FilterConfig>>,
    output_dir_path: &str,
    max_threads: usize,
) {
    let output_files = create_output_files(all_filter_configs, output_dir_path);
    write_headers_to_output_files(all_filter_configs, &output_files);
    process_csv(
        &output_files,
        all_filter_configs,
        csv_file_path,
        max_threads,
    );
}

/// Processes the CSV file.
///
/// # Arguments
/// * `output_files` - A map that maps a filename to its CSV file writer
/// * `filters` - A list of filter configurations
/// * `csv_file_path` - The path of the CSV file to read data from
/// * `max_threads` - The maximum number of threads to use
fn process_csv(
    output_files: &OutputFileMap,
    filters: &Vec<Arc<FilterConfig>>,
    csv_file_path: &str,
    max_threads: usize,
) {
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(csv_file_path)
        .expect("Cannot read CSV file");
    let headers = create_headers_map(&mut csv_reader);
    let row_counter = Arc::new(AtomicUsize::new(0));

    // We use a bounded channel here to limit how many CSV records can be queued at a time.
    // If an unbounded data structure is being used, memory consumption can become exhaustive.
    let (channel_sender, channel_receiver) = bounded_channel(1024);
    let mut threads = Vec::new();

    // The following will create channel consumer threads that will be consuming CSV records.
    for _ in 0..max_threads {
        let channel_receiver = channel_receiver.clone();
        let filters = filters.clone();
        let output_files = output_files.clone();
        let row_counter = row_counter.clone();
        let headers = headers.clone();

        threads.push(thread::spawn(move || {
            for csv_record in &channel_receiver {
                process_csv_record(csv_record, &filters, &output_files, &headers);

                let num = row_counter.fetch_add(1, Ordering::Relaxed);
                if num % 1000 == 0 {
                    println!("Number of processed CSV rows: {}", num);
                }
            }
        }));
    }

    // The following code will read from the CSV file record by record, and write each record into
    // the channel. The records will then be consumed by one of the consumer threads created above.
    for csv_record in csv_reader.records() {
        let csv_record = csv_record.expect("Cannot parse CSV record");
        channel_sender
            .send(csv_record)
            .expect("Error sending record to channel");
    }

    // Stopping the channel and wait for all threads to finish
    drop(channel_sender);
    for t in threads {
        t.join().expect("Cannot join thread.")
    }
}

/// Processes one CSV record. If the record matches the criteria of any filter configuration,
/// the row will be written out to its corresponding output file.
///
/// # Arguments
/// * `csv_record` - The record that needs to be processed
/// * `filters` -  A list of filter configurations
/// * `output_files` - Maps that maps a filename to its CSV file writer
/// * `headers` - Maps a CSV column name to its index in the current CSV file
fn process_csv_record(
    csv_record: StringRecord,
    filters: &Vec<Arc<FilterConfig>>,
    output_files: &OutputFileMap,
    headers: &HeadersMap,
) {
    for filter_config in filters {
        if record_matches_filter_config(&csv_record, &filter_config, &headers) {
            let output_record = build_output_record(&csv_record, &filter_config, &headers);
            write_record_to_file(output_record, &filter_config, &output_files);
        }
    }
}

/// Checks if a CSV record does match the filter criteria of one filter configuration item.
///
/// # Arguments
/// * `csv_record` - The record that needs to be checked
/// * `config` -  The filter configuration to check the CSV record against
/// * `headers` - Maps a CSV column name to its index in the current CSV file
fn record_matches_filter_config(
    csv_record: &StringRecord,
    config: &FilterConfig,
    headers: &HeadersMap,
) -> bool {
    for column_filter in &config.filters {
        if let Some(&idx) = headers.get(&column_filter.column) {
            let column_value = csv_record[idx].to_string();

            if let Some(allowed_values) = &column_filter.values {
                if !allowed_values.contains(&column_value) {
                    return false;
                }
            }

            if let Some(min) = &column_filter.min {
                if column_value < *min {
                    return false;
                }
            }

            if let Some(max) = &column_filter.max {
                if column_value > *max {
                    return false;
                }
            }
        }
    }

    true
}

/// Performs the actual writing of data to an output CSV file. This function is thread-safe.
///
/// # Arguments
/// * `output_record` - The record that needs to be written out
/// * `config` - The filter configuration to write the record for
/// * `output_files` - A map that maps a filename to its CSV file writer
fn write_record_to_file(
    output_record: Vec<String>,
    config: &Arc<FilterConfig>,
    output_files: &OutputFileMap,
) {
    let mutex = &output_files[&config.output];

    let write_result;
    {
        let mut writer = mutex.lock().unwrap();
        write_result = writer.write_record(&output_record);
    }
    write_result.expect(&format!("Error writing to CSV file '{}'", &config.output))
}

/// Creates a CSV row with all necessary column values according to a [`FilterConfig`].
///
/// # Arguments
/// * `csv_record` - The record that needs to be mapped to an output file row.
/// * `config` - The configuration to use.
/// * `headers` - Maps a CSV column name to its index in the current CSV file
fn build_output_record(
    csv_record: &StringRecord,
    config: &Arc<FilterConfig>,
    headers: &HeadersMap,
) -> Vec<String> {
    let mut vec: Vec<String> = vec![];

    let output_column_names = get_output_columns(&config);

    for colum_name in output_column_names {
        let header_index = *headers
            .get(&colum_name)
            .expect(&format!("Cannot find index of '{}' header", colum_name));
        let v = csv_record[header_index].to_string();
        vec.push(v);
    }

    vec
}

/// Retrieves all columns from a [`FilterConfig`] which should be included in the output.
///
/// # Arguments
/// * `config` - The configuration to read the columns from.
fn get_output_columns(config: &FilterConfig) -> Vec<String> {
    config
        .filters
        .iter()
        .filter(|f| f.include)
        .map(|f| f.column.to_string())
        .collect()
}

/// Creates an output file for each filter configuration. The output file is expected to be a CSV file.
///
/// # Arguments
/// * `all_filter_configs` - A list of all filter configurations.
/// * `output_dir_path` - Path of the output directory where all files need to be written to.
fn create_output_files(
    all_filter_configs: &Vec<Arc<FilterConfig>>,
    output_dir_path: &str,
) -> OutputFileMap {
    if !util::path_exists(output_dir_path) {
        fs::create_dir_all(output_dir_path).expect(&format!(
            "Creating output directory '{}' failed:",
            output_dir_path
        ));
    }

    let mut map = HashMap::new();

    for config in all_filter_configs {
        let path = Path::new(output_dir_path).join(&config.output);
        util::create_file(&path);
        let writer = csv::Writer::from_path(&path)
            .expect(&format!("Error opening output file '{:?}'", path));
        map.insert(config.output.clone(), Mutex::new(writer));
    }

    Arc::new(map)
}

/// Writes CSV headers into all output files according to the corresponding configuration.
///
/// # Arguments
/// * `all_filter_configs` - A vector containing all configuration items
/// * `output_files` - A map that maps a filename to its CSV file writer
fn write_headers_to_output_files(
    all_filter_configs: &Vec<Arc<FilterConfig>>,
    output_files: &OutputFileMap,
) {
    for cfg in all_filter_configs {
        let mutex = output_files.get(&cfg.output).unwrap();
        let mut file = mutex.lock().unwrap();

        let headers_record: Vec<String> = cfg
            .filters
            .iter()
            .filter(|f| f.include)
            .map(|f| f.column.to_string())
            .collect();

        file.write_record(headers_record)
            .expect("Error writing headers to output CSV file");
        file.flush()
            .expect("Error flushing headers to output CSV file");
    }
}

/// Creates a map that maps a CSV column name to its index in the current CSV file.
///
/// # Arguments
/// * `csv_reader` - The CSV reader of the input CSV file.
fn create_headers_map(csv_reader: &mut Reader<File>) -> HeadersMap {
    let headers = csv_reader.headers().expect("Cannot read CSV headers");
    let mut map = HashMap::new();

    let mut index = 0;
    for h in headers {
        map.insert(h.to_string(), index);
        index += 1;
    }

    Arc::new(map)
}
