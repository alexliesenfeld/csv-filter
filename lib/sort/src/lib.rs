//! The `sort` crate provides a CSV file processor that is able to sort CSV files.
extern crate crossbeam;
extern crate csv;
extern crate csv_filter_config as config;
extern crate csv_filter_util as util;

use config::FilterConfig;
use crossbeam::channel::bounded as bounded_channel;

use csv::{ReaderBuilder, StringRecord};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

// These type definitions are only here for abbreviation
type SortConfig = HashMap<PathBuf, Option<Vec<String>>>;

/// Sorts all output CSV files according to the provided configuration.
///
/// # Arguments
/// * `all_filter_configs` - A vector containing all configuration items
/// * `output_dir_path` - Path to the directory containing all output files
/// * `max_threads` - The maximum number of threads to use
///
/// # Panics
/// This function will panic on any error.
pub fn sort_output_files(
    all_filter_configs: &Vec<Arc<FilterConfig>>,
    output_dir_path: &str,
    max_threads: usize,
) {
    let files = read_sort_config(all_filter_configs, output_dir_path);

    let (channel_sender, channel_receiver) = bounded_channel(256);
    let mut threads = Vec::new();

    // The following will create channel consumer threads that will be consuming CSV records.
    for _ in 0..max_threads {
        let channel_receiver = channel_receiver.clone();

        threads.push(thread::spawn(move || {
            for (path, sort_columns) in &channel_receiver {
                println!("Sorting file '{}'", util::path_to_string(&path));
                if let Some(sc) = sort_columns {
                    sort_csv_file(&path, &sc);
                }
            }
        }));
    }

    // The following will iterate over all files and write pairs of (path, sort_columns) into
    // the channel. The records will then be consumed by one of the consumer threads created above.
    for path_and_columns in files {
        channel_sender
            .send(path_and_columns)
            .expect("Error sending record to channel");
    }

    // Stopping the channel and wait for all threads to finish
    drop(channel_sender);
    for t in threads {
        t.join().expect("Cannot join thread.")
    }
}

/// Reads parts from configuration relevant for sorting output files.
///
/// # Arguments
/// * `all_filter_configs` - A vector containing all configuration items
/// * `output_dir_path` - Path to the directory containing all output files
///
/// # Panics
/// This function will panic if a CSV output file specified in one of the provided
/// configurations cannot be found.
fn read_sort_config(
    all_filter_configs: &Vec<Arc<FilterConfig>>,
    output_dir_path: &str,
) -> SortConfig {
    let mut files = SortConfig::new();
    for cfg in all_filter_configs {
        let path = Path::new(output_dir_path).join(&cfg.output);
        if !path.exists() {
            panic!(format!(
                "Cannot sort file '{:?}' because it does not exists.",
                path
            ));
        }

        let sort_columns: Option<Vec<String>> = match &cfg.sort_columns {
            Some(sc) => Option::Some(Vec::from_iter(sc.iter().cloned())),
            None => Option::None,
        };

        files.insert(path, sort_columns);
    }
    files
}

/// Sorts a CSV file.
///
/// # Arguments
/// * `path` - Path to the file to be sorted
/// * `sort_columns` - An ordered collection of columns to sort by
///
/// # Panics
/// This function will panic on any error.
fn sort_csv_file(path: &PathBuf, sort_columns: &Vec<String>) {
    let mut csv_reader = get_reader(path);

    let header_row = get_headers(&mut csv_reader);
    let sort_order = get_sort_order(&header_row, sort_columns);

    let mut records: Vec<StringRecord> = csv_reader.records().map(|r| r.unwrap()).collect();
    records.sort_by(|a, b| record_comparator(a, b, &sort_order));

    drop(csv_reader);

    let mut writer = csv::Writer::from_path(path).unwrap();
    writer.write_record(header_row).unwrap();

    for record in records {
        writer.write_record(&record).expect(&format!(
            "Error writing record to output file '{}'",
            util::path_to_string(&path)
        ));
    }
}

/// Creates a vector holding the names of all headers from the CSV file.
///
/// # Arguments
/// * `csv_file_reader` - The CSV file reader to read headers from
fn get_headers(csv_file_reader: &mut csv::Reader<File>) -> Vec<String> {
    csv_file_reader
        .headers()
        .unwrap()
        .iter()
        .map(|h| h.to_string())
        .collect()
}

/// Creates a CSV file reader for the file at the provided location.
///
/// # Arguments
/// * `path` - The path to the CSV file.
fn get_reader(path: &PathBuf) -> csv::Reader<File> {
    ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .expect("Cannot read CSV file")
}

/// Creates a vector holding the column sort order in the form of column indexes.
///
/// # Arguments
/// * `header_row` - The header row from the CSV file holding the column names.
/// * `sort_columns` - The sort order as a list of column names.
fn get_sort_order(header_row: &Vec<String>, sort_columns: &Vec<String>) -> Vec<usize> {
    let mut sort_order = Vec::new();

    for sort_column in sort_columns {
        let mut index: usize = 0;
        for h in header_row {
            if sort_column.cmp(&h) == Ordering::Equal {
                sort_order.push(index);
            }
            index += 1;
        }
    }

    sort_order
}

/// A comparator function providing a total ordering of [`StringRecord`] objects.
///
/// # Arguments
/// * `a` - First record
/// * `b` - Second record
/// * `header_map` - Maps a column name to its corresponding index inside both [`StringRecord`] objects.
fn record_comparator(a: &StringRecord, b: &StringRecord, header_map: &Vec<usize>) -> Ordering {
    let mut order = Ordering::Equal;
    for &column_index in header_map {
        if order != Ordering::Equal {
            return order;
        }

        let column_value_a = a.get(column_index).unwrap();
        let column_value_b = b.get(column_index).unwrap();

        order = order.then(column_value_a.cmp(column_value_b));
    }
    order
}
