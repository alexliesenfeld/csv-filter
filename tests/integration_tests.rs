extern crate csv_filter;

extern crate tempfile;
use crate::util::*;

mod util;

/// This test ensures that only columns from the input CSV are being written out
/// to an output file, which were defined in the corresponding filter configuration
/// for that particular output file. All other columns are being ignored for that
/// particular output file.
#[test]
fn writes_included_headers_to_output_files() {
    // Arrange
    let config = Fixture::copy("default.json");
    let input_csv = Fixture::copy("default_input.csv");
    let expected_output_csv = Fixture::copy("default_output.csv");
    let output_dir = tempfile::tempdir().unwrap();
    let expected_output_file_path = output_dir.path().join("f1.csv");

    // Act
    csv_filter::process(
        &path_to_string(&input_csv.path),
        &path_to_string(&config.path),
        &path_to_string(&output_dir.path()),
        false,
        0,
        0,
    );

    // Assert
    assert_eq!(true, expected_output_file_path.exists());
    assert_eq!(
        &std::fs::read_to_string(&expected_output_csv.path).unwrap(),
        &std::fs::read_to_string(&expected_output_file_path).unwrap()
    );
}

/// This test ensures that the processor does panic if a [`ColumnFilter`] definition
/// does not define any output columns (i.e. no [`ColumnFilter`] has been defined with
/// attribute value `included` = `true`).
#[test]
#[should_panic(expected = "does not contain any output columns")]
fn config_validation_fails_no_included_filters() {
    // Arrange
    let config = Fixture::copy("invalid_no_included_filters.json");
    let input_csv = Fixture::copy("default_input.csv");
    let output_dir = tempfile::tempdir().unwrap();

    // Act
    csv_filter::process(
        &path_to_string(&input_csv.path),
        &path_to_string(&config.path),
        &path_to_string(&output_dir.path()),
        false,
        0,
        0,
    );

    // Assert
    // See macro 'should_panic'
}

/// This test ensures that the processor does panic if a [`ColumnFilter`] definition
/// does not define any output columns (i.e. no [`ColumnFilter`] has been defined at all).
#[test]
#[should_panic(expected = "does not contain any output columns")]
fn config_validation_fails_no_filters() {
    // Arrange
    let config = Fixture::copy("invalid_no_filters.json");
    let input_csv = Fixture::copy("default_input.csv");
    let output_dir = tempfile::tempdir().unwrap();

    // Act
    csv_filter::process(
        &path_to_string(&input_csv.path),
        &path_to_string(&config.path),
        &path_to_string(&output_dir.path()),
        true,
        0,
        0,
    );

    // Assert
    // See macro 'should_panic'
}

/// This test ensures that the processor does panic if the configuration
/// contains at least one [`ColumnFilter`] which defines both, the `values`
/// attribute, as well as one of the `min` or `max` attributes.
#[test]
#[should_panic(expected = "defines values and a range (min/max)")]
fn config_validation_fails_values_and_range_defined() {
    // Arrange
    let config = Fixture::copy("invalid_value_and_range.json");
    let input_csv = Fixture::copy("default_input.csv");
    let output_dir = tempfile::tempdir().unwrap();

    // Act
    csv_filter::process(
        &path_to_string(&input_csv.path),
        &path_to_string(&config.path),
        &path_to_string(&output_dir.path()),
        true,
        0,
        0,
    );

    // Assert
    // See macro 'should_panic'
}

/// This test makes sure that the program panics if an invalid input CSV path is provided.
#[test]
#[should_panic(expected = "Cannot read CSV file: Error(Io(Os { code: 2, kind: NotFound")]
fn panics_on_missing_input_file() {
    // Arrange
    let config = Fixture::copy("default.json");
    let output_dir = tempfile::tempdir().unwrap();
    let input_file_path = output_dir.path().join("does_no_exist.csv");

    // Act
    csv_filter::process(
        &path_to_string(&input_file_path),
        &path_to_string(&config.path),
        &path_to_string(&output_dir.path()),
        true,
        0,
        0,
    );

    // Assert
    // See macro 'should_panic'
}

/// This test makes sure that the program panics if an invalid configuration path is provided.
#[test]
#[should_panic(expected = "Cannot read config file: Os { code: 2, kind: NotFound")]
fn panics_on_missing_config_file() {
    // Arrange
    let input_csv = Fixture::copy("default_input.csv");
    let output_dir = tempfile::tempdir().unwrap();
    let config_file_path = output_dir.path().join("does_no_exist.csv");

    // Act
    csv_filter::process(
        &path_to_string(&input_csv.path),
        &path_to_string(&config_file_path),
        &path_to_string(&output_dir.path()),
        true,
        0,
        0,
    );

    // Assert
    // See macro 'should_panic'
}

/// This test ensures that the CSV processor makes sure that only those CSV rows
/// are being written to the output file that match the `min` and `max` attributes
/// in the [`ColumnFilter`] definitions.
#[test]
fn filters_min_max() {
    // Arrange
    let config = Fixture::copy("min_max.json");
    let input_csv = Fixture::copy("default_input.csv");
    let expected_output_csv = Fixture::copy("min_max_output.csv");
    let output_dir = tempfile::tempdir().unwrap();
    let expected_output_file_path = output_dir.path().join("f1.csv");

    // Act
    csv_filter::process(
        &path_to_string(&input_csv.path),
        &path_to_string(&config.path),
        &path_to_string(&output_dir.path()),
        true,
        0,
        0,
    );

    // Assert
    assert_eq!(true, expected_output_file_path.exists());
    assert_eq!(
        &std::fs::read_to_string(&expected_output_csv.path).unwrap(),
        &std::fs::read_to_string(&expected_output_file_path).unwrap()
    );
}

/// This test ensures that the CSV processor makes sure that only those CSV rows
/// are being written to the output file that match the `values` attribute in the
/// [`ColumnFilter`] definitions.
#[test]
fn filters_values() {
    // Arrange
    let config = Fixture::copy("values.json");
    let input_csv = Fixture::copy("values_input.csv");
    let expected_output_csv = Fixture::copy("values_output.csv");
    let output_dir = tempfile::tempdir().unwrap();
    let expected_output_file_path = output_dir.path().join("f1.csv");

    // Act
    csv_filter::process(
        &path_to_string(&input_csv.path),
        &path_to_string(&config.path),
        &path_to_string(&output_dir.path()),
        true,
        0,
        0,
    );

    // Assert
    assert_eq!(true, expected_output_file_path.exists());
    assert_eq!(
        &std::fs::read_to_string(&expected_output_csv.path).unwrap(),
        &std::fs::read_to_string(&expected_output_file_path).unwrap()
    );
}

/// This test ensures that the CSV processor works correctly, if a config file is provided
/// which defines multiple [`FilterConfig`] elements, each also containing multiple
/// [`ColumnFilter`] definitions.
#[test]
fn filters_multiple_configs_with_multiple_filters() {
    // Arrange
    let config = Fixture::copy("multiple.json");
    let input_csv = Fixture::copy("default_input.csv");
    let expected_output_csv_1 = Fixture::copy("multiple_output_1.csv");
    let expected_output_csv_2 = Fixture::copy("multiple_output_2.csv");
    let output_dir = tempfile::tempdir().unwrap();
    let expected_output_file_path_1 = output_dir.path().join("f1.csv");
    let expected_output_file_path_2 = output_dir.path().join("f2.csv");

    // Act
    csv_filter::process(
        &path_to_string(&input_csv.path),
        &path_to_string(&config.path),
        &path_to_string(&output_dir.path()),
        true,
        0,
        0,
    );

    // Assert
    assert_eq!(true, expected_output_file_path_1.exists());
    assert_eq!(true, expected_output_file_path_2.exists());
    assert_eq!(
        &std::fs::read_to_string(&expected_output_csv_1.path).unwrap(),
        &std::fs::read_to_string(&expected_output_file_path_1).unwrap()
    );
    assert_eq!(
        &std::fs::read_to_string(&expected_output_csv_2.path).unwrap(),
        &std::fs::read_to_string(&expected_output_file_path_2).unwrap()
    );
}

/// This test ensures that the CSV processor sorts output files according to the columns
/// and column order specified in the configuration.
#[test]
fn sorts_files() {
    // Arrange
    let config = Fixture::copy("sort.json");
    let input_csv = Fixture::copy("sort_input.csv");
    let expected_output_csv = Fixture::copy("sort_output.csv");
    let output_dir = tempfile::tempdir().unwrap();
    let expected_output_file_path = output_dir.path().join("f1.csv");

    // Act
    csv_filter::process(
        &path_to_string(&input_csv.path),
        &path_to_string(&config.path),
        &path_to_string(&output_dir.path()),
        false,
        0,
        0,
    );

    // Assert
    assert_eq!(true, expected_output_file_path.exists());
    assert_eq!(
        &std::fs::read_to_string(&expected_output_csv.path).unwrap(),
        &std::fs::read_to_string(&expected_output_file_path).unwrap()
    );
}

/// This test ensures that the program panics if a [`ColumnFilter`] definition
/// includes sort columns which are not part of the corresponding output file.
#[test]
#[should_panic(expected = "contains sort column \\'col3\\' which is not part of the output file")]
fn config_validation_fails_not_all_sort_columns_included() {
    // Arrange
    let config = Fixture::copy("invalid_not_included_sort_column.json");
    let input_csv = Fixture::copy("default_input.csv");
    let output_dir = tempfile::tempdir().unwrap();

    // Act
    csv_filter::process(
        &path_to_string(&input_csv.path),
        &path_to_string(&config.path),
        &path_to_string(&output_dir.path()),
        false,
        0,
        0,
    );

    // Assert
    // See macro 'should_panic'
}
