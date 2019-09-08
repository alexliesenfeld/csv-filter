use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};

/// Checks if a directory of a file does exist on a given path.
pub fn path_exists(path: &str) -> bool {
    fs::metadata(path).is_ok()
}

/// Creates a file on a given path. If the parent directory does not exist yet, all non existent
/// parent directories will be created automatically.
///
/// # Panics
/// This function panics on any error.
pub fn create_file(file_path: &Path) -> File {
    let parent_path = file_path.parent().expect(&format!(
        "Output file path of config with file path '{:?}' seems to be broken:",
        file_path
    ));
    fs::create_dir_all(parent_path).expect(&format!(
        "Creating output directory for config with file path '{:?}' failed:",
        file_path
    ));

    File::create(file_path).expect(&format!(
        "Creating output file for config with file path '{:?}' failed:",
        file_path
    ))
}

pub fn path_to_string(path: &PathBuf) -> String {
    path.clone().as_os_str().to_str().unwrap().to_string()
}
