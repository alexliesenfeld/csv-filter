extern crate tempfile;
use self::tempfile::TempDir;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::{env, fs};

pub struct Fixture {
    pub path: PathBuf,
    _temp_dir: TempDir,
}

impl Fixture {
    pub fn blank(fixture_filename: &str) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let mut path = PathBuf::from(&tempdir.path());
        path.push(&fixture_filename);

        Fixture {
            _temp_dir: tempdir,
            path,
        }
    }

    pub fn copy(fixture_filename: &str) -> Self {
        let fixture = Fixture::blank(fixture_filename);
        let root_dir = &env::var("CARGO_MANIFEST_DIR").expect("$CARGO_MANIFEST_DIR");
        let mut source = PathBuf::from(root_dir);
        source.push("tests");
        source.push("fixtures");
        source.push(&fixture_filename);
        fs::copy(&source, &fixture.path)
            .expect(&format!("Cannot copy fixture '{}'", fixture_filename));
        fixture
    }
}

impl Deref for Fixture {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        self.path.deref()
    }
}

pub fn path_to_string(path: &Path) -> String {
    path.to_str().unwrap().to_string()
}
