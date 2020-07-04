use rand;
use std::{
    fs,
    path::{Path, PathBuf},
};

pub struct TestDir {
    path: PathBuf,
}

impl TestDir {
    pub fn new() -> TestDir {
        let mut path_buf = PathBuf::new();
        path_buf.push("target");
        path_buf.push("test-data");
        path_buf.push(format!("test-{:020}", rand::random::<u64>()));
        fs::create_dir_all(&path_buf).unwrap();
        TestDir { path: path_buf }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self).expect("Unable to delete test data directory");
    }
}

impl AsRef<Path> for TestDir {
    fn as_ref(&self) -> &Path {
        self.path.as_ref()
    }
}
