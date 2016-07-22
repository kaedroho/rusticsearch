use std::sync::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;

use rocksdb;

use search::index::Index;
use search::index::store::IndexStore;
use search::index::store::rocksdb::RocksDBIndexStore;


pub struct System {
    data_dir: PathBuf,
    pub indices: RwLock<HashMap<String, Index>>,
}


impl System {
    pub fn new(data_dir: PathBuf) -> System {
        System {
            data_dir: data_dir,
            indices: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_indices_dir(&self) -> PathBuf {
        let mut dir = self.data_dir.clone();
        dir.push("indices");
        dir
    }

    fn load_index(&self, path: &Path) -> Index {
        // TODO: THIS IS REALLY AWFUL. DO NOT MERGE THIS CODE
        let mut opts = rocksdb::Options::new();
        let db = rocksdb::DB::open(&opts, path.to_str().unwrap()).unwrap();
        Index::new(RocksDBIndexStore::new(db))
    }

    pub fn load_indices(&self) -> HashMap<String, Index> {
        let mut indices = HashMap::new();

        for file in fs::read_dir(self.get_indices_dir()).unwrap() {
            let path = file.unwrap().path();
            let index_name: String = path.file_stem().unwrap().to_str().unwrap().to_owned();

            if let Some(ext) = path.extension() {
                if ext.to_str() == Some("rsi") {
                    info!("Loaded index: {}", index_name);
                    self.indices.write().unwrap().insert(index_name, self.load_index(path.as_path()));
                }
            }
        }

        indices
    }
}
