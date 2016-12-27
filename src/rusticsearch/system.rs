use std::sync::RwLock;
use std::path::{Path, PathBuf};
use std::fs;

use slog::Logger;
use kite_rocksdb::RocksDBIndexStore;

use index::Index;
use index::registry::IndexRegistry;


pub struct System {
    pub log: Logger,
    data_dir: PathBuf,
    pub indices: RwLock<IndexRegistry>,
}


impl System {
    pub fn new(log: Logger, data_dir: PathBuf) -> System {
        System {
            log: log,
            data_dir: data_dir,
            indices: RwLock::new(IndexRegistry::new()),
        }
    }

    pub fn get_indices_dir(&self) -> PathBuf {
        let mut dir = self.data_dir.clone();
        dir.push("indices");
        dir
    }

    fn load_index(&self, name: String, path: &Path) -> Result<Index, String> {
        let store = try!(RocksDBIndexStore::open(path));

        Ok(Index::new(name, store))
    }

    pub fn load_indices(&self) {
        let indices_dir = self.get_indices_dir();
        match fs::read_dir(indices_dir.clone()) {
            Ok(files) => {
                for file in files {
                    let path = file.unwrap().path();
                    if path.is_dir() {
                        let index_name: String = path.file_name().unwrap().to_str().unwrap().to_owned();

                        match self.load_index(index_name.clone().to_owned(), path.as_path()) {
                            Ok(index) => {
                                let mut indices_w = self.indices.write().unwrap();
                                let index_ref = indices_w.insert(index);
                                indices_w.names.insert_canonical(index_name.clone(), index_ref).unwrap();

                                self.log.info("[sys] loaded index", b!("index" => index_name));
                            }
                            Err(e) => {
                                self.log.error("[sys] failed to open index", b!(
                                    "index" => index_name,
                                    "error" => e
                                ));
                            }
                        }
                    }
                }
            }
            Err(error) => {
                self.log.error("[sys] cannot open indices directory", b!(
                    "dir" => indices_dir.to_str().unwrap(),
                    "error" => format!("{}", error)
                ));
            }
        }
    }
}
