use std::sync::RwLock;
use std::path::{Path, PathBuf};
use std::fs;

use slog::Logger;
use kite_rocksdb::RocksDBStore;
use uuid::Uuid;

use index::Index;
use index::metadata::IndexMetadata;
use cluster::metadata::ClusterMetadata;


pub struct System {
    pub log: Logger,
    data_dir: PathBuf,
    pub metadata: RwLock<ClusterMetadata>,
}


impl System {
    pub fn new(log: Logger, data_dir: PathBuf) -> System {
        System {
            log: log,
            data_dir: data_dir,
            metadata: RwLock::new(ClusterMetadata::new()),
        }
    }

    pub fn get_indices_dir(&self) -> PathBuf {
        let mut dir = self.data_dir.clone();
        dir.push("indices");
        dir
    }

    fn load_index(&self, id: Uuid, name: String, path: &Path) -> Result<Index, String> {
        let store = RocksDBStore::open(path)?;

        // Load metadata
        let mut metadata_path = path.to_path_buf();
        metadata_path.push("metadata.json");
        let metadata = IndexMetadata::load(metadata_path)?;

        Ok(Index::new(id, name, metadata, store))
    }

    pub fn load_indices(&self) {
        let indices_dir = self.get_indices_dir();
        match fs::read_dir(indices_dir.clone()) {
            Ok(files) => {
                for file in files {
                    let path = file.unwrap().path();
                    if path.is_dir() {
                        let index_name: String = path.file_name().unwrap().to_str().unwrap().to_owned();

                        match self.load_index(Uuid::new_v4(), index_name.clone().to_owned(), path.as_path()) {
                            Ok(index) => {
                                let mut cluster_metadata = self.metadata.write().unwrap();
                                let index_ref = cluster_metadata.insert_index(index);
                                cluster_metadata.names.insert_canonical(index_name.clone(), index_ref).unwrap();

                                info!(self.log, "loaded index"; "index" => index_name);
                            }
                            Err(e) => {
                                error!(self.log, "load index failed"; "index" => index_name, "error" => e);
                            }
                        }
                    }
                }
            }
            Err(error) => {
                error!(self.log, "could not open indices directory"; "dir" => indices_dir.to_str().unwrap(), "error" => format!("{}", error));
            }
        }
    }
}
