extern crate abra;
extern crate rocksdb;

use rocksdb::{DB, Options};


pub struct RocksDBIndexStore {
    db: DB,
}


impl RocksDBIndexStore {
    pub fn create(path: &str) -> Result<RocksDBIndexStore, String> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = try!(DB::open(&opts, path));

        // TODO: Initialise

        Ok(RocksDBIndexStore {
            db: db,
        })
    }

    pub fn open(path: &str) -> Result<RocksDBIndexStore, String> {
        let mut opts = Options::default();
        let db = try!(DB::open(&opts, path));

        Ok(RocksDBIndexStore {
            db: db,
        })
    }
}


#[cfg(test)]
mod tests {
    use rocksdb::{DB, Options};

    use super::RocksDBIndexStore;

    #[test]
    fn test_create() {
        let store = RocksDBIndexStore::create("test_indices/test_create");
        assert!(store.is_ok());
    }

    #[test]
    fn test_open() {
        let store = RocksDBIndexStore::open("test_indices/test_open");
        assert!(store.is_err());

        // Create DB
        let mut opts = Options::default();
        opts.create_if_missing(true);
        DB::open(&opts, "test_indices/test_open").unwrap();

        let store = RocksDBIndexStore::open("test_indices/test_open");
        assert!(store.is_ok());
    }
}
