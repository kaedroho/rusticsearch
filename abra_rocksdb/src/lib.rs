extern crate abra;
extern crate rocksdb;
extern crate rustc_serialize;

use std::sync::Arc;

use rocksdb::{DB, Writable, Options};
use abra::schema::{Schema, FieldType, FieldRef, AddFieldError};
use rustc_serialize::{json, Encodable};


pub struct RocksDBIndexStore {
    schema: Arc<Schema>,
    db: DB,
}


impl RocksDBIndexStore {
    pub fn create(path: &str) -> Result<RocksDBIndexStore, String> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = try!(DB::open(&opts, path));

        // Schema
        let schema = Schema::new();
        db.put(b"schema", json::encode(&schema).unwrap().as_bytes());

        Ok(RocksDBIndexStore {
            schema: Arc::new(schema),
            db: db,
        })
    }

    pub fn open(path: &str) -> Result<RocksDBIndexStore, String> {
        let mut opts = Options::default();
        let db = try!(DB::open(&opts, path));

        let schema = match db.get(b"schema") {
            Ok(Some(schema)) => {
                let schema = schema.to_utf8().unwrap().to_string();
                json::decode(&schema).unwrap()
            }
            Ok(None) => Schema::new(),  // TODO: error
            Err(_) => Schema::new(),  // TODO: error
        };

        Ok(RocksDBIndexStore {
            schema: Arc::new(schema),
            db: db,
        })
    }

    pub fn add_field(&mut self, name: String, field_type: FieldType) -> Result<FieldRef, AddFieldError> {
        let mut schema_copy = (*self.schema).clone();
        let field_ref = try!(schema_copy.add_field(name, field_type));
        self.schema = Arc::new(schema_copy);

        self.db.put(b"schema", json::encode(&self.schema).unwrap().as_bytes());

        Ok(field_ref)
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
