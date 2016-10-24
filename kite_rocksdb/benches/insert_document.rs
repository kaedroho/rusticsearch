#![feature(test)]

#[macro_use]
extern crate maplit;
extern crate test;
extern crate kite;
extern crate kite_rocksdb;

use test::Bencher;
use std::fs::remove_dir_all;

use kite::term::Term;
use kite::token::Token;
use kite::schema::{FieldType, FIELD_INDEXED, FIELD_STORED};
use kite::document::{Document, FieldValue};

use kite_rocksdb::RocksDBIndexStore;


#[bench]
fn bench_insert_single_document(b: &mut Bencher) {
    remove_dir_all("test_indices/bench_insert_single_document");

    let mut store = RocksDBIndexStore::create("test_indices/bench_insert_single_document").unwrap();
    store.add_field("title".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
    store.add_field("body".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
    store.add_field("id".to_string(), FieldType::I64, FIELD_STORED).unwrap();

    let mut tokens = Vec::new();
    for t in 0..5000 {
        tokens.push(Token {
            term: Term::String(t.to_string()),
            position: t
        });
    }

    let mut i = 0;
    b.iter(|| {
        i += 1;

        store.insert_or_update_document(Document {
            key: i.to_string(),
            indexed_fields: hashmap! {
                "body".to_string() => tokens.clone(),
                "title".to_string() => vec![Token { term: Term::String(i.to_string()), position: 1}],
            },
            stored_fields: hashmap! {
                "id".to_string() => FieldValue::Integer(i),
            },
        });
    });
}
