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
fn bench_merge_segments(b: &mut Bencher) {
    remove_dir_all("test_indices/bench_merge_segments");

    let mut store = RocksDBIndexStore::create("test_indices/bench_merge_segments").unwrap();
    let title_field = store.add_field("title".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
    let body_field = store.add_field("body".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
    let id_field = store.add_field("id".to_string(), FieldType::I64, FIELD_STORED).unwrap();

    let mut tokens = Vec::new();
    for t in 0..500 {
        tokens.push(Token {
            term: Term::String(t.to_string()),
            position: t
        });
    }

    // Make 1000 single-document segments
    for i in 0..1000 {
        store.insert_or_update_document(&Document {
            key: i.to_string(),
            indexed_fields: hashmap! {
                body_field => tokens.clone(),
                title_field => vec![Token { term: Term::String(i.to_string()), position: 1}],
            },
            stored_fields: hashmap! {
                id_field => FieldValue::Integer(i),
            },
        });
    }

    // Merge them together in groups of 100
    // This is only run about 5 times so only half of the documents will be merged
    let mut i = 0;
    b.iter(|| {
        let start = i * 100;
        let stop = start + 100;
        let segments = (start..stop).collect::<Vec<u32>>();

        store.merge_segments(&segments);
        store.purge_segments(&segments);

        i += 1;
    });
}
