#![feature(test)]

extern crate test;
extern crate kite;
extern crate kite_rocksdb;
extern crate fnv;

use test::Bencher;
use std::fs::remove_dir_all;

use fnv::FnvHashMap;

use search::term::Term;
use search::token::Token;
use search::schema::{FieldType, FIELD_INDEXED, FIELD_STORED};
use search::document::{Document, FieldValue};

use search::backends::rocksdb::RocksDBStore;

#[bench]
fn bench_merge_segments(b: &mut Bencher) {
    remove_dir_all("test_indices/bench_merge_segments");

    let mut store = RocksDBStore::create("test_indices/bench_merge_segments").unwrap();
    let title_field = store.add_field("title".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
    let body_field = store.add_field("body".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
    let id_field = store.add_field("id".to_string(), FieldType::I64, FIELD_STORED).unwrap();

    let mut tokens = Vec::new();
    for t in 0..500 {
        tokens.push(Token {
            term: Term::from_string(&t.to_string()),
            position: t
        });
    }

    // Make 1000 single-document segments
    for i in 0..1000 {
        let mut indexed_fields = FnvHashMap::default();
        indexed_fields.insert(body_field, tokens.clone().into());
        indexed_fields.insert(title_field, vec![Token { term: Term::from_string(&i.to_string()), position: 1}].into());

        let mut stored_fields = FnvHashMap::default();
        stored_fields.insert(id_field, FieldValue::Integer(i));

        store.insert_or_update_document(&Document {
            key: i.to_string(),
            indexed_fields: indexed_fields,
            stored_fields: stored_fields,
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
