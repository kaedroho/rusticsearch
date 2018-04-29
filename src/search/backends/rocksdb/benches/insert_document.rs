#![feature(test)]

extern crate test;
extern crate kite;
extern crate kite_rocksdb;
extern crate rayon;
extern crate fnv;

use test::Bencher;
use std::fs::remove_dir_all;

use rayon::par_iter::{ParallelIterator, IntoParallelRefIterator};
use fnv::FnvHashMap;

use search::term::Term;
use search::token::Token;
use search::schema::{FieldType, FIELD_INDEXED, FIELD_STORED};
use search::document::{Document, FieldValue};

use search::backends::rocksdb::RocksDBStore;

#[bench]
fn bench_insert_single_document(b: &mut Bencher) {
    remove_dir_all("test_indices/bench_insert_single_document");

    let mut store = RocksDBStore::create("test_indices/bench_insert_single_document").unwrap();
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

    let mut i = 0;
    b.iter(|| {
        i += 1;

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
    });
}

#[bench]
fn bench_insert_documents_parallel(b: &mut Bencher) {
    remove_dir_all("test_indices/bench_insert_single_document_parallel");

    let mut store = RocksDBStore::create("test_indices/bench_insert_single_document_parallel").unwrap();
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

    let mut docs = Vec::new();
    for i in 0..8 {
        let mut indexed_fields = FnvHashMap::default();
        indexed_fields.insert(body_field, tokens.clone().into());
        indexed_fields.insert(title_field, vec![Token { term: Term::from_string(&(i + 1).to_string()), position: 1}].into());

        let mut stored_fields = FnvHashMap::default();
        stored_fields.insert(id_field, FieldValue::Integer(i));

        docs.push(Document {
            key: (i + 1).to_string(),
            indexed_fields: indexed_fields,
            stored_fields: stored_fields,
        });
    }

    b.iter(move|| {
        docs.par_iter().for_each(|doc| {
            store.insert_or_update_document(doc);
        });
    });
}
