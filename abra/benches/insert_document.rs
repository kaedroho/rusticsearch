#![feature(test)]

#[macro_use]
extern crate maplit;
extern crate test;
extern crate abra;

use test::Bencher;

use abra::term::Term;
use abra::token::Token;
use abra::schema::{SchemaRead, FieldType};
use abra::document::Document;
use abra::store::{IndexStore, IndexReader};
use abra::store::memory::{MemoryIndexStore, MemoryIndexStoreReader};
use abra::query_set::QuerySetIterator;


fn make_test_store() -> MemoryIndexStore {
    let mut store = MemoryIndexStore::new();
    let body_field = store.add_field("body".to_string(), FieldType::Text).unwrap();

    store
}


#[bench]
fn bench_insert_document(b: &mut Bencher) {
    let mut store = MemoryIndexStore::new();
    let body_field = store.add_field("body".to_string(), FieldType::Text).unwrap();

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
            fields: hashmap! {
                "body".to_string() => tokens.clone()
            }
        });
    });
}
