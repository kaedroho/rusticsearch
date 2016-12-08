#![feature(test)]

#[macro_use]
extern crate maplit;
extern crate test;
extern crate kite;

use test::Bencher;

use kite::term::Term;
use kite::token::Token;
use kite::schema::{FieldType, FIELD_INDEXED};
use kite::document::Document;
use kite::store::{IndexStore, IndexReader};
use kite::store::memory::{MemoryIndexStore, MemoryIndexStoreReader};


#[bench]
fn bench_insert_document(b: &mut Bencher) {
    let mut store = MemoryIndexStore::new();
    let body_field = store.add_field("body".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

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
                body_field => tokens.clone()
            },
            stored_fields: hashmap! {},
        });
    });
}
