#![feature(test)]

#[macro_use]
extern crate maplit;
extern crate test;
extern crate kite;

use test::Bencher;

use kite::term::Term;
use kite::token::Token;
use kite::schema::FieldType;
use kite::document::Document;
use kite::store::{IndexStore, IndexReader};
use kite::store::memory::{MemoryIndexStore, MemoryIndexStoreReader};
use kite::query_set::QuerySetIterator;


fn make_test_store() -> MemoryIndexStore {
    let mut store = MemoryIndexStore::new();
    let body_field = store.add_field("body".to_string(), FieldType::Text).unwrap();

    for i in 0..10000 {
        let mut tokens = Vec::new();

        if i % 3 == 0 {
            let position = tokens.len() as u32 + 1;
            tokens.push(Token {
                term: Term::String("fizz".to_string()),
                position: position,
            });
        }

        if i % 5 == 0 {
            let position = tokens.len() as u32 + 1;
            tokens.push(Token {
                term: Term::String("buzz".to_string()),
                position: position,
            });
        }

        store.insert_or_update_document(Document {
            key: i.to_string(),
            fields: hashmap! {
                "body".to_string() => tokens
            }
        });
    }

    store
}


#[bench]
fn bench_all(b: &mut Bencher) {
    let store = make_test_store();
    let reader = store.reader();

    b.iter(|| {
        let mut iterator: QuerySetIterator<MemoryIndexStoreReader> = QuerySetIterator::All {
            iter: reader.iter_all_docs(),
        };

        for doc_id in iterator {}
    });
}


#[bench]
fn bench_fizz_term(b: &mut Bencher) {
    let store = make_test_store();
    let reader = store.reader();
    let body_field = reader.schema().get_field_by_name("body").unwrap();

    let fizz_term = Term::String("fizz".to_string()).to_bytes();

    b.iter(|| {
        let mut iterator: QuerySetIterator<MemoryIndexStoreReader> = QuerySetIterator::Term {
            iter: reader.iter_docs_with_term(&fizz_term, &body_field).unwrap(),
        };

        for doc_id in iterator {}
    });
}


#[bench]
fn bench_buzz_term(b: &mut Bencher) {
    let store = make_test_store();
    let reader = store.reader();
    let body_field = reader.schema().get_field_by_name("body").unwrap();

    let buzz_term = Term::String("buzz".to_string()).to_bytes();

    b.iter(|| {
        let mut iterator: QuerySetIterator<MemoryIndexStoreReader> = QuerySetIterator::Term {
            iter: reader.iter_docs_with_term(&buzz_term, &body_field).unwrap(),
        };

        for doc_id in iterator {}
    });
}


#[bench]
fn bench_fizzbuzz_conjunction(b: &mut Bencher) {
    let store = make_test_store();
    let reader = store.reader();
    let body_field = reader.schema().get_field_by_name("body").unwrap();

    let fizz_term = Term::String("fizz".to_string()).to_bytes();
    let buzz_term = Term::String("buzz".to_string()).to_bytes();

    b.iter(|| {
        let mut fizz_iterator: QuerySetIterator<MemoryIndexStoreReader> = QuerySetIterator::Term {
            iter: reader.iter_docs_with_term(&fizz_term, &body_field).unwrap(),
        };
        let mut buzz_iterator: QuerySetIterator<MemoryIndexStoreReader> = QuerySetIterator::Term {
            iter: reader.iter_docs_with_term(&buzz_term, &body_field).unwrap(),
        };
        let mut iterator: QuerySetIterator<MemoryIndexStoreReader> = QuerySetIterator::Conjunction {
            iter_a: Box::new(fizz_iterator),
            iter_b: Box::new(buzz_iterator),
            initialised: false,
            current_doc_a: None,
            current_doc_b: None,
        };

        for doc_id in iterator {}
    });
}


#[bench]
fn bench_fizzbuzz_disjunction(b: &mut Bencher) {
    let store = make_test_store();
    let reader = store.reader();
    let body_field = reader.schema().get_field_by_name("body").unwrap();

    let fizz_term = Term::String("fizz".to_string()).to_bytes();
    let buzz_term = Term::String("buzz".to_string()).to_bytes();

    b.iter(|| {
        let mut fizz_iterator: QuerySetIterator<MemoryIndexStoreReader> = QuerySetIterator::Term {
            iter: reader.iter_docs_with_term(&fizz_term, &body_field).unwrap(),
        };
        let mut buzz_iterator: QuerySetIterator<MemoryIndexStoreReader> = QuerySetIterator::Term {
            iter: reader.iter_docs_with_term(&buzz_term, &body_field).unwrap(),
        };
        let mut iterator: QuerySetIterator<MemoryIndexStoreReader> = QuerySetIterator::Disjunction {
            iter_a: Box::new(fizz_iterator),
            iter_b: Box::new(buzz_iterator),
            initialised: false,
            current_doc_a: None,
            current_doc_b: None,
        };

        for doc_id in iterator {}
    });
}


#[bench]
fn bench_fizzbuzz_exclusion(b: &mut Bencher) {
    let store = make_test_store();
    let reader = store.reader();
    let body_field = reader.schema().get_field_by_name("body").unwrap();

    let fizz_term = Term::String("fizz".to_string()).to_bytes();
    let buzz_term = Term::String("buzz".to_string()).to_bytes();

    b.iter(|| {
        let mut fizz_iterator: QuerySetIterator<MemoryIndexStoreReader> = QuerySetIterator::Term {
            iter: reader.iter_docs_with_term(&fizz_term, &body_field).unwrap(),
        };
        let mut buzz_iterator: QuerySetIterator<MemoryIndexStoreReader> = QuerySetIterator::Term {
            iter: reader.iter_docs_with_term(&buzz_term, &body_field).unwrap(),
        };
        let mut iterator: QuerySetIterator<MemoryIndexStoreReader> = QuerySetIterator::Exclusion {
            iter_a: Box::new(fizz_iterator),
            iter_b: Box::new(buzz_iterator),
            initialised: false,
            current_doc_a: None,
            current_doc_b: None,
        };

        for doc_id in iterator {}
    });
}
