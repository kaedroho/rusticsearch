extern crate kite;
extern crate rocksdb;
extern crate kite_rocksdb;
#[macro_use]
extern crate maplit;

use std::fs::remove_dir_all;

use rocksdb::{DB, Options};
use kite::{Term, Token, Document};
use kite::document::FieldValue;
use kite::schema::{FieldType, FIELD_INDEXED, FIELD_STORED};
use kite::query::Query;
use kite::query::term_scorer::TermScorer;
use kite::collectors::top_score::TopScoreCollector;
use kite_rocksdb::RocksDBIndexStore;


fn get_store_path(name: &str) -> String {
    "test_indices/test_indexing_".to_string() + name
}


fn create_store(name: &str) -> RocksDBIndexStore {
    let path = get_store_path(name);
    RocksDBIndexStore::create(path).unwrap()
}


#[test]
fn test_schema() {
    let mut store = create_store("add_field");

    let title_field = store.add_field("title".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
    let pk_field = store.add_field("pk".to_string(), FieldType::I64, FIELD_STORED).unwrap();

    let reader = store.reader();
    assert_eq!(reader.schema().get_field_by_name("title"), Some(title_field));
    assert_eq!(reader.schema().get_field_by_name("pk"), Some(pk_field));
    assert_eq!(reader.schema().get_field_by_name("foo"), None);
}


#[test]
fn test_insert_document() {
    {
        let mut store = create_store("insert_document");

        let title_field = store.add_field("title".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
        let pk_field = store.add_field("pk".to_string(), FieldType::I64, FIELD_STORED).unwrap();

        store.insert_or_update_document(&Document {
            key: "test_doc".to_string(),
            indexed_fields: hashmap! {
                title_field => vec![
                    Token { term: Term::from_string("hello"), position: 1 },
                    Token { term: Term::from_string("world"), position: 2 },
                ],
            },
            stored_fields: hashmap! {
                pk_field => FieldValue::Integer(1),
            }
        }).unwrap();
    }

    // Check the DB
    let db = DB::open(&Options::default(), get_store_path("insert_document")).unwrap();

    fn print_keys(db: &DB) {
        fn bytes_to_string(bytes: &[u8]) -> String {
            use std::char;

            let mut string = String::new();

            for byte in bytes.iter() {
                if *byte < 128 {
                    // ASCII character
                    string.push(char::from_u32(*byte as u32).unwrap());
                } else {
                    string.push('?');
                }
            }

            string
        }

        let mut iter = db.iterator();
        while iter.next() {
            println!("{} = {:?}", bytes_to_string(&iter.key().unwrap()), iter.value().unwrap());
        }
    }

    print_keys(&db);

    /*
    .next_segment = [50]
    .next_term_ref = [51]
    .schema = [123, 34, 110, 101, 120, 116, 95, 102, 105, 101, 108, 100, 95, 105, 100, 34, 58, 51, 44, 34, 102, 105, 101, 108, 100, 115, 34, 58, 123, 34, 49, 34, 58, 123, 34, 110, 97, 109, 101, 34, 58, 34, 116, 105, 116, 108, 101, 34, 44, 34, 102, 105, 101, 108, 100, 95, 116, 121, 112, 101, 34, 58, 34, 84, 101, 120, 116, 34, 44, 34, 102, 105, 101, 108, 100, 95, 102, 108, 97, 103, 115, 34, 58, 34, 73, 78, 68, 69, 88, 69, 68, 34, 125, 44, 34, 50, 34, 58, 123, 34, 110, 97, 109, 101, 34, 58, 34, 112, 107, 34, 44, 34, 102, 105, 101, 108, 100, 95, 116, 121, 112, 101, 34, 58, 34, 73, 54, 52, 34, 44, 34, 102, 105, 101, 108, 100, 95, 102, 108, 97, 103, 115, 34, 58, 34, 83, 84, 79, 82, 69, 68, 34, 125, 125, 44, 34, 102, 105, 101, 108, 100, 95, 110, 97, 109, 101, 115, 34, 58, 123, 34, 116, 105, 116, 108, 101, 34, 58, 49, 44, 34, 112, 107, 34, 58, 50, 125, 125]
    a1 = []
    d1/1/1 = [0, 0]
    d1/2/1 = [0, 0]
    ktest_doc = [0, 0, 0, 1, 0, 0]
    s1/ftdoc-1 = [0, 0, 0, 0, 0, 0, 0, 1]
    s1/fttok-1 = [0, 0, 0, 0, 0, 0, 0, 2]
    s1/tdf-1-0 = [0, 0, 0, 0, 0, 0, 0, 1]
    s1/tdf-1-1 = [0, 0, 0, 0, 0, 0, 0, 1]
    s1/total_docs = [0, 0, 0, 0, 0, 0, 0, 1]
    thello = [50]
    tworld = [49]
    v1/0/1/len = [1]
    v1/0/2/val = [0, 0, 0, 0, 0, 0, 0, 1]
    */
}


fn make_test_store(path: &str) -> RocksDBIndexStore {
    let mut store = RocksDBIndexStore::create(path).unwrap();
    let title_field = store.add_field("title".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
    let body_field = store.add_field("body".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
    let pk_field = store.add_field("pk".to_string(), FieldType::I64, FIELD_STORED).unwrap();

    store.insert_or_update_document(&Document {
        key: "test_doc".to_string(),
        indexed_fields: hashmap! {
            title_field => vec![
                Token { term: Term::from_string("hello"), position: 1 },
                Token { term: Term::from_string("world"), position: 2 },
            ],
            body_field => vec![
                Token { term: Term::from_string("lorem"), position: 1 },
                Token { term: Term::from_string("ipsum"), position: 2 },
                Token { term: Term::from_string("dolar"), position: 3 },
            ],
        },
        stored_fields: hashmap! {
            pk_field => FieldValue::Integer(1),
        }
    }).unwrap();

    store.insert_or_update_document(&Document {
        key: "another_test_doc".to_string(),
        indexed_fields: hashmap! {
            title_field => vec![
                Token { term: Term::from_string("howdy"), position: 1 },
                Token { term: Term::from_string("partner"), position: 2 },
            ],
            body_field => vec![
                Token { term: Term::from_string("lorem"), position: 1 },
                Token { term: Term::from_string("ipsum"), position: 2 },
                Token { term: Term::from_string("dolar"), position: 3 },
            ],
        },
        stored_fields: hashmap! {
            pk_field => FieldValue::Integer(2),
        }
    }).unwrap();

    store.merge_segments(&vec![1, 2]).unwrap();
    store.purge_segments(&vec![1, 2]).unwrap();

    store
}


pub fn print_keys(db: &DB) {
    fn bytes_to_string(bytes: &[u8]) -> String {
        use std::char;

        let mut string = String::new();

        for byte in bytes.iter() {
            if *byte < 128 {
                // ASCII character
                string.push(char::from_u32(*byte as u32).unwrap());
            } else {
                string.push('?');
            }
        }

        string
    }

    let mut iter = db.iterator();
    while iter.next() {
        println!("{} = {:?}", bytes_to_string(&iter.key().unwrap()), iter.value().unwrap());
    }
}


#[test]
fn test_search_with_term_query() {
    remove_dir_all("test_indices/test");

    make_test_store("test_indices/test");

    let store = RocksDBIndexStore::open("test_indices/test").unwrap();

    let index_reader = store.reader();
    let title_field = index_reader.schema().get_field_by_name("title").unwrap();

    let query = Query::Term {
        field: title_field,
        term: Term::from_string("howdy"),
        scorer: TermScorer::default_with_boost(2.0f64),
    };

    let mut collector = TopScoreCollector::new(10);
    index_reader.search(&mut collector, &query).unwrap();

    let docs = collector.into_sorted_vec();
    println!("{:?}", docs);
}
