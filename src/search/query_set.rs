use std::collections::VecDeque;

use search::term::Term;
use search::index::reader::IndexReader;
use search::query::Query;
use search::query::term_matcher::TermMatcher;


pub enum QuerySetIterator<'a, T: IndexReader<'a>> {
    None,
    All {
        iter: T::AllDocRefIterator,
    },
    Term {
        iter: T::TermDocRefIterator,
    },
    Conjunction {
        iter_a: Box<QuerySetIterator<'a, T>>,
        iter_b: Box<QuerySetIterator<'a, T>>,
        initialised: bool,
        current_doc_a: Option<u64>,
        current_doc_b: Option<u64>,
    },
    Disjunction {
        iter_a: Box<QuerySetIterator<'a, T>>,
        iter_b: Box<QuerySetIterator<'a, T>>,
        initialised: bool,
        current_doc_a: Option<u64>,
        current_doc_b: Option<u64>,
    },
    Exclusion {
        iter_a: Box<QuerySetIterator<'a, T>>,
        iter_b: Box<QuerySetIterator<'a, T>>,
        initialised: bool,
        current_doc_a: Option<u64>,
        current_doc_b: Option<u64>,
    },
}


impl <'a, T: IndexReader<'a>> Iterator for QuerySetIterator<'a, T> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        match *self {
            QuerySetIterator::None => None,
            QuerySetIterator::All{ref mut iter} => {
                iter.next()
            }
            QuerySetIterator::Term{ref mut iter} => {
                iter.next()
            }
            QuerySetIterator::Conjunction{ref mut iter_a, ref mut iter_b, ref mut initialised, ref mut current_doc_a, ref mut current_doc_b} => {
                if !*initialised {
                    *current_doc_a = iter_a.next();
                    *current_doc_b = iter_b.next();
                    *initialised = true;
                }

                loop {
                    if let Some(doc_id_a) = *current_doc_a {
                        if let Some(doc_id_b) = *current_doc_b {
                            if doc_id_a == doc_id_b {
                                *current_doc_a = iter_a.next();
                                *current_doc_b = iter_b.next();

                                return Some(doc_id_a);
                            } else if doc_id_a < doc_id_b {
                                *current_doc_a = iter_a.next();
                            } else {
                                *current_doc_b = iter_b.next();
                            }
                        } else {
                            return None;
                        }
                    } else {
                        return None;
                    }
                }
            }
            QuerySetIterator::Disjunction{ref mut iter_a, ref mut iter_b, ref mut initialised, ref mut current_doc_a, ref mut current_doc_b} => {
                if !*initialised {
                    *current_doc_a = iter_a.next();
                    *current_doc_b = iter_b.next();
                    *initialised = true;
                }

                if let Some(doc_id_a) = *current_doc_a {
                    if let Some(doc_id_b) = *current_doc_b {
                        if doc_id_a == doc_id_b {
                            *current_doc_a = iter_a.next();
                            *current_doc_b = iter_a.next();
                            Some(doc_id_a)
                        } else if doc_id_a < doc_id_b {
                            *current_doc_a = iter_a.next();
                            Some(doc_id_a)
                        } else {
                            *current_doc_b = iter_b.next();
                            Some(doc_id_b)
                        }
                    } else {
                        *current_doc_a = iter_a.next();
                        Some(doc_id_a)
                    }
                } else {
                    if let Some(doc_id_b) = *current_doc_b {
                        *current_doc_b = iter_b.next();
                        Some(doc_id_b)
                    } else {
                        None
                    }
                }
            }
            QuerySetIterator::Exclusion{ref mut iter_a, ref mut iter_b, ref mut initialised, ref mut current_doc_a, ref mut current_doc_b} => {
                if !*initialised {
                    *current_doc_a = iter_a.next();
                    *current_doc_b = iter_b.next();
                    *initialised = true;
                }

                loop {
                    if let Some(doc_id_a) = *current_doc_a {
                        if let Some(doc_id_b) = *current_doc_b {
                            if doc_id_a == doc_id_b {
                                *current_doc_a = iter_a.next();
                                *current_doc_b = iter_b.next();
                            } else if doc_id_a < doc_id_b {
                                *current_doc_a = iter_a.next();
                                return Some(doc_id_a);
                            } else {
                                *current_doc_b = iter_b.next();
                            }
                        } else {
                            *current_doc_a = iter_a.next();
                            return Some(doc_id_a);
                        }
                    } else {
                        return None;
                    }
                }
            }
        }
    }
}


fn build_conjunction_iterator<'a, T: IndexReader<'a>>(mut iters: VecDeque<QuerySetIterator<'a, T>>) -> QuerySetIterator<'a, T>  {
    if iters.len() == 0 {
        return QuerySetIterator::None;
    }

    // TODO: Order by lowest probability first

    let mut new_iters = VecDeque::with_capacity(iters.len() / 2 + 1);
    while iters.len() >= 2 {
        new_iters.push_back(QuerySetIterator::Conjunction {
            iter_a: Box::new(iters.pop_front().unwrap()),
            iter_b: Box::new(iters.pop_back().unwrap()),
            initialised: false,
            current_doc_a: None,
            current_doc_b: None,
        });
    }

    if iters.len() == 1 {
        new_iters.push_back(iters.pop_front().unwrap());
    }

    if new_iters.len() == 1 {
        // Done!
        new_iters.pop_front().unwrap()
    } else if new_iters.len() > 1 {
        // Still some compaction to do
        build_conjunction_iterator(new_iters)
    } else {
        // Check at top of this function should prevent this
        unreachable!()
    }
}


fn build_disjunction_iterator<'a, T: IndexReader<'a>>(mut iters: VecDeque<QuerySetIterator<'a, T>>) -> QuerySetIterator<'a, T> {
    if iters.len() == 0 {
        return QuerySetIterator::None;
    }

    // TODO: Order by lowest probability first

    let mut new_iters = VecDeque::with_capacity(iters.len() / 2 + 1);
    while iters.len() >= 2 {
        new_iters.push_back(QuerySetIterator::Disjunction {
            iter_a: Box::new(iters.pop_front().unwrap()),
            iter_b: Box::new(iters.pop_back().unwrap()),
            initialised: false,
            current_doc_a: None,
            current_doc_b: None,
        });
    }

    if iters.len() == 1 {
        new_iters.push_back(iters.pop_front().unwrap());
    }

    if new_iters.len() == 1 {
        // Done!
        new_iters.pop_front().unwrap()
    } else if new_iters.len() > 1 {
        // Still some compaction to do
        build_disjunction_iterator(new_iters)
    } else {
        // Check at top of this function should prevent this
        unreachable!()
    }
}


pub fn build_iterator_from_query<'a, T: IndexReader<'a>>(reader: &'a T, query: &Query) -> QuerySetIterator<'a, T> {
    match *query {
        Query::MatchAll => {
            QuerySetIterator::All {
                iter: reader.iter_docids_all(),
            }
        }
        Query::MatchNone => {
            QuerySetIterator::None
        }
        Query::MatchTerm{ref field, ref term, ref matcher} => {
            match *matcher {
                TermMatcher::Exact => {
                    QuerySetIterator::Term {
                        iter: reader.iter_docids_with_term(term.clone(), field.clone()),
                    }
                }
                TermMatcher::Prefix => {
                    // Find all terms in the index that match the prefix
                    let terms = match *term {
                         Term::String(ref term) => {
                             reader.iter_terms().filter_map(|k| {
                                 if let Term::String(ref k) = *k {
                                     if k.starts_with(term) {
                                         return Some(Term::String(k.clone()));
                                     }
                                 }

                                 None
                             }).collect::<Vec<Term>>()
                         }
                         _ => return QuerySetIterator::None,
                    };

                    match terms.len() {
                        0 => QuerySetIterator::None,
                        1 => {
                            QuerySetIterator::Term {
                                iter: reader.iter_docids_with_term(terms[0].clone(), field.clone()),
                            }
                        }
                        _ => {
                            // Produce a disjunction iterator for all the terms
                            let mut iters = VecDeque::new();
                            for term in terms.iter() {
                                iters.push_back(QuerySetIterator::Term {
                                    iter: reader.iter_docids_with_term(term.clone(), field.clone()),
                                });
                            }

                            build_disjunction_iterator(iters)
                        }
                    }
                }
            }
        }
        Query::Conjunction{ref queries} => {
            let mut iters = VecDeque::with_capacity(queries.len());

            for query in queries.iter() {
                iters.push_back(build_iterator_from_query(reader, query));
            }

            build_conjunction_iterator(iters)
        }
        Query::Disjunction{ref queries} => {
            let mut iters = VecDeque::with_capacity(queries.len());

            for query in queries.iter() {
                iters.push_back(build_iterator_from_query(reader, query));
            }

            build_disjunction_iterator(iters)
        }
        Query::NDisjunction{ref queries, minimum_should_match} => {
            // TODO
            QuerySetIterator::None
        }
        Query::DisjunctionMax{ref queries} => {
            let mut iters = VecDeque::with_capacity(queries.len());

            for query in queries.iter() {
                iters.push_back(build_iterator_from_query(reader, query));
            }

            build_disjunction_iterator(iters)
        }
        Query::Exclude{ref query, ref exclude} => {
            QuerySetIterator::Exclusion {
                iter_a: Box::new(build_iterator_from_query(reader, query)),
                iter_b: Box::new(build_iterator_from_query(reader, exclude)),
                initialised: false,
                current_doc_a: None,
                current_doc_b: None,
            }
        }
        Query::Filter{ref query, ref filter} => {
            QuerySetIterator::Conjunction {
                iter_a: Box::new(build_iterator_from_query(reader, query)),
                iter_b: Box::new(build_iterator_from_query(reader, filter)),
                initialised: false,
                current_doc_a: None,
                current_doc_b: None,
            }
        }
        Query::Score{ref query, mul, add} => {
            build_iterator_from_query(reader, query)
        }
    }
}


#[cfg(test)]
mod benches {
    use test::Bencher;

    use search::term::Term;
    use search::token::Token;
    use search::document::Document;
    use search::index::reader::IndexReader;
    use search::index::store::memory::MemoryIndexStore;
    use search::query_set::QuerySetIterator;


    fn make_test_store() -> MemoryIndexStore {
        let mut store = MemoryIndexStore::new();

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
                fields: btreemap! {
                    "body".to_string() => tokens
                }
            });
        }

        store
    }


    #[bench]
    fn bench_all(b: &mut Bencher) {
        let store = make_test_store();

        b.iter(|| {
            let mut iterator: QuerySetIterator<MemoryIndexStore> = QuerySetIterator::All {
                iter: store.iter_docids_all(),
            };

            for doc_id in iterator {}
        });
    }


    #[bench]
    fn bench_fizz_term(b: &mut Bencher) {
        let store = make_test_store();

        b.iter(|| {
            let mut iterator: QuerySetIterator<MemoryIndexStore> = QuerySetIterator::Term {
                iter: store.iter_docids_with_term(Term::String("fizz".to_string()), "body".to_string()),
            };

            for doc_id in iterator {}
        });
    }


    #[bench]
    fn bench_buzz_term(b: &mut Bencher) {
        let store = make_test_store();

        b.iter(|| {
            let mut iterator: QuerySetIterator<MemoryIndexStore> = QuerySetIterator::Term {
                iter: store.iter_docids_with_term(Term::String("buzz".to_string()), "body".to_string()),
            };

            for doc_id in iterator {}
        });
    }


    #[bench]
    fn bench_fizzbuzz_conjunction(b: &mut Bencher) {
        let store = make_test_store();

        b.iter(|| {
            let mut fizz_iterator: QuerySetIterator<MemoryIndexStore> = QuerySetIterator::Term {
                iter: store.iter_docids_with_term(Term::String("buzz".to_string()), "body".to_string()),
            };
            let mut buzz_iterator: QuerySetIterator<MemoryIndexStore> = QuerySetIterator::Term {
                iter: store.iter_docids_with_term(Term::String("buzz".to_string()), "body".to_string()),
            };
            let mut iterator: QuerySetIterator<MemoryIndexStore> = QuerySetIterator::Conjunction {
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

        b.iter(|| {
            let mut fizz_iterator: QuerySetIterator<MemoryIndexStore> = QuerySetIterator::Term {
                iter: store.iter_docids_with_term(Term::String("buzz".to_string()), "body".to_string()),
            };
            let mut buzz_iterator: QuerySetIterator<MemoryIndexStore> = QuerySetIterator::Term {
                iter: store.iter_docids_with_term(Term::String("buzz".to_string()), "body".to_string()),
            };
            let mut iterator: QuerySetIterator<MemoryIndexStore> = QuerySetIterator::Disjunction {
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

        b.iter(|| {
            let mut fizz_iterator: QuerySetIterator<MemoryIndexStore> = QuerySetIterator::Term {
                iter: store.iter_docids_with_term(Term::String("buzz".to_string()), "body".to_string()),
            };
            let mut buzz_iterator: QuerySetIterator<MemoryIndexStore> = QuerySetIterator::Term {
                iter: store.iter_docids_with_term(Term::String("buzz".to_string()), "body".to_string()),
            };
            let mut iterator: QuerySetIterator<MemoryIndexStore> = QuerySetIterator::Exclusion {
                iter_a: Box::new(fizz_iterator),
                iter_b: Box::new(buzz_iterator),
                initialised: false,
                current_doc_a: None,
                current_doc_b: None,
            };

            for doc_id in iterator {}
        });
    }
}
