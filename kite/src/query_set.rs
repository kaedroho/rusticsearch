use std::collections::VecDeque;

use store::IndexReader;
use query::Query;
use query::term_selector::TermSelector;


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
        Query::MatchAll{..} => {
            QuerySetIterator::All {
                iter: reader.iter_all_docs(),
            }
        }
        Query::MatchNone => {
            QuerySetIterator::None
        }
        Query::MatchTerm{ref field, ref term, ..} => {
            if let Some(field_ref) = reader.schema().get_field_by_name(field) {
                match reader.iter_docs_with_term(&term.to_bytes(), &field_ref) {
                    Some(iter) => {
                        QuerySetIterator::Term {
                            iter: iter,
                        }
                    }
                    None => {
                        // Term/field doesn't exist
                        QuerySetIterator::None
                    }
                }
            } else {
                // Field doesn't exist
                QuerySetIterator::None
            }
        }
        Query::MatchMultiTerm{ref field, ref term_selector, ..} => {
            if let Some(field_ref) = reader.schema().get_field_by_name(field) {
                match *term_selector {
                    TermSelector::Prefix(ref prefix) => {
                        let prefix_bytes = prefix.as_bytes();

                        // Find all terms in the index that match the prefix
                        let terms = match reader.iter_all_terms(&field_ref) {
                            Some(terms) => {
                                terms.filter_map(|k| {
                                    if k.starts_with(&prefix_bytes) {
                                        return Some(k.clone());
                                    }

                                    None
                                }).collect::<Vec<&[u8]>>()
                            }
                            None => return QuerySetIterator::None,
                        };

                        match terms.len() {
                            0 => QuerySetIterator::None,
                            1 => {
                                match reader.iter_docs_with_term(&terms[0], &field_ref) {
                                    Some(iter) => {
                                        QuerySetIterator::Term {
                                            iter: iter,
                                        }
                                    }
                                    None => {
                                        // Term/field doesn't exist
                                        QuerySetIterator::None
                                    }
                                }
                            }
                            _ => {
                                // Produce a disjunction iterator for all the terms
                                let mut iters = VecDeque::new();
                                for term in terms.iter() {
                                    match reader.iter_docs_with_term(term, &field_ref) {
                                        Some(iter) => {
                                            iters.push_back(QuerySetIterator::Term {
                                                iter: iter,
                                            });
                                        }
                                        None => {
                                            // Term/field doesn't exist
                                            continue;
                                        }
                                    }
                                }

                                build_disjunction_iterator(iters)
                            }
                        }
                    }
                }
            } else {
                // Field doesn't exist
                QuerySetIterator::None
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
        Query::NDisjunction{..} => {
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
    }
}
