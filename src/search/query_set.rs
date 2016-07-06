use std::collections::VecDeque;

use search::term::Term;
use search::index::Index;
use search::index::reader::IndexReader;
use search::query::{Query, TermMatcher};


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



pub fn next_doc(index: &Index, query: &Query, position: Option<u64>) -> Option<(u64, f64)> {
    match *query {
        Query::MatchAll => {
            match index.store.next_doc_all(position) {
                Some(doc_id) => Some((doc_id, 1.0f64)),
                None => None
            }
        }
        Query::MatchNone => None,
        Query::MatchTerm{ref field, ref term, ref matcher} => {
            match *matcher {
                TermMatcher::Exact => {
                    match index.store.next_doc(term, field, position) {
                        Some((doc_id, term_freq)) => {
                            let score: f64 = term_freq as f64; // TODO: better scoring

                            Some((doc_id, score))
                        }
                        None => None
                    }
                }
                TermMatcher::Prefix => {
                    // Find all terms in the index that match the prefix
                    let terms = match *term {
                         Term::String(ref term) => {
                             index.store.iter_terms().filter_map(|k| {
                                 if let Term::String(ref k) = *k {
                                     if k.starts_with(term) {
                                         return Some(Term::String(k.clone()));
                                     }
                                 }

                                 None
                             }).collect::<Vec<Term>>()
                         }
                         _ => return None,
                    };

                    // Generate a sub query to match those terms
                    let sub_query = match terms.len() {
                        0 => return None,
                        1 => {
                            Query::MatchTerm {
                                field: field.clone(),
                                term: terms[0].clone(),
                                matcher: TermMatcher::Exact,
                            }
                        }
                        _ => {
                            // Produce a disjunction query for all the terms
                            // FIXME: Building a query on every call to next_doc could get very
                            // slow if there are many terms that match the prefix
                            Query::Disjunction {
                                queries: terms.iter().map(|term| {
                                    Query::MatchTerm {
                                        field: field.clone(),
                                        term: term.clone(),
                                        matcher: TermMatcher::Exact,
                                    }
                                }).collect(),
                            }
                        }
                    };

                    // Run the sub query
                    next_doc(index, &sub_query, position)
                }
            }
        }
        Query::Conjunction{ref queries} => {
            // Iterate the docs returned from the first query and check they match the other queries
            // TODO: Instead of just using the first query, we should pick the query that's least
            //       likely to match something so we don't have to shift though as much junk.
            let first_query = match queries.first() {
                Some(query) => query,
                None => return None,
            };

            let mut position = next_doc(index, first_query, position);

            while let Some(pos) = position {
                let mut broke = false;
                let mut total_score = pos.1;

                for query in queries.iter().skip(1) {
                    // Check the next query matches this document
                    match next_doc(index, query, Some(pos.0 - 1)) {
                        Some(query_doc) => {
                            if query_doc.0 != pos.0 {
                                // This query doesn't match the document
                                broke = true;
                                break;
                            }

                            total_score += query_doc.1
                        }
                        None => {
                            // This query doesn't match the document
                            broke = true;
                            break;
                        }
                    }
                }

                if !broke {
                    return Some((pos.0, total_score / queries.len() as f64));
                }

                position = next_doc(index, first_query, Some(pos.0));
            }

            None
        }
        Query::Disjunction{ref queries} => {
            // Get next doc for each query and return lowest one
            // If multiple queries match the same document, average out the scores
            let mut lowest_doc: Option<(u64, f64)> = None;

            for query in queries.iter() {
                if let Some((doc_id, score)) = next_doc(index, query, position) {
                    match lowest_doc {
                        Some((matched_doc_id, total_score)) => {
                            if doc_id < matched_doc_id {
                                // Before current matched doc. Override it
                                lowest_doc = Some((doc_id, score));
                            } else if doc_id == matched_doc_id {
                                // Same doc, merge scores
                                lowest_doc = Some((doc_id, total_score + score))
                            }
                        }
                        None => {
                            // First match
                            lowest_doc = Some((doc_id, score));
                        }
                    }
                }
            }

            match lowest_doc {
                Some((matched_doc_id, total_score)) => Some((matched_doc_id, total_score / queries.len() as f64)),
                None => None,
            }
        }
        Query::NDisjunction{ref queries, minimum_should_match} => {
            None
        }
        Query::DisjunctionMax{ref queries} => {
            // Get next doc for each query and return lowest one
            // If multiple queries match the same document, average out the scores
            let mut lowest_doc: Option<(u64, f64)> = None;

            for query in queries.iter() {
                if let Some((doc_id, score)) = next_doc(index, query, position) {
                    match lowest_doc {
                        Some((matched_doc_id, max_score)) => {
                            if doc_id < matched_doc_id {
                                // Before current matched doc. Override it
                                lowest_doc = Some((doc_id, score));
                            } else if doc_id == matched_doc_id {
                                // Same doc, merge scores
                                if score > max_score {
                                    lowest_doc = Some((doc_id, score))
                                }
                            }
                        }
                        None => {
                            // First match
                            lowest_doc = Some((doc_id, score));
                        }
                    }
                }
            }

            lowest_doc
        }
        Query::Filter{ref query, ref filter} => {
            let mut query_doc = match next_doc(index, query, position) {
                Some((doc_id, score)) => (doc_id, score),
                None => return None,
            };

            loop {
                // See if there is a filter in the same doc
                let filter_doc = match next_doc(index, filter, Some(query_doc.0 - 1)) {
                    Some((doc_id, score)) => doc_id,
                    None => return None,
                };

                if query_doc.0 == filter_doc {
                    return Some(query_doc)
                }

                query_doc = match next_doc(index, query, Some(query_doc.0)) {
                    Some((doc_id, score)) => (doc_id, score),
                    None => return None,
                };
            }
        }
        Query::Exclude{ref query, ref exclude} => {
            let mut query_doc = match next_doc(index, query, position) {
                Some((doc_id, score)) => (doc_id, score),
                None => return None,
            };

            loop {
                // See if there is not a filter in the same doc
                let exclude_doc = match next_doc(index, exclude, Some(query_doc.0 - 1)) {
                    Some((doc_id, score)) => doc_id,
                    None => return Some(query_doc),
                };

                if query_doc.0 == exclude_doc {
                    query_doc = match next_doc(index, query, Some(query_doc.0)) {
                        Some((doc_id, score)) => (doc_id, score),
                        None => return None,
                    };
                    continue
                }

                return Some(query_doc)
            }
        }
        Query::Score{ref query, mul, add} => {
            match next_doc(index, query, position) {
                Some((doc_id, score)) => {
                    Some((doc_id, score.mul_add(mul, add)))
                }
                None => None
            }
        }
    }
}
