use search::term::Term;
use search::index::Index;
use search::query::{Query, TermMatcher};


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
