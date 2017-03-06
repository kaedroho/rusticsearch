pub mod term_selector;
pub mod term_scorer;

use std::collections::HashMap;

use term::Term;
use document::Document;
use schema::FieldRef;
use store::IndexReader;
use query::term_selector::TermSelector;
use query::term_scorer::TermScorer;


#[derive(Debug, PartialEq)]
pub enum Query {
    All {
        score: f64,
    },
    None,
    Term {
        field: FieldRef,
        term: Term,
        scorer: TermScorer,
    },
    MultiTerm {
        field: FieldRef,
        term_selector: TermSelector,
        scorer: TermScorer,
    },
    MatchHasField {
        field: FieldRef,
        score: f64,
    },
    Conjunction {
        queries: Vec<Query>,
    },
    Disjunction {
        queries: Vec<Query>,
    },
    DisjunctionMax {
        queries: Vec<Query>,
    },
    Filter {
        query: Box<Query>,
        filter: Box<Query>
    },
    Exclude {
        query: Box<Query>,
        exclude: Box<Query>
    },
}


impl Query {
    pub fn new_all() -> Query {
        Query::All {
            score: 1.0f64,
        }
    }

    pub fn new_conjunction(queries: Vec<Query>) -> Query {
        match queries.len() {
            0 => Query::None,
            1 => {
                // Single query, unpack it from queries array and return it
                for query in queries {
                    return query;
                }

                unreachable!();
            }
            _ => {
                Query::Conjunction {
                    queries: queries,
                }
            }
        }
    }

    pub fn new_disjunction(queries: Vec<Query>) -> Query {
        match queries.len() {
            0 => Query::None,
            1 => {
                // Single query, unpack it from queries array and return it
                for query in queries {
                    return query;
                }

                unreachable!();
            }
            _ => {
                Query::Disjunction {
                    queries: queries,
                }
            }
        }
    }

    pub fn new_disjunction_max(queries: Vec<Query>) -> Query {
        match queries.len() {
            0 => Query::None,
            1 => {
                // Single query, unpack it from queries array and return it
                for query in queries {
                    return query;
                }

                unreachable!();
            }
            _ => {
                Query::DisjunctionMax {
                    queries: queries,
                }
            }
        }
    }

    pub fn boost(&mut self, add_boost: f64) {
        if add_boost == 1.0f64 {
            // This boost query won't have any effect
            return;
        }

        match *self {
            Query::All{ref mut score} => {
                *score *= add_boost;
            },
            Query::None => (),
            Query::Term{ref mut scorer, ..} => {
                scorer.boost *= add_boost;
            }
            Query::MultiTerm{ref mut scorer, ..} => {
                scorer.boost *= add_boost;
            }
            Query::MatchHasField{ref mut score, ..} => {
                *score *= add_boost;
            }
            Query::Conjunction{ref mut queries} => {
                for query in queries {
                    query.boost(add_boost);
                }
            }
            Query::Disjunction{ref mut queries} => {
                for query in queries {
                    query.boost(add_boost);
                }
            }
            Query::DisjunctionMax{ref mut queries} => {
                for query in queries {
                    query.boost(add_boost);
                }
            }
            Query::Filter{ref mut query, ..} => {
                query.boost(add_boost);
            }
            Query::Exclude{ref mut query, ..} => {
                query.boost(add_boost);
            }
        }
    }

    pub fn matches(&self, doc: &Document) -> bool {
        match *self {
            Query::All{..} => true,
            Query::None => false,
            Query::Term{ref field, ref term, ..} => {
                if let Some(field_value) = doc.indexed_fields.get(field) {
                    for field_token in field_value.iter() {
                        if &field_token.term == term {
                            return true;
                        }
                    }
                }

                false
            }
            Query::MultiTerm{ref field, ref term_selector, ..} => {
                if let Some(field_value) = doc.indexed_fields.get(field) {
                    for field_token in field_value.iter() {
                        if term_selector.matches(&field_token.term) {
                            return true;
                        }
                    }
                }

                false
            }
            Query::MatchHasField{ref field, ..} => {
                doc.indexed_fields.contains_key(field)
            }
            Query::Conjunction{ref queries} => {
                for query in queries {
                    if !query.matches(doc) {
                        return false;
                    }
                }

                return true;
            }
            Query::Disjunction{ref queries} => {
                for query in queries {
                    if query.matches(doc) {
                        return true;
                    }
                }

                return false;
            }
            Query::DisjunctionMax{ref queries} => {
                for query in queries {
                    if query.matches(doc) {
                        return true;
                    }
                }

                return false;
            }
            Query::Filter{ref query, ref filter} => {
                query.matches(doc) && filter.matches(doc)
            }
            Query::Exclude{ref query, ref exclude} => {
                query.matches(doc) && !exclude.matches(doc)
            }
        }
    }

    pub fn rank<'a, R: IndexReader<'a>>(&self, index_reader: &'a R, doc: &Document) -> Option<f64> {
        match *self {
            Query::All{score} => Some(score),
            Query::None => None,
            Query::Term{ref field, ref term, ref scorer} => {
                if let Some(field_value) = doc.indexed_fields.get(field) {
                    let mut term_freq: u32 = 0;
                    for field_token in field_value.iter() {
                        if &field_token.term == term {
                            term_freq += 1;
                        }
                    }

                    if term_freq > 0 {
                        return Some(scorer.score(index_reader, field, term, term_freq, field_value.len() as f64));
                    }
                }

                None
            }
            Query::MultiTerm{ref field, ref term_selector, ref scorer} => {
                if let Some(field_value) = doc.indexed_fields.get(field) {
                    let mut term_frequencies = HashMap::new();
                    for field_token in field_value.iter() {
                        if term_selector.matches(&field_token.term) {
                            let freq = term_frequencies.entry(field_token.term.clone()).or_insert(0);
                            *freq += 1;
                        }
                    }

                    if !term_frequencies.is_empty() {
                        let mut score = 0.0f64;

                        for (term, term_freq) in term_frequencies.iter() {
                            score += scorer.score(index_reader, field, term, *term_freq, field_value.len() as f64);
                        }

                        return Some(score);
                    }
                }

                None
            }
            Query::MatchHasField{ref field, score} => {
                if doc.indexed_fields.contains_key(field) {
                    Some(score)
                } else {
                    None
                }
            }
            Query::Conjunction{ref queries} => {
                let mut total_score = 0.0f64;

                for query in queries {
                    match query.rank(index_reader, doc) {
                        Some(score) => {
                            total_score += score;
                        }
                        None => return None
                    }
                }

                Some(total_score / queries.len() as f64)
            }
            Query::Disjunction{ref queries} => {
                let mut something_matched = false;
                let mut total_score = 0.0f64;

                for query in queries {
                    if let Some(score) = query.rank(index_reader, doc) {
                        something_matched = true;
                        total_score += score;
                    }
                }

                if something_matched {
                    Some(total_score / queries.len() as f64)
                } else {
                    None
                }
            }
            Query::DisjunctionMax{ref queries} => {
                let mut something_matched = false;
                let mut max_score = 0.0f64;

                for query in queries {
                    match query.rank(index_reader, doc) {
                        Some(score) => {
                            something_matched = true;
                            if score > max_score {
                                max_score = score;
                            }
                        }
                        None => continue,
                    }
                }

                if something_matched {
                    Some(max_score)
                } else {
                    None
                }
            }
            Query::Filter{ref query, ref filter} => {
                if filter.matches(doc) {
                    query.rank(index_reader, doc)
                } else {
                    None
                }
            }
            Query::Exclude{ref query, ref exclude} => {
                if !exclude.matches(doc) {
                    query.rank(index_reader, doc)
                } else {
                    None
                }
            }
        }
    }
}
