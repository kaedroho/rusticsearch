pub mod term_selector;
pub mod term_scorer;

use std::collections::HashMap;

use term::Term;
use schema::SchemaRead;
use document::Document;
use store::IndexReader;
use query::term_selector::TermSelector;
use query::term_scorer::TermScorer;


#[derive(Debug, PartialEq)]
pub enum Query {
    MatchAll {
        score: f64,
    },
    MatchNone,
    MatchTerm {
        field: String,
        term: Term,
        scorer: TermScorer,
    },
    MatchMultiTerm {
        field: String,
        term_selector: TermSelector,
        scorer: TermScorer,
    },
    Conjunction {
        queries: Vec<Query>,
    },
    Disjunction {
        queries: Vec<Query>,
    },
    NDisjunction {
        queries: Vec<Query>,
        minimum_should_match: i32,
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
    pub fn new_match_all() -> Query {
        Query::MatchAll {
            score: 1.0f64,
        }
    }

    pub fn new_conjunction(queries: Vec<Query>) -> Query {
        match queries.len() {
            0 => Query::MatchNone,
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
            0 => Query::MatchNone,
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
            0 => Query::MatchNone,
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
            Query::MatchAll{ref mut score} => {
                *score *= add_boost;
            },
            Query::MatchNone => (),
            Query::MatchTerm{ref mut scorer, ..} => {
                scorer.boost *= add_boost;
            }
            Query::MatchMultiTerm{ref mut scorer, ..} => {
                scorer.boost *= add_boost;
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
            Query::NDisjunction{ref mut queries, ..} => {
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
            Query::MatchAll{..} => true,
            Query::MatchNone => false,
            Query::MatchTerm{ref field, ref term, ..} => {
                if let Some(field_value) = doc.indexed_fields.get(field) {
                    for field_token in field_value.iter() {
                        if &field_token.term == term {
                            return true;
                        }
                    }
                }

                false
            }
            Query::MatchMultiTerm{ref field, ref term_selector, ..} => {
                if let Some(field_value) = doc.indexed_fields.get(field) {
                    for field_token in field_value.iter() {
                        if term_selector.matches(&field_token.term) {
                            return true;
                        }
                    }
                }

                false
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
            Query::NDisjunction{ref queries, minimum_should_match} => {
                let mut should_matched = 0;

                for query in queries {
                    if query.matches(doc) {
                        should_matched += 1;

                        if should_matched >= minimum_should_match {
                            return true;
                        }
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
            Query::MatchAll{score} => Some(score),
            Query::MatchNone => None,
            Query::MatchTerm{ref field, ref term, ref scorer} => {
                if let Some(field_ref) = index_reader.schema().get_field_by_name(field) {
                    if let Some(field_value) = doc.indexed_fields.get(field) {
                        let mut term_freq: u32 = 0;
                        for field_token in field_value.iter() {
                            if &field_token.term == term {
                                term_freq += 1;
                            }
                        }

                        if term_freq > 0 {
                            return Some(scorer.score(index_reader, &field_ref, term, term_freq, field_value.len() as u32));
                        }
                    }
                }

                None
            }
            Query::MatchMultiTerm{ref field, ref term_selector, ref scorer} => {
                if let Some(field_ref) = index_reader.schema().get_field_by_name(field) {
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
                                score += scorer.score(index_reader, &field_ref, term, *term_freq, field_value.len() as u32);
                            }

                            return Some(score);
                        }
                    }
                }

                None
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
            Query::NDisjunction{ref queries, minimum_should_match} => {
                let mut should_matched = 0;
                let mut total_score = 0.0f64;

                for query in queries {
                    if let Some(score) = query.rank(index_reader, doc) {
                        should_matched += 1;
                        total_score += score;
                    }
                }

                if should_matched < minimum_should_match {
                    return None;
                }

                Some(total_score / queries.len() as f64)
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
