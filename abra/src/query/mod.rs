pub mod term_matcher;
pub mod term_scorer;
pub mod parser;

use term::Term;
use document::Document;
use store::IndexReader;
use query::term_matcher::TermMatcher;
use query::term_scorer::TermScorer;


#[derive(Debug, PartialEq)]
pub enum Query {
    MatchAll,
    MatchNone,
    MatchTerm {
        field: String,
        term: Term,
        matcher: TermMatcher,
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
    Boost {
        query: Box<Query>,
        boost: f64,
    },
}


impl Query {
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

    pub fn new_boost(query: Query, boost: f64) -> Query {
        if boost == 1.0f64 {
            // This boost query won't have any effect
            return query;
        }

        Query::Boost {
            query: Box::new(query),
            boost: boost,
        }
    }

    pub fn matches(&self, doc: &Document) -> bool {
        match *self {
            Query::MatchAll => true,
            Query::MatchNone => false,
            Query::MatchTerm{ref field, ref term, ref matcher, ref scorer} => {
                if let Some(field_value) = doc.fields.get(field) {
                    for field_token in field_value.iter() {
                        if matcher.matches(&field_token.term, term) {
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
            Query::Boost{ref query, boost} => {
                query.matches(doc)
            }
        }
    }

    pub fn rank<'a, R: IndexReader<'a>>(&self, index_reader: &'a R, doc: &Document) -> Option<f64> {
        match *self {
            Query::MatchAll => Some(1.0f64),
            Query::MatchNone => None,
            Query::MatchTerm{ref field, ref term, ref matcher, ref scorer} => {
                if let Some(field_value) = doc.fields.get(field) {
                    let mut term_freq: u32 = 0;
                    for field_token in field_value.iter() {
                        if matcher.matches(&field_token.term, term) {
                            term_freq += 1;
                        }
                    }

                    if term_freq > 0 {
                        return Some(scorer.score(index_reader, field, term, term_freq, field_value.len() as u32));
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
            Query::Boost{ref query, boost} => {
                if boost == 0.0f64 {
                    // Score of inner query isn't needed
                    if query.matches(doc) {
                        Some(0.0f64)
                    } else {
                        None
                    }
                } else {
                    match query.rank(index_reader, doc) {
                        Some(score) => Some(score * boost),
                        None => None
                    }
                }
            }
        }
    }
}
