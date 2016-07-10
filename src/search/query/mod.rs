pub mod parser;
pub mod matching;
pub mod ranking;

use search::term::Term;


#[derive(Debug, PartialEq)]
pub enum TermMatcher {
    Exact,
    Prefix,
}


#[derive(Debug, PartialEq)]
pub enum Query {
    MatchAll,
    MatchNone,
    MatchTerm {
        field: String,
        term: Term,
        matcher: TermMatcher,
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
    Score {
        query: Box<Query>,
        mul: f64,
        add: f64,
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

    pub fn new_score(query: Query, mul: f64, add: f64) -> Query {
        if mul == 1.0f64 && add == 0.0f64 {
            // This score query won't have any effect
            return query;
        }

        Query::Score {
            query: Box::new(query),
            mul: mul,
            add: add,
        }
    }
}
