pub mod parser;
pub mod matching;
pub mod ranking;

use term::Term;


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
    Bool {
        must: Vec<Query>,
        must_not: Vec<Query>,
        should: Vec<Query>,
        filter: Vec<Query>,
        minimum_should_match: i32,
    },
    And {
        queries: Vec<Query>,
    },
    Or {
        queries: Vec<Query>,
    },
    MultiOr {
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
    BoostScore {
        query: Box<Query>,
        boost: f64,
    },
    ConstantScore {
        query: Box<Query>,
        score: f64,
    },
}
