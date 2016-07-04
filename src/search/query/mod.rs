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
