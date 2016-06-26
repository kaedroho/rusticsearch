pub mod parser;
pub mod matching;
pub mod ranking;
pub mod boolean;

use term::Term;
use query::boolean::BooleanQuery;


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
        filter: Box<BooleanQuery>
    },
    Score {
        query: Box<Query>,
        mul: f64,
        add: f64,
    },
}
