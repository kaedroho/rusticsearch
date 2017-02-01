pub mod term_selector;
pub mod term_scorer;

use term::Term;
use schema::FieldRef;
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
}
