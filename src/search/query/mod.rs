pub mod multi_term_selector;
pub mod term_scorer;

use search::term::Term;
use search::schema::FieldId;
use search::query::multi_term_selector::MultiTermSelector;
use search::query::term_scorer::TermScorer;

#[derive(Debug, PartialEq)]
pub enum Query {
    /// Matches all documents, assigning the specified score to each one
    All {
        /// The score to assign to each document
        score: f32,
    },

    /// Matches nothing
    None,

    /// Matches documents that contain the specified term in the specified field
    Term {
        /// The field being searched
        field: FieldId,

        /// The term to search for
        term: Term,

        /// The method of scoring each match
        scorer: TermScorer,
    },

    /// Matches documents by a multi term selector
    /// Used for prefix, fuzzy and regex queries
    MultiTerm {
        /// The field being searched
        field: FieldId,

        /// The term selector to use. All terms that match this selector will be searched
        term_selector: MultiTermSelector,

        /// The method of scoring each match.
        scorer: TermScorer,
    },

    /// Joins two queries with an AND operator
    /// This intersects the results of the queries. The scores are combined by average
    Conjunction {
        queries: Vec<Query>,
    },

    /// Joins two queries with an OR operator
    /// This unites the results of the queries. The scores are combined by average
    Disjunction {
        queries: Vec<Query>,
    },

    /// Joins two queries with an OR operator
    /// This unites the results of the queries.
    /// Unlike a regular Disjunction query, this takes the highest score of each query for a particular match
    DisjunctionMax {
        queries: Vec<Query>,
    },

    /// Removes documents that do not match the "filter" query from the results
    /// Basically the same as a Conjunction query except that the "filter" query does not affect the score
    Filter {
        query: Box<Query>,
        filter: Box<Query>
    },

    /// Removes documents that match the "exclude" query from the results
    Exclude {
        query: Box<Query>,
        exclude: Box<Query>
    },
}

impl Query {
    /// Creates a new All query
    pub fn all() -> Query {
        Query::All {
            score: 1.0f32,
        }
    }

    /// Creates a new Term query
    pub fn term(field: FieldId, term: Term) -> Query {
        Query::Term {
            field: field,
            term: term,
            scorer: TermScorer::default(),
        }
    }

    /// Filters the query by another query
    /// Only documents that match the other query will remain in the results but the other query will not affect the score
    pub fn filter(self, filter: Query) -> Query {
        Query::Filter {
            query: Box::new(self),
            filter: Box::new(filter),
        }
    }

    /// Filters the query to exclude documents that match the other query
    pub fn exclude(self, exclude: Query) -> Query {
        Query::Exclude {
            query: Box::new(self),
            exclude: Box::new(exclude),
        }
    }

    #[inline]
    /// Multiplies the score of documents that match the query by the specified "boost" value
    pub fn boost(mut self, boost: f32) -> Query {
        self.add_boost(boost);
        self
    }

    fn add_boost(&mut self, add_boost: f32) {
        if add_boost == 1.0f32 {
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
                    query.add_boost(add_boost);
                }
            }
            Query::Disjunction{ref mut queries} => {
                for query in queries {
                    query.add_boost(add_boost);
                }
            }
            Query::DisjunctionMax{ref mut queries} => {
                for query in queries {
                    query.add_boost(add_boost);
                }
            }
            Query::Filter{ref mut query, ..} => {
                query.add_boost(add_boost);
            }
            Query::Exclude{ref mut query, ..} => {
                query.add_boost(add_boost);
            }
        }
    }
}
