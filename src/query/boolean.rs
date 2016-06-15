use term::Term;
use document::Document;
use query::{Query, TermMatcher};


#[derive(Debug, PartialEq)]
pub enum BooleanQuery {
    MatchAll,
    MatchNone,
    MatchTerm {
        field: String,
        term: Term,
        matcher: TermMatcher,
    },
    And {
        queries: Vec<BooleanQuery>,
    },
    Or {
        queries: Vec<BooleanQuery>,
    },
    Not {
        query: Box<BooleanQuery>,
    }
}


impl BooleanQuery {
    pub fn matches(&self, doc: &Document) -> bool {
        match *self {
            BooleanQuery::MatchAll => true,
            BooleanQuery::MatchNone => false,
            BooleanQuery::MatchTerm{ref field, ref term, ref matcher} => {
                if let Some(field_value) = doc.fields.get(field) {
                    for field_token in field_value.iter() {
                        if matcher.matches(&field_token.term, term) {
                            return true;
                        }
                    }
                }

                false
            }
            BooleanQuery::And{ref queries} => {
                for query in queries {
                    if !query.matches(doc) {
                        return false;
                    }
                }

                return true;
            }
            BooleanQuery::Or{ref queries} => {
                for query in queries {
                    if query.matches(doc) {
                        return true;
                    }
                }

                return false;
            }
            BooleanQuery::Not{ref query} => {
                !query.matches(doc)
            }
        }
    }
}

impl Query {
    pub fn to_boolean_query(self) -> BooleanQuery {
        match self {
            Query::MatchAll => BooleanQuery::MatchAll,
            Query::MatchNone => BooleanQuery::MatchNone,
            Query::MatchTerm{field, term, matcher} => {
                BooleanQuery::MatchTerm{
                    field: field,
                    term: term,
                    matcher: matcher,
                }
            }
            Query::And{queries} => {
                let mut boolean_queries = Vec::with_capacity(queries.len());

                for query in queries {
                    boolean_queries.push(query.to_boolean_query());
                }

                BooleanQuery::And {
                    queries: boolean_queries,
                }
            }
            Query::Or{queries} => {
                let mut boolean_queries = Vec::with_capacity(queries.len());

                for query in queries {
                    boolean_queries.push(query.to_boolean_query());
                }

                BooleanQuery::Or {
                    queries: boolean_queries,
                }
            }
            Query::MultiOr{queries, minimum_should_match} => {
                let mut boolean_queries = Vec::with_capacity(queries.len());

                for query in queries {
                    boolean_queries.push(query.to_boolean_query());
                }

                BooleanQuery::Or {
                    queries: boolean_queries,
                }
            }
            Query::DisjunctionMax{queries} => {
                let mut boolean_queries = Vec::with_capacity(queries.len());

                for query in queries {
                    boolean_queries.push(query.to_boolean_query());
                }

                BooleanQuery::Or {
                    queries: boolean_queries,
                }
            }
            Query::Filter{query, filter} => {
                BooleanQuery::And {
                    queries: vec![
                        query.to_boolean_query(),
                        *filter,
                    ]
                }
            }
            Query::Exclude{query, exclude} => {
                BooleanQuery::And {
                    queries: vec![
                        query.to_boolean_query(),
                        BooleanQuery::Not {
                            query: exclude,
                        }
                    ]
                }
            }
            Query::Score{query, mul, add} => {
                query.to_boolean_query()
            }
        }
    }
}
