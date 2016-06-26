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
    NotMatchTerm {
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
            BooleanQuery::NotMatchTerm{ref field, ref term, ref matcher} => {
                if let Some(field_value) = doc.fields.get(field) {
                    for field_token in field_value.iter() {
                        if matcher.matches(&field_token.term, term) {
                            return false;
                        }
                    }
                }

                true
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
        }
    }

    pub fn negate(self) -> BooleanQuery {
        match self {
            BooleanQuery::MatchAll => BooleanQuery::MatchNone,
            BooleanQuery::MatchNone => BooleanQuery::MatchAll,
            BooleanQuery::MatchTerm{field, term, matcher} => {
                BooleanQuery::NotMatchTerm {
                    field: field,
                    term: term,
                    matcher: matcher,
                }
            }
            BooleanQuery::NotMatchTerm{field, term, matcher} => {
                BooleanQuery::MatchTerm {
                    field: field,
                    term: term,
                    matcher: matcher,
                }
            }
            BooleanQuery::And{queries} => {
                let mut negated_queries = Vec::new();
                for query in queries {
                    negated_queries.push(query);
                }

                BooleanQuery::Or{
                    queries: negated_queries,
                }
            }
            BooleanQuery::Or{queries} => {
                let mut negated_queries = Vec::new();
                for query in queries {
                    negated_queries.push(query);
                }

                BooleanQuery::And{
                    queries: negated_queries,
                }
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
            Query::Score{query, mul, add} => {
                query.to_boolean_query()
            }
        }
    }
}