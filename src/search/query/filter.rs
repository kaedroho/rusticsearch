use term::Term;
use document::Document;
use search::query::{Query, TermMatcher};


#[derive(Debug, PartialEq)]
pub enum Filter {
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
    Conjunction {
        filters: Vec<Filter>,
    },
    Disjunction {
        filters: Vec<Filter>,
    },
}


impl Filter {
    pub fn matches(&self, doc: &Document) -> bool {
        match *self {
            Filter::MatchAll => true,
            Filter::MatchNone => false,
            Filter::MatchTerm{ref field, ref term, ref matcher} => {
                if let Some(field_value) = doc.fields.get(field) {
                    for field_token in field_value.iter() {
                        if matcher.matches(&field_token.term, term) {
                            return true;
                        }
                    }
                }

                false
            }
            Filter::NotMatchTerm{ref field, ref term, ref matcher} => {
                if let Some(field_value) = doc.fields.get(field) {
                    for field_token in field_value.iter() {
                        if matcher.matches(&field_token.term, term) {
                            return false;
                        }
                    }
                }

                true
            }
            Filter::Conjunction{ref filters} => {
                for filter in filters {
                    if !filter.matches(doc) {
                        return false;
                    }
                }

                return true;
            }
            Filter::Disjunction{ref filters} => {
                for filter in filters {
                    if filter.matches(doc) {
                        return true;
                    }
                }

                return false;
            }
        }
    }

    pub fn negate(self) -> Filter {
        match self {
            Filter::MatchAll => Filter::MatchNone,
            Filter::MatchNone => Filter::MatchAll,
            Filter::MatchTerm{field, term, matcher} => {
                Filter::NotMatchTerm {
                    field: field,
                    term: term,
                    matcher: matcher,
                }
            }
            Filter::NotMatchTerm{field, term, matcher} => {
                Filter::MatchTerm {
                    field: field,
                    term: term,
                    matcher: matcher,
                }
            }
            Filter::Conjunction{filters} => {
                let mut negated_filters = Vec::new();
                for filter in filters {
                    negated_filters.push(filter);
                }

                Filter::Disjunction{
                    filters: negated_filters,
                }
            }
            Filter::Disjunction{filters} => {
                let mut negated_filters = Vec::new();
                for filter in filters {
                    negated_filters.push(filter);
                }

                Filter::Conjunction{
                    filters: negated_filters,
                }
            }
        }
    }
}

impl Query {
    pub fn to_filter(self) -> Filter {
        match self {
            Query::MatchAll => Filter::MatchAll,
            Query::MatchNone => Filter::MatchNone,
            Query::MatchTerm{field, term, matcher} => {
                Filter::MatchTerm{
                    field: field,
                    term: term,
                    matcher: matcher,
                }
            }
            Query::Conjunction{queries} => {
                let mut filters = Vec::with_capacity(queries.len());

                for query in queries {
                    filters.push(query.to_filter());
                }

                Filter::Conjunction {
                    filters: filters,
                }
            }
            Query::Disjunction{queries} => {
                let mut filters = Vec::with_capacity(queries.len());

                for query in queries {
                    filters.push(query.to_filter());
                }

                Filter::Disjunction {
                    filters: filters,
                }
            }
            Query::NDisjunction{queries, minimum_should_match} => {
                let mut filters = Vec::with_capacity(queries.len());

                for query in queries {
                    filters.push(query.to_filter());
                }

                Filter::Disjunction {
                    filters: filters,
                }
            }
            Query::DisjunctionMax{queries} => {
                let mut filters = Vec::with_capacity(queries.len());

                for query in queries {
                    filters.push(query.to_filter());
                }

                Filter::Disjunction {
                    filters: filters,
                }
            }
            Query::Filter{query, filter} => {
                Filter::Conjunction {
                    filters: vec![
                        query.to_filter(),
                        *filter,
                    ]
                }
            }
            Query::Score{query, mul, add} => {
                query.to_filter()
            }
        }
    }
}
