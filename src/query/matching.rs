use term::Term;
use document::Document;
use query::{TermMatcher, Query};


impl TermMatcher {
    pub fn matches(&self, value: &Term, query: &Term) -> bool {
        match *self {
            TermMatcher::Exact => value == query,
            TermMatcher::Prefix => {
                if let Term::String(ref value) = *value {
                    if let Term::String(ref query) = *query {
                        return value.starts_with(query);
                    }
                }

                false
            }
        }
    }
}


impl Query {
    pub fn matches(&self, doc: &Document) -> bool {
        match *self {
            Query::MatchAll => true,
            Query::MatchNone => false,
            Query::MatchTerm{ref field, ref term, ref matcher} => {
                if let Some(field_value) = doc.fields.get(field) {
                    for field_token in field_value.iter() {
                        if matcher.matches(&field_token.term, term) {
                            return true;
                        }
                    }
                }

                false
            }
            Query::And{ref queries} => {
                for query in queries {
                    if !query.matches(doc) {
                        return false;
                    }
                }

                return true;
            }
            Query::Or{ref queries} => {
                for query in queries {
                    if query.matches(doc) {
                        return true;
                    }
                }

                return false;
            }
            Query::MultiOr{ref queries, minimum_should_match} => {
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
            Query::NegativeFilter{ref query, ref filter} => {
                query.matches(doc) && !filter.matches(doc)
            }
            Query::Score{ref query, mul, add} => {
                query.matches(doc)
            }
        }
    }
}
