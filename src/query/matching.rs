use {Document, Value};

use query::{TermMatcher, Query};


impl TermMatcher {
    pub fn matches(&self, value: &str, query: &str) -> bool {
        match *self {
            TermMatcher::Exact => value == query,
            TermMatcher::Prefix => value.starts_with(query),
        }
    }
}


impl Query {
    pub fn matches(&self, doc: &Document) -> bool {
        match *self {
            Query::MatchAll{ref boost} => true,
            Query::MatchNone => false,
            Query::MatchTerm{ref field, ref value, ref matcher, boost} => {
                if let Some(field_value) = doc.fields.get(field) {
                    match *field_value {
                        Value::String(ref field_value) => {
                            return matcher.matches(field_value, value);
                        }
                        Value::TSVector(ref field_value) => {
                            for field_term in field_value.iter() {
                                if matcher.matches(field_term, value) {
                                    return true;
                                }
                            }
                        }
                        _ => return false
                    }
                }

                false
            }
            Query::Bool{ref must, ref must_not, ref should, ref filter, minimum_should_match, boost} => {
                // Must not
                for query in must_not {
                    if query.matches(doc) {
                        return false;
                    }
                }

                // Filter
                for filter in filter {
                    if !filter.matches(doc) {
                        return false;
                    }
                }

                // Must
                for query in must {
                    if !query.matches(doc) {
                        return false;
                    }
                }

                // Should
                if minimum_should_match > 0 {
                    let mut should_matched: i32 = 0;
                    for query in should {
                        if query.matches(doc) {
                            should_matched += 1;

                            if should_matched >= minimum_should_match {
                                return true;
                            }
                        }
                    }

                    return false;
                }

                return true;
            }
            Query::DisjunctionMax{ref queries, boost} => {
                for query in queries {
                    if query.matches(doc) {
                        return true;
                    }
                }

                return false;
            }
        }
    }
}
