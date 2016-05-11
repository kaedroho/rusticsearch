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
            Query::MatchTerm{ref fields, ref value, ref matcher, boost} => {
                for field in fields.iter() {
                    if let Some(&Value::String(ref field_value)) = doc.fields.get(field) {
                        return matcher.matches(field_value, value);
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
        }
    }
}
