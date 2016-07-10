use search::term::Term;
use search::document::Document;
use search::query::Query;


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
            Query::Conjunction{ref queries} => {
                for query in queries {
                    if !query.matches(doc) {
                        return false;
                    }
                }

                return true;
            }
            Query::Disjunction{ref queries} => {
                for query in queries {
                    if query.matches(doc) {
                        return true;
                    }
                }

                return false;
            }
            Query::NDisjunction{ref queries, minimum_should_match} => {
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
            Query::Exclude{ref query, ref exclude} => {
                query.matches(doc) && !exclude.matches(doc)
            }
            Query::Score{ref query, mul, add} => {
                query.matches(doc)
            }
        }
    }
}
