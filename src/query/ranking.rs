use document::Document;
use query::Query;


impl Query {
    pub fn rank(&self, doc: &Document) -> Option<f64> {
        match *self {
            Query::MatchAll => Some(1.0f64),
            Query::MatchNone => None,
            Query::MatchTerm{ref field, ref term, ref matcher} => {
                if let Some(field_value) = doc.fields.get(field) {
                    for field_token in field_value.iter() {
                        if matcher.matches(&field_token.term, term) {
                            return Some(1.0f64);
                        }
                    }
                }

                None
            }
            Query::And{ref queries} => {
                let mut total_score = 0.0f64;

                for query in queries {
                    match query.rank(doc) {
                        Some(score) => {
                            total_score += score;
                        }
                        None => return None
                    }
                }

                Some(total_score / queries.len() as f64)
            }
            Query::Or{ref queries} => {
                let mut something_matched = false;
                let mut total_score = 0.0f64;

                for query in queries {
                    if let Some(score) = query.rank(doc) {
                        something_matched = true;
                        total_score += score;
                    }
                }

                if something_matched {
                    Some(total_score / queries.len() as f64)
                } else {
                    None
                }
            }
            Query::MultiOr{ref queries, minimum_should_match} => {
                let mut should_matched = 0;
                let mut total_score = 0.0f64;

                for query in queries {
                    if let Some(score) = query.rank(doc) {
                        should_matched += 1;
                        total_score += score;
                    }
                }

                if should_matched < minimum_should_match {
                    return None;
                }

                Some(total_score / queries.len() as f64)
            }
            Query::DisjunctionMax{ref queries} => {
                let mut something_matched = false;
                let mut max_score = 0.0f64;

                for query in queries {
                    match query.rank(doc) {
                        Some(score) => {
                            something_matched = true;
                            if score > max_score {
                                max_score = score;
                            }
                        }
                        None => continue,
                    }
                }

                if something_matched {
                    Some(max_score)
                } else {
                    None
                }
            }
            Query::Filter{ref query, ref filter} => {
                if filter.matches(doc) {
                    query.rank(doc)
                } else {
                    None
                }
            }
            Query::Exclude{ref query, ref exclude} => {
                if !exclude.matches(doc) {
                    query.rank(doc)
                } else {
                    None
                }
            }
            Query::Score{ref query, mul, add} => {
                if mul == 0.0f64 {
                    // Score of inner query isn't needed
                    if query.matches(doc) {
                        Some(add)
                    } else {
                        None
                    }
                } else {
                    match query.rank(doc) {
                        Some(score) => Some(score.mul_add(mul, add)),
                        None => None
                    }
                }
            }
        }
    }
}
