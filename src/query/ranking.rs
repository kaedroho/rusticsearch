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
            Query::Bool{ref must, ref must_not, ref should, ref filter, minimum_should_match} => {
                let mut total_score: f64 = 0.0;

                // Must not
                for query in must_not {
                    if query.matches(doc) {
                        return None;
                    }
                }

                // Filter
                for filter in filter {
                    if !filter.matches(doc) {
                        return None;
                    }
                }

                // Must
                for query in must {
                    match query.rank(doc) {
                        Some(score) => {
                            total_score += score;
                        }
                        None => return None,
                    }
                }

                // Should
                let mut should_matched: i32 = 0;
                for query in should {
                    if let Some(score) = query.rank(doc) {
                        should_matched += 1;
                        total_score += score;
                    }
                }

                if should_matched < minimum_should_match {
                    return None;
                }

                // Return average score of matched queries
                Some(total_score / (must.len() + should.len()) as f64)
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
            Query::Or{ref queries, minimum_should_match} => {
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
            Query::BoostScore{ref query, boost} => {
                match query.rank(doc) {
                    Some(score) => Some(score * boost),
                    None => None
                }
            }
            Query::ConstantScore{ref query, score} => {
                if query.matches(doc) {
                    Some(score)
                } else {
                    None
                }
            }
        }
    }
}
