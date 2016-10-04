use term::Term;

#[derive(Debug, PartialEq)]
pub enum TermMatcher {
    Exact,
    Prefix,
}


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
