use search::term::Term;

#[derive(Debug, PartialEq)]
pub enum MultiTermSelector {
    Prefix(String),
}

impl MultiTermSelector {
    pub fn matches(&self, term: &Term) -> bool {
        match *self {
            MultiTermSelector::Prefix(ref prefix) => {
                return term.as_bytes().starts_with(prefix.as_bytes());
            }
        }
    }
}
