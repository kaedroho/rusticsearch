use term::Term;


#[derive(Debug, PartialEq)]
pub enum TermSelector {
    Prefix(String),
}


impl TermSelector {
    pub fn matches(&self, term: &Term) -> bool {
        match *self {
            TermSelector::Prefix(ref prefix) => {
                return term.as_bytes().starts_with(prefix.as_bytes());
            }
        }
    }
}
