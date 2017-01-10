use term::Term;


#[derive(Debug, PartialEq)]
pub enum TermSelector {
    Prefix(String),
}


impl TermSelector {
    pub fn matches(&self, term: &Term) -> bool {
        match *self {
            TermSelector::Prefix(ref prefix) => {
                return term.to_bytes().starts_with(prefix.as_bytes());
            }
        }
    }

    pub fn matches_bytes(&self, term: &Vec<u8>) -> bool {
        match *self {
            TermSelector::Prefix(ref prefix) => {
                term.starts_with(prefix.as_bytes())
            }
        }
    }
}
