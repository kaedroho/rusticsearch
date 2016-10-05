use term::Term;


#[derive(Debug, PartialEq)]
pub enum TermSelector {
    Prefix(String),
}


impl TermSelector {
    pub fn matches(&self, term: &Term) -> bool {
        match *self {
            TermSelector::Prefix(ref prefix) => {
                if let Term::String(ref term) = *term {
                    return term.starts_with(prefix);
                }

                false
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
