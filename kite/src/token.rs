use term::Term;

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub term: Term,
    pub position: u32,
}
