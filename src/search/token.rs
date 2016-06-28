use search::term::Term;

#[derive(Debug, Clone)]
pub struct Token {
    pub term: Term,
    pub position: u32,
}
