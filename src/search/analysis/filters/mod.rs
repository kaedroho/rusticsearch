pub mod lowercase;

use search::token::Token;
use search::analysis::filters::lowercase::LowercaseFilter;


#[derive(Debug)]
pub enum FilterSpec {
    Lowercase,
}


impl FilterSpec {
    pub fn initialise<'a>(&self, input: Box<Iterator<Item=Token> + 'a>) -> Box<Iterator<Item=Token> + 'a> {
        match *self {
            FilterSpec::Lowercase => {
                Box::new(LowercaseFilter::new(input))
            }
        }
    }
}
