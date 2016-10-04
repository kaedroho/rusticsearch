use std::collections::HashMap;

use token::Token;


#[derive(Debug)]
pub struct Document {
    pub key: String,
    pub fields: HashMap<String, Vec<Token>>,
}
