use std::collections::BTreeMap;

use token::Token;


#[derive(Debug)]
pub struct Document {
    pub key: String,
    pub fields: BTreeMap<String, Vec<Token>>,
}
