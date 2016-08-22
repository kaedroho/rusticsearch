use std::collections::HashMap;

use token::Token;
use schema::FieldRef;


#[derive(Debug)]
pub struct Document {
    pub key: String,
    pub fields: HashMap<FieldRef, Vec<Token>>,
}
