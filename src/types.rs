#[derive(Debug)]
pub enum ESType {
    String,
    Binary,
    Number{bits: u8, is_float: bool},
    Boolean,
    Date,
}


impl Default for ESType {
    fn default() -> ESType { ESType::String }
}


#[derive(Debug)]
pub enum ESValue {
    None,
    String(String),
    Binary(Vec<u8>),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    // Date
}
