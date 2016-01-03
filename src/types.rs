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
