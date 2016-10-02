use std::rc::Rc;

use abra::schema::FieldRef;

use term_dictionary::TermRef;


#[derive(Debug, Clone)]
pub enum BooleanQueryOp {
    PushEmpty,
    PushFull,
    PushTermDirectory(FieldRef, TermRef, u8),
    PushDeletionList,
    And,
    Or,
    AndNot,
}
