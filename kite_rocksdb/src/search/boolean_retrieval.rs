use std::rc::Rc;

use kite::schema::FieldRef;

use term_dictionary::TermRef;


#[derive(Debug, Clone)]
pub enum BooleanQueryOp {
    PushEmpty,
    PushFull,
    PushTermDirectory(FieldRef, TermRef),
    PushDeletionList,
    And,
    Or,
    AndNot,
}
