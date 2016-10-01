use abra::schema::FieldRef;
use abra::query::term_scorer::TermScorer;

use TermRef;


#[derive(Debug, Clone)]
pub enum CompoundScorer {
    Avg,
    Max,
}


#[derive(Debug, Clone)]
pub enum ScoreFunctionOp {
    Literal(f64),
    TermScore(FieldRef, TermRef, TermScorer, u8),
    CompoundScorer(u32, CompoundScorer),
}
