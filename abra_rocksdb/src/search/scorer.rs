use abra::schema::FieldRef;
use abra::query::term_scorer::TermScorer;

use TermRef;


#[derive(Debug, Clone)]
pub enum CombinatorScorer {
    Avg,
    Max,
}


#[derive(Debug, Clone)]
pub enum ScoreFunctionOp {
    Literal(f64),
    TermScorer(FieldRef, TermRef, TermScorer, u8),
    CombinatorScorer(u32, CombinatorScorer),
}
