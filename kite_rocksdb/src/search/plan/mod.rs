pub mod boolean_query_builder;

use search::boolean_retrieval::BooleanQueryOp;
use search::scoring::ScoreFunctionOp;


#[derive(Debug)]
pub struct SearchPlan {
    pub boolean_query: Vec<BooleanQueryOp>,
    pub boolean_query_is_negated: bool,
    pub score_function: Vec<ScoreFunctionOp>,
}


impl SearchPlan {
    pub fn new() -> SearchPlan {
        SearchPlan {
            boolean_query: Vec::new(),
            boolean_query_is_negated: false,
            score_function: Vec::new(),
        }
    }
}
