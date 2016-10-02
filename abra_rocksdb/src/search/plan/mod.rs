pub mod boolean_query_builder;

use search::boolean_retrieval::BooleanQueryOp;
use search::scoring::ScoreFunctionOp;


#[derive(Debug)]
pub struct SearchPlan {
    pub boolean_query: Vec<BooleanQueryOp>,
    pub boolean_query_is_negated: bool,
    pub score_function: Vec<ScoreFunctionOp>,
    current_tag: u8,
}


impl SearchPlan {
    pub fn new() -> SearchPlan {
        SearchPlan {
            boolean_query: Vec::new(),
            boolean_query_is_negated: false,
            score_function: Vec::new(),
            current_tag: 0,
        }
    }

    pub fn allocate_tag(&mut self) -> Option<u8> {
        if self.current_tag == 255 {
            None
        } else {
            self.current_tag += 1;
            Some(self.current_tag)
        }
    }
}
