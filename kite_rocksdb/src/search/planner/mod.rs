pub mod boolean_query;

use kite::Query;

use RocksDBIndexReader;
use search::boolean_retrieval::BooleanQueryOp;
use search::scoring::{CombinatorScorer, ScoreFunctionOp};
use search::planner::boolean_query::BooleanQueryBuilder;


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


fn plan_boolean_query_combinator(index_reader: &RocksDBIndexReader, mut plan: &mut SearchPlan, queries: &Vec<Query>, join_op: BooleanQueryOp) {
    match queries.len() {
        0 => {
            plan.boolean_query.push(BooleanQueryOp::PushEmpty);
        }
        1 =>  plan_boolean_query(index_reader, &mut plan, &queries[0]),
        _ => {
            let mut query_iter = queries.iter();
            plan_boolean_query(index_reader, &mut plan, query_iter.next().unwrap());

            for query in query_iter {
                plan_boolean_query(index_reader, &mut plan, query);
                plan.boolean_query.push(join_op.clone());
            }
        }
    }
}


fn plan_boolean_query(index_reader: &RocksDBIndexReader, mut plan: &mut SearchPlan, query: &Query) {
    match *query {
        Query::MatchAll{ref score} => {
            plan.boolean_query.push(BooleanQueryOp::PushFull);
        }
        Query::MatchNone => {
            plan.boolean_query.push(BooleanQueryOp::PushEmpty);
        }
        Query::MatchTerm{ref field, ref term, ref scorer} => {
            // Get field
            let field_ref = match index_reader.schema().get_field_by_name(field) {
                Some(field_ref) => field_ref,
                None => {
                    // Field doesn't exist, so will never match
                    plan.boolean_query.push(BooleanQueryOp::PushEmpty);
                    return
                }
            };

            // Get term
            let term_bytes = term.to_bytes();
            let term_ref = match index_reader.store.term_dictionary.get(&term_bytes) {
                Some(term_ref) => term_ref,
                None => {
                    // Term doesn't exist, so will never match
                    plan.boolean_query.push(BooleanQueryOp::PushEmpty);
                    return
                }
            };

            plan.boolean_query.push(BooleanQueryOp::PushTermDirectory(field_ref, term_ref));
        }
        Query::MatchMultiTerm{ref field, ref term_selector, ref scorer} => {
            // Get field
            let field_ref = match index_reader.schema().get_field_by_name(field) {
                Some(field_ref) => field_ref,
                None => {
                    // Field doesn't exist, so will never match
                    plan.boolean_query.push(BooleanQueryOp::PushEmpty);
                    return
                }
            };

            // Get terms
            plan.boolean_query.push(BooleanQueryOp::PushEmpty);
            for term_ref in index_reader.store.term_dictionary.select(term_selector) {
                plan.boolean_query.push(BooleanQueryOp::PushTermDirectory(field_ref, term_ref));
                plan.boolean_query.push(BooleanQueryOp::Or);
            }
        }
        Query::Conjunction{ref queries} => {
            plan_boolean_query_combinator(index_reader, &mut plan, queries, BooleanQueryOp::And);
        }
        Query::Disjunction{ref queries} => {
            plan_boolean_query_combinator(index_reader, &mut plan, queries, BooleanQueryOp::Or);
        }
        Query::DisjunctionMax{ref queries} => {
            plan_boolean_query_combinator(index_reader, &mut plan, queries, BooleanQueryOp::Or);
        }
        Query::Filter{ref query, ref filter} => {
            plan_boolean_query(index_reader, &mut plan, query);
            plan_boolean_query(index_reader, &mut plan, filter);
            plan.boolean_query.push(BooleanQueryOp::And);
        }
        Query::Exclude{ref query, ref exclude} => {
            plan_boolean_query(index_reader, &mut plan, query);
            plan_boolean_query(index_reader, &mut plan, exclude);
            plan.boolean_query.push(BooleanQueryOp::AndNot);
        }
    }
}


fn plan_score_function_combinator(index_reader: &RocksDBIndexReader, mut plan: &mut SearchPlan, queries: &Vec<Query>, scorer: CombinatorScorer) {
    match queries.len() {
        0 => {
            plan.score_function.push(ScoreFunctionOp::Literal(0.0f64));
        }
        1 =>  plan_score_function(index_reader, &mut plan, &queries[0]),
        _ => {
            let mut query_iter = queries.iter();
            plan_score_function(index_reader, &mut plan, query_iter.next().unwrap());

            for query in query_iter {
                plan_score_function(index_reader, &mut plan, query);
            }
        }
    }

    plan.score_function.push(ScoreFunctionOp::CombinatorScorer(queries.len() as u32, scorer));
}


fn plan_score_function(index_reader: &RocksDBIndexReader, mut plan: &mut SearchPlan, query: &Query) {
    match *query {
        Query::MatchAll{ref score} => {
            plan.score_function.push(ScoreFunctionOp::Literal(*score));
        }
        Query::MatchNone => {
            plan.score_function.push(ScoreFunctionOp::Literal(0.0f64));
        }
        Query::MatchTerm{ref field, ref term, ref scorer} => {
            // Get field
            let field_ref = match index_reader.schema().get_field_by_name(field) {
                Some(field_ref) => field_ref,
                None => {
                    // Field doesn't exist, so will never match
                    plan.score_function.push(ScoreFunctionOp::Literal(0.0f64));
                    return
                }
            };

            // Get term
            let term_bytes = term.to_bytes();
            let term_ref = match index_reader.store.term_dictionary.get(&term_bytes) {
                Some(term_ref) => term_ref,
                None => {
                    // Term doesn't exist, so will never match
                    plan.score_function.push(ScoreFunctionOp::Literal(0.0f64));
                    return
                }
            };

            plan.score_function.push(ScoreFunctionOp::TermScorer(field_ref, term_ref, scorer.clone()));
        }
        Query::MatchMultiTerm{ref field, ref term_selector, ref scorer} => {
            // Get field
            let field_ref = match index_reader.schema().get_field_by_name(field) {
                Some(field_ref) => field_ref,
                None => {
                    // Field doesn't exist, so will never match
                    plan.score_function.push(ScoreFunctionOp::Literal(0.0f64));
                    return
                }
            };

            // Get terms
            let mut total_terms = 0;
            for term_ref in index_reader.store.term_dictionary.select(term_selector) {
                plan.score_function.push(ScoreFunctionOp::TermScorer(field_ref, term_ref, scorer.clone()));
                total_terms += 1;
            }

            // This query must push only one score value onto the stack.
            // If we haven't pushed any score operations, Push a literal 0.0
            // If we have pushed more than one score operations, which will lead to more
            // than one score value being pushed to the stack, combine the score values
            // with a combinator operation.
            match total_terms {
                0 => plan.score_function.push(ScoreFunctionOp::Literal(0.0f64)),
                1 => {},
                _ => plan.score_function.push(ScoreFunctionOp::CombinatorScorer(total_terms, CombinatorScorer::Avg)),
            }
        }
        Query::Conjunction{ref queries} => {
            plan_score_function_combinator(index_reader, &mut plan, queries, CombinatorScorer::Avg);
        }
        Query::Disjunction{ref queries} => {
            plan_score_function_combinator(index_reader, &mut plan, queries, CombinatorScorer::Avg);
        }
        Query::DisjunctionMax{ref queries} => {
            plan_score_function_combinator(index_reader, &mut plan, queries, CombinatorScorer::Max);
        }
        Query::Filter{ref query, ref filter} => {
            plan_score_function(index_reader, &mut plan, query);
        }
        Query::Exclude{ref query, ref exclude} => {
            plan_score_function(index_reader, &mut plan, query);
        }
    }
}


pub fn plan_query(index_reader: &RocksDBIndexReader, mut plan: &mut SearchPlan, query: &Query, score: bool) {
    // Plan boolean query
    plan_boolean_query(index_reader, plan, query);

    // Add operations to exclude deleted documents to boolean query
    plan.boolean_query.push(BooleanQueryOp::PushDeletionList);
    plan.boolean_query.push(BooleanQueryOp::AndNot);

    // Optimise boolean query
    let mut optimiser = BooleanQueryBuilder::new();
    for op in plan.boolean_query.iter() {
        optimiser.push_op(op);
    }
    let (boolean_query, boolean_query_is_negated) = optimiser.build();
    plan.boolean_query = boolean_query;
    plan.boolean_query_is_negated = boolean_query_is_negated;

    // Plan score function
    if score {
        plan_score_function(index_reader, plan, query);
    } else {
        plan.score_function.push(ScoreFunctionOp::Literal(0.0f64));
    }
}