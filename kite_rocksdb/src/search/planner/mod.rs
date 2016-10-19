pub mod boolean_query;

use kite::Query;

use RocksDBIndexReader;
use search::boolean_retrieval::BooleanQueryOp;
use search::scoring::{CombinatorScorer, ScoreFunctionOp};


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


fn plan_query_combinator(index_reader: &RocksDBIndexReader, mut plan: &mut SearchPlan, queries: &Vec<Query>, join_op: BooleanQueryOp, score: bool, scorer: CombinatorScorer) {
    match queries.len() {
        0 => {
            plan.boolean_query.push(BooleanQueryOp::PushEmpty);
            plan.score_function.push(ScoreFunctionOp::Literal(0.0f64));
        }
        1 =>  plan_query(index_reader, &mut plan, &queries[0], score),
        _ => {
            let mut query_iter = queries.iter();
            plan_query(index_reader, &mut plan, query_iter.next().unwrap(), score);

            for query in query_iter {
                plan_query(index_reader, &mut plan, query, score);
                plan.boolean_query.push(join_op.clone());
            }
        }
    }

    plan.score_function.push(ScoreFunctionOp::CombinatorScorer(queries.len() as u32, scorer));
}


pub fn plan_query(index_reader: &RocksDBIndexReader,mut plan: &mut SearchPlan, query: &Query, score: bool) {
    match *query {
        Query::MatchAll{ref score} => {
            plan.boolean_query.push(BooleanQueryOp::PushFull);
            plan.score_function.push(ScoreFunctionOp::Literal(*score));
        }
        Query::MatchNone => {
            plan.boolean_query.push(BooleanQueryOp::PushEmpty);
            plan.score_function.push(ScoreFunctionOp::Literal(0.0f64));
        }
        Query::MatchTerm{ref field, ref term, ref scorer} => {
            // Get field
            let field_ref = match index_reader.schema().get_field_by_name(field) {
                Some(field_ref) => field_ref,
                None => {
                    // Field doesn't exist, so will never match
                    plan.boolean_query.push(BooleanQueryOp::PushEmpty);
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
                    plan.boolean_query.push(BooleanQueryOp::PushEmpty);
                    plan.score_function.push(ScoreFunctionOp::Literal(0.0f64));
                    return
                }
            };

            plan.boolean_query.push(BooleanQueryOp::PushTermDirectory(field_ref, term_ref));
            plan.score_function.push(ScoreFunctionOp::TermScorer(field_ref, term_ref, scorer.clone()));
        }
        Query::MatchMultiTerm{ref field, ref term_selector, ref scorer} => {
            // Get field
            let field_ref = match index_reader.schema().get_field_by_name(field) {
                Some(field_ref) => field_ref,
                None => {
                    // Field doesn't exist, so will never match
                    plan.boolean_query.push(BooleanQueryOp::PushEmpty);
                    plan.score_function.push(ScoreFunctionOp::Literal(0.0f64));
                    return
                }
            };

            // Get terms
            plan.boolean_query.push(BooleanQueryOp::PushEmpty);
            let mut total_terms = 0;
            for term_ref in index_reader.store.term_dictionary.select(term_selector) {
                plan.boolean_query.push(BooleanQueryOp::PushTermDirectory(field_ref, term_ref));
                plan.boolean_query.push(BooleanQueryOp::Or);
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
            plan_query_combinator(index_reader, &mut plan, queries, BooleanQueryOp::And, score, CombinatorScorer::Avg);
        }
        Query::Disjunction{ref queries} => {
            plan_query_combinator(index_reader, &mut plan, queries, BooleanQueryOp::Or, score, CombinatorScorer::Avg);
        }
        Query::DisjunctionMax{ref queries} => {
            plan_query_combinator(index_reader, &mut plan, queries, BooleanQueryOp::Or, score, CombinatorScorer::Max);
        }
        Query::Filter{ref query, ref filter} => {
            plan_query(index_reader, &mut plan, query, score);
            plan_query(index_reader, &mut plan, filter, false);
            plan.boolean_query.push(BooleanQueryOp::And);
        }
        Query::Exclude{ref query, ref exclude} => {
            plan_query(index_reader, &mut plan, query, score);
            plan_query(index_reader, &mut plan, exclude, false);
            plan.boolean_query.push(BooleanQueryOp::AndNot);
        }
    }
}
