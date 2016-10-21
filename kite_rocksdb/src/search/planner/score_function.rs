use kite::schema::FieldRef;
use kite::Query;
use kite::query::term_scorer::TermScorer;

use RocksDBIndexReader;
use term_dictionary::TermRef;
use search::planner::SearchPlan;


#[derive(Debug, Clone)]
pub enum CombinatorScorer {
    Avg,
    Max,
}


#[derive(Debug, Clone)]
pub enum ScoreFunctionOp {
    Literal(f64),
    TermScorer(FieldRef, TermRef, TermScorer),
    CombinatorScorer(u32, CombinatorScorer),
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


pub fn plan_score_function(index_reader: &RocksDBIndexReader, mut plan: &mut SearchPlan, query: &Query) {
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
