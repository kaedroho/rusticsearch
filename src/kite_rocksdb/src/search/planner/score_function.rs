use kite::schema::FieldRef;
use kite::term::TermRef;
use kite::Query;
use kite::query::term_scorer::TermScorer;

use RocksDBIndexReader;


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


fn plan_score_function_combinator(index_reader: &RocksDBIndexReader, mut score_function: &mut Vec<ScoreFunctionOp>, queries: &Vec<Query>, scorer: CombinatorScorer) {
    match queries.len() {
        0 => {
            score_function.push(ScoreFunctionOp::Literal(0.0f64));
        }
        1 =>  plan_score_function(index_reader, &mut score_function, &queries[0]),
        _ => {
            let mut query_iter = queries.iter();
            plan_score_function(index_reader, &mut score_function, query_iter.next().unwrap());

            for query in query_iter {
                plan_score_function(index_reader, &mut score_function, query);
            }
        }
    }

    score_function.push(ScoreFunctionOp::CombinatorScorer(queries.len() as u32, scorer));
}


pub fn plan_score_function(index_reader: &RocksDBIndexReader, mut score_function: &mut Vec<ScoreFunctionOp>, query: &Query) {
    match *query {
        Query::All{ref score} => {
            score_function.push(ScoreFunctionOp::Literal(*score));
        }
        Query::None => {
            score_function.push(ScoreFunctionOp::Literal(0.0f64));
        }
        Query::Term{field, ref term, ref scorer} => {
            // Get term
            let term_bytes = term.as_bytes().to_vec();
            let term_ref = match index_reader.store.term_dictionary.get(&term_bytes) {
                Some(term_ref) => term_ref,
                None => {
                    // Term doesn't exist, so will never match
                    score_function.push(ScoreFunctionOp::Literal(0.0f64));
                    return
                }
            };

            score_function.push(ScoreFunctionOp::TermScorer(field, term_ref, scorer.clone()));
        }
        Query::MultiTerm{field, ref term_selector, ref scorer} => {
            // Get terms
            let mut total_terms = 0;
            for term_ref in index_reader.store.term_dictionary.select(term_selector) {
                score_function.push(ScoreFunctionOp::TermScorer(field, term_ref, scorer.clone()));
                total_terms += 1;
            }

            // This query must push only one score value onto the stack.
            // If we haven't pushed any score operations, Push a literal 0.0
            // If we have pushed more than one score operations, which will lead to more
            // than one score value being pushed to the stack, combine the score values
            // with a combinator operation.
            match total_terms {
                0 => score_function.push(ScoreFunctionOp::Literal(0.0f64)),
                1 => {},
                _ => score_function.push(ScoreFunctionOp::CombinatorScorer(total_terms, CombinatorScorer::Avg)),
            }
        }
        Query::Conjunction{ref queries} => {
            plan_score_function_combinator(index_reader, &mut score_function, queries, CombinatorScorer::Avg);
        }
        Query::Disjunction{ref queries} => {
            plan_score_function_combinator(index_reader, &mut score_function, queries, CombinatorScorer::Avg);
        }
        Query::DisjunctionMax{ref queries} => {
            plan_score_function_combinator(index_reader, &mut score_function, queries, CombinatorScorer::Max);
        }
        Query::Filter{ref query, ..} => {
            plan_score_function(index_reader, &mut score_function, query);
        }
        Query::Exclude{ref query, ..} => {
            plan_score_function(index_reader, &mut score_function, query);
        }
    }
}
