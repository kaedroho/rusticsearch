use abra::schema::{FieldRef, SchemaRead};
use abra::query::Query;
use abra::collectors::Collector;

use super::{RocksDBIndexReader, TermRef};


#[derive(Debug, Clone)]
enum BooleanQueryOp {
    Zero,
    One,
    Load(FieldRef, TermRef),
    And,
    Or,
    AndNot,
}


#[derive(Debug)]
struct SearchPlan {
    boolean_query: Vec<BooleanQueryOp>,
}


impl SearchPlan {
    fn new() -> SearchPlan {
        SearchPlan {
            boolean_query: Vec::new(),
        }
    }
}


impl<'a> RocksDBIndexReader<'a> {
    fn plan_query_combinator(&self, mut plan: &mut SearchPlan, queries: &Vec<Query>, join_op: BooleanQueryOp) {
        match queries.len() {
            0 => plan.boolean_query.push(BooleanQueryOp::Zero),
            1 => {
                self.plan_query(&mut plan, &queries[0]);
            }
            _ => {
                // TODO: organise queries into a binary tree structure

                let mut query_iter = queries.iter();
                self.plan_query(&mut plan, query_iter.next().unwrap());

                for query in query_iter {
                    self.plan_query(&mut plan, query);
                    plan.boolean_query.push(join_op.clone());
                }
            }
        }
    }

    fn plan_query(&self, mut plan: &mut SearchPlan, query: &Query) {
        match *query {
            Query::MatchAll{ref score} => {
                plan.boolean_query.push(BooleanQueryOp::One);
            }
            Query::MatchNone => {
                plan.boolean_query.push(BooleanQueryOp::Zero);
            }
            Query::MatchTerm{ref field, ref term, ref matcher, ref scorer} => {
                // Get term
                let term_bytes = term.to_bytes();
                let term_ref = match self.store.term_dictionary.read().unwrap().get(&term_bytes) {
                    Some(term_ref) => *term_ref,
                    None => {
                        // Term doesn't exist, so will never match
                        plan.boolean_query.push(BooleanQueryOp::Zero);
                        return
                    }
                };

                // Get field
                let field_ref = match self.schema().get_field_by_name(field) {
                    Some(field_ref) => field_ref,
                    None => {
                        // Field doesn't exist, so will never match
                        plan.boolean_query.push(BooleanQueryOp::Zero);
                        return
                    }
                };

                plan.boolean_query.push(BooleanQueryOp::Load(field_ref, term_ref));
            }
            Query::Conjunction{ref queries} => {
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::And);
            }
            Query::Disjunction{ref queries} => {
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::Or);
            }
            Query::NDisjunction{ref queries, minimum_should_match} => {
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::Or);
            }
            Query::DisjunctionMax{ref queries} => {
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::Or);
            }
            Query::Filter{ref query, ref filter} => {
                self.plan_query(&mut plan, query);
                self.plan_query(&mut plan, filter);
                plan.boolean_query.push(BooleanQueryOp::And);
            }
            Query::Exclude{ref query, ref exclude} => {
                self.plan_query(&mut plan, query);
                self.plan_query(&mut plan, exclude);
                plan.boolean_query.push(BooleanQueryOp::AndNot);
            }
        }
    }

    pub fn search<C: Collector>(&self, collector: &mut C, query: &Query) {
        let mut plan = SearchPlan::new();
        self.plan_query(&mut plan, query);
        println!("Plan {:?}", plan);
    }
}