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


struct SearchPlan {
    boolean_query: Vec<BooleanQueryOp>,
}


impl<'a> RocksDBIndexReader<'a> {
    fn plan_boolean_query_combinator(&self, mut boolean_query: &mut Vec<BooleanQueryOp>, queries: &Vec<Query>, join_op: BooleanQueryOp) {
        match queries.len() {
            0 => boolean_query.push(BooleanQueryOp::Zero),
            1 => {
                self.plan_boolean_query(&mut boolean_query, &queries[0]);
            }
            _ => {
                // TODO: organise queries into a binary tree structure

                let mut query_iter = queries.iter();
                self.plan_boolean_query(&mut boolean_query, query_iter.next().unwrap());

                for query in query_iter {
                    self.plan_boolean_query(&mut boolean_query, query);
                    boolean_query.push(join_op.clone());
                }
            }
        }
    }

    fn plan_boolean_query(&self, mut boolean_query: &mut Vec<BooleanQueryOp>, query: &Query) {
        match *query {
            Query::MatchAll{ref score} => {
                boolean_query.push(BooleanQueryOp::One);
            },
            Query::MatchNone => {
                boolean_query.push(BooleanQueryOp::Zero);
            },
            Query::MatchTerm{ref field, ref term, ref matcher, ref scorer} => {
                // Get term
                let term_bytes = term.to_bytes();
                let term_ref = match self.store.term_dictionary.read().unwrap().get(&term_bytes) {
                    Some(term_ref) => *term_ref,
                    None => {
                        // Term doesn't exist, so will never match
                        boolean_query.push(BooleanQueryOp::Zero);
                        return
                    }
                };

                // Get field
                let field_ref = match self.schema().get_field_by_name(field) {
                    Some(field_ref) => field_ref,
                    None => {
                        // Field doesn't exist, so will never match
                        boolean_query.push(BooleanQueryOp::Zero);
                        return
                    }
                };

                boolean_query.push(BooleanQueryOp::Load(field_ref, term_ref));
            }
            Query::Conjunction{ref queries} => {
                self.plan_boolean_query_combinator(&mut boolean_query, queries, BooleanQueryOp::And);
            }
            Query::Disjunction{ref queries} => {
                self.plan_boolean_query_combinator(&mut boolean_query, queries, BooleanQueryOp::Or);
            }
            Query::NDisjunction{ref queries, minimum_should_match} => {
                self.plan_boolean_query_combinator(&mut boolean_query, queries, BooleanQueryOp::Or);
            }
            Query::DisjunctionMax{ref queries} => {
                self.plan_boolean_query_combinator(&mut boolean_query, queries, BooleanQueryOp::Or);
            }
            Query::Filter{ref query, ref filter} => {
                self.plan_boolean_query(&mut boolean_query, query);
                self.plan_boolean_query(&mut boolean_query, filter);
                boolean_query.push(BooleanQueryOp::And);
            }
            Query::Exclude{ref query, ref exclude} => {
                self.plan_boolean_query(&mut boolean_query, query);
                self.plan_boolean_query(&mut boolean_query, exclude);
                boolean_query.push(BooleanQueryOp::AndNot);
            }
        }
    }

    pub fn search<C: Collector>(&self, collector: &mut C, query: &Query) {
        let mut boolean_query = Vec::new();
        self.plan_boolean_query(&mut boolean_query, query);
        println!("BQ {:?}", boolean_query);

        let plan = SearchPlan {
            boolean_query: boolean_query,
        };
    }
}