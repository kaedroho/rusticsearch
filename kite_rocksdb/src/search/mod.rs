pub mod doc_id_set;
pub mod boolean_retrieval;
pub mod scoring;
pub mod plan;

use kite::query::Query;
use kite::collectors::{Collector, DocumentMatch};
use byteorder::{ByteOrder, BigEndian};

use key_builder::KeyBuilder;
use document_index::DocRef;
use super::RocksDBIndexReader;
use search::doc_id_set::DocIdSet;
use search::boolean_retrieval::BooleanQueryOp;
use search::scoring::{CombinatorScorer, ScoreFunctionOp};
use search::plan::SearchPlan;
use search::plan::boolean_query_builder::BooleanQueryBuilder;


impl<'a> RocksDBIndexReader<'a> {
    fn plan_query_combinator(&self, mut plan: &mut SearchPlan, queries: &Vec<Query>, join_op: BooleanQueryOp, score: bool, scorer: CombinatorScorer) {
        match queries.len() {
            0 => {
                plan.boolean_query.push(BooleanQueryOp::PushEmpty);
                plan.score_function.push(ScoreFunctionOp::Literal(0.0f64));
            }
            1 =>  self.plan_query(&mut plan, &queries[0], score),
            _ => {
                let mut query_iter = queries.iter();
                self.plan_query(&mut plan, query_iter.next().unwrap(), score);

                for query in query_iter {
                    self.plan_query(&mut plan, query, score);
                    plan.boolean_query.push(join_op.clone());
                }
            }
        }

        plan.score_function.push(ScoreFunctionOp::CombinatorScorer(queries.len() as u32, scorer));
    }

    fn plan_query(&self, mut plan: &mut SearchPlan, query: &Query, score: bool) {
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
                let field_ref = match self.schema().get_field_by_name(field) {
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
                let term_ref = match self.store.term_dictionary.get(&term_bytes) {
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
                let field_ref = match self.schema().get_field_by_name(field) {
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
                for term_ref in self.store.term_dictionary.select(term_selector) {
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
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::And, score, CombinatorScorer::Avg);
            }
            Query::Disjunction{ref queries} => {
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::Or, score, CombinatorScorer::Avg);
            }
            Query::DisjunctionMax{ref queries} => {
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::Or, score, CombinatorScorer::Max);
            }
            Query::Filter{ref query, ref filter} => {
                self.plan_query(&mut plan, query, score);
                self.plan_query(&mut plan, filter, false);
                plan.boolean_query.push(BooleanQueryOp::And);
            }
            Query::Exclude{ref query, ref exclude} => {
                self.plan_query(&mut plan, query, score);
                self.plan_query(&mut plan, exclude, false);
                plan.boolean_query.push(BooleanQueryOp::AndNot);
            }
        }
    }

    fn search_chunk_boolean_phase(&self, plan: &SearchPlan, chunk: u32) -> DocIdSet {
        // Execute boolean query
        let mut stack = Vec::new();
        for op in plan.boolean_query.iter() {
            match *op {
                BooleanQueryOp::PushEmpty => {
                    stack.push(DocIdSet::new_filled(0));
                }
                BooleanQueryOp::PushFull => {
                    stack.push(DocIdSet::new_filled(65536));
                }
                BooleanQueryOp::PushTermDirectory(field_ref, term_ref) => {
                    let kb = KeyBuilder::chunk_dir_list(chunk, field_ref.ord(), term_ref.ord());
                    match self.snapshot.get(&kb.key()) {
                        Ok(Some(docid_set)) => {
                            let data = DocIdSet::FromRDB(docid_set);
                            stack.push(data);
                        }
                        Ok(None) => stack.push(DocIdSet::new_filled(0)),
                        Err(e) => {},  // FIXME
                    }
                }
                BooleanQueryOp::PushDeletionList => {
                    let kb = KeyBuilder::chunk_del_list(chunk);
                    match self.snapshot.get(&kb.key()) {
                        Ok(Some(deletion_list)) => {
                            let data = DocIdSet::FromRDB(deletion_list);
                            stack.push(data);
                        }
                        Ok(None) => stack.push(DocIdSet::new_filled(0)),
                        Err(e) => {},  // FIXME
                    }
                }
                BooleanQueryOp::And => {
                    let b = stack.pop().expect("stack underflow");
                    let a = stack.pop().expect("stack underflow");
                    stack.push(a.intersection(&b));
                }
                BooleanQueryOp::Or => {
                    let b = stack.pop().expect("stack underflow");
                    let a = stack.pop().expect("stack underflow");
                    stack.push(a.union(&b));
                }
                BooleanQueryOp::AndNot => {
                    let b = stack.pop().expect("stack underflow");
                    let a = stack.pop().expect("stack underflow");
                    stack.push(a.exclusion(&b));
                }
            }
        }

        if !stack.len() == 1 {
            // TODO: Error
        }
        let mut matches = stack.pop().unwrap();

        // Invert the list if the query is negated
        if plan.boolean_query_is_negated {
            let kb = KeyBuilder::chunk_stat(chunk, b"total_docs");
            let total_docs = match self.snapshot.get(&kb.key()) {
                Ok(Some(total_docs)) => BigEndian::read_i64(&total_docs) as u32,
                Ok(None) => 0,
                Err(e) => 0,  // FIXME
            };

            let all_docs = DocIdSet::new_filled(total_docs);
            matches = all_docs.exclusion(&matches);
        }

        matches
    }

    fn score_doc(&self, doc_id: u16, plan: &SearchPlan, chunk: u32) -> f64 {
        // Execute score function
        let mut stack = Vec::new();
        for op in plan.score_function.iter() {
            match *op {
                ScoreFunctionOp::Literal(val) => stack.push(val),
                ScoreFunctionOp::TermScorer(field_ref, term_ref, ref scorer) => {
                    let kb = KeyBuilder::chunk_dir_list(chunk, field_ref.ord(), term_ref.ord());
                    match self.snapshot.get(&kb.key()) {
                        Ok(Some(data)) => {
                            let docid_set = DocIdSet::FromRDB(data);

                            if docid_set.contains_doc(doc_id) {
                                stack.push(1.0f64);
                            } else {
                                stack.push(0.0f64);
                            }
                        }
                        Ok(None) => stack.push(0.0f64),
                        Err(e) => {},  // FIXME
                    }
                }
                ScoreFunctionOp::CombinatorScorer(num_vals, ref scorer) => {
                    let score = match *scorer {
                        CombinatorScorer::Avg => {
                            let mut total_score = 0.0f64;

                            for _ in 0..num_vals {
                                total_score += stack.pop().expect("stack underflow");
                            }

                            total_score / num_vals as f64
                        }
                        CombinatorScorer::Max => {
                            let mut max_score = 0.0f64;

                            for _ in 0..num_vals {
                                let score = stack.pop().expect("stack underflow");
                                if score > max_score {
                                    max_score = score
                                }
                            }

                            max_score
                        }
                    };

                    stack.push(score);
                }
            }
        }

        stack.pop().expect("stack underflow")
    }

    fn search_chunk<C: Collector>(&self, collector: &mut C, plan: &SearchPlan, chunk: u32) {
        let matches = self.search_chunk_boolean_phase(plan, chunk);

        // Score documents and pass to collector
        for doc in matches.iter() {
            let score = self.score_doc(doc, plan, chunk);

            let doc_ref = DocRef::from_chunk_ord(chunk, doc);
            let doc_match = DocumentMatch::new_scored(doc_ref.as_u64(), score);
            collector.collect(doc_match);
        }
    }

    pub fn search<C: Collector>(&self, collector: &mut C, query: &Query) {
        let mut plan = SearchPlan::new();
        self.plan_query(&mut plan, query, true);

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

        // Run query on each chunk
        for chunk in self.store.chunks.iter_active(&self.snapshot) {
            self.search_chunk(collector, &plan, chunk);
        }
    }
}
