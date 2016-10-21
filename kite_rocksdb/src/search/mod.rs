pub mod doc_id_set;
pub mod statistics;
pub mod planner;

use kite::query::Query;
use kite::collectors::{Collector, DocumentMatch};
use byteorder::{ByteOrder, BigEndian};

use key_builder::KeyBuilder;
use document_index::DocRef;
use super::RocksDBIndexReader;
use search::doc_id_set::DocIdSet;
use search::statistics::StatisticsReader;
use search::planner::{SearchPlan, plan_query};
use search::planner::boolean_query::BooleanQueryOp;
use search::planner::score_function::{CombinatorScorer, ScoreFunctionOp};



impl<'a> RocksDBIndexReader<'a> {
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

    fn score_doc(&self, doc_id: u16, plan: &SearchPlan, chunk: u32, mut stats: &mut StatisticsReader) -> f64 {
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
                                // Read field length
                                // TODO: we only need this for BM25
                                let kb = KeyBuilder::stored_field_value(chunk, doc_id, field_ref.ord(), b"len");
                                let field_length = match self.snapshot.get(&kb.key()) {
                                    Ok(Some(value)) => {
                                        let length_sqrt = (value[0] as f64) / 3.0 + 1.0;
                                        length_sqrt * length_sqrt
                                    }
                                    Ok(None) => 1.0,
                                    Err(e) => 1.0,  // TODO Error
                                };

                                // Read term frequency
                                let mut value_type = vec![b't', b'f'];
                                value_type.extend(term_ref.ord().to_string().as_bytes());
                                let kb = KeyBuilder::stored_field_value(chunk, doc_id, field_ref.ord(), &value_type);
                                let term_frequency = match self.snapshot.get(&kb.key()) {
                                    Ok(Some(value)) => BigEndian::read_i64(&value),
                                    Ok(None) => 1,
                                    Err(e) => 1,  // TODO Error
                                };

                                let score = scorer.similarity_model.score(1, field_length, stats.total_tokens(field_ref) as u64, stats.total_docs(field_ref) as u64, stats.term_document_frequency(field_ref, term_ref) as u64);
                                stack.push(score * scorer.boost);
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

    fn search_chunk<C: Collector>(&self, collector: &mut C, plan: &SearchPlan, chunk: u32, mut stats: &mut StatisticsReader) {
        let matches = self.search_chunk_boolean_phase(plan, chunk);

        // Score documents and pass to collector
        for doc in matches.iter() {
            let score = self.score_doc(doc, plan, chunk, &mut stats);

            let doc_ref = DocRef::from_chunk_ord(chunk, doc);
            let doc_match = DocumentMatch::new_scored(doc_ref.as_u64(), score);
            collector.collect(doc_match);
        }
    }

    pub fn search<C: Collector>(&self, collector: &mut C, query: &Query) {
        // Plan query
        let plan = plan_query(&self, query, collector.needs_score());

        // Initialise statistics reader
        let mut stats = StatisticsReader::new(&self);

        // Run query on each chunk
        for chunk in self.store.chunks.iter_active(&self.snapshot) {
            self.search_chunk(collector, &plan, chunk, &mut stats);
        }
    }
}
