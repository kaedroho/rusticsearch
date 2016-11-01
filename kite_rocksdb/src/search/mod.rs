mod statistics;
mod planner;

use kite::query::Query;
use kite::collectors::{Collector, DocumentMatch};
use byteorder::{ByteOrder, BigEndian};
use rocksdb;

use key_builder::KeyBuilder;
use document_index::DocRef;
use super::RocksDBIndexReader;
use doc_id_set::DocIdSet;
use search::statistics::StatisticsReader;
use search::planner::{SearchPlan, plan_query};
use search::planner::boolean_query::BooleanQueryOp;
use search::planner::score_function::{CombinatorScorer, ScoreFunctionOp};



impl<'a> RocksDBIndexReader<'a> {
    fn search_segment_boolean_phase(&self, boolean_query: &Vec<BooleanQueryOp>, is_negated: bool, segment: u32) -> Result<DocIdSet, rocksdb::Error> {
        // Execute boolean query
        let mut stack = Vec::new();
        for op in boolean_query.iter() {
            match *op {
                BooleanQueryOp::PushEmpty => {
                    stack.push(DocIdSet::new_filled(0));
                }
                BooleanQueryOp::PushFull => {
                    stack.push(DocIdSet::new_filled(65536));
                }
                BooleanQueryOp::PushTermDirectory(field_ref, term_ref) => {
                    let kb = KeyBuilder::segment_dir_list(segment, field_ref.ord(), term_ref.ord());
                    match try!(self.snapshot.get(&kb.key())) {
                        Some(docid_set) => {
                            let data = DocIdSet::FromRDB(docid_set);
                            stack.push(data);
                        }
                        None => stack.push(DocIdSet::new_filled(0)),
                    }
                }
                BooleanQueryOp::PushDeletionList => {
                    let kb = KeyBuilder::segment_del_list(segment);
                    match try!(self.snapshot.get(&kb.key())) {
                        Some(deletion_list) => {
                            let data = DocIdSet::FromRDB(deletion_list);
                            stack.push(data);
                        }
                        None => stack.push(DocIdSet::new_filled(0)),
                    }
                }
                BooleanQueryOp::And => {
                    let b = stack.pop().expect("boolean query executor: stack underflow");
                    let a = stack.pop().expect("boolean query executor: stack underflow");
                    stack.push(a.intersection(&b));
                }
                BooleanQueryOp::Or => {
                    let b = stack.pop().expect("boolean query executor: stack underflow");
                    let a = stack.pop().expect("boolean query executor: stack underflow");
                    stack.push(a.union(&b));
                }
                BooleanQueryOp::AndNot => {
                    let b = stack.pop().expect("boolean query executor: stack underflow");
                    let a = stack.pop().expect("boolean query executor: stack underflow");
                    stack.push(a.exclusion(&b));
                }
            }
        }

        if !stack.len() == 1 {
            // This shouldn't be possible unless there's a bug in the planner
            panic!("boolean query executor: stack size too big ({})", stack.len());
        }
        let mut matches = stack.pop().unwrap();

        // Invert the list if the query is negated
        if is_negated {
            let kb = KeyBuilder::segment_stat(segment, b"total_docs");
            let total_docs = match try!(self.snapshot.get(&kb.key())) {
                Some(total_docs) => BigEndian::read_i64(&total_docs) as u32,
                None => 0,
            };

            let all_docs = DocIdSet::new_filled(total_docs);
            matches = all_docs.exclusion(&matches);
        }

        Ok(matches)
    }

    fn score_doc(&self, doc_id: u16, score_function: &Vec<ScoreFunctionOp>, segment: u32, mut stats: &mut StatisticsReader) -> Result<f64, rocksdb::Error> {
        // Execute score function
        let mut stack = Vec::new();
        for op in score_function.iter() {
            match *op {
                ScoreFunctionOp::Literal(val) => stack.push(val),
                ScoreFunctionOp::TermScorer(field_ref, term_ref, ref scorer) => {
                    let kb = KeyBuilder::segment_dir_list(segment, field_ref.ord(), term_ref.ord());
                    match try!(self.snapshot.get(&kb.key())) {
                        Some(data) => {
                            let docid_set = DocIdSet::FromRDB(data);

                            if docid_set.contains_doc(doc_id) {
                                // Read field length
                                // TODO: we only need this for BM25
                                let kb = KeyBuilder::stored_field_value(segment, doc_id, field_ref.ord(), b"len");
                                let field_length = match try!(self.snapshot.get(&kb.key())) {
                                    Some(value) => {
                                        let length_sqrt = (value[0] as f64) / 3.0 + 1.0;
                                        length_sqrt * length_sqrt
                                    }
                                    None => 1.0,
                                };

                                // Read term frequency
                                let mut value_type = vec![b't', b'f'];
                                value_type.extend(term_ref.ord().to_string().as_bytes());
                                let kb = KeyBuilder::stored_field_value(segment, doc_id, field_ref.ord(), &value_type);
                                let term_frequency = match try!(self.snapshot.get(&kb.key())) {
                                    Some(value) => BigEndian::read_i64(&value),
                                    None => 1,
                                };

                                let score = scorer.similarity_model.score(term_frequency as u32, field_length, try!(stats.total_tokens(field_ref)) as u64, try!(stats.total_docs(field_ref)) as u64, try!(stats.term_document_frequency(field_ref, term_ref)) as u64);
                                stack.push(score * scorer.boost);
                            } else {
                                stack.push(0.0f64);
                            }
                        }
                        None => stack.push(0.0f64),
                    }
                }
                ScoreFunctionOp::CombinatorScorer(num_vals, ref scorer) => {
                    let score = match *scorer {
                        CombinatorScorer::Avg => {
                            let mut total_score = 0.0f64;

                            for _ in 0..num_vals {
                                total_score += stack.pop().expect("document scorer: stack underflow");
                            }

                            total_score / num_vals as f64
                        }
                        CombinatorScorer::Max => {
                            let mut max_score = 0.0f64;

                            for _ in 0..num_vals {
                                let score = stack.pop().expect("document scorer: stack underflow");
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

        if !stack.len() == 1 {
            // This shouldn't be possible unless there's a bug in the planner
            panic!("document scorer: stack size too big ({})", stack.len());
        }

        Ok(stack.pop().expect("document scorer: stack underflow"))
    }

    fn search_segment<C: Collector>(&self, collector: &mut C, plan: &SearchPlan, segment: u32, mut stats: &mut StatisticsReader) -> Result<(), rocksdb::Error> {
        let matches = try!(self.search_segment_boolean_phase(&plan.boolean_query, plan.boolean_query_is_negated, segment));

        // Score documents and pass to collector
        for doc in matches.iter() {
            let score = try!(self.score_doc(doc, &plan.score_function, segment, &mut stats));

            let doc_ref = DocRef::from_segment_ord(segment, doc);
            let doc_match = DocumentMatch::new_scored(doc_ref.as_u64(), score);
            collector.collect(doc_match);
        }

        Ok(())
    }

    pub fn search<C: Collector>(&self, collector: &mut C, query: &Query) -> Result<(), rocksdb::Error> {
        // Plan query
        let plan = plan_query(&self, query, collector.needs_score());

        // Initialise statistics reader
        let mut stats = StatisticsReader::new(&self);

        // Run query on each segment
        for segment in self.store.segments.iter_active(&self.snapshot) {
            try!(self.search_segment(collector, &plan, segment, &mut stats));
        }

        Ok(())
    }
}
