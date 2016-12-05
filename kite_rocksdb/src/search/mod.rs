mod statistics;
mod planner;

use kite::doc_id_set::DocIdSet;
use kite::query::Query;
use kite::collectors::{Collector, DocumentMatch};
use byteorder::{ByteOrder, BigEndian};

use super::RocksDBIndexReader;
use segment::Segment;
use search::statistics::{StatisticsReader, RocksDBStatisticsReader};
use search::planner::{SearchPlan, plan_query};
use search::planner::boolean_query::BooleanQueryOp;
use search::planner::score_function::{CombinatorScorer, ScoreFunctionOp};


fn search_segment_boolean_phase<S: Segment>(boolean_query: &Vec<BooleanQueryOp>, is_negated: bool, segment: &S) -> Result<DocIdSet, String> {
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
                match try!(segment.load_term_directory(field_ref, term_ref)) {
                    Some(doc_id_set) => stack.push(doc_id_set),
                    None => stack.push(DocIdSet::new_filled(0)),
                }
            }
            BooleanQueryOp::PushDeletionList => {
                    match try!(segment.load_deletion_list()) {
                    Some(doc_id_set) => stack.push(doc_id_set),
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
        let total_docs = try!(segment.load_statistic(b"total_docs")).unwrap_or(0);
        let all_docs = DocIdSet::new_filled(total_docs as u32);
        matches = all_docs.exclusion(&matches);
    }

    Ok(matches)
}


fn score_doc<S: Segment, R: StatisticsReader>(doc_id: u16, score_function: &Vec<ScoreFunctionOp>, segment: &S, mut stats: &mut R) -> Result<f64, String> {
    // Execute score function
    let mut stack = Vec::new();
    for op in score_function.iter() {
        match *op {
            ScoreFunctionOp::Literal(val) => stack.push(val),
            ScoreFunctionOp::TermScorer(field_ref, term_ref, ref scorer) => {
                // TODO: Check this isn't really slow
                match try!(segment.load_term_directory(field_ref, term_ref)) {
                    Some(doc_id_set) => {
                        if doc_id_set.contains_doc(doc_id) {
                            // Read field length
                            // TODO: we only need this for BM25
                            let field_length_raw = try!(segment.load_stored_field_value_raw(doc_id, field_ref, b"len"));
                            let field_length = match field_length_raw {
                                Some(value) => {
                                    let length_sqrt = (value[0] as f64) / 3.0 + 1.0;
                                    length_sqrt * length_sqrt
                                }
                                None => 1.0
                            };

                            // Read term frequency
                            let mut value_type = vec![b't', b'f'];
                            value_type.extend(term_ref.ord().to_string().as_bytes());
                            let term_frequency_raw = try!(segment.load_stored_field_value_raw(doc_id, field_ref, &value_type));
                            let term_frequency = match term_frequency_raw {
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


fn search_segment<C: Collector, S: Segment, R: StatisticsReader>(collector: &mut C, plan: &SearchPlan, segment: &S, mut stats: &mut R) -> Result<(), String> {
    let matches = try!(search_segment_boolean_phase(&plan.boolean_query, plan.boolean_query_is_negated, segment));

    // Score documents and pass to collector
    for doc in matches.iter() {
        let score = try!(score_doc(doc, &plan.score_function, segment, stats));

        let doc_ref = segment.doc_ref(doc);
        let doc_match = DocumentMatch::new_scored(doc_ref.as_u64(), score);
        collector.collect(doc_match);
    }

    Ok(())
}


impl<'a> RocksDBIndexReader<'a> {
    pub fn search<C: Collector>(&self, collector: &mut C, query: &Query) -> Result<(), String> {
        // Plan query
        let plan = plan_query(&self, query, collector.needs_score());

        // Initialise statistics reader
        let mut stats = RocksDBStatisticsReader::new(&self);

        // Run query on each segment
        for segment in self.store.segments.iter_active(&self) {
            try!(search_segment(collector, &plan, &segment, &mut stats));
        }

        Ok(())
    }
}
