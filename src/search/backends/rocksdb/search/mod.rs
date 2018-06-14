mod statistics;
mod planner;

use roaring::RoaringBitmap;
use search::segment::Segment;
use search::query::Query;
use search::collectors::{Collector, DocumentMatch};
use byteorder::{ByteOrder, LittleEndian};

use super::RocksDBReader;
use self::statistics::{StatisticsReader, RocksDBStatisticsReader};
use self::planner::{SearchPlan, plan_query};
use self::planner::boolean_query::BooleanQueryOp;
use self::planner::score_function::{CombinatorScorer, ScoreFunctionOp};

fn run_boolean_query<S: Segment>(boolean_query: &Vec<BooleanQueryOp>, is_negated: bool, segment: &S) -> Result<RoaringBitmap, String> {
    // Execute boolean query
    let mut stack = Vec::new();
    for op in boolean_query.iter() {
        match *op {
            BooleanQueryOp::PushEmpty => {
                stack.push(RoaringBitmap::new());
            }
            BooleanQueryOp::PushPostingsList(field_id, term_id) => {
                match try!(segment.load_postings_list(field_id, term_id)) {
                    Some(doc_id_set) => stack.push(doc_id_set),
                    None => stack.push(RoaringBitmap::new()),
                }
            }
            BooleanQueryOp::PushDeletionList => {
                    match try!(segment.load_deletion_list()) {
                    Some(doc_id_set) => stack.push(doc_id_set),
                    None => stack.push(RoaringBitmap::new()),
                }
            }
            BooleanQueryOp::And => {
                let b = stack.pop().expect("boolean query executor: stack underflow");
                let a = stack.last_mut().expect("boolean query executor: stack underflow");

                a.intersect_with(&b);
            }
            BooleanQueryOp::Or => {
                let b = stack.pop().expect("boolean query executor: stack underflow");
                let a = stack.last_mut().expect("boolean query executor: stack underflow");

                a.union_with(&b);
            }
            BooleanQueryOp::AndNot => {
                let b = stack.pop().expect("boolean query executor: stack underflow");
                let a = stack.last_mut().expect("boolean query executor: stack underflow");

                a.difference_with(&b);
            }
        }
    }

    if !stack.len() == 1 {
        // This shouldn't be possible unless there's a bug in the planner
        panic!("boolean query executor: stack size too big ({})", stack.len());
    }

    let mut matches = stack.pop().unwrap();

    if is_negated {
        // Query returns a negated result so we need to correct this by inverting the returned bitmap
        let total_docs = try!(segment.load_statistic(b"total_docs")).unwrap_or(0);
        let mut all_docs = RoaringBitmap::new();
        for doc_id in 0..total_docs {
            all_docs.insert(doc_id as u32);
        }

        all_docs.difference_with(&matches);
        matches = all_docs;
    }

    Ok(matches)
}

fn score_doc<S: Segment, R: StatisticsReader>(doc_id: u16, score_function: &Vec<ScoreFunctionOp>, segment: &S, stats: &mut R) -> Result<f32, String> {
    // Execute score function
    let mut stack = Vec::new();
    for op in score_function.iter() {
        match *op {
            ScoreFunctionOp::Literal(val) => stack.push(val),
            ScoreFunctionOp::TermScorer(field_id, term_id, ref scorer) => {
                // TODO: Check this isn't really slow
                match try!(segment.load_postings_list(field_id, term_id)) {
                    Some(postings) => {
                        if postings.contains(doc_id as u32) {
                            // Read field length
                            // TODO: we only need this for BM25
                            let field_length_raw = try!(segment.load_stored_field_value_raw(doc_id, field_id, b"len"));
                            let field_length = match field_length_raw {
                                Some(value) => {
                                    let length_sqrt = (value[0] as f32) / 3.0 + 1.0;
                                    length_sqrt * length_sqrt
                                }
                                None => 1.0
                            };

                            // Read term frequency
                            let mut value_type = vec![b't', b'f'];
                            value_type.extend(term_id.0.to_string().as_bytes());
                            let term_frequency_raw = try!(segment.load_stored_field_value_raw(doc_id, field_id, &value_type));
                            let term_frequency = match term_frequency_raw {
                                Some(value) => LittleEndian::read_i64(&value),
                                None => 1,
                            };

                            let score = scorer.similarity_model.score(term_frequency as u32, field_length, try!(stats.total_tokens(field_id)) as u64, try!(stats.total_docs(field_id)) as u64, try!(stats.term_document_frequency(field_id, term_id)) as u64);
                            stack.push(score * scorer.boost);
                        } else {
                            stack.push(0.0f32);
                        }
                    }
                    None => stack.push(0.0f32),
                }
            }
            ScoreFunctionOp::CombinatorScorer(num_vals, ref scorer) => {
                let score = match *scorer {
                    CombinatorScorer::Avg => {
                        let mut total_score = 0.0f32;

                        for _ in 0..num_vals {
                            total_score += stack.pop().expect("document scorer: stack underflow");
                        }

                        total_score / num_vals as f32
                    }
                    CombinatorScorer::Max => {
                        let mut max_score = 0.0f32;

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

fn search_segment<C: Collector, S: Segment, R: StatisticsReader>(collector: &mut C, plan: &SearchPlan, segment: &S, stats: &mut R) -> Result<(), String> {
    let matches = try!(run_boolean_query(&plan.boolean_query, plan.boolean_query_is_negated, segment));

    // Score documents and pass to collector
    for doc in matches.iter() {
        let score = try!(score_doc(doc as u16, &plan.score_function, segment, stats));

        let doc_id = segment.doc_id(doc as u16);
        let doc_match = DocumentMatch::new_scored(doc_id.as_u64(), score);
        collector.collect(doc_match);
    }

    Ok(())
}

impl<'a> RocksDBReader<'a> {
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
