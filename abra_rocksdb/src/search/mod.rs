pub mod boolean_retrieval;
pub mod scorer;

use std::fmt;
use std::io::{Cursor, Read, Write};
use std::collections::HashMap;
use std::rc::Rc;

use abra::schema::{FieldRef, SchemaRead};
use abra::query::Query;
use abra::query::term_scorer::TermScorer;
use abra::collectors::{Collector, DocumentMatch};
use rocksdb::DBVector;
use byteorder::{ByteOrder, BigEndian};

use key_builder::KeyBuilder;
use super::{RocksDBIndexReader, TermRef, DocRef};
use search::boolean_retrieval::{BooleanQueryOp, BooleanQueryBuilder};
use search::scorer::{CombinatorScorer, ScoreFunctionOp};


enum DocIdSet {
    Owned(Vec<u8>),
    FromRDB(DBVector),
}


impl DocIdSet {
    fn new_filled(num_docs: u16) -> DocIdSet {
        let mut data: Vec<u8> = Vec::new();

        for doc_id in 0..num_docs {
            let mut doc_id_bytes = [0; 2];
            BigEndian::write_u16(&mut doc_id_bytes, doc_id);

            data.push(doc_id_bytes[0]);
            data.push(doc_id_bytes[1]);
        }

        DocIdSet::Owned(data)
    }

    fn get_cursor(&self) -> Cursor<&[u8]> {
        match *self {
            DocIdSet::Owned(ref data) => {
                Cursor::new(&data[..])
            }
            DocIdSet::FromRDB(ref data) => {
                Cursor::new(&data[..])
            }
        }
    }

    fn iter<'a>(&'a self) -> DocIdSetIterator<'a> {
        DocIdSetIterator {
            cursor: self.get_cursor(),
        }
    }

    fn contains_doc(&self, doc_id: u16) -> bool {
        // TODO: optimise
        for d in self.iter() {
            if d == doc_id {
                return true;
            }
        }

        false
    }

    fn union(&self, other: &DocIdSet) -> DocIdSet {
        // TODO: optimise
        let mut data: Vec<u8> = Vec::new();

        let mut a = self.iter().peekable();
        let mut b = other.iter().peekable();

        loop {
            let mut next_a = false;
            let mut next_b = false;

            match (a.peek(), b.peek()) {
                (Some(a_doc), Some(b_doc)) => {
                    let mut doc_id_bytes = [0; 2];
                    BigEndian::write_u16(&mut doc_id_bytes, *a_doc);

                    data.push(doc_id_bytes[0]);
                    data.push(doc_id_bytes[1]);

                    if a_doc == b_doc {
                        next_a = true;
                        next_b = true;
                    } else if a_doc > b_doc {
                        next_b = true;
                    } else if a_doc < b_doc {
                        next_a = true;
                    }
                }
                (Some(a_doc), None) => {
                    let mut doc_id_bytes = [0; 2];
                    BigEndian::write_u16(&mut doc_id_bytes, *a_doc);

                    data.push(doc_id_bytes[0]);
                    data.push(doc_id_bytes[1]);

                    next_a = true;
                }
                (None, Some(b_doc)) => {
                    let mut doc_id_bytes = [0; 2];
                    BigEndian::write_u16(&mut doc_id_bytes, *b_doc);

                    data.push(doc_id_bytes[0]);
                    data.push(doc_id_bytes[1]);

                    next_b = true;
                }
                (None, None) => break
            }

            if next_a {
                a.next();
            }

            if next_b {
                b.next();
            }
        }

        DocIdSet::Owned(data)
    }

    fn intersection(&self, other: &DocIdSet) -> DocIdSet {
        // TODO: optimise
        let mut data: Vec<u8> = Vec::new();

        let mut a = self.iter().peekable();
        let mut b = other.iter().peekable();

        loop {
            let a_doc = match a.peek() {
                Some(a) => *a,
                None => break,
            };
            let b_doc = match b.peek() {
                Some(b) => *b,
                None => break,
            };

            if a_doc == b_doc {
                let mut doc_id_bytes = [0; 2];
                BigEndian::write_u16(&mut doc_id_bytes, a_doc);

                data.push(doc_id_bytes[0]);
                data.push(doc_id_bytes[1]);

                a.next();
                b.next();
            } else if a_doc > b_doc {
                b.next();
            } else if a_doc < b_doc {
                a.next();
            }
        }

        DocIdSet::Owned(data)
    }

    fn exclusion(&self, other: &DocIdSet) -> DocIdSet {
        // TODO: optimise
        let mut data: Vec<u8> = Vec::new();

        let mut a = self.iter().peekable();
        let mut b = other.iter().peekable();

        loop {
            let a_doc = match a.peek() {
                Some(a) => *a,
                None => break,
            };
            let b_doc = match b.peek() {
                Some(b) => *b,
                None => {
                    let mut doc_id_bytes = [0; 2];
                    BigEndian::write_u16(&mut doc_id_bytes, a_doc);

                    data.push(doc_id_bytes[0]);
                    data.push(doc_id_bytes[1]);

                    a.next();

                    continue;
                },
            };

            if a_doc == b_doc {
                a.next();
                b.next();
            } else if a_doc > b_doc {
                b.next();
            } else if a_doc < b_doc {
                let mut doc_id_bytes = [0; 2];
                BigEndian::write_u16(&mut doc_id_bytes, a_doc);

                data.push(doc_id_bytes[0]);
                data.push(doc_id_bytes[1]);

                a.next();
            }
        }

        DocIdSet::Owned(data)
    }
}


impl Clone for DocIdSet {
    fn clone(&self) -> DocIdSet {
        match *self {
            DocIdSet::Owned(ref data) => {
                DocIdSet::Owned(data.clone())
            }
            DocIdSet::FromRDB(ref data) => {
                let mut new_data = Vec::with_capacity(data.len());
                new_data.write_all(data);
                DocIdSet::Owned(new_data)
            }
        }
    }
}


impl fmt::Debug for DocIdSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut iterator = self.iter();

        try!(write!(f, "["));

        let first_item = iterator.next();
        if let Some(first_item) = first_item {
            try!(write!(f, "{:?}", first_item));
        }

        for item in iterator {
            try!(write!(f, ", {:?}", item));
        }

        write!(f, "]")
    }
}


struct DocIdSetIterator<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl<'a> Iterator for DocIdSetIterator<'a> {
    type Item = u16;

    fn next(&mut self) -> Option<u16> {
        let mut buf = [0, 2];
        match self.cursor.read_exact(&mut buf) {
            Ok(()) => {
                Some(BigEndian::read_u16(&buf))
            }
            Err(_) => None
        }
    }
}


#[derive(Debug)]
struct SearchPlan {
    boolean_query: Vec<BooleanQueryOp>,
    boolean_query_is_negated: bool,
    score_function: Vec<ScoreFunctionOp>,
    current_tag: u8,
}


impl SearchPlan {
    fn new() -> SearchPlan {
        SearchPlan {
            boolean_query: Vec::new(),
            boolean_query_is_negated: false,
            score_function: Vec::new(),
            current_tag: 0,
        }
    }

    fn allocate_tag(&mut self) -> Option<u8> {
        if self.current_tag == 255 {
            None
        } else {
            self.current_tag += 1;
            Some(self.current_tag)
        }
    }
}


impl<'a> RocksDBIndexReader<'a> {
    fn plan_query_combinator(&self, mut plan: &mut SearchPlan, queries: &Vec<Query>, join_op: BooleanQueryOp, score: bool, scorer: CombinatorScorer) {
        match queries.len() {
            0 => plan.boolean_query.push(BooleanQueryOp::PushEmpty),
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
            Query::MatchTerm{ref field, ref term, ref matcher, ref scorer} => {
                // Get term
                let term_bytes = term.to_bytes();
                let term_ref = match self.store.term_dictionary.read().unwrap().get(&term_bytes) {
                    Some(term_ref) => *term_ref,
                    None => {
                        // Term doesn't exist, so will never match
                        plan.boolean_query.push(BooleanQueryOp::PushEmpty);
                        return
                    }
                };

                // Get field
                let field_ref = match self.schema().get_field_by_name(field) {
                    Some(field_ref) => field_ref,
                    None => {
                        // Field doesn't exist, so will never match
                        plan.boolean_query.push(BooleanQueryOp::PushEmpty);
                        return
                    }
                };

                let tag = plan.allocate_tag().unwrap_or(0);
                plan.boolean_query.push(BooleanQueryOp::PushTermDirectory(field_ref, term_ref, tag));
                plan.score_function.push(ScoreFunctionOp::TermScorer(field_ref, term_ref, scorer.clone(), tag));
            }
            Query::Conjunction{ref queries} => {
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::And, score, CombinatorScorer::Avg);
            }
            Query::Disjunction{ref queries} => {
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::Or, score, CombinatorScorer::Avg);
            }
            Query::NDisjunction{ref queries, minimum_should_match} => {
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::Or, score, CombinatorScorer::Avg);  // FIXME
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

    fn search_chunk_boolean_phase(&self, plan: &SearchPlan, chunk: u32) -> (DocIdSet, HashMap<u8, DocIdSet>) {
        let mut tagged_docid_sets = HashMap::new();

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
                BooleanQueryOp::PushTermDirectory(field_ref, term_ref, tag) => {
                    let kb = KeyBuilder::chunk_dir_list(chunk, field_ref.ord(), term_ref.ord());
                    match self.snapshot.get(&kb.key()) {
                        Ok(Some(docid_set)) => {
                            let data = DocIdSet::FromRDB(docid_set);
                            tagged_docid_sets.insert(tag, data.clone());
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

        (matches, tagged_docid_sets)
    }

    fn score_doc(&self, doc_id: u16, tagged_docid_sets: &HashMap<u8, DocIdSet>, plan: &SearchPlan) -> f64 {
        // Execute score function
        let mut stack = Vec::new();
        for op in plan.score_function.iter() {
            match *op {
                ScoreFunctionOp::Literal(val) => stack.push(val),
                ScoreFunctionOp::TermScorer(field_ref, term_ref, ref scorer, tag) => {
                    match tagged_docid_sets.get(&tag) {
                        Some(docid_set) => {
                            if docid_set.contains_doc(doc_id) {
                                stack.push(1.0f64);
                            } else {
                                stack.push(0.0f64);
                            }
                        }
                        None => stack.push(0.0f64)
                    }
                }
                ScoreFunctionOp::CombinatorScorer(num_vals, ref scorer) => {
                    let score = match *scorer {
                        CombinatorScorer::Avg => {
                            let mut total_score = 0.0f64;

                            for i in 0..num_vals {
                                total_score += stack.pop().expect("stack underflow");
                            }

                            total_score / num_vals as f64
                        }
                        CombinatorScorer::Max => {
                            let mut max_score = 0.0f64;

                            for i in 0..num_vals {
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
        let (matches, tagged_docid_sets) = self.search_chunk_boolean_phase(plan, chunk);

        // Score documents and pass to collector
        for doc in matches.iter() {
            let score = self.score_doc(doc, &tagged_docid_sets, plan);

            let doc_ref = DocRef(chunk, doc);
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
