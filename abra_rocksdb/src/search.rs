use std::fmt;
use std::io::{Cursor, Read, Write};
use std::collections::HashMap;

use abra::schema::{FieldRef, SchemaRead};
use abra::query::Query;
use abra::query::term_scorer::TermScorer;
use abra::collectors::{Collector, DocumentMatch};
use rocksdb::DBVector;
use byteorder::{ByteOrder, BigEndian};

use key_builder::KeyBuilder;
use super::{RocksDBIndexReader, TermRef, DocRef};


#[derive(Debug, Clone)]
enum BooleanQueryOp {
    PushZero,
    PushOne,
    LoadTermDirectory(FieldRef, TermRef, u8),
    And,
    Or,
    AndNot,
}


enum DirectoryListData {
    Owned(Vec<u8>),
    FromRDB(DBVector),
}


impl DirectoryListData {
    fn new_filled(num_docs: u16) -> DirectoryListData {
        let mut data: Vec<u8> = Vec::new();

        for doc_id in 0..num_docs {
            let mut doc_id_bytes = [0; 2];
            BigEndian::write_u16(&mut doc_id_bytes, doc_id);

            data.push(doc_id_bytes[0]);
            data.push(doc_id_bytes[1]);
        }

        DirectoryListData::Owned(data)
    }

    fn get_cursor(&self) -> Cursor<&[u8]> {
        match *self {
            DirectoryListData::Owned(ref data) => {
                Cursor::new(&data[..])
            }
            DirectoryListData::FromRDB(ref data) => {
                Cursor::new(&data[..])
            }
        }
    }

    fn iter<'a>(&'a self) -> DirectoryListDataIterator<'a> {
        DirectoryListDataIterator {
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

    fn union(&self, other: &DirectoryListData) -> DirectoryListData {
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

        DirectoryListData::Owned(data)
    }

    fn intersection(&self, other: &DirectoryListData) -> DirectoryListData {
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

        DirectoryListData::Owned(data)
    }

    fn exclusion(&self, other: &DirectoryListData) -> DirectoryListData {
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

        DirectoryListData::Owned(data)
    }
}


impl Clone for DirectoryListData {
    fn clone(&self) -> DirectoryListData {
        match *self {
            DirectoryListData::Owned(ref data) => {
                DirectoryListData::Owned(data.clone())
            }
            DirectoryListData::FromRDB(ref data) => {
                let mut new_data = Vec::with_capacity(data.len());
                new_data.write_all(data);
                DirectoryListData::Owned(new_data)
            }
        }
    }
}


impl fmt::Debug for DirectoryListData {
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


struct DirectoryListDataIterator<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl<'a> Iterator for DirectoryListDataIterator<'a> {
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


#[derive(Debug, Clone)]
enum DirectoryList {
    Empty,
    Full,
    Sparse(DirectoryListData, bool),
    //Packed(Bitmap),
}


impl DirectoryList {
    fn intersection(self, other: DirectoryList) -> DirectoryList {
        match self {
            DirectoryList::Empty => DirectoryList::Empty,
            DirectoryList::Full => other,
            DirectoryList::Sparse(data, false) => {
                match other {
                    DirectoryList::Empty => DirectoryList::Empty,
                    DirectoryList::Full => DirectoryList::Sparse(data, false),
                    DirectoryList::Sparse(other_data, false) => {
                        // Intersection (data AND other_data)
                        DirectoryList::Sparse(data.intersection(&other_data), false)
                    }
                    DirectoryList::Sparse(other_data, true) => {
                        // Exclusion (data AND NOT other_data)
                        DirectoryList::Sparse(data.exclusion(&other_data), false)
                    }
                }
            }
            DirectoryList::Sparse(data, true) => {
                match other {
                    DirectoryList::Empty => DirectoryList::Empty,
                    DirectoryList::Full => DirectoryList::Sparse(data, true),
                    DirectoryList::Sparse(other_data, false) => {
                        // Exclusion (other_data AND NOT data)
                        DirectoryList::Sparse(other_data.exclusion(&data), false)
                    }
                    DirectoryList::Sparse(other_data, true) => {
                        // Negated union (NOT (data OR other_data))
                        // Equivilent to (NOT data AND NOT other_data)
                        DirectoryList::Sparse(data.union(&other_data), true)
                    }
                }
            }
        }
    }

    fn union(self, other: DirectoryList) -> DirectoryList {
        match self {
            DirectoryList::Empty => other,
            DirectoryList::Full => DirectoryList::Full,
            DirectoryList::Sparse(data, false) => {
                match other {
                    DirectoryList::Empty => DirectoryList::Sparse(data, false),
                    DirectoryList::Full => DirectoryList::Full,
                    DirectoryList::Sparse(other_data, false) => {
                        // Union (data OR other_data)
                        DirectoryList::Sparse(data.union(&other_data), false)
                    }
                    DirectoryList::Sparse(other_data, true) => {
                        // Negated exclusion (NOT (other_data AND NOT data))
                        // Equivilant to (data OR NOT other_data)
                        DirectoryList::Sparse(other_data.exclusion(&data), true)
                    }
                }
            }
            DirectoryList::Sparse(data, true) => {
                match other {
                    DirectoryList::Empty => DirectoryList::Sparse(data, true),
                    DirectoryList::Full => DirectoryList::Full,
                    DirectoryList::Sparse(other_data, false) => {
                        // Negated exclusion (NOT (data AND NOT other_data))
                        // Equivilant to (other_data OR NOT data)
                        DirectoryList::Sparse(data.exclusion(&other_data), true)
                    }
                    DirectoryList::Sparse(other_data, true) => {
                        // Negated intersection (NOT (data AND other_data))
                        // Equivilent to (NOT data OR NOT other_data)
                        DirectoryList::Sparse(data.intersection(&other_data), true)
                    }
                }
            }
        }
    }

    fn exclusion(self, other: DirectoryList) -> DirectoryList {
        match self {
            DirectoryList::Empty => DirectoryList::Empty,
            DirectoryList::Full => {
                match other {
                    DirectoryList::Empty => DirectoryList::Full,
                    DirectoryList::Full => DirectoryList::Empty,
                    DirectoryList::Sparse(other_data, false) => {
                        // Negation (NOT other_data)
                        // Equivilent to (ALL AND NOT other_data)
                        DirectoryList::Sparse(other_data, true)
                    }
                    DirectoryList::Sparse(other_data, true) => {
                        // De-negation (NOT (NOT other_data))
                        // Equivilent to (ALL AND NOT (NOT other_data))
                        DirectoryList::Sparse(other_data, false)
                    }
                }
            },
            DirectoryList::Sparse(data, false) => {
                match other {
                    DirectoryList::Empty => DirectoryList::Sparse(data, false),
                    DirectoryList::Full => DirectoryList::Full,
                    DirectoryList::Sparse(other_data, false) => {
                        // Exclusion (data AND NOT other_data)
                        DirectoryList::Sparse(data.exclusion(&other_data), false)
                    }
                    DirectoryList::Sparse(other_data, true) => {
                        // Intersection (data AND other_data)
                        // Equivilent to (data AND NOT (NOT other_data))
                        DirectoryList::Sparse(data.intersection(&other_data), false)
                    }
                }
            }
            DirectoryList::Sparse(data, true) => {
                match other {
                    DirectoryList::Empty => DirectoryList::Sparse(data, true),
                    DirectoryList::Full => DirectoryList::Full,
                    DirectoryList::Sparse(other_data, false) => {
                        // Negated union (NOT (data OR other_data))
                        // Equivilant to (NOT data AND NOT other_data)
                        DirectoryList::Sparse(data.union(&other_data), true)
                    }
                    DirectoryList::Sparse(other_data, true) => {
                        // Exclusion (other_data AND NOT data)
                        // Equivilant to (NOT data AND NOT (NOT other_data))
                        DirectoryList::Sparse(other_data.exclusion(&data), false)
                    }
                }
            }
        }
    }
}


#[derive(Debug, Clone)]
enum CompoundScorer {
    Avg,
    Max,
}


#[derive(Debug, Clone)]
enum ScoreFunctionOp {
    Literal(f64),
    TermScore(FieldRef, TermRef, TermScorer, u8),
    CompoundScorer(u32, CompoundScorer),
}


#[derive(Debug)]
struct SearchPlan {
    boolean_query: Vec<BooleanQueryOp>,
    score_function: Vec<ScoreFunctionOp>,
    current_tag: u8,
}


impl SearchPlan {
    fn new() -> SearchPlan {
        SearchPlan {
            boolean_query: Vec::new(),
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
    fn plan_query_combinator(&self, mut plan: &mut SearchPlan, queries: &Vec<Query>, join_op: BooleanQueryOp, score: bool, scorer: CompoundScorer) {
        match queries.len() {
            0 => plan.boolean_query.push(BooleanQueryOp::PushZero),
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

        plan.score_function.push(ScoreFunctionOp::CompoundScorer(queries.len() as u32, scorer));
    }

    fn plan_query(&self, mut plan: &mut SearchPlan, query: &Query, score: bool) {
        match *query {
            Query::MatchAll{ref score} => {
                plan.boolean_query.push(BooleanQueryOp::PushOne);
                plan.score_function.push(ScoreFunctionOp::Literal(*score));
            }
            Query::MatchNone => {
                plan.boolean_query.push(BooleanQueryOp::PushZero);
                plan.score_function.push(ScoreFunctionOp::Literal(0.0f64));
            }
            Query::MatchTerm{ref field, ref term, ref matcher, ref scorer} => {
                // Get term
                let term_bytes = term.to_bytes();
                let term_ref = match self.store.term_dictionary.read().unwrap().get(&term_bytes) {
                    Some(term_ref) => *term_ref,
                    None => {
                        // Term doesn't exist, so will never match
                        plan.boolean_query.push(BooleanQueryOp::PushZero);
                        return
                    }
                };

                // Get field
                let field_ref = match self.schema().get_field_by_name(field) {
                    Some(field_ref) => field_ref,
                    None => {
                        // Field doesn't exist, so will never match
                        plan.boolean_query.push(BooleanQueryOp::PushZero);
                        return
                    }
                };

                let tag = plan.allocate_tag().unwrap_or(0);
                plan.boolean_query.push(BooleanQueryOp::LoadTermDirectory(field_ref, term_ref, tag));
                plan.score_function.push(ScoreFunctionOp::TermScore(field_ref, term_ref, scorer.clone(), tag));
            }
            Query::Conjunction{ref queries} => {
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::And, score, CompoundScorer::Avg);
            }
            Query::Disjunction{ref queries} => {
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::Or, score, CompoundScorer::Avg);
            }
            Query::NDisjunction{ref queries, minimum_should_match} => {
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::Or, score, CompoundScorer::Avg);  // FIXME
            }
            Query::DisjunctionMax{ref queries} => {
                self.plan_query_combinator(&mut plan, queries, BooleanQueryOp::Or, score, CompoundScorer::Max);
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

    fn search_chunk_boolean_phase(&self, plan: &SearchPlan, chunk: u32) -> (DirectoryListData, HashMap<u8, DirectoryListData>) {
        let mut tagged_directory_lists = HashMap::new();

        // Execute boolean query
        let mut stack = Vec::new();
        for op in plan.boolean_query.iter() {
            match *op {
                BooleanQueryOp::PushZero => {
                    stack.push(DirectoryList::Empty);
                }
                BooleanQueryOp::PushOne => {
                    stack.push(DirectoryList::Full);
                }
                BooleanQueryOp::LoadTermDirectory(field_ref, term_ref, tag) => {
                    let kb = KeyBuilder::chunk_dir_list(chunk, field_ref.ord(), term_ref.ord());
                    match self.snapshot.get(&kb.key()) {
                        Ok(Some(directory_list)) => {
                            let data = DirectoryListData::FromRDB(directory_list);
                            tagged_directory_lists.insert(tag, data.clone());
                            stack.push(DirectoryList::Sparse(data, false));
                        }
                        Ok(None) => stack.push(DirectoryList::Empty),
                        Err(e) => {},  // FIXME
                    }
                }
                BooleanQueryOp::And => {
                    let b = stack.pop().expect("stack underflow");
                    let a = stack.pop().expect("stack underflow");
                    stack.push(a.intersection(b));
                }
                BooleanQueryOp::Or => {
                    let b = stack.pop().expect("stack underflow");
                    let a = stack.pop().expect("stack underflow");
                    stack.push(a.union(b));
                }
                BooleanQueryOp::AndNot => {
                    let b = stack.pop().expect("stack underflow");
                    let a = stack.pop().expect("stack underflow");
                    stack.push(a.exclusion(b));
                }
            }
        }

        if !stack.len() == 1 {
            // TODO: Error
        }
        let mut matches = stack.pop().unwrap();

        // Exclude deleted docs
        let kb = KeyBuilder::chunk_del_list(chunk);
        match self.snapshot.get(&kb.key()) {
            Ok(Some(deletion_list)) => {
                let deletion_list = DirectoryList::Sparse(DirectoryListData::FromRDB(deletion_list), false);
                matches = matches.exclusion(deletion_list);
            }
            Ok(None) => {},
            Err(e) => {},  // FIXME
        }

        // Convert matches into a list of ids
        let matches = match matches {
            DirectoryList::Sparse(data, false) => data,
            DirectoryList::Sparse(data, true) => {
                // List is negated, get list of all docs and remove the ones currently
                // in matches
                let kb = KeyBuilder::chunk_stat(chunk, b"total_docs");
                let total_docs = match self.snapshot.get(&kb.key()) {
                    Ok(Some(total_docs)) => BigEndian::read_i64(&total_docs) as u16,
                    Ok(None) => 0,
                    Err(e) => 0,  // FIXME
                };

                let all_docs = DirectoryListData::new_filled(total_docs);
                all_docs.exclusion(&data)
            }
            DirectoryList::Empty => DirectoryListData::new_filled(0),
            DirectoryList::Full => {
                let kb = KeyBuilder::chunk_stat(chunk, b"total_docs");
                let total_docs = match self.snapshot.get(&kb.key()) {
                    Ok(Some(total_docs)) => BigEndian::read_i64(&total_docs) as u16,
                    Ok(None) => 0,
                    Err(e) => 0,  // FIXME
                };

                DirectoryListData::new_filled(total_docs)
            }
        };

        (matches, tagged_directory_lists)
    }

    fn score_doc(&self, doc_id: u16, tagged_directory_lists: &HashMap<u8, DirectoryListData>, plan: &SearchPlan) -> f64 {
        // Execute score function
        let mut stack = Vec::new();
        for op in plan.score_function.iter() {
            match *op {
                ScoreFunctionOp::Literal(val) => stack.push(val),
                ScoreFunctionOp::TermScore(field_ref, term_ref, ref scorer, tag) => {
                    match tagged_directory_lists.get(&tag) {
                        Some(directory_list) => {
                            if directory_list.contains_doc(doc_id) {
                                stack.push(1.0f64);
                            } else {
                                stack.push(0.0f64);
                            }
                        }
                        None => stack.push(0.0f64)
                    }
                }
                ScoreFunctionOp::CompoundScorer(num_vals, ref scorer) => {
                    let score = match *scorer {
                        CompoundScorer::Avg => {
                            let mut total_score = 0.0f64;

                            for i in 0..num_vals {
                                total_score += stack.pop().expect("stack underflow");
                            }

                            total_score / num_vals as f64
                        }
                        CompoundScorer::Max => {
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
        let (matches, tagged_directory_lists) = self.search_chunk_boolean_phase(plan, chunk);

        // Score documents and pass to collector
        for doc in matches.iter() {
            let score = self.score_doc(doc, &tagged_directory_lists, plan);

            let doc_ref = DocRef(chunk, doc);
            let doc_match = DocumentMatch::new_scored(doc_ref.as_u64(), score);
            collector.collect(doc_match);
        }
    }

    pub fn search<C: Collector>(&self, collector: &mut C, query: &Query) {
        let mut plan = SearchPlan::new();
        self.plan_query(&mut plan, query, true);

        for chunk in self.store.chunks.iter_active(&self.snapshot) {
            self.search_chunk(collector, &plan, chunk);
        }
    }
}
