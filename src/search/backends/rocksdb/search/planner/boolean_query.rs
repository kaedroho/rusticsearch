use std::rc::Rc;

use search::schema::FieldId;
use search::term::TermId;
use search::Query;

use super::super::RocksDBReader;

#[derive(Debug, Clone, PartialEq)]
pub enum BooleanQueryOp {
    PushEmpty,
    PushPostingsList(FieldId, TermId),
    PushDeletionList,
    And,
    Or,
    AndNot,
}

#[derive(Clone, Copy, PartialEq)]
enum BooleanQueryBlockReturnType {
    Full,
    Empty,
    Sparse,
    NegatedSparse,
}

#[derive(Clone)]
enum BooleanQueryBlock {
    Leaf {
        op: BooleanQueryOp,
        return_type: BooleanQueryBlockReturnType,
    },
    Combinator {
        op: BooleanQueryOp,
        child_a: Rc<BooleanQueryBlock>,
        child_b: Rc<BooleanQueryBlock>,
        return_type: BooleanQueryBlockReturnType,
    }
}

impl BooleanQueryBlock {
    fn return_type(&self) -> BooleanQueryBlockReturnType {
        use self::BooleanQueryBlock::*;

        match *self {
            Leaf{return_type, ..} => return_type,
            Combinator{return_type, ..} => return_type,
        }
    }

    fn set_return_type(&mut self, new_type: BooleanQueryBlockReturnType) {
        use self::BooleanQueryBlock::*;

        match *self {
            Leaf{ref mut return_type, ..} => *return_type = new_type,
            Combinator{ref mut return_type, ..} => *return_type = new_type,
        }
    }

    fn build(&self, boolean_query: &mut Vec<BooleanQueryOp>) {
        use self::BooleanQueryBlock::*;

        match *self {
            Leaf{ref op, ..} => {
                boolean_query.push(op.clone());
            }
            Combinator{ref op, ref child_a, ref child_b, ..} => {
                child_a.build(boolean_query);
                child_b.build(boolean_query);
                boolean_query.push(op.clone());
            }
        }
    }
}

pub struct BooleanQueryBuilder {
    stack: Vec<Rc<BooleanQueryBlock>>,
}

impl BooleanQueryBuilder {
    pub fn new() -> BooleanQueryBuilder {
        BooleanQueryBuilder {
            stack: Vec::new(),
        }
    }

    pub fn push_empty(&mut self) {
        use self::BooleanQueryOp::*;
        use self::BooleanQueryBlock::*;
        use self::BooleanQueryBlockReturnType::*;

        self.stack.push(Rc::new(Leaf{
            op: PushEmpty,
            return_type: Empty,
        }));
    }

    pub fn push_full(&mut self) {
        use self::BooleanQueryOp::*;
        use self::BooleanQueryBlock::*;
        use self::BooleanQueryBlockReturnType::*;

        self.stack.push(Rc::new(Leaf{
            op: PushEmpty,
            return_type: Full,
        }));
    }

    pub fn push_postings_list(&mut self, field_id: FieldId, term_id: TermId) {
        use self::BooleanQueryOp::*;
        use self::BooleanQueryBlock::*;
        use self::BooleanQueryBlockReturnType::*;

        self.stack.push(Rc::new(Leaf{
            op: PushPostingsList(field_id, term_id),
            return_type: Sparse,
        }));
    }

    pub fn push_deletion_list(&mut self) {
        use self::BooleanQueryOp::*;
        use self::BooleanQueryBlock::*;
        use self::BooleanQueryBlockReturnType::*;

        self.stack.push(Rc::new(Leaf{
            op: PushDeletionList,
            return_type: Sparse,
        }));
    }

    pub fn and_combinator(&mut self) {
        use self::BooleanQueryOp::*;
        use self::BooleanQueryBlock::*;
        use self::BooleanQueryBlockReturnType::*;

        let b = self.stack.pop().expect("stack underflow");
        let a = self.stack.pop().expect("stack underflow");

        match (a.return_type(), b.return_type()) {
            // If either block is "full", replace this block with the other block
            (Full, _) => self.stack.push(b),
            (_, Full) => self.stack.push(a),

            // If either block is "empty", this block will be empty too
            (Empty, _) => self.push_empty(),
            (_, Empty) => self.push_empty(),

            (Sparse, Sparse) => {  // (a AND b)
                // Intersection
                self.stack.push(Rc::new(Combinator{
                    op: And,
                    child_a: a,
                    child_b: b,
                    return_type: Sparse,
                }));
            }

            (Sparse, NegatedSparse) => {  // (a AND NOT b)
                // Exclusion
                self.stack.push(Rc::new(Combinator{
                    op: AndNot,
                    child_a: a,
                    child_b: b,
                    return_type: Sparse,
                }));
            }

            (NegatedSparse, Sparse) => {  // (NOT a AND b)
                // Exclusion, with operands swapped
                self.stack.push(Rc::new(Combinator{
                    op: AndNot,
                    child_a: b,
                    child_b: a,
                    return_type: Sparse,
                }));
            }

            (NegatedSparse, NegatedSparse) => {  // (NOT a AND NOT b)
                // Negated union (NOT (a OR b))
                self.stack.push(Rc::new(Combinator{
                    op: Or,
                    child_a: a,
                    child_b: b,
                    return_type: NegatedSparse,
                }));
            }
        }
    }

    pub fn or_combinator(&mut self) {
        use self::BooleanQueryOp::*;
        use self::BooleanQueryBlock::*;
        use self::BooleanQueryBlockReturnType::*;

        let b = self.stack.pop().expect("stack underflow");
        let a = self.stack.pop().expect("stack underflow");

        match (a.return_type(), b.return_type()) {
            // If either block is "full", this block will be full too
            (Full, _) => self.push_full(),
            (_, Full) => self.push_full(),

            // If either block is "empty", replace this block with the other block
            (Empty, _) => self.stack.push(b),
            (_, Empty) => self.stack.push(a),

            (Sparse, Sparse) => {  // (a OR b)
                // Union
                self.stack.push(Rc::new(Combinator{
                    op: Or,
                    child_a: a,
                    child_b: b,
                    return_type: Sparse,
                }));
            }

            (Sparse, NegatedSparse) => {  // (a OR NOT b)
                // Negated exclusion, with operands swapped (NOT (b AND NOT a))
                self.stack.push(Rc::new(Combinator{
                    op: AndNot,
                    child_a: b,
                    child_b: a,
                    return_type: NegatedSparse,
                }));
            }

            (NegatedSparse, Sparse) => {  // (NOT a OR b)
                // Negated exclusion (NOT (a AND NOT b))
                self.stack.push(Rc::new(Combinator{
                    op: AndNot,
                    child_a: a,
                    child_b: b,
                    return_type: NegatedSparse,
                }));
            }

            (NegatedSparse, NegatedSparse) => {  // (NOT a OR NOT b)
                // Negated intersection (NOT (a AND b))
                self.stack.push(Rc::new(Combinator{
                    op: And,
                    child_a: a,
                    child_b: b,
                    return_type: NegatedSparse,
                }));
            }
        }
    }

    pub fn andnot_combinator(&mut self) {
        use self::BooleanQueryOp::*;
        use self::BooleanQueryBlock::*;
        use self::BooleanQueryBlockReturnType::*;

        let b = self.stack.pop().expect("stack underflow");
        let a = self.stack.pop().expect("stack underflow");

        match (a.return_type(), b.return_type()) {
            // If the right block is full, this block will be empty
            (_, Full) => self.push_empty(),

            // If the left block is empty, this block will be empty too
            (Empty, _) => self.push_empty(),

            // If the right block is empty, replace this block with the left block
            (_, Empty) => self.stack.push(a),

            (Full, Sparse) => {  // (ALL AND NOT b)
                // Negation of b (NOT b)
                let mut b_new = Rc::make_mut(&mut b.clone()).clone();
                b_new.set_return_type(NegatedSparse);
                self.stack.push(Rc::new(b_new));
            }

            (Full, NegatedSparse) => {  // (ALL AND NOT (NOT b))
                // De-Negation of b (NOT (NOT b))
                let mut b_new = Rc::make_mut(&mut b.clone()).clone();
                b_new.set_return_type(Sparse);
                self.stack.push(Rc::new(b_new));
            }

            (Sparse, Sparse) => {  // (a AND NOT b)
                // Exclusion
                self.stack.push(Rc::new(Combinator{
                    op: AndNot,
                    child_a: a,
                    child_b: b,
                    return_type: Sparse,
                }));
            }

            (Sparse, NegatedSparse) => {  // (a AND NOT (NOT b))
                // Intersection (data AND other_data)
                self.stack.push(Rc::new(Combinator{
                    op: And,
                    child_a: a,
                    child_b: b,
                    return_type: Sparse,
                }));
            }

            (NegatedSparse, Sparse) => {  // (NOT a AND NOT b)
                // Negated union (NOT (data OR other_data))
                self.stack.push(Rc::new(Combinator{
                    op: Or,
                    child_a: a,
                    child_b: b,
                    return_type: NegatedSparse,
                }));
            }

            (NegatedSparse, NegatedSparse) => {  // (NOT a AND NOT (NOT b))
                // Exclusion, with operands swapped (b AND NOT a)
                self.stack.push(Rc::new(Combinator{
                    op: AndNot,
                    child_a: b,
                    child_b: a,
                    return_type: Sparse,
                }));
            }
        }
    }

    pub fn build(&self) -> (Vec<BooleanQueryOp>, bool) {
        use self::BooleanQueryBlockReturnType::*;

        let mut boolean_query = Vec::new();

        // If the query was valid, should be exactly one item on the stack
        let root_block = self.stack.last().unwrap();
        root_block.build(&mut boolean_query);

        (boolean_query, root_block.return_type() == NegatedSparse || root_block.return_type() == Full)
    }
}

fn plan_boolean_query_combinator<J: Fn(&mut BooleanQueryBuilder) -> ()> (index_reader: &RocksDBReader, mut builder: &mut BooleanQueryBuilder, queries: &Vec<Query>, join_cb: J) {
    match queries.len() {
        0 => {
            builder.push_empty();
        }
        1 =>  plan_boolean_query(index_reader, &mut builder, &queries[0]),
        _ => {
            let mut query_iter = queries.iter();
            plan_boolean_query(index_reader, &mut builder, query_iter.next().unwrap());

            for query in query_iter {
                plan_boolean_query(index_reader, &mut builder, query);

                // Add the join operation
                join_cb(&mut builder);
            }
        }
    }
}

pub fn plan_boolean_query(index_reader: &RocksDBReader, mut builder: &mut BooleanQueryBuilder, query: &Query) {
    match *query {
        Query::All{..} => {
            builder.push_full();
        }
        Query::None => {
            builder.push_empty();
        }
        Query::Term{field, ref term, ..} => {
            // Get term
            let term_id = match index_reader.store.term_dictionary.get(term) {
                Some(term_id) => term_id,
                None => {
                    // Term doesn't exist, so will never match
                    builder.push_empty();
                    return
                }
            };

            builder.push_postings_list(field, term_id);
        }
        Query::MultiTerm{field, ref term_selector, ..} => {
            // Get terms
            builder.push_empty();
            for term_id in index_reader.store.term_dictionary.select(term_selector) {
                builder.push_postings_list(field, term_id);
                builder.or_combinator();
            }
        }
        Query::Conjunction{ref queries} => {
            plan_boolean_query_combinator(index_reader, &mut builder, queries, |builder| builder.and_combinator());
        }
        Query::Disjunction{ref queries} => {
            plan_boolean_query_combinator(index_reader, &mut builder, queries, |builder| builder.or_combinator());
        }
        Query::DisjunctionMax{ref queries} => {
            plan_boolean_query_combinator(index_reader, &mut builder, queries, |builder| builder.or_combinator());
        }
        Query::Filter{ref query, ref filter} => {
            plan_boolean_query(index_reader, &mut builder, query);
            plan_boolean_query(index_reader, &mut builder, filter);
            builder.and_combinator();
        }
        Query::Exclude{ref query, ref exclude} => {
            plan_boolean_query(index_reader, &mut builder, query);
            plan_boolean_query(index_reader, &mut builder, exclude);
            builder.andnot_combinator();
        }
    }
}

#[cfg(test)]
mod builder_tests {
    use search::schema::FieldId;
    use search::term::TermId;

    use super::BooleanQueryOp;
    use super::BooleanQueryBuilder;

    #[test]
    fn test_push_empty() {
        let mut builder = BooleanQueryBuilder::new();

        builder.push_empty();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushEmpty,
        ]);
        assert_eq!(negated, false);
    }

    #[test]
    fn test_push_full() {
        let mut builder = BooleanQueryBuilder::new();

        builder.push_full();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushEmpty,
        ]);
        assert_eq!(negated, true);
    }

    #[test]
    fn test_push_postings_list() {
        let mut builder = BooleanQueryBuilder::new();

        builder.push_postings_list(FieldId(1), TermId(1));

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushPostingsList(FieldId(1), TermId(1)),
        ]);
        assert_eq!(negated, false);
    }

    #[test]
    fn test_push_deletion_list() {
        let mut builder = BooleanQueryBuilder::new();

        builder.push_deletion_list();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushDeletionList,
        ]);
        assert_eq!(negated, false);
    }

    #[test]
    fn test_and_combinator() {
        let mut builder = BooleanQueryBuilder::new();

        builder.push_postings_list(FieldId(1), TermId(1));
        builder.push_postings_list(FieldId(1), TermId(2));
        builder.and_combinator();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushPostingsList(FieldId(1), TermId(1)),
            BooleanQueryOp::PushPostingsList(FieldId(1), TermId(2)),
            BooleanQueryOp::And,
        ]);
        assert_eq!(negated, false);
    }

    #[test]
    fn test_or_combinator() {
        let mut builder = BooleanQueryBuilder::new();

        builder.push_postings_list(FieldId(1), TermId(1));
        builder.push_postings_list(FieldId(1), TermId(2));
        builder.or_combinator();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushPostingsList(FieldId(1), TermId(1)),
            BooleanQueryOp::PushPostingsList(FieldId(1), TermId(2)),
            BooleanQueryOp::Or,
        ]);
        assert_eq!(negated, false);
    }

    #[test]
    fn test_andnot_combinator() {
        let mut builder = BooleanQueryBuilder::new();

        builder.push_postings_list(FieldId(1), TermId(1));
        builder.push_postings_list(FieldId(1), TermId(2));
        builder.andnot_combinator();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushPostingsList(FieldId(1), TermId(1)),
            BooleanQueryOp::PushPostingsList(FieldId(1), TermId(2)),
            BooleanQueryOp::AndNot,
        ]);
        assert_eq!(negated, false);
    }

    #[test]
    fn test_push_full_with_or_combinator() {
        // If one of the operands to an or combinator is full, the or combinator should be replaced with empty and the query negated
        let mut builder = BooleanQueryBuilder::new();

        builder.push_full();
        builder.push_postings_list(FieldId(1), TermId(2));
        builder.or_combinator();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushEmpty,
        ]);
        assert_eq!(negated, true);
    }

    #[test]
    fn test_push_empty_with_or_combinator() {
        // If one of the operands to an or combinator is empty, the or combinator should be replaced with the other operand
        let mut builder = BooleanQueryBuilder::new();

        builder.push_empty();
        builder.push_postings_list(FieldId(1), TermId(2));
        builder.or_combinator();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushPostingsList(FieldId(1), TermId(2)),
        ]);
        assert_eq!(negated, false);
    }

    #[test]
    fn test_push_full_with_and_combinator() {
        // If one of the operands to an and combinator is full, the and combinator should be replaced with the other operand
        let mut builder = BooleanQueryBuilder::new();

        builder.push_full();
        builder.push_postings_list(FieldId(1), TermId(2));
        builder.and_combinator();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushPostingsList(FieldId(1), TermId(2)),
        ]);
        assert_eq!(negated, false);
    }

    #[test]
    fn test_push_empty_with_and_combinator() {
        // If one of the operands to an and combinator is empty, the and combinator should be replaced with empty
        let mut builder = BooleanQueryBuilder::new();

        builder.push_empty();
        builder.push_postings_list(FieldId(1), TermId(2));
        builder.and_combinator();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushEmpty,
        ]);
        assert_eq!(negated, false);
    }

    #[test]
    fn test_push_full_to_left_of_andnot_operator() {
        // If the left operand to the andnot operator is full, the andnot combinator should be replaced with the right operand and the whole query should be negated
        // (basically: we're filtering a full set. This is effectively a NOT query)
        let mut builder = BooleanQueryBuilder::new();

        builder.push_full();
        builder.push_postings_list(FieldId(1), TermId(2));
        builder.andnot_combinator();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushPostingsList(FieldId(1), TermId(2)),
        ]);
        assert_eq!(negated, true);
    }

    #[test]
    fn test_push_empty_to_left_of_andnot_operator() {
        // If the left operand to the andnot operator is empty, the andnot combinator should be replaced with empty
        // (basically: we're filtering an empty set)
        let mut builder = BooleanQueryBuilder::new();

        builder.push_empty();
        builder.push_postings_list(FieldId(1), TermId(2));
        builder.andnot_combinator();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushEmpty,
        ]);
        assert_eq!(negated, false);
    }

    #[test]
    fn test_push_full_to_right_of_andnot_operator() {
        // If the right operand to the andnot operator is full, the andnot combinator should be replaced with empty
        // (basically: we're filtering a set by a full set, so there can't be anything left)
        let mut builder = BooleanQueryBuilder::new();

        builder.push_postings_list(FieldId(1), TermId(2));
        builder.push_full();
        builder.andnot_combinator();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushEmpty,
        ]);
        assert_eq!(negated, false);
    }

    #[test]
    fn test_push_empty_to_right_of_andnot_operator() {
        // If the right operand to the andnot operator is empty, the andnot combinator should be replaced with the left operand
        // (basically: we're filtering a set by an empty set, leaving the set untouched)
        let mut builder = BooleanQueryBuilder::new();

        builder.push_postings_list(FieldId(1), TermId(2));
        builder.push_empty();
        builder.andnot_combinator();

        let (query, negated) = builder.build();

        assert_eq!(query, vec![
            BooleanQueryOp::PushPostingsList(FieldId(1), TermId(2)),
        ]);
        assert_eq!(negated, false);
    }

    #[test]
    fn test_complex_query() {
        // There's a lot going on here. This checks that a complex query gets optimised as much as possible
        let mut builder = BooleanQueryBuilder::new();

        // (ALL NOT TD(1, 2)) OR (TD(1,1) AND (TD(1, 3) AND NOT ALL))
        builder.push_full();
        builder.push_postings_list(FieldId(1), TermId(2));
        builder.andnot_combinator();
        builder.push_postings_list(FieldId(1), TermId(1));
        builder.push_postings_list(FieldId(1), TermId(3));
        builder.push_full();
        builder.andnot_combinator();
        builder.and_combinator();
        builder.or_combinator();

        let (query, negated) = builder.build();

        // Should be optimised down to just "NOT TD(1, 2)"
        assert_eq!(query, vec![
            BooleanQueryOp::PushPostingsList(FieldId(1), TermId(2)),
        ]);
        assert_eq!(negated, true);
    }
}
