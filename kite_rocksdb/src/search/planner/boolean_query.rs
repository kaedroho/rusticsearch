use std::rc::Rc;

use search::boolean_retrieval::BooleanQueryOp;


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
        self.stack.push(Rc::new(BooleanQueryBlock::Leaf{
            op: BooleanQueryOp::PushEmpty,
            return_type: BooleanQueryBlockReturnType::Empty,
        }));
    }

    pub fn push_full(&mut self) {
        self.stack.push(Rc::new(BooleanQueryBlock::Leaf{
            op: BooleanQueryOp::PushFull,
            return_type: BooleanQueryBlockReturnType::Full,
        }));
    }

    pub fn push_op(&mut self, op: BooleanQueryOp) {
        use search::boolean_retrieval::BooleanQueryOp::*;
        use self::BooleanQueryBlock::*;
        use self::BooleanQueryBlockReturnType::*;

        match op {
            PushEmpty => {
                self.push_empty();
            }
            PushFull => {
                self.push_full();
            }
            PushTermDirectory(field_ref, term_ref) => {
                self.stack.push(Rc::new(Leaf{
                    op: PushTermDirectory(field_ref, term_ref),
                    return_type: Sparse,
                }));
            }
            PushDeletionList => {
                self.stack.push(Rc::new(Leaf{
                    op: PushDeletionList,
                    return_type: Sparse,
                }));
            }
            And => {
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
            Or => {
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
            AndNot => {
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
        }
    }

    pub fn build(&self) -> (Vec<BooleanQueryOp>, bool) {
        use self::BooleanQueryBlockReturnType::*;

        let mut boolean_query = Vec::new();

        // If the query was valid, should be exactly one item on the stack
        let root_block = self.stack.last().unwrap();
        root_block.build(&mut boolean_query);

        (boolean_query, root_block.return_type() == NegatedSparse)
    }
}
