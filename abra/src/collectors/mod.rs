pub trait Collector {
    fn needs_score(&self) -> bool;
    fn collect(&mut self, doc: u64);
}
