use collectors::{Collector, DocumentMatch};


pub struct TotalCountCollector {
    total_count: u64,
}


impl TotalCountCollector {
    pub fn new() -> TotalCountCollector {
        TotalCountCollector {
            total_count: 0,
        }
    }

    pub fn get_total_count(&self) -> u64 {
        self.total_count
    }
}


impl Collector for TotalCountCollector {
    fn needs_score(&self) -> bool {
        false
    }

    fn collect(&mut self, _doc: DocumentMatch) {
        self.total_count += 1;
    }
}


#[cfg(test)]
mod tests {
    use collectors::{Collector, DocumentMatch};
    use super::TotalCountCollector;


    #[test]
    fn test_total_count_collector_inital_state() {
        let collector = TotalCountCollector::new();

        assert_eq!(collector.get_total_count(), 0);
    }

    #[test]
    fn test_total_count_collector_needs_score() {
        let collector = TotalCountCollector::new();

        assert_eq!(collector.needs_score(), false);
    }

    #[test]
    fn test_total_count_collector_collect() {
        let mut collector = TotalCountCollector::new();

        collector.collect(DocumentMatch::new_unscored(0));
        collector.collect(DocumentMatch::new_unscored(1));
        collector.collect(DocumentMatch::new_unscored(2));

        assert_eq!(collector.get_total_count(), 3);
    }
}
