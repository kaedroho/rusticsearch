use std::collections::HashMap;


#[derive(Debug)]
pub struct Statistics {
    stats: HashMap<Vec<u8>, i64>,
}


impl Default for Statistics {
    fn default() -> Statistics {
        Statistics {
            stats: HashMap::new(),
        }
    }
}


impl Statistics {
    pub fn increment_statistic(&mut self, name: &[u8], value: i64) {
        if let Some(stat) = self.stats.get_mut(name) {
            *stat += value;
            return;
        }

        self.stats.insert(name.to_vec(), value);
    }

    pub fn get_statistic(&self, name: &[u8]) -> Option<i64> {
        self.stats.get(&name.to_vec()).cloned()
    }
}
