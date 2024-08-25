use super::*;

pub struct DataCollator {}

impl DataCollator {
    pub fn new() -> Self {
        Self {}
    }
}

impl OrderIter for DataCollator {
    type Item = L3OrderRef;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
