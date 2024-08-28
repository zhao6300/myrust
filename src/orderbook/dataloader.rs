use super::*;
#[derive(Debug)]
pub struct DataCollator {
    is_last: bool,
}

impl DataCollator {
    pub fn new() -> Self {
        Self { is_last: true }
    }
}

impl OrderIter for DataCollator {
    type Item = OrderRef;

    fn next(&self) -> Option<&Self::Item> {
        None
    }

    fn is_last(&self) -> bool {
        self.is_last
    }
}
