use super::*;

use skiplist::SkipMap;
use std::{cell::RefCell, rc::Rc};

pub type Key = i64;
#[derive(Debug)]
struct Depth<T> {
    pub sorted_map: SkipMap<i64, Rc<RefCell<T>>>,
    pub scan_vec: Vec<(Key, Option<Rc<RefCell<T>>>)>,
    pub capacity: usize,
    pub len: usize,
}

impl<T> Depth<T>
where
    T: PriceLevelOp,
{
    pub fn new() -> Self {
        let capacity = 200;
        Self {
            sorted_map: SkipMap::with_capacity(capacity),
            scan_vec: vec![(Key::MAX, None); capacity * 3],
            capacity: capacity,
            len: 0,
        }
    }

    pub fn insert(&mut self, key: Key, value: Rc<RefCell<T>>) {
        self.sorted_map.insert(key, value.clone());
        self.scan_vec[self.len] = (key, Some(value));
        self.len += 1;
    }

    pub fn get(&self, key: &Key) -> Option<&Rc<RefCell<T>>> {
        self.sorted_map.get(key)
    }

    pub fn get_mut(&mut self, key: &Key) -> Option<&mut Rc<RefCell<T>>> {
        self.sorted_map.get_mut(key)
    }

    pub fn remove(&mut self, key: &Key) -> Option<Rc<RefCell<T>>> {
        if let Some(value) = self.sorted_map.remove(key) {
            value.borrow_mut().set_deleted();
            Some(value)
        } else {
            None
        }
    }

    pub fn only_get_from_skipmap(
        &mut self,
        output: &mut Vec<(f64, f64, i64)>,
        tick_size: f64,
        lot_size: f64,
        use_shadow: bool,
    ) {
        for (price_tick, level) in self.sorted_map.iter() {
            let (vol, vol_shadow, count) = level.borrow().get_level_info();
            let price = price_tick.abs() as f64 * tick_size;
            let qty = if use_shadow {
                vol_shadow as f64 * lot_size
            } else {
                vol as f64 * lot_size
            };
            if qty > 0.0 {
                output.push((price, qty, count));
            }
        }
    }

    pub fn get_orderbook_level(
        &mut self,
        output: &mut Vec<(f64, f64, i64)>,
        tick_size: f64,
        lot_size: f64,
        use_shadow: bool,
    ) {
        let mut idx = 0;
        self.scan_vec.sort_by(|a, b| a.0.cmp(&b.0));
        let mut merged = 0;
        while idx < self.len {
            let (price, _) = self.scan_vec[idx];
            let mut newest: usize = idx + 1;
            while newest < self.len && self.scan_vec[newest].0 == price {
                self.scan_vec[idx] = (i64::MAX, None);
                idx += 1;
                newest += 1;
                merged += 1;
            }

            let (price_tick, order_ref) = self.scan_vec.get(idx).unwrap();

            if !order_ref.as_ref().unwrap().borrow().is_deleted() {
                let (vol, vol_shadow, count) =
                    order_ref.as_ref().unwrap().borrow().get_level_info();
                let price = price_tick.abs() as f64 * tick_size;
                let qty = if use_shadow {
                    vol_shadow as f64 * lot_size
                } else {
                    vol as f64 * lot_size
                };

                if qty > 0.0 {
                    output.push((price, qty, count));
                }
            }

            idx += 1;
        }
        self.scan_vec.sort_by(|a, b| a.0.cmp(&b.0));
        self.len -= merged;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Instant;
    // Assuming these are the traits that PriceLevel implements
    use super::PriceLevelOp;

    #[derive(Debug, Clone)]
    pub struct MockPriceLevel {
        price: i64,
        vol: i64,
        order_count: i64,
        deleted: bool,
    }

    impl MockPriceLevel {
        pub fn new(price: i64, vol: i64, count: i64) -> Self {
            MockPriceLevel {
                price: price,
                vol: vol,
                order_count: count,
                deleted: false,
            }
        }
    }

    impl PriceLevelOp for MockPriceLevel {
        fn get_level_info(&self) -> (i64, i64, i64) {
            (self.price, self.vol, self.order_count)
        }

        fn is_deleted(&self) -> bool {
            self.deleted
        }

        fn set_deleted(&mut self) {
            self.deleted = true;
        }
    }

    fn setup_depth() -> Depth<MockPriceLevel> {
        let mut depth = Depth::new();
        for i in 0..50 {
            let price_level = Rc::new(RefCell::new(MockPriceLevel::new(i, i * 10, i * 100)));
            depth.insert(i, price_level);
        }
        depth
    }

    #[test]
    fn test_insert() {
        let start = Instant::now();
        let depth = setup_depth();
        let duration = start.elapsed();
        println!("Execution time: {:?}", duration);
    }

    #[test]
    fn test_only_get_from_skipmap() {
        let mut output: Vec<(f64, f64, i64)> = Vec::with_capacity(100);
        let mut depth = setup_depth();
        let start = Instant::now();
        depth.only_get_from_skipmap(&mut output, 0.01, 1.0, false);
        let duration = start.elapsed();
        println!("Execution time: {:?}", duration);
        // print!("{output:?}")
    }

    #[test]
    fn test_get_orderbook_level() {
        let mut output: Vec<(f64, f64, i64)> = Vec::with_capacity(100);
        let mut depth = setup_depth();
        let start = Instant::now();
        depth.get_orderbook_level(&mut output, 0.01, 1.0, false);
        let duration = start.elapsed();
        println!("Execution time: {:?}", duration);
        // print!("{output:?}")
    }
}
