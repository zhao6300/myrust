use super::statistics::StatisticsInfo;
use super::*;
use std::any::Any;
#[derive(Debug)]
pub struct Hook {
    pub object: Rc<RefCell<dyn Any>>,
    pub handler: OrderbookHook,
    pub max_level: i64,
}

pub type OrderbookHook = fn(
    &Rc<RefCell<dyn Any>>,
    &StatisticsInfo,       // aggregated info
    &Vec<(f64, f64, i64)>, // bid orderbook
    &Vec<(f64, f64, i64)>, // ask orderbook
    l3order: &L3OrderRef,  // current order info
) -> bool;

#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize, Hash)]
#[repr(u8)]
pub enum HookType {
    Orderbook = 0,
}
