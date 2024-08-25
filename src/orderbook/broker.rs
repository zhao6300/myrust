use super::dataloader::DataCollator;
use super::*;
use polars::export::num::ToPrimitive;
use polars::prelude::*;
use rayon::{prelude::*, vec};
use skiplist::SkipMap;
use std::borrow::BorrowMut;
use std::collections::VecDeque;
use std::collections::{hash_map::Entry, HashMap};
use std::i64;
pub struct Broker<MD> {
    pub mode: ExchangeMode,
    pub stock_code: String,
    pub market_depth: Box<MD>,
    pub pending_orders: VecDeque<OrderRef>,
    pub waiting_orders: SkipMap<i64, VecDeque<OrderRef>>,
    pub timestamp: i64,
    pub orders: HashMap<OrderId, OrderRef>,
    pub latest_order_number: OrderId,
    pub tick_size: f64,
    pub lot_size: f64,
    pub quotes: HashMap<String, DataCollator>,
}

impl<MD> Broker<MD>
where
    MD: MarketDepth,
{
    pub fn new(mode: ExchangeMode, stock_code: String, tick_size: f64, lot_size: f64) -> Self {
        let restrict_aggressive_order =
            !stock_code.is_empty() && stock_code.chars().nth(0) == Some('3');
        let exchange_code = if stock_code.ends_with("SH") {
            "SH".to_string()
        } else {
            "SZ".to_string()
        };

        Self {
            mode: mode,
            stock_code: stock_code,
            market_depth: MD::new_box(mode.clone(), tick_size.clone(), lot_size.clone()),
            pending_orders: VecDeque::new(),
            waiting_orders: SkipMap::new(),
            timestamp: 0,
            orders: HashMap::new(),
            latest_order_number: 0,
            tick_size: tick_size,
            lot_size: lot_size,
            quotes: HashMap::new(),
        }
    }

    pub fn generate_order_number(&mut self) -> OrderId {
        self.latest_order_number = self.latest_order_number - 1;
        self.latest_order_number
    }

    pub fn add_data(
        &mut self,
        name: String,
        datasource: DataCollator,
    ) -> Result<bool, MarketError> {
        match self.quotes.contains_key(&name) {
            true => return Err(MarketError::StockDataExist),
            false => self.quotes.insert(name, datasource).unwrap(),
        };
        Ok(true)
    }

    pub fn match_order_l(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let filled = self.market_depth.match_order(order_ref.clone(), i64::MAX)?;
        if order_ref.borrow().vol > 0 {
            self.market_depth.add(order_ref);
        }

        Ok(filled)
    }

    pub fn match_order_m(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let filled = 0;
        Ok(filled)
    }

    pub fn match_order_n(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let filled = 0;
        Ok(filled)
    }

    pub fn match_order_b(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let filled = 0;
        Ok(filled)
    }

    pub fn match_order_c(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let filled = 0;
        Ok(filled)
    }

    pub fn match_order_d(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let filled = 0;
        Ok(filled)
    }

    pub fn match_order(
        &mut self,
        order_type: OrdType,
        source: OrderSourceType,
        account: Option<String>,
        order_id: OrderId,
        price_tick: i64,
        vol: i64,
        side: Side,
        timestamp: i64,
    ) -> Result<i64, MarketError> {
        let l3order_ref =
            L3Order::new_ref(source, account, order_id, side, price_tick, vol, timestamp);
        let result = match order_type {
            OrdType::L => self.match_order_l(l3order_ref),
            OrdType::M => self.match_order_m(l3order_ref),
            OrdType::N => self.match_order_n(l3order_ref),
            OrdType::B => self.match_order_b(l3order_ref),
            OrdType::C => self.match_order_c(l3order_ref),
            OrdType::D => self.match_order_d(l3order_ref),
            _ => Err(MarketError::OrderTypeUnsupported),
        };

        result
    }

    pub fn submit_order(&mut self, order_ref: OrderRef) -> Result<OrderId, MarketError> {
        match self.orders.contains_key(&(order_ref.borrow().order_id)) {
            true => return Err(MarketError::OrderIdExist),
            false => self
                .orders
                .insert(order_ref.borrow().order_id.clone(), order_ref.clone()),
        };

        let order_number = self.generate_order_number();
        let mut order_mut = RefCell::borrow_mut(&order_ref);
        order_mut.order_id = order_number;
        order_mut.price_tick = (order_mut.price / self.tick_size).round() as i64;

        if order_mut.local_time > self.timestamp {
            let queue = match self.waiting_orders.get_mut(&order_mut.price_tick) {
                Some(q) => q,
                None => &mut self
                    .waiting_orders
                    .insert(order_mut.price_tick.clone(), VecDeque::new())
                    .unwrap(),
            };
            queue.push_back(order_ref.clone());
        } else {
            self.pending_orders.push_back(order_ref.clone());
        }

        Ok(order_number)
    }

    pub fn elapse(&mut self, duration: i64) -> Result<bool, MarketError> {
        Ok(true)
    }

    pub fn cancel_order(&mut self, order_number: i64) -> Result<bool, MarketError> {
        Ok(true)
    }
}
