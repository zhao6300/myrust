use super::dataloader::DataCollator;
use super::*;

use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    fmt::Debug,
};

use super::order::{Order, OrderRef};

#[derive(Debug)]
pub struct Broker<MD> {
    pub mode: ExchangeMode,
    pub stock_type: String,
    pub stock_code: String,
    pub market_depth: Box<MD>,
    pub pending_orders: VecDeque<OrderRef>,
    pub waiting_orders: VecDeque<(i64, OrderRef)>,
    pub timestamp: i64,
    pub orders: HashMap<OrderId, OrderRef>,
    pub latest_seq_number: i64,
    pub tick_size: f64,
    pub lot_size: f64,
    /// 历史数据源
    pub history: Option<DataCollator>,
    //交易的统计信息
}

impl<MD> Broker<MD>
where
    MD: L3MarketDepth,
    MarketError: From<<MD as L3MarketDepth>::Error>,
{
    pub fn new(
        mode: ExchangeMode,
        stock_type: String,
        stock_code: String,
        tick_size: f64,
        lot_size: f64,
    ) -> Self {
        let restrict_aggressive_order =
            !stock_code.is_empty() && stock_code.chars().nth(0) == Some('3');
        let exchange_code = if stock_code.ends_with("SH") {
            "SH".to_string()
        } else {
            "SZ".to_string()
        };

        Self {
            mode: mode,
            stock_type: stock_type,
            stock_code: stock_code,
            market_depth: MD::new_box(mode.clone(), tick_size.clone(), lot_size.clone()),
            pending_orders: VecDeque::new(),
            waiting_orders: VecDeque::new(),
            timestamp: 0,
            orders: HashMap::new(),
            latest_seq_number: 0,
            tick_size: tick_size,
            lot_size: lot_size,
            history: None,
        }
    }

    pub fn generate_seq_number(&mut self) -> i64 {
        self.latest_seq_number += 1;
        self.latest_seq_number
    }

    pub fn add_data(&mut self, history: Option<DataCollator>) -> Result<bool, MarketError> {
        self.history = history;
        Ok(true)
    }

    pub fn match_order_l(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let filled = self.market_depth.match_order(order_ref.clone(), i64::MAX)?;
        if order_ref.borrow().vol > 0 {
            let best_tick = self.market_depth.add(order_ref)?;
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

    pub fn process_order(
        &mut self,
        order_type: OrdType,
        l3order_ref: L3OrderRef,
    ) -> Result<i64, MarketError> {
        let result = match order_type {
            OrdType::L => self.match_order_l(l3order_ref),
            OrdType::M => self.match_order_m(l3order_ref),
            OrdType::N => self.match_order_n(l3order_ref),
            OrdType::B => self.match_order_b(l3order_ref),
            OrdType::C => self.match_order_c(l3order_ref),
            OrdType::D => self.match_order_d(l3order_ref),
            OrdType::Cancel => self.cancel_order(l3order_ref),
            _ => Err(MarketError::OrderTypeUnsupported),
        };

        result
    }

    pub fn submit_order(&mut self, order_ref: OrderRef) -> Result<usize, MarketError> {
        match self.orders.contains_key(&(order_ref.borrow().order_id)) {
            true => return Err(MarketError::OrderIdExist),
            false => self
                .orders
                .insert(order_ref.borrow().order_id.clone(), order_ref.clone()),
        };

        let mut order_mut = RefCell::borrow_mut(&order_ref);

        order_mut.price_tick = (order_mut.price / self.tick_size).round() as i64;

        if order_mut.local_time > self.timestamp {
            self.waiting_orders
                .push_back((order_mut.local_time, order_ref.clone()));
        } else {
            order_mut.seq = self.generate_seq_number();
            self.pending_orders.push_back(order_ref.clone());
        }
        let queue_position: usize = self.pending_orders.len() + self.waiting_orders.len();
        Ok(queue_position)
    }

    pub fn elapse(self: &'_ mut Self, duration: i64) -> Result<bool, MarketError> {
        let mut time_point = self.timestamp + duration;
        while !self.pending_orders.is_empty() {
            let order_ref = self.pending_orders.pop_front().unwrap();
            let mut order = order_ref.borrow_mut();
            let vol = (order.qty / self.lot_size).round() as i64;
            let l3order_ref = L3Order::new_ref(
                order.source.clone(),
                order.account.clone(),
                order.order_id,
                order.side.clone(),
                order.price_tick.clone(),
                vol,
                order.local_time,
            );
            let fillid = self.process_order(order.order_type, l3order_ref)?;
            order.filled_qty = fillid as f64 * self.lot_size;
        }

        self.waiting_orders.make_contiguous().sort();

        while !self.waiting_orders.is_empty() {
            let timestamp = self.waiting_orders[0].0;
            if timestamp > time_point {
                break;
            }
            let (_, order_ref) = self.waiting_orders.pop_front().unwrap();
            let _ = self.goto(timestamp.clone());
            let mut order = order_ref.borrow_mut();
            let vol = (order.qty / self.lot_size).round() as i64;
            let l3order_ref = L3Order::new_ref(
                order.source.clone(),
                order.account.clone(),
                order.order_id,
                order.side.clone(),
                order.price_tick.clone(),
                vol,
                order.local_time,
            );
            order.seq = self.generate_seq_number();
            let fillid = self.process_order(order.order_type, l3order_ref)?;
            order.filled_qty = fillid as f64 * self.lot_size;
        }

        Ok(true)
    }

    pub fn goto_end_of_day(&mut self) -> Result<bool, MarketError> {
        self.goto(i64::MAX)
    }

    pub fn goto(&mut self, time_point: i64) -> Result<bool, MarketError> {
        if self.history.is_none() {
            return Err(MarketError::HistoryIsNone);
        }

        while self.timestamp < time_point {
            let order_ref = match self.history.as_ref().unwrap().next() {
                Some(value) => value.clone(),
                None => continue,
            };

            let order = order_ref.borrow();
            self.timestamp = order.local_time.clone();
            let vol = (order.qty / self.lot_size).round() as i64;
            let l3order_ref = L3Order::new_ref(
                order.source.clone(),
                order.account.clone(),
                order.order_id,
                order.side.clone(),
                order.price_tick.clone(),
                vol,
                order.local_time,
            );
            let filled = self.process_order(order.order_type, l3order_ref)?;
        }

        Ok(true)
    }

    pub fn cancel_order(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let _ = self
            .market_depth
            .cancel_order(order_ref.borrow().order_id, order_ref.borrow().timestamp)?;
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use order::Order;
    use skiplist_orderbook::SkipListMarketDepth;
    #[test]
    fn test_submit_order() {
        let mode = ExchangeMode::Backtest;
        let stock_code = String::from("stock");
        let account = "user1".to_string();
        let timestamp = 1;
        let price = 11.2;
        let qty = 100.0;
        let bs_flag = "b";
        let order_type = OrdType::L;
        let source = OrderSourceType::LocalOrder;
        let mut broker: Broker<SkipListMarketDepth> =
            Broker::new(mode, "stock".to_string(), "stock".to_string(), 0.01, 100.0);
        let order_ref = Order::new_ref(
            Some(account),
            stock_code,
            timestamp,
            price,
            qty,
            &bs_flag,
            order_type,
            source,
        );
        broker.submit_order(order_ref);
        print!("broker = {:?}\n", broker);
    }
}
