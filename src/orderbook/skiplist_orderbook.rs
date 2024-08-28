use super::skiplist_helper::skiplist_serde;
use super::types::ExchangeMode;
use super::*;
use chrono::{Duration, NaiveDate, NaiveDateTime, Utc};
use polars::export::num::ToPrimitive;
use polars::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use skiplist::SkipMap;
use statistics::Statistics;
use std::collections::VecDeque;

use std::cmp;
use std::collections::{hash_map::Entry, HashMap};
use std::time;
use std::{cell::RefCell, rc::Rc};

#[derive(Serialize, Deserialize, Debug)]
pub struct PriceLevel {
    pub mode: ExchangeMode,
    pub orders: VecDeque<Option<L3OrderRef>>,
    pub vol: i64,
    pub vol_shadow: i64,
    pub count: i64,
}

impl PriceLevel {
    pub fn new(mode: ExchangeMode) -> Self {
        Self {
            mode: mode,
            orders: VecDeque::new(),
            vol: 0,
            vol_shadow: 0,
            count: 0,
        }
    }

    pub fn add_order(&mut self, order_ref: L3OrderRef) -> Result<bool, MarketError> {
        self.orders.push_back(Some(Rc::clone(&order_ref)));
        let mut order = order_ref.borrow_mut();
        order.idx = self.orders.len();
        self.vol_shadow += order.vol_shadow;
        if self.mode == ExchangeMode::Live || order.source == OrderSourceType::LocalOrder {
            self.vol += order.vol;
        }
        self.count += 1;
        Ok(true)
    }

    pub fn delete_order(&mut self, order_ref: &L3OrderRef) -> Result<bool, MarketError> {
        let order = RefCell::borrow(order_ref);
        self.orders[order.idx - 1] = None;

        if self.mode == ExchangeMode::Live || order.source == OrderSourceType::LocalOrder {
            self.vol -= order.vol;
        }
        self.vol_shadow -= order.vol_shadow;
        self.count -= 1;
        Ok(true)
    }

    pub fn clear(&mut self) {
        self.orders.clear();
    }
    /// 返回成交量
    pub fn match_order(&mut self, order: L3OrderRef) -> Result<i64, MarketError> {
        match self.mode {
            ExchangeMode::Backtest => self.shadow_match(order),
            ExchangeMode::Live => self.live_match(order),
            _ => Err(MarketError::ExchangeModeUnsupproted),
        }
    }

    pub fn shadow_match(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let mut filled: i64 = 0;
        for idx in 0..self.orders.len() {
            let other_ref = match &self.orders[idx] {
                Some(value) => value.clone(),
                None => continue,
            };
            let mut order = order_ref.borrow_mut();
            let mut other = other_ref.borrow_mut();

            if order.account.is_some() && other.account.is_some() && order.account == other.account
            {
                break;
            }

            if order.source == OrderSourceType::LocalOrder {
                if other.source == OrderSourceType::LocalOrder {
                    if order.vol >= other.vol {
                        filled += other.vol;
                        order.vol -= other.vol;
                        self.vol -= other.vol;
                        self.vol_shadow -= other.vol_shadow;
                        //order在多个level匹配时，可能先与用户订单匹配，然后再与本地订单匹配
                        order.vol_shadow = cmp::min(order.vol_shadow.clone(), order.vol.clone());
                        other.vol = 0;
                        self.orders[idx] = None;
                        self.count -= 1;
                    } else {
                        filled += order.vol;
                        other.vol -= order.vol;
                        self.vol -= order.vol;
                        self.vol_shadow -= other.vol_shadow;
                        other.vol_shadow = cmp::min(other.vol_shadow.clone(), other.vol.clone());
                        self.vol_shadow += other.vol_shadow;
                        order.vol = 0;
                    }
                } else if other.source == OrderSourceType::UserOrder {
                    if order.vol_shadow >= other.vol {
                        filled += other.vol;
                        order.vol_shadow -= other.vol;
                        self.vol_shadow -= other.vol;
                        self.orders[idx] = None;
                        other.vol = 0;
                        self.count -= 1;
                    } else {
                        filled += order.vol_shadow;
                        other.vol -= order.vol_shadow;
                        self.vol_shadow -= order.vol_shadow;
                        order.vol_shadow = 0;
                    }
                }
            } else if order.source == OrderSourceType::UserOrder {
                if other.source == OrderSourceType::LocalOrder {
                    if order.vol >= other.vol_shadow {
                        filled += other.vol_shadow;
                        order.vol -= other.vol_shadow;
                        self.vol_shadow -= other.vol_shadow;
                        other.vol_shadow = 0;
                    } else {
                        filled += order.vol;
                        other.vol_shadow -= order.vol;
                        self.vol_shadow -= order.vol;
                        order.vol = 0;
                    }
                } else if other.source == OrderSourceType::UserOrder {
                    if order.vol >= other.vol {
                        filled += other.vol;
                        self.vol_shadow -= other.vol;
                        other.vol = 0;
                        self.orders[idx] = None;
                        self.count -= 1
                    } else {
                        filled += order.vol;
                        other.vol -= order.vol;
                        self.vol_shadow -= order.vol;
                        order.vol = 0;
                    }
                }
            }

            if order.vol == 0 {
                break;
            }
        }

        Ok(filled)
    }
    pub fn live_match(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let mut filled: i64 = 0;
        for idx in 1..self.orders.len() {
            let other_ref = match &self.orders[idx] {
                Some(value) => value.clone(),
                None => continue,
            };
            let mut order = order_ref.borrow_mut();
            let mut other = other_ref.borrow_mut();

            if order.account != other.account {
                if order.vol >= other.vol {
                    filled += other.vol;
                    order.vol -= other.vol;
                    other.vol = 0;
                    self.orders[idx] = None;
                    self.count -= 1;
                } else {
                    filled += order.vol;
                    other.vol -= order.vol;
                    order.vol = 0;
                }
            }

            if order.vol == 0 {
                break;
            }
        }
        self.vol -= filled;
        Ok(filled)
    }
}
#[derive(Serialize, Deserialize, Debug)]
pub struct SkipListMarketDepth {
    #[serde(with = "skiplist_serde")]
    pub ask_depth: SkipMap<PriceTick, PriceLevel>,
    #[serde(with = "skiplist_serde")]
    pub bid_depth: SkipMap<PriceTick, PriceLevel>,
    pub tick_size: f64,
    pub lot_size: f64,
    pub timestamp: i64,
    pub best_bid_tick: i64,
    pub best_ask_tick: i64,
    pub last_tick: i64,
    pub orders: HashMap<OrderId, L3OrderRef>,
    pub mode: ExchangeMode,

    pub market_statistics: Statistics,
}

impl SkipListMarketDepth {
    fn new(mode: ExchangeMode, tick_size: f64, lot_size: f64) -> Self {
        Self {
            ask_depth: SkipMap::new(),
            bid_depth: SkipMap::new(),
            tick_size: tick_size,
            lot_size: lot_size,
            timestamp: 0,
            best_bid_tick: INVALID_MIN,
            best_ask_tick: INVALID_MAX,
            last_tick: INVALID_MIN,
            orders: HashMap::new(),
            mode: mode,
            market_statistics: Statistics::new(),
        }
    }

    fn statistics_mut(&mut self) -> &mut Statistics {
        &mut self.market_statistics
    }

    fn last_tick(&self) -> i64 {
        self.last_tick
    }
}

impl MarketDepth for SkipListMarketDepth {
    fn new_box(mode: ExchangeMode, tick_size: f64, lot_size: f64) -> Box<Self> {
        Box::new(Self::new(mode, tick_size, lot_size))
    }

    #[inline(always)]
    fn best_bid(&self) -> f64 {
        if self.best_bid_tick == INVALID_MIN {
            f64::NAN
        } else {
            self.best_bid_tick as f64 * self.tick_size
        }
    }

    #[inline(always)]
    fn best_ask(&self) -> f64 {
        if self.best_ask_tick == INVALID_MAX {
            f64::NAN
        } else {
            self.best_ask_tick as f64 * self.tick_size
        }
    }

    #[inline(always)]
    fn best_bid_tick(&self) -> i64 {
        self.best_bid_tick
    }

    #[inline(always)]
    fn best_ask_tick(&self) -> i64 {
        self.best_ask_tick
    }

    #[inline(always)]
    fn tick_size(&self) -> f64 {
        self.tick_size
    }

    #[inline(always)]
    fn lot_size(&self) -> f64 {
        self.lot_size
    }

    #[inline(always)]
    fn bid_vol_at_tick(&self, price_tick: i64) -> i64 {
        let price_level = self
            .bid_depth
            .get(&PriceTick::new(price_tick, true))
            .unwrap();
        match self.mode {
            ExchangeMode::Backtest => price_level.vol_shadow,
            _ => price_level.vol,
        }
    }

    #[inline(always)]
    fn ask_vol_at_tick(&self, price_tick: i64) -> i64 {
        let price_level = self
            .ask_depth
            .get(&PriceTick::new(price_tick, false))
            .unwrap();
        match self.mode {
            ExchangeMode::Backtest => price_level.vol_shadow,
            _ => price_level.vol,
        }
    }

    fn add(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let order = match self.orders.entry(order_ref.borrow().order_id) {
            Entry::Occupied(_) => return Err(MarketError::OrderIdExist),
            Entry::Vacant(entry) => entry.insert(order_ref.clone()),
        };
        let mut best_tick: i64 = 0;
        let price_tick = order.borrow().price_tick;
        if order.borrow().side == Side::Buy {
            let price_level = match self.bid_depth.get_mut(&price_tick) {
                Some(value) => value,
                None => {
                    self.bid_depth
                        .insert(price_tick.clone(), PriceLevel::new(self.mode.clone()));

                    self.bid_depth.get_mut(&price_tick).unwrap()
                }
            };
            let _ = price_level.add_order(order.clone());
            self.best_bid_tick = cmp::max(self.best_bid_tick, price_tick.price_tick);
            best_tick = self.best_bid_tick.clone();
            self.market_statistics.total_bid_order += 1;
        } else {
            let price_level = match self.ask_depth.get_mut(&price_tick) {
                Some(value) => value,
                None => {
                    self.ask_depth
                        .insert(price_tick.clone(), PriceLevel::new(self.mode.clone()));
                    self.ask_depth.get_mut(&price_tick).unwrap()
                }
            };
            let _ = price_level.add_order(order.clone());
            self.best_ask_tick = cmp::min(self.best_ask_tick, price_tick.price_tick);
            best_tick = self.best_ask_tick.clone();
            self.market_statistics.total_ask_order += 1;
        }
        Ok(best_tick)
    }

    fn match_order(&mut self, order_ref: L3OrderRef, max_depth: i64) -> Result<i64, MarketError> {
        let side = order_ref.borrow().side.clone();
        let filled = match side {
            Side::Buy => self.match_ask_depth(order_ref.clone(), max_depth),
            Side::Sell => self.match_bid_depth(order_ref.clone(), max_depth),
            _ => return Err(MarketError::MarketSideError),
        };
        filled
    }

    fn match_bid_depth(
        &mut self,
        order_ref: L3OrderRef,
        max_depth: i64,
    ) -> Result<i64, MarketError> {
        let mut filled: i64 = 0;
        let mut count = 1;
        for (price_tick, price_level) in &mut self.bid_depth {
            if count > max_depth
                || &order_ref.borrow().price_tick.price_tick > &price_tick.price_tick
                || order_ref.borrow().vol == 0
            {
                break;
            }

            let this_filled = price_level.match_order(order_ref.clone()).unwrap();
            filled += this_filled;
            count += 1;
            self.last_tick = price_tick.price_tick;
            self.market_statistics.total_bid_vol += this_filled;
            self.market_statistics.total_bid_tick += (filled * price_tick.price_tick);
            self.market_statistics
                .update_high_low(price_tick.price_tick);
        }

        self.best_bid_tick = self.update_bid_depth()?;
        Ok(filled)
    }

    fn match_ask_depth(
        &mut self,
        order_ref: L3OrderRef,
        max_depth: i64,
    ) -> Result<i64, MarketError> {
        let mut filled: i64 = 0;

        let mut count = 0;
        for (price_tick, price_level) in &mut self.ask_depth {
            count += 1;
            if count > max_depth
                || &order_ref.borrow().price_tick.price_tick < &price_tick.price_tick
                || order_ref.borrow().vol == 0
            {
                break;
            }
            let this_filled = price_level.match_order(order_ref.clone()).unwrap();
            filled += this_filled;

            self.last_tick = price_tick.price_tick;
            self.market_statistics.total_ask_vol += this_filled;
            self.market_statistics.total_ask_tick += (filled * price_tick.price_tick);
            self.market_statistics
                .update_high_low(price_tick.price_tick);
        }

        self.best_ask_tick = self.update_ask_depth()?;

        Ok(filled)
    }
}

impl L3MarketDepth for SkipListMarketDepth {
    type Error = MarketError;

    fn add_buy_order(
        &mut self,
        source: OrderSourceType,
        account: Option<String>,
        order_id: OrderId,
        price: f64,
        vol: i64,
        timestamp: i64,
    ) -> Result<(i64, i64), Self::Error> {
        let price_tick = (price / self.tick_size).round() as i64;
        let order_ref = L3OrderRef::new(RefCell::new(L3Order::new(
            source,
            account,
            order_id,
            Side::Buy,
            price_tick,
            vol,
            timestamp,
        )));
        self.add(order_ref)?;
        let prev_best_tick = self.best_bid_tick;
        if price_tick > self.best_bid_tick {
            self.best_bid_tick = price_tick;
        }
        Ok((prev_best_tick, self.best_bid_tick))
    }

    fn add_sell_order(
        &mut self,
        source: OrderSourceType,
        account: Option<String>,
        order_id: OrderId,
        price: f64,
        vol: i64,
        timestamp: i64,
    ) -> Result<(i64, i64), Self::Error> {
        let price_tick = (price / self.tick_size).round() as i64;
        let order_ref = L3OrderRef::new(RefCell::new(L3Order::new(
            source,
            account,
            order_id,
            Side::Sell,
            price_tick,
            vol,
            timestamp,
        )));
        self.add(order_ref)?;
        let prev_best_tick = self.best_bid_tick;
        if price_tick < self.best_ask_tick {
            self.best_ask_tick = price_tick;
        }
        Ok((prev_best_tick, self.best_ask_tick))
    }

    fn update_bid_depth(&mut self) -> Result<i64, MarketError> {
        loop {
            match self.bid_depth.front_mut() {
                Some((price_tick, price_level)) => {
                    if price_level.count == 0 {
                        self.bid_depth.pop_front();
                    } else {
                        self.best_bid_tick = price_tick.price_tick.clone();
                        break;
                    }
                }
                None => {
                    self.best_bid_tick = INVALID_MIN;
                    break;
                }
            }
        }

        Ok(self.best_bid_tick)
    }

    fn update_ask_depth(&mut self) -> Result<i64, MarketError> {
        loop {
            match self.ask_depth.front_mut() {
                Some((price_tick, price_level)) => {
                    if price_level.count == 0 {
                        self.ask_depth.pop_front();
                    } else {
                        self.best_ask_tick = price_tick.price_tick.clone();
                        break;
                    }
                }
                None => {
                    self.best_ask_tick = INVALID_MAX;
                    break;
                }
            }
        }

        Ok(self.best_ask_tick)
    }

    fn cancel_order(
        &mut self,
        order_id: OrderId,
        timestamp: i64,
    ) -> Result<(Side, i64, i64), Self::Error> {
        let order_rc = self
            .orders
            .remove(&order_id)
            .ok_or(MarketError::OrderNotFound)?;

        let order = order_rc.borrow();

        if order.side == Side::Buy {
            let prev_best_tick = self.best_bid_tick;
            let price_level = self.bid_depth.get_mut(&order.price_tick).unwrap();

            let _ = price_level.delete_order(&order_rc);
            self.best_bid_tick = self.update_bid_depth().unwrap();
            Ok((Side::Buy, prev_best_tick, self.best_bid_tick))
        } else {
            let prev_best_tick = self.best_ask_tick;
            let price_level = self.ask_depth.get_mut(&order.price_tick).unwrap();

            let _ = price_level.delete_order(&order_rc);
            self.best_ask_tick = self.update_ask_depth().unwrap();
            Ok((Side::Sell, prev_best_tick, self.best_ask_tick))
        }
    }

    fn modify_order(
        &mut self,
        order_id: OrderId,
        price: f64,
        vol: f64,
        timestamp: i64,
    ) -> Result<(Side, i64, i64), Self::Error> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(MarketError::OrderNotFound)?;
        let order = order.borrow();
        if order.side == Side::Buy {
            let prev_best_tick = self.best_bid_tick;
            let price_tick = (price / self.tick_size).round() as i64;
            Ok((Side::Buy, prev_best_tick, self.best_bid_tick))
        } else {
            let prev_best_tick = self.best_ask_tick;
            let price_tick = (price / self.tick_size).round() as i64;
            Ok((Side::Sell, self.best_ask_tick, self.best_ask_tick))
        }
    }

    fn clear_orders(&mut self, side: Side) {}

    fn orders(&self) -> &HashMap<OrderId, L3OrderRef> {
        &self.orders
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use SkipListMarketDepth;

    #[test]
    fn test_new() {
        let depth = SkipListMarketDepth::new(ExchangeMode::Backtest, 0.01, 100.0);
    }

    #[test]
    fn test_price_level() {
        let mut price_level_backtest = PriceLevel::new(ExchangeMode::Backtest);
        let mut price_level_live = PriceLevel::new(ExchangeMode::Backtest);

        for i in 1..=2 {
            let order_ref = L3Order::new_ref(
                OrderSourceType::LocalOrder,
                Some("user1".to_string()),
                i,
                Side::Buy,
                100,
                100,
                1,
            );
            price_level_backtest.add_order(order_ref);
        }
        print!("{:?}\n", price_level_backtest);

        let order_ref = L3Order::new_ref(
            OrderSourceType::LocalOrder,
            Some("user2".to_string()),
            10,
            Side::Buy,
            100,
            120,
            1,
        );

        price_level_backtest.match_order(order_ref);

        let order_ref = L3Order::new_ref(
            OrderSourceType::UserOrder,
            Some("user2".to_string()),
            11,
            Side::Buy,
            100,
            100,
            1,
        );
        price_level_backtest.match_order(order_ref);
        print!("{:?}\n", price_level_backtest);
    }

    #[test]
    fn test_add_order() {
        let mut depth = SkipListMarketDepth::new(ExchangeMode::Backtest, 0.01, 100.0);
        let order_ref = L3Order::new_ref(
            OrderSourceType::UserOrder,
            Some("user1".to_string()),
            1,
            Side::Buy,
            100,
            100,
            1,
        );
        depth.add(order_ref);
        let order_ref = L3Order::new_ref(
            OrderSourceType::UserOrder,
            Some("user1".to_string()),
            1,
            Side::Sell,
            100,
            100,
            1,
        );
        depth.add(order_ref);
        print!("{:?}\n", depth);
    }

    #[test]
    fn test_match_bid() {
        let mut depth = SkipListMarketDepth::new(ExchangeMode::Backtest, 0.01, 100.0);
        let order_ref = L3Order::new_ref(
            OrderSourceType::LocalOrder,
            Some("user1".to_string()),
            1,
            Side::Buy,
            100,
            100,
            1,
        );
        depth.add(order_ref);

        print!("{:?}\n", depth);
        let order_sell = L3Order::new_ref(
            OrderSourceType::LocalOrder,
            Some("user2".to_string()),
            100,
            Side::Sell,
            100,
            90,
            1,
        );
        let filled = depth.match_bid_depth(order_sell.clone(), 100);
        print!("{:?}\n", depth);
        print!("{:?}\n", filled.unwrap());
    }

    #[test]
    fn test_local_match() {
        let mut depth = SkipListMarketDepth::new(ExchangeMode::Backtest, 0.01, 100.0);

        for i in 1..=2 {
            let order_ref = L3Order::new_ref(
                OrderSourceType::LocalOrder,
                Some("user1".to_string()),
                i,
                Side::Buy,
                100,
                100,
                1,
            );

            depth.add(order_ref);
        }

        for i in 1..=2 {
            let order_ref = L3Order::new_ref(
                OrderSourceType::LocalOrder,
                Some("user1".to_string()),
                i + 10,
                Side::Sell,
                110,
                100,
                1,
            );

            depth.add(order_ref);
        }
        print!("{:?}\n", depth);
        let order_sell = L3Order::new_ref(
            OrderSourceType::LocalOrder,
            Some("user2".to_string()),
            100,
            Side::Sell,
            100,
            120,
            1,
        );
        let filled = depth.match_order(order_sell.clone(), 100);

        let order_sell = L3Order::new_ref(
            OrderSourceType::UserOrder,
            Some("user2".to_string()),
            120,
            Side::Buy,
            110,
            120,
            1,
        );
        let filled = depth.match_order(order_sell.clone(), 100);

        print!("{:?}\n", depth);
        print!("filled = {:?}\n", filled.unwrap());
    }

    #[test]
    fn test_update() {
        let mut depth = SkipListMarketDepth::new(ExchangeMode::Backtest, 0.01, 100.0);
        for i in 0..=2 {
            let order_ref = L3Order::new_ref(
                OrderSourceType::LocalOrder,
                Some("user1".to_string()),
                i,
                Side::Buy,
                100 + i as i64,
                100,
                1,
            );

            depth.add(order_ref);
        }
        depth.update_bid_depth();
        depth.update_ask_depth();
    }

    #[test]
    fn test_multiple_depth() {
        let mut depth = SkipListMarketDepth::new(ExchangeMode::Backtest, 0.01, 100.0);

        for i in 0..=2 {
            let order_ref = L3Order::new_ref(
                OrderSourceType::LocalOrder,
                Some("user1".to_string()),
                i,
                Side::Buy,
                100 + i as i64,
                100,
                1,
            );

            depth.add(order_ref);
        }
        print!("{:?}\n", depth);
        let order_sell = L3Order::new_ref(
            OrderSourceType::UserOrder,
            Some("user2".to_string()),
            100,
            Side::Sell,
            100,
            120,
            1,
        );
        let filled = depth.match_order(order_sell.clone(), 100);
        print!("{:?}\n", depth);
        print!("{:?}\n", depth.market_statistics);
    }
}
