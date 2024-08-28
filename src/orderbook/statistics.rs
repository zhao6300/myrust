use std::{cmp, i64};

use serde::{Deserialize, Serialize};

use super::Side;

#[derive(Serialize, Deserialize, Debug)]
pub struct Statistics {
    ///提交的总的买入委托数量
    pub total_bid_num: usize,
    ///提交的总的卖出委托数量
    pub total_ask_num: usize,
    ///总的撤单委托
    pub total_cancel: usize,
    ///总的买入成交额
    pub total_bid_tick: i64,
    ///总的卖出成交额
    pub total_ask_tick: i64,
    ///总的买入成交量
    pub total_bid_vol: i64,
    ///总的卖出成交量
    pub total_ask_vol: i64,
    ///总的买入成交委托单
    pub total_bid_order: i64,
    ///总的卖出成交委托单
    pub total_ask_order: i64,
    ///最高成交价
    pub high: i64,
    ///最低成交价
    pub low: i64,
}

impl Statistics {
    pub fn new() -> Self {
        Self {
            total_bid_num: 0,
            total_ask_num: 0,
            total_cancel: 0,
            total_bid_tick: 0,
            total_ask_tick: 0,
            total_bid_vol: 0,
            total_ask_vol: 0,
            total_bid_order: 0,
            total_ask_order: 0,
            high: i64::MIN,
            low: i64::MAX,
        }
    }

    pub fn total_volume(&self) -> i64 {
        self.total_bid_vol + self.total_ask_vol
    }

    pub fn total_price(&self) -> i64 {
        self.total_bid_tick + self.total_ask_tick
    }

    pub fn avg_bid_price(&self) -> i64 {
        self.total_price() / self.total_volume()
    }

    pub fn high(&self) -> i64 {
        self.high
    }

    pub fn low(&self) -> i64 {
        self.low
    }

    pub fn add_total_qty(&mut self, side: Side, filedd: i64) {
        match side {
            Side::Buy => self.total_bid_vol += filedd,
            Side::Sell => self.total_ask_vol += filedd,
            _ => (),
        }
    }

    pub fn update_high_low(&mut self, price_tick: i64) {
        self.high = cmp::max(self.high, price_tick);
        self.low = cmp::min(self.low, price_tick);
    }
}
