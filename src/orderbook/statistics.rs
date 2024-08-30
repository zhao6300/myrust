use std::{cmp, i64};

use serde::{Deserialize, Serialize};

use super::Side;
/// `Statistics` 结构体用于跟踪交易统计信息，包括委托数量、成交额、成交量、成交单等。
///
/// 主要用途是提供对市场订单活动的详细统计信息，如总买入/卖出委托数量、成交总额、最高和最低成交价等。
#[derive(Serialize, Deserialize, Debug)]
pub struct Statistics {
    ///提交的总的买入委托数量
    pub total_bid_num: usize,
    ///提交的总的卖出委托数量
    pub total_ask_num: usize,
    ///总的撤单委托数量
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
    /// 计算并返回总成交量（买入成交量 + 卖出成交量）。
    ///
    /// # 返回
    ///
    /// 返回一个 `i64` 类型的值，表示总成交量。
    pub fn total_volume(&self) -> i64 {
        self.total_bid_vol + self.total_ask_vol
    }
    /// 计算并返回总成交额（买入成交额 + 卖出成交额）。
    ///
    /// # 返回
    ///
    /// 返回一个 `i64` 类型的值，表示总成交额。
    pub fn total_price(&self) -> i64 {
        self.total_bid_tick + self.total_ask_tick
    }
    /// 计算并返回平均买入价格。若总成交量为0，则此方法可能会引发除以0的错误。
    ///
    /// # 返回
    ///
    /// 返回一个 `i64` 类型的值，表示平均买入价格。如果总成交量为0，则返回0。
    pub fn avg_bid_price(&self) -> i64 {
        if self.total_volume() == 0 {
            0
        } else {
            self.total_price() / self.total_volume()
        }
    }

    pub fn high(&self) -> i64 {
        self.high
    }

    pub fn low(&self) -> i64 {
        self.low
    }
    /// 更新总成交量，根据订单方向（买入或卖出）调整对应的成交量。
    ///
    /// # 参数
    ///
    /// - `side`: 订单方向，表示是买入还是卖出。
    /// - `filedd`: 本次成交的数量（以基础货币的最小单位计量）。
    pub fn add_total_qty(&mut self, side: Side, filedd: i64) {
        match side {
            Side::Buy => self.total_bid_vol += filedd,
            Side::Sell => self.total_ask_vol += filedd,
            _ => (),
        }
    }
    /// 更新最高和最低成交价。
    ///
    /// # 参数
    ///
    /// - `price_tick`: 本次成交的价格（以基础货币的最小单位计量）。
    pub fn update_high_low(&mut self, price_tick: i64) {
        self.high = cmp::max(self.high, price_tick);
        self.low = cmp::min(self.low, price_tick);
    }
}
