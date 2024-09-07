use std::{cmp, i64};

use serde::{Deserialize, Serialize};

use super::Side;
/// `Statistics` 结构体用于跟踪交易统计信息，包括委托数量、成交额、成交量、成交单等。
///
/// 主要用途是提供对市场订单活动的详细统计信息，如总买入/卖出委托数量、成交总额、最高和最低成交价等。
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
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
    pub open_tick: i64,
    pub close_tick: i64,
    pub previous_close_tick: i64,
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
            open_tick: 0,
            close_tick: 0,
            previous_close_tick: 0,
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
    pub fn avg_price(&self) -> i64 {
        if self.total_volume() == 0 {
            0
        } else {
            self.total_price() / self.total_volume()
        }
    }
    /// 返回当前的最高成交价。
    ///
    /// # 返回
    ///
    /// 返回一个 `i64` 类型的值，表示最高成交价。
    pub fn high(&self) -> i64 {
        self.high
    }

    /// 返回当前的最低成交价。
    ///
    /// # 返回
    ///
    /// 返回一个 `i64` 类型的值，表示最低成交价。
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

pub struct StatisticsInfo {
    pub tick_size: f64,
    pub lot_size: f64,
    pub last_price: f64,
    pub prev_close_price: f64,
    /// 提交的总的买入委托数量
    pub total_bid_num: usize,
    /// 提交的总的卖出委托数量
    pub total_ask_num: usize,
    /// 总的撤单委托数量
    pub total_cancel: usize,
    /// 总的买入成交额
    pub total_bid: f64,
    /// 总的卖出成交额
    pub total_ask: f64,
    /// 总的买入成交量
    pub total_bid_qty: f64,
    /// 总的卖出成交量
    pub total_ask_qty: f64,
    /// 总的买入成交委托单
    pub total_bid_order: i64,
    /// 总的卖出成交委托单
    pub total_ask_order: i64,
    /// 最高成交价
    pub high: f64,
    /// 最低成交价
    pub low: f64,
    /// 平均价格
    pub avg_price: f64,
}

impl StatisticsInfo {
    pub fn new() -> Self {
        Self {
            tick_size: 0.0,
            lot_size: 0.0,
            last_price: 0.0,
            prev_close_price: 0.0,
            total_bid_num: 0,
            total_ask_num: 0,
            total_cancel: 0,
            total_bid: 0.0,
            total_ask: 0.0,
            total_bid_qty: 0.0,
            total_ask_qty: 0.0,
            total_bid_order: 0,
            total_ask_order: 0,
            high: 0.0,
            low: 0.0,
            avg_price: 0.0,
        }
    }

    /// 从 `Statistics` 结构体转换并更新 `StatisticsOut` 的字段值。
    ///
    /// # 参数
    ///
    /// - `statistics`: 一个引用，指向要转换的 `Statistics` 实例。
    /// - `tick_size`: 每个价格跳动的大小。
    /// - `lot_size`: 每手合约的大小。
    pub fn from_statistics(&mut self, statistics: &Statistics, tick_size: f64, lot_size: f64) {
        let keep = 1000.0;
        self.total_bid_num = statistics.total_bid_num;
        self.total_ask_num = statistics.total_ask_num;
        self.total_cancel = statistics.total_cancel;
        self.total_bid = statistics.total_bid_tick as f64 * tick_size;
        self.total_ask = statistics.total_ask_tick as f64 * tick_size;
        self.total_bid_qty = statistics.total_bid_vol as f64 * lot_size;
        self.total_ask_qty = statistics.total_ask_vol as f64 * lot_size;
        self.total_bid_order = statistics.total_bid_order;
        self.total_ask_order = statistics.total_ask_order;
        self.high = statistics.high as f64 * tick_size;
        self.low = statistics.low as f64 * tick_size;
        self.avg_price =
            ((statistics.avg_price() as f64 * tick_size / lot_size) * keep).round() / keep.round();
        self.tick_size = tick_size;
        self.lot_size = lot_size;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistics_new() {
        let stats = Statistics::new();
        assert_eq!(stats.total_bid_num, 0);
        assert_eq!(stats.total_ask_num, 0);
        assert_eq!(stats.total_cancel, 0);
        assert_eq!(stats.total_bid_tick, 0);
        assert_eq!(stats.total_ask_tick, 0);
        assert_eq!(stats.total_bid_vol, 0);
        assert_eq!(stats.total_ask_vol, 0);
        assert_eq!(stats.total_bid_order, 0);
        assert_eq!(stats.total_ask_order, 0);
        assert_eq!(stats.high, i64::MIN);
        assert_eq!(stats.low, i64::MAX);
    }

    #[test]
    fn test_statistics_out_new() {
        let stats_out = StatisticsInfo::new();
        assert_eq!(stats_out.total_bid_num, 0);
        assert_eq!(stats_out.total_ask_num, 0);
        assert_eq!(stats_out.total_cancel, 0);
        assert_eq!(stats_out.total_bid, 0.0);
        assert_eq!(stats_out.total_ask, 0.0);
        assert_eq!(stats_out.total_bid_qty, 0.0);
        assert_eq!(stats_out.total_ask_qty, 0.0);
        assert_eq!(stats_out.total_bid_order, 0);
        assert_eq!(stats_out.total_ask_order, 0);
        assert_eq!(stats_out.high, 0.0);
        assert_eq!(stats_out.low, 0.0);
        assert_eq!(stats_out.avg_price, 0.0);
    }

    #[test]
    fn test_from_statistics() {
        let mut stats = Statistics::new();
        stats.total_bid_num = 10;
        stats.total_ask_num = 15;
        stats.total_cancel = 5;
        stats.total_bid_tick = 5000;
        stats.total_ask_tick = 3000;
        stats.total_bid_vol = 200;
        stats.total_ask_vol = 150;
        stats.total_bid_order = 7;
        stats.total_ask_order = 8;
        stats.high = 120;
        stats.low = 80;

        let tick_size = 0.01;
        let lot_size = 100.0;

        let mut stats_out = StatisticsInfo::new();
        stats_out.from_statistics(&stats, tick_size, lot_size);

        assert_eq!(stats_out.total_bid_num, 10);
        assert_eq!(stats_out.total_ask_num, 15);
        assert_eq!(stats_out.total_cancel, 5);
        assert_eq!(stats_out.total_bid, 50.0); // 5000 * 0.01
        assert_eq!(stats_out.total_ask, 30.0); // 3000 * 0.01
        assert_eq!(stats_out.total_bid_qty, 20000.0); // 200 * 100.0
        assert_eq!(stats_out.total_ask_qty, 15000.0); // 150 * 100.0
        assert_eq!(stats_out.total_bid_order, 7);
        assert_eq!(stats_out.total_ask_order, 8);
        assert_eq!(stats_out.high, 1.20); // 120 * 0.01
        assert_eq!(stats_out.low, 0.80); // 80 * 0.01

        let expected_avg_price =
            ((stats.avg_price() as f64 * tick_size / lot_size) * 1000.0).round() / 1000.0;
        assert_eq!(stats_out.avg_price, expected_avg_price);
    }
}
