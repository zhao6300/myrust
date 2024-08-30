use super::skiplist_helper::skiplist_serde;
use super::types::ExchangeMode;
use super::*;
use chrono::{Duration, NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use skiplist::SkipMap;
use statistics::Statistics;
use std::collections::VecDeque;

use std::cmp;
use std::collections::{hash_map::Entry, HashMap};
use std::process::id;
use std::time;
use std::{cell::RefCell, rc::Rc};
/// `PriceLevel` 结构体表示市场中的一个价格层级。一个价格层级包含该价格的所有订单及其相关的状态和交易数据。
#[derive(Serialize, Deserialize, Debug)]
pub struct PriceLevel {
    // 当前的交易模式
    pub mode: ExchangeMode,
    // 存储当前价格层级中的所有订单
    pub orders: VecDeque<Option<L3OrderRef>>,
    // 当前价格层级的总交易量
    pub vol: i64,
    // 当前价格层级的预留交易量，仅在回测模式下使用
    pub vol_shadow: i64,
    // 当前价格层级中的订单总数
    pub count: i64,
}

impl PriceLevel {
    /// 创建一个新的 `PriceLevel` 实例。
    ///
    /// # 参数
    /// - `mode`: 当前的交易模式。可以是 `Live` 或 `Backtest`。
    ///
    /// # 返回值
    /// 返回一个新的 `PriceLevel` 实例，初始化时，订单队列为空，交易量和订单数量均为零。
    pub fn new(mode: ExchangeMode) -> Self {
        Self {
            mode: mode,
            orders: VecDeque::new(),
            vol: 0,
            vol_shadow: 0,
            count: 0,
        }
    }

    /// 将一个订单添加到当前价格层级中。
    ///
    /// 根据交易模式和订单来源更新并调整订单数量。
    ///
    /// # 参数
    /// - `order_ref`: 要添加的订单的引用。
    ///
    /// # 返回值
    /// 如果添加成功，则返回 `Ok(true)`；如果发生错误（如添加失败），则返回相应的 `MarketError`。
    pub fn add_order(&mut self, order_ref: L3OrderRef) -> Result<bool, MarketError> {
        self.orders.push_back(Some(Rc::clone(&order_ref)));
        let mut order = order_ref.borrow_mut();
        order.idx = self.orders.len();
        order.position = order.idx as i64;
        self.vol_shadow += order.vol_shadow;
        if self.mode == ExchangeMode::Live || order.source == OrderSourceType::LocalOrder {
            self.vol += order.vol;
        }
        self.count += 1;
        Ok(true)
    }
    /// 删除当前价格层级中的订单。
    ///
    /// 从订单队列中移除指定的订单，并更新交易量和订单数量。
    ///
    /// # 参数
    /// - `order_ref`: 要删除的订单的引用。
    ///
    /// # 返回值
    /// 如果删除成功，则返回 `Ok(true)`；如果发生错误（如订单未找到），则返回相应的 `MarketError`。
    pub fn delete_order(&mut self, order_ref: &L3OrderRef) -> Result<bool, MarketError> {
        // 获取订单的可变引用
        let mut order = order_ref.borrow_mut();

        // 订单在 `orders` 中的索引
        let idx = order.idx;

        // 验证订单的索引是否有效
        if idx > self.orders.len() || self.orders[idx - 1].as_ref() != Some(order_ref) {
            return Err(MarketError::OrderNotFound);
        }

        self.orders[order.idx - 1] = None;

        if self.mode == ExchangeMode::Live || order.source == OrderSourceType::LocalOrder {
            self.vol -= order.vol;
        }
        self.vol_shadow -= order.vol_shadow;
        self.count -= 1;
        // 标记订单为删除状态
        order.side = Side::None;
        Ok(true)
    }
    /// 更新当前价格层级中所有订单的位置。
    ///
    /// 该方法遍历价格层级中的所有订单，重新计算并更新每个订单的位置。订单的位置是根据订单的来源（市场订单或用户订单）和其在价格层级中的相对位置来确定的。
    ///
    /// - **市场订单**（`OrderSourceType::LocalOrder`）: 其位置是基于市场订单的起始索引和订单在价格层级中的实际索引来计算的。
    /// - **用户订单**（`OrderSourceType::UserOrder`）: 其位置是基于用户订单的起始索引和订单在价格层级中的实际索引来计算的。
    ///
    pub fn update_order_position(&mut self) {
        let mut market_start: i64 = -1;
        let mut user_start: i64 = -1;
        for idx in 0..self.orders.len() {
            if self.orders[idx].is_some() {
                let mut order = self.orders[idx].as_ref().unwrap().borrow_mut();
                if market_start == -1 {
                    market_start = idx as i64;
                }

                if order.vol_shadow > 0 && user_start == -1 {
                    user_start = idx as i64;
                }

                if order.source == OrderSourceType::LocalOrder {
                    order.position = idx as i64 - market_start;
                } else {
                    order.position = idx as i64 - user_start;
                }
            }
        }
    }

    pub fn clear(&mut self) {
        self.orders.clear();
    }
    /// 根据市场模式匹配订单并返回成交量。
    ///
    /// - 在回测模式下，调用 `shadow_match` 方法进行匹配。
    /// - 在实时模式下，调用 `live_match` 方法进行匹配。
    ///
    /// # 参数
    /// - `order`: 要匹配的订单。
    ///
    /// # 返回值
    /// 成功匹配时，返回已成交的总量；如果发生错误（如模式不支持），则返回相应的 `MarketError`。
    pub fn match_order(&mut self, order: L3OrderRef) -> Result<i64, MarketError> {
        match self.mode {
            ExchangeMode::Backtest => self.shadow_match(order),
            ExchangeMode::Live => self.live_match(order),
            _ => Err(MarketError::ExchangeModeUnsupproted),
        }
    }

    /// 在当前价格层级中匹配指定的订单。该方法会遍历同一价格层级中的所有订单，并根据订单的来源和剩余量进行匹配。
    ///
    /// **说明:**
    /// - `vol` 表示订单的实际成交量。每当订单进行匹配时，`vol` 会根据匹配情况减少，同时市场中的总成交量也会减少。
    /// - `vol_shadow` 表示订单的影子成交量。在涉及本地订单和用户订单之间的匹配时，影子成交量用于模拟实际成交量。
    ///
    /// # 参数
    ///
    /// * `order_ref` - 要匹配的订单对象，该订单将在当前价格层级中与其他订单进行匹配。
    ///
    /// # 返回值
    ///
    /// * `Ok(i64)` - 返回成交的总数量。
    /// * `Err(MarketError)` - 如果在匹配过程中发生错误。
    ///
    /// # 错误处理
    ///
    /// 如果在更新市场数据时发生错误，将返回相应的 `MarketError`。

    pub fn shadow_match(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let mut filled: i64 = 0;
        // 遍历当前价格层级中的所有订单
        for idx in 0..self.orders.len() {
            let other_ref = match &self.orders[idx] {
                Some(value) => value.clone(),
                None => continue,
            };
            let mut order = order_ref.borrow_mut();
            let mut other = other_ref.borrow_mut();
            other.dirty = true;
            if order.account.is_some() && other.account.is_some() && order.account == other.account
            {
                continue;
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
                        other.vol_shadow -= other.vol;
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

    /// 在实盘环境中匹配指定的订单，与市场中其他订单进行配对。
    /// 匹配过程中更新订单的成交量，并从市场中移除已完成的订单。
    ///
    /// # 参数
    ///
    /// * `order_ref` - 要匹配的订单对象。
    ///
    /// # 返回值
    ///
    /// * `Ok(i64)` - 返回成交的总数量。
    /// * `Err(MarketError)` - 如果在匹配过程中发生错误。
    ///
    /// # 错误处理
    ///
    /// 如果在更新市场数据时发生错误，将返回相应的 `MarketError`。

    pub fn live_match(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let mut filled: i64 = 0;
        for idx in 1..self.orders.len() {
            let other_ref = match &self.orders[idx] {
                Some(value) => value.clone(),
                None => continue,
            };
            let mut order = order_ref.borrow_mut();
            let mut other = other_ref.borrow_mut();

            // 如果两个订单的账户相同，则跳过匹配
            if order.account.is_some() && other.account.is_some() && order.account == other.account
            {
                continue;
            }

            other.dirty = true;

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

            if order.vol == 0 {
                break;
            }
        }
        self.vol -= filled;
        Ok(filled)
    }
}

/// 表示交易工具的市场深度，使用跳表实现以高效管理订单簿。
/// 维护订单簿的当前状态，包括买卖深度、市场统计信息和各种配置参数。
///
/// # 字段
///
/// - `ask_depth`: 存储卖出价格及对应价格层次的跳表映射。
/// - `bid_depth`: 存储买入价格及对应价格层次的跳表映射。
/// - `tick_size`: 工具的最小价格增量或减量。
/// - `lot_size`: 工具的最小交易单位。
/// - `timestamp`: 市场深度最后更新时间的时间戳。
/// - `best_bid_tick`: 当前最佳买入价的 tick 价格。
/// - `best_ask_tick`: 当前最佳卖出价的 tick 价格。
/// - `last_tick`: 最新交易的 tick 价格。
/// - `orders`: 活跃订单的哈希映射，通过唯一标识符索引。
/// - `mode`: 当前交易所的操作模式（例如，实时交易、模拟）。
/// - `market_statistics`: 与市场活动相关的统计数据
#[derive(Serialize, Deserialize, Debug)]
pub struct SkipListMarketDepth {
    #[serde(with = "skiplist_serde")]
    pub ask_depth: SkipMap<PriceTick, PriceLevel>,
    #[serde(with = "skiplist_serde")]
    pub bid_depth: SkipMap<PriceTick, PriceLevel>,
    /// 工具的最小价格增量或减量。
    pub tick_size: f64,

    /// 工具的最小交易单位。
    pub lot_size: f64,

    /// 市场深度最后更新时间的时间戳，以毫秒为单位，从纪元开始计算。
    pub timestamp: i64,

    /// 当前最佳买入价的 tick 价格。
    pub best_bid_tick: i64,

    /// 当前最佳卖出价的 tick 价格。
    pub best_ask_tick: i64,

    /// 最新交易的 tick 价格。
    pub last_tick: i64,

    /// 活跃订单的哈希映射，通过唯一标识符索引。
    pub orders: HashMap<OrderId, L3OrderRef>,

    /// 当前交易所的操作模式（例如，实时交易、模拟）。
    pub mode: ExchangeMode,

    /// 与市场活动相关的统计数据（例如，成交量、波动性）。
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

    // 获取当前最佳买入价（以价格为单位）。
    ///
    /// 如果 `best_bid_tick` 为 `INVALID_MIN`，则返回 `NaN`，表示没有有效的买入报价。
    /// 否则，返回最佳买入价，通过将 `best_bid_tick` 转换为 `f64` 并乘以 `tick_size` 计算得到。
    #[inline(always)]
    fn best_bid(&self) -> f64 {
        if self.best_bid_tick == INVALID_MIN {
            f64::NAN
        } else {
            self.best_bid_tick as f64 * self.tick_size
        }
    }

    /// 获取当前最佳卖出价（以价格为单位）。
    ///
    /// 如果 `best_ask_tick` 为 `INVALID_MAX`，则返回 `NaN`，表示没有有效的卖出报价。
    /// 否则，返回最佳卖出价，通过将 `best_ask_tick` 转换为 `f64` 并乘以 `tick_size` 计算得到。
    #[inline(always)]
    fn best_ask(&self) -> f64 {
        if self.best_ask_tick == INVALID_MAX {
            f64::NAN
        } else {
            self.best_ask_tick as f64 * self.tick_size
        }
    }

    /// 获取当前最佳买入价的 tick 价格。
    ///
    /// 直接返回 `best_bid_tick` 的值。
    #[inline(always)]
    fn best_bid_tick(&self) -> i64 {
        self.best_bid_tick
    }

    /// 获取当前最佳卖出价的 tick 价格。
    ///
    /// 直接返回 `best_ask_tick` 的值。
    #[inline(always)]
    fn best_ask_tick(&self) -> i64 {
        self.best_ask_tick
    }

    /// 获取市场的最小价格增量。
    ///
    /// 直接返回 `tick_size` 的值。
    #[inline(always)]
    fn tick_size(&self) -> f64 {
        self.tick_size
    }

    /// 获取市场的最小交易单位。
    ///
    /// 直接返回 `lot_size` 的值。
    #[inline(always)]
    fn lot_size(&self) -> f64 {
        self.lot_size
    }

    /// 获取指定价格档位的买方订单数量。
    ///
    /// 根据当前的市场模式（例如回测模式），返回相应的订单数量。
    ///
    /// # 参数
    ///
    /// * `price_tick` - 要查询的价格档位。
    ///
    /// # 返回值
    ///
    /// * `i64` - 返回指定价格档位的买方订单数量。如果该价格档位不存在，则返回 0。
    ///
    /// # 说明
    ///
    /// 在回测模式下，返回 `vol_shadow`，否则返回实际的订单数量 `vol`。
    #[inline(always)]
    fn bid_vol_at_tick(&self, price_tick: i64) -> i64 {
        let price_level = match self.bid_depth.get(&PriceTick::new(price_tick, true)) {
            Some(level) => level,
            None => return 0,
        };
        match self.mode {
            ExchangeMode::Backtest => price_level.vol_shadow,
            _ => price_level.vol,
        }
    }

    /// 获取指定价格档位的卖方订单数量。
    ///
    /// 根据当前的市场模式（例如回测模式），返回相应的订单数量。
    ///
    /// # 参数
    ///
    /// * `price_tick` - 要查询的价格档位。
    ///
    /// # 返回值
    ///
    /// * `i64` - 返回指定价格档位的卖方订单数量。如果该价格档位不存在，则返回 0。
    ///
    /// # 说明
    ///
    /// 在回测模式下，返回 `vol_shadow`，否则返回实际的订单数量 `vol`。

    #[inline(always)]
    fn ask_vol_at_tick(&self, price_tick: i64) -> i64 {
        let price_level = match self.ask_depth.get(&PriceTick::new(price_tick, true)) {
            Some(level) => level,
            None => return 0,
        };

        match self.mode {
            ExchangeMode::Backtest => price_level.vol_shadow,
            _ => price_level.vol,
        }
    }

    /// 将一个订单添加到市场深度中，并更新最佳价格。
    /// 如果订单来源为用户订单且订单 ID 已存在，则返回错误。
    ///
    /// # 参数
    ///
    /// * `order_ref` - 引用的订单对象。
    ///
    /// # 返回值
    ///
    /// * `Ok(i64)` - 返回更新后的最佳价格档位。
    /// * `Err(MarketError)` - 如果订单 ID 已存在或者在添加过程中发生其他错误。
    ///
    /// # 错误处理
    ///
    /// 如果订单 ID 已存在于市场中，将返回 `MarketError::OrderIdExist`。
    fn add(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        // 获取订单的相关信息
        let order = order_ref.borrow();
        let order_id = order.order_id;
        let price_tick = order.price_tick;
        let side = order.side;
        let source = order.source;

        if source == OrderSourceType::UserOrder {
            match self.orders.entry(order_ref.borrow().order_id) {
                Entry::Occupied(_) => return Err(MarketError::OrderIdExist),
                Entry::Vacant(entry) => entry.insert(order_ref.clone()),
            };
        }

        let mut best_tick: i64 = 0;

        if side == Side::Buy {
            let price_level = match self.bid_depth.get_mut(&price_tick) {
                Some(value) => value,
                None => {
                    self.bid_depth
                        .insert(price_tick.clone(), PriceLevel::new(self.mode.clone()));

                    self.bid_depth.get_mut(&price_tick).unwrap()
                }
            };

            let _ = price_level.add_order(order_ref.clone());
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
            let _ = price_level.add_order(order_ref.clone());
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

    /// 在买方市场深度中匹配订单，直到满足指定的最大深度或订单完全成交。
    /// 更新最佳买价并返回成交的总数量。
    ///
    /// # 参数
    ///
    /// * `order_ref` - 引用的订单对象。
    /// * `max_depth` - 最大的匹配深度（即最多可以匹配多少个价格档位）。
    ///
    /// # 返回值
    ///
    /// * `Ok(i64)` - 返回总的成交数量。
    /// * `Err(MarketError)` - 如果在更新市场深度时出现错误。
    ///
    /// # 错误处理
    ///
    /// 在匹配订单过程中，如果发生错误，将返回相应的 `MarketError`。
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
            self.market_statistics.total_bid_tick += filled * price_tick.price_tick;
            self.market_statistics
                .update_high_low(price_tick.price_tick);
        }

        self.best_bid_tick = self.update_bid_depth()?;
        Ok(filled)
    }

    /// 在卖方市场深度中匹配订单，直到满足指定的最大深度或订单完全成交。
    /// 更新最佳卖价并返回成交的总数量。
    ///
    /// # 参数
    ///
    /// * `order_ref` - 引用的订单对象。
    /// * `max_depth` - 最大的匹配深度（即最多可以匹配多少个价格档位）。
    ///
    /// # 返回值
    ///
    /// * `Ok(i64)` - 返回总的成交数量。
    /// * `Err(MarketError)` - 如果在更新市场深度时出现错误。
    ///
    /// # 错误处理
    ///
    /// 在匹配订单过程中，如果发生错误，将返回相应的 `MarketError`。
    fn match_ask_depth(
        &mut self,
        order_ref: L3OrderRef,
        max_depth: i64,
    ) -> Result<i64, MarketError> {
        let mut filled: i64 = 0;
        let mut count = 0;

        // 遍历卖方深度中的价格档位，进行订单匹配
        for (price_tick, price_level) in &mut self.ask_depth {
            count += 1;

            // 检查是否达到最大匹配深度，或者订单已完全成交，或者当前价格档位超过订单价格
            if count > max_depth
                || &order_ref.borrow().price_tick.price_tick < &price_tick.price_tick
                || order_ref.borrow().vol == 0
            {
                break;
            }
            // 匹配当前价格档位的订单，并更新成交量
            let this_filled = price_level.match_order(order_ref.clone()).unwrap();
            filled += this_filled;

            // 更新市场统计数据
            self.last_tick = price_tick.price_tick;
            self.market_statistics.total_ask_vol += this_filled;
            self.market_statistics.total_ask_tick += filled * price_tick.price_tick;
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

    /// 添加一个卖单到市场深度，并更新最佳买卖价位。
    ///
    /// # 参数
    ///
    /// * `source` - 订单的来源类型。
    /// * `account` - 可选的账户名称。
    /// * `order_id` - 订单的唯一标识符。
    /// * `price` - 订单的价格。
    /// * `vol` - 订单的数量。
    /// * `timestamp` - 订单的时间戳。
    ///
    /// # 返回值
    ///
    /// * `Ok((i64, i64))` - 返回添加订单前的最佳买价和更新后的最佳卖价。
    /// * `Err(MarketError)` - 如果在添加订单过程中出现错误。
    ///
    /// # 错误处理
    ///
    /// 如果订单添加失败，将返回相应的 `MarketError`。
    fn add_sell_order(
        &mut self,
        source: OrderSourceType,
        account: Option<String>,
        order_id: OrderId,
        price: f64,
        vol: i64,
        timestamp: i64,
    ) -> Result<(i64, i64), Self::Error> {
        // 将价格转换为价格档位
        let price_tick = (price / self.tick_size).round() as i64;

        // 创建新的订单引用
        let order_ref = L3OrderRef::new(RefCell::new(L3Order::new(
            source,
            account,
            order_id,
            Side::Sell,
            price_tick,
            vol,
            timestamp,
        )));

        // 尝试将订单添加到市场深度中
        self.add(order_ref)?;

        // 获取当前的最佳买价
        let prev_best_tick = self.best_bid_tick;

        // 如果新订单的价格低于当前最佳卖价，更新最佳卖价
        if price_tick < self.best_ask_tick {
            self.best_ask_tick = price_tick;
        }

        // 返回更新前的最佳买价和更新后的最佳卖价
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
                        price_level.update_order_position();
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

    /// 更新卖方市场深度，找出新的最佳卖价（最佳买单价格）。
    /// 如果没有有效的卖单，最佳卖价将被设置为 `INVALID_MAX`。
    ///
    /// # 返回值
    ///
    /// * `Ok(i64)` - 返回更新后的最佳卖价。
    /// * `Err(MarketError)` - 如果在更新过程中出现错误。
    ///
    /// # 错误处理
    ///
    /// 如果深度中没有订单，最佳卖价会被设置为 `INVALID_MAX`。
    fn update_ask_depth(&mut self) -> Result<i64, MarketError> {
        loop {
            match self.ask_depth.front_mut() {
                // 如果卖方深度中有价格层次
                Some((price_tick, price_level)) => {
                    if price_level.count == 0 {
                        // 如果该价格层次已经没有订单，将其移除
                        self.ask_depth.pop_front();
                    } else {
                        self.best_ask_tick = price_tick.price_tick.clone();
                        price_level.update_order_position();
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

    /// 取消指定的订单，并更新市场的最佳买卖价位。
    /// 如果订单被取消，返回订单的买卖方向、取消前和取消后的最佳价格。
    ///
    /// # 参数
    ///
    /// * `order_id` - 要取消的订单的 ID。
    ///
    /// # 返回值
    ///
    /// * `Ok((Side, i64, i64))` - 返回订单的买卖方向、取消前的最佳价位和取消后的最佳价位。
    /// * `Err(MarketError::OrderNotFound)` - 如果找不到订单。
    ///
    /// # 错误处理
    ///
    /// 如果在订单取消过程中无法找到订单或更新深度失败，将返回相应的错误。
    fn cancel_order(&mut self, order_id: OrderId) -> Result<(Side, i64, i64), Self::Error> {
        let order_ref = match self.orders.get_mut(&order_id) {
            Some(order) => order.clone(),
            None => return Err(MarketError::OrderNotFound),
        };

        let order = order_ref.borrow();
        // 根据订单的买卖方向更新相应的市场深度
        if order.side == Side::Buy {
            let prev_best_tick = self.best_bid_tick;

            if let Some(price_level) = self.bid_depth.get_mut(&order.price_tick) {
                price_level.delete_order(&order_ref);
            }

            self.best_bid_tick = self.update_bid_depth().unwrap_or(prev_best_tick);
            Ok((Side::Buy, prev_best_tick, self.best_bid_tick))
        } else {
            let prev_best_tick = self.best_ask_tick;

            if let Some(price_level) = self.ask_depth.get_mut(&order.price_tick) {
                price_level.delete_order(&order_ref);
            }

            self.best_ask_tick = self.update_ask_depth().unwrap_or(prev_best_tick);
            Ok((Side::Sell, prev_best_tick, self.best_ask_tick))
        }
    }

    /// 修改指定订单的价格和数量，并更新订单簿。
    ///
    /// # 参数
    ///
    /// - `order_id`: 要修改的订单的唯一标识符。
    /// - `price`: 修改后的价格。
    /// - `qty`: 修改后的数量。
    /// - `timestamp`: 修改操作的时间戳。
    ///
    /// # 返回值
    ///
    /// 返回一个 `Result`，成功时包含一个元组 `(Side, i64, i64)`，其中：
    ///
    /// - `Side`: 订单的方向（买或卖）。
    /// - `i64`: 修改前的最佳买入价或卖出价的 tick 价格。
    /// - `i64`: 修改后的最佳买入价或卖出价的 tick 价格。
    ///
    /// 失败时返回 `Self::Error`，表示订单修改失败。
    ///
    /// # 错误
    ///
    /// - `MarketError::OrderNotFound`: 如果指定的订单未找到。
    fn modify_order(
        &mut self,
        order_id: OrderId,
        price: f64,
        qty: f64,
        timestamp: i64,
    ) -> Result<(Side, i64, i64), Self::Error> {
        let order_ref: L3OrderRef;

        let order_ref = match self.orders.get_mut(&order_id) {
            Some(value) => value.clone(),
            None => return Err(MarketError::OrderNotFound),
        };

        let mut order = order_ref.borrow_mut();

        // 计算价格和数量的 tick 价格
        let price_tick = (price / self.tick_size).round() as i64;
        let vol = (qty / self.lot_size).round() as i64;

        self.cancel_order(order_id);
        order.price_tick.price_tick = price_tick;
        order.vol = vol;
        order.vol_shadow = vol;
        self.add(order_ref.clone());
        if order.side == Side::Buy {
            let prev_best_tick = self.best_bid_tick;
            Ok((Side::Buy, prev_best_tick, self.best_bid_tick))
        } else {
            let prev_best_tick = self.best_ask_tick;
            Ok((Side::Sell, self.best_ask_tick, self.best_ask_tick))
        }
    }

    fn clean_orders(&mut self) {}

    fn orders(&self) -> &HashMap<OrderId, L3OrderRef> {
        &self.orders
    }

    fn orders_mut(&mut self) -> &mut HashMap<OrderId, L3OrderRef> {
        &mut self.orders
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use SkipListMarketDepth;

    ///下面是测试PriceLevel
    fn create_test_order(
        source: OrderSourceType,
        account: Option<String>,
        side: Side,
        price_tick: i64,
        vol: i64,
        timestamp: i64,
        order_id: OrderId,
    ) -> L3OrderRef {
        Rc::new(RefCell::new(L3Order::new(
            source, account, order_id, side, price_tick, vol, timestamp,
        )))
    }
    #[test]
    fn test_add_order() {
        let mut price_level = PriceLevel::new(ExchangeMode::Backtest);

        let buy_order1 = create_test_order(
            OrderSourceType::LocalOrder,
            Some("Account1".to_string()),
            Side::Buy,
            100,
            10,
            1,
            1,
        );
        let buy_order2 = create_test_order(
            OrderSourceType::UserOrder,
            Some("Account2".to_string()),
            Side::Buy,
            100,
            15,
            2,
            2,
        );
        let sell_order1 = create_test_order(
            OrderSourceType::LocalOrder,
            Some("Account3".to_string()),
            Side::Sell,
            100,
            5,
            3,
            3,
        );

        price_level.add_order(buy_order1.clone());
        price_level.add_order(buy_order2.clone());
        price_level.add_order(sell_order1.clone());

        assert_eq!(price_level.orders.len(), 3);
        assert_eq!(price_level.orders[0].as_ref().unwrap().borrow().order_id, 1);
        assert_eq!(price_level.orders[1].as_ref().unwrap().borrow().order_id, 2);
        assert_eq!(price_level.orders[2].as_ref().unwrap().borrow().order_id, 3);
    }

    #[test]
    fn test_delete_order_success() {
        let mut price_level = PriceLevel::new(ExchangeMode::Live);

        // Create a new order and add it to the price level
        let order_ref = create_test_order(
            OrderSourceType::LocalOrder,
            Some("account1".to_string()),
            Side::Buy,
            1,
            50,
            100,
            1638390000,
        );

        // Add the order
        price_level.add_order(Rc::clone(&order_ref)).unwrap();

        // Ensure the order is added
        assert_eq!(price_level.count, 1);
        assert_eq!(price_level.vol, 50);

        // Delete the order
        let result = price_level.delete_order(&order_ref);

        // Verify the result
        assert!(result.is_ok());
        assert_eq!(price_level.count, 0);
        assert_eq!(price_level.vol, 0);
    }

    #[test]
    fn test_delete_order_not_found() {
        let mut price_level = PriceLevel::new(ExchangeMode::Live);

        // Create an order reference but do not add it to the price level
        let order_ref = create_test_order(
            OrderSourceType::LocalOrder,
            Some("account1".to_string()),
            Side::Sell,
            200,
            30,
            1638390001,
            2,
        );

        // Attempt to delete an order that was not added
        let result = price_level.delete_order(&order_ref);

        // Verify the result
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_order_with_shadow_vol() {
        let mut price_level = PriceLevel::new(ExchangeMode::Backtest);

        // Create a new order and add it to the price level
        let order_ref = create_test_order(
            OrderSourceType::LocalOrder,
            Some("account1".to_string()),
            Side::Buy,
            300,
            75,
            1638390002,
            3,
        );

        // Add the order
        price_level.add_order(Rc::clone(&order_ref)).unwrap();

        // Verify the order is added
        assert_eq!(price_level.count, 1);
        assert_eq!(price_level.vol, 75);
        assert_eq!(price_level.vol_shadow, 75);

        // Modify order to include shadow volume
        let mut order = order_ref.borrow_mut();
        order.vol_shadow = 50;
        drop(order);

        // Delete the order
        let result = price_level.delete_order(&order_ref);

        // Verify the result
        assert!(result.is_ok());
        assert_eq!(price_level.count, 0);
        assert_eq!(price_level.vol, 0);
        assert_eq!(price_level.vol_shadow, 25);
    }

    #[test]
    fn test_shadow_match_success() {
        let mut price_level = PriceLevel::new(ExchangeMode::Backtest);

        // Add a matching order to the price level
        let order_ref1 = create_test_order(
            OrderSourceType::LocalOrder,
            Some("account1".to_string()),
            Side::Buy,
            100,
            50,
            1638390000,
            1,
        );
        let order_ref2 = create_test_order(
            OrderSourceType::LocalOrder,
            Some("account2".to_string()),
            Side::Buy,
            100,
            50,
            1638390001,
            2,
        );
        price_level.add_order(Rc::clone(&order_ref1)).unwrap();
        price_level.add_order(Rc::clone(&order_ref2)).unwrap();

        // Match the order
        let matching_order = create_test_order(
            OrderSourceType::LocalOrder,
            Some("account1".to_string()),
            Side::Sell,
            100,
            50,
            1638390002,
            3,
        );
        let result = price_level
            .shadow_match(Rc::clone(&matching_order))
            .unwrap();

        // Verify the result
        assert_eq!(result, 50); // The total volume matched should be 50
        assert_eq!(price_level.count, 1); // Only one order should remain in the price level
        assert_eq!(price_level.vol, 50); // The remaining order volume should be 50
        assert_eq!(price_level.vol_shadow, 50); // The shadow volume should match the remaining order volume
    }

    #[test]
    fn test_shadow_match_partial() {
        let mut price_level = PriceLevel::new(ExchangeMode::Backtest);

        // Add a matching order to the price level
        let order_ref1 = create_test_order(
            OrderSourceType::UserOrder,
            Some("account1".to_string()),
            Side::Buy,
            100,
            50,
            1638390000,
            1,
        );
        let order_ref2 = create_test_order(
            OrderSourceType::LocalOrder,
            Some("account2".to_string()),
            Side::Buy,
            100,
            30,
            1638390001,
            2,
        );
        price_level.add_order(Rc::clone(&order_ref1)).unwrap();
        price_level.add_order(Rc::clone(&order_ref2)).unwrap();

        // Match the order
        let matching_order = create_test_order(
            OrderSourceType::UserOrder,
            Some("account1".to_string()),
            Side::Sell,
            100,
            20,
            1638390002,
            3,
        );
        let result = price_level
            .shadow_match(Rc::clone(&matching_order))
            .unwrap();

        // Verify the result
        assert_eq!(result, 20); // The total volume matched should be 20
        assert_eq!(price_level.count, 2); // Only one order should remain in the price level
        assert_eq!(price_level.vol, 30); // The remaining order volume should be 60
        assert_eq!(price_level.vol_shadow, 60); // The shadow volume should match the remaining order volume
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
                OrderSourceType::UserOrder,
                Some("user1".to_string()),
                i,
                Side::Buy,
                100,
                100,
                1,
            );

            let _ = depth.add(order_ref);
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
