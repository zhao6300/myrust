/// `broker` 模块提供与经纪人相关的功能。
pub mod broker;

/// `dataloader` 模块处理数据加载操作。
pub mod dataloader;

/// `exchange` 模块定义交易所的行为。
pub mod exchange;

/// `order` 模块管理订单相关操作和定义。
pub mod order;

/// `skiplist_helper` 模块包含跳表操作的辅助函数。
pub mod skiplist_helper;

/// `skiplist_orderbook` 模块定义基于跳表的订单簿。
pub mod skiplist_orderbook;

pub mod depth_manager;
/// `statistics` 模块收集和处理交易统计数据。
pub mod statistics;

pub mod dataapi;
pub mod hook;
pub mod prelude;
/// `types` 模块定义系统中使用的各种类型。
pub mod types;
pub mod utils;
use log::{debug, info};
use order::OrderRef;
use serde::{Deserialize, Serialize};
use statistics::Statistics;
use std::cell::RefCell;
use std::cmp;
use std::rc::Rc;
use std::{collections::HashMap, io::Error as IoError};
use std::{i64, usize};
use thiserror::Error;
use types::*;

/// 表示无最佳买入价的最小值（以 ticks 为单位）。
pub const INVALID_MIN: i64 = i64::MIN;

/// 表示无最佳卖出价的最大值（以 ticks 为单位）。
pub const INVALID_MAX: i64 = i64::MAX;

pub type OrderId = i64;
/// Represents no best bid in ticks.

#[derive(Error, Debug, PartialEq, Eq)]
pub enum MarketError {
    #[error("market type unknown")]
    MarketTypeUnknownError,
    #[error("invalid timestamp")]
    RecoverFailed,
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error("parse time error")]
    ParseError,
    #[error("stock type is not supported")]
    StockTypeUnSupported,
    #[error("history data is none ")]
    HistoryIsNone,
    #[error("market side error")]
    MarketSideError,
    #[error("broker for stock already exists")]
    StockBrokerIdExist,
    #[error("broker is not exists")]
    StockBrokerNotExist,
    #[error("data for stock already exists")]
    StockDataExist,
    #[error("Order related to a given order id already exists")]
    OrderIdExist,
    #[error("Order type is not supported")]
    OrderTypeUnsupported,
    #[error("Order request is in process")]
    OrderRequestInProcess,
    #[error("Order not found")]
    OrderNotFound,
    #[error("order request is invalid")]
    InvalidOrderRequest,
    #[error("order status is invalid to proceed the request")]
    InvalidOrderStatus,
    #[error("end of data")]
    EndOfData,
    #[error("exchange mode is not supported")]
    ExchangeModeUnsupproted,
    // #[error("data error: {0:?}")]
    // DataError(#[from] IoError),
}

/// 定义市场深度操作的方法的 trait。
pub trait MarketDepth {
    /// 使用给定的模式、tick 大小和 lot 大小创建新的实现类型实例。
    fn new_box(mode: ExchangeMode, tick_size: f64, lot_size: f64) -> Box<Self>;

    /// 返回最佳买入价格（浮点数表示）。
    /// 如果没有最佳买入价，返回 [`f64::NAN`]。
    fn best_bid(&self, source: &OrderSourceType) -> f64;

    /// 返回最佳卖出价格（浮点数表示）。
    /// 如果没有最佳卖出价，返回 [`f64::NAN`]。
    fn best_ask(&self, source: &OrderSourceType) -> f64;

    /// 返回最佳买入价格的 ticks 值。
    /// 如果没有最佳买入价，返回 [`INVALID_MIN`]。
    fn best_bid_tick(&self, source: &OrderSourceType) -> i64;

    ///返回上次的成交价
    fn last_tick(&self, source: &OrderSourceType) -> i64;
    fn last_price(&self, source: &OrderSourceType) -> f64;
    /// 返回最佳卖出价格的 ticks 值。
    /// 如果没有最佳卖出价，返回 [`INVALID_MAX`]。
    fn best_ask_tick(&self, source: &OrderSourceType) -> i64;

    /// 返回 tick 大小。
    fn tick_size(&self) -> f64;

    /// 返回 lot 大小。
    fn lot_size(&self) -> f64;

    /// 返回给定价格的买入市场深度的数量（以 ticks 为单位）。
    fn bid_vol_at_tick(&self, price_tick: i64) -> i64;

    /// 返回给定价格的卖出市场深度的数量（以 ticks 为单位）。
    fn ask_vol_at_tick(&self, price_tick: i64) -> i64;

    /// 将订单添加到市场深度中，并返回结果。
    fn add(&mut self, order: L3OrderRef) -> Result<i64, MarketError>;

    /// 匹配订单并返回结果。
    fn match_order(&mut self, order_ref: L3OrderRef, max_depth: i64) -> Result<i64, MarketError>;
    fn try_match_order(
        &mut self,
        order_ref: L3OrderRef,
        max_depth: i64,
    ) -> Result<bool, MarketError>;
    /// 匹配买入深度并返回结果。
    fn match_bid_depth(
        &mut self,
        order_ref: L3OrderRef,
        max_depth: i64,
    ) -> Result<i64, MarketError>;

    /// 匹配卖出深度并返回结果。
    fn match_ask_depth(
        &mut self,
        order_ref: L3OrderRef,
        max_depth: i64,
    ) -> Result<i64, MarketError>;

    fn get_bid_level(&self, level_num: usize) -> String;
    fn get_ask_level(&self, level_num: usize) -> String;
    ///返回开盘价和成交量，如果时间不在集合竞价阶段返回错误
    fn call_auction(&mut self) -> Result<(i64, i64), MarketError>;
    fn set_previous_close_tick(&mut self, previous_close_price: i64);
}

///用于辅助还原市场下单的
#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
pub struct L30LocalOrderInfo {
    pub match_price: f64,
    pub match_seq: i64,
    pub match_qty: f64,
    pub match_count: i64,
    pub orderbook_price: f64,
    pub orderbook_qty: f64,
    pub orderbook_seq: i64,
    pub initial_qty: f64,
    pub initial_seq: i64,
    pub initial_price: f64,
    pub cancel_seq: i64,
}

impl Default for L30LocalOrderInfo {
    fn default() -> Self {
        Self {
            match_price: 0.0,
            match_seq: i64::MAX,
            match_qty: 0.0,
            match_count: 0,
            orderbook_price: 0.0,
            orderbook_qty: 0.0,
            orderbook_seq: i64::MAX,
            initial_qty: 0.0,
            initial_seq: i64::MAX,
            initial_price: 0.0,
            cancel_seq: i64::MAX,
        }
    }
}

impl L30LocalOrderInfo {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn orderbook_seq(&self) -> i64 {
        let small = cmp::min(self.initial_seq, self.match_seq);
        cmp::min(small, self.orderbook_seq)
    }
}

/// `L3Order` 结构体表示一个高级订单（Level 3 订单），用于记录交易中的订单信息。
///
/// # 字段
/// - `source`：订单来源类型，表示订单的发起者或来源，类型为 `OrderSourceType`。
/// - `account`：可选的账户信息，用于识别订单所属的账户，类型为 `Option<String>`。
/// - `order_id`：订单的唯一标识符，类型为 `OrderId`。
/// - `side`：订单方向，表示买入还是卖出，类型为 `Side`。
/// - `price_tick`：订单价格，单位为 ticks。ticks 是根据 `tick_size` 计算的整数值，类型为 `PriceTick`。
/// - `vol`：订单的交易量，单位为 lot。表示实际需要买入或卖出的数量，类型为 `i64`。
/// - `vol_shadow`：订单的影子交易量，用于在不改变历史数据的情况下计算，类型为 `i64`。
/// - `idx`：订单在队列中的位置，用于快速删除订单，类型为 `usize`。
/// - `timestamp`：订单的时间戳，表示订单被创建的时间，类型为 `i64`。
/// - `position`：订单在队列中的位置索引，默认为 -1，类型为 `i64`。
/// - `dirty`：标志位，表示订单是否被修改过，类型为 `bool`，用于追踪订单的脏状态。
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct L3Order {
    #[serde(skip)]
    pub seq: i64,
    pub source: OrderSourceType,
    pub account: Option<String>,
    pub order_id: OrderId,
    pub side: Side,
    /// 除以tick size后的值
    pub price_tick: i64,
    /// 除以lot_size之后的值，比如股票的lot_size是100，这里就是手
    pub vol: i64,
    /// 用于不改变历史时的计算
    pub vol_shadow: i64,
    /// 在队列中的位置，用来快速删除订单的
    pub idx: usize,
    pub timestamp: i64,
    pub order_type: OrderType,
    #[serde(skip)]
    pub total_vol_before: i64,
    // #[serde(skip)]
    // pub should_add: i64,–
    #[serde(skip)]
    pub dirty: bool,
    pub auxiliary_info: Option<L30LocalOrderInfo>,
}

impl L3Order {
    pub fn new(
        source: OrderSourceType,
        account: Option<String>,
        order_id: OrderId,
        side: Side,
        price_tick: i64,
        vol: i64,
        timestamp: i64,
        order_type: OrderType,
    ) -> Self {
        let reverse = match side {
            Side::Buy => true,
            _ => false,
        };

        let auxiliary_info = if source == OrderSourceType::LocalOrder {
            Some(L30LocalOrderInfo::default())
        } else {
            None
        };

        Self {
            seq: 0,
            source: source,
            account: account,
            order_id: order_id,
            side: side,
            price_tick: price_tick,
            vol: vol,
            vol_shadow: vol,
            idx: 0,
            timestamp: timestamp,
            total_vol_before: 0,
            dirty: false,
            auxiliary_info: auxiliary_info,
            order_type: order_type,
        }
    }

    pub fn new_ref(
        source: OrderSourceType,
        account: Option<String>,
        order_id: OrderId,
        side: Side,
        price_tick: i64,
        vol: i64,
        timestamp: i64,
        order_type: OrderType,
    ) -> L3OrderRef {
        Rc::new(RefCell::new(Self::new(
            source, account, order_id, side, price_tick, vol, timestamp, order_type,
        )))
    }
}

pub type L3OrderRef = Rc<RefCell<L3Order>>;
/// `L3MarketDepth` trait 定义了 L3 市场深度操作的方法，继承自 `MarketDepth` trait。
/// 它扩展了市场深度的功能，特别是涉及订单操作的部分。
///
/// # 关联类型
/// - `Error`：用于表示方法中可能发生的错误类型。
pub trait L3MarketDepth: MarketDepth {
    type Error;

    /// 将买入订单添加到订单簿，并返回一个元组，其中包含（之前的最佳买入 tick 值，当前的最佳买入 tick 值）。
    fn add_buy_order(
        &mut self,
        source: OrderSourceType,
        account: Option<String>,
        order_id: OrderId,
        price: f64,
        vol: i64,
        timestamp: i64,
        order_type: OrderType,
    ) -> Result<(i64, i64), Self::Error>;

    /// Adds a sell order to the order book and returns a tuple containing (the previous best ask
    ///  in ticks, the current best ask in ticks).
    fn add_sell_order(
        &mut self,
        source: OrderSourceType,
        account: Option<String>,
        order_id: OrderId,
        price: f64,
        vol: i64,
        timestamp: i64,
        order_type: OrderType,
    ) -> Result<(i64, i64), Self::Error>;

    /// Deletes the order in the order book.
    fn cancel_order(&mut self, order_id: OrderId) -> Result<(Side, i64, i64), Self::Error>;
    fn cancel_order_from_ref(
        &mut self,
        order_ref: L3OrderRef,
    ) -> Result<(Side, i64, i64), Self::Error>;
    fn update_bid_depth(&mut self) -> Result<i64, MarketError>;
    fn update_ask_depth(&mut self) -> Result<i64, MarketError>;

    /// Modifies the order in the order book and returns a tuple containing (side, the previous best
    /// in ticks, the current best in ticks).
    fn modify_order(
        &mut self,
        order_id: OrderId,
        px: f64,
        qty: f64,
        timestamp: i64,
    ) -> Result<(Side, i64, i64), Self::Error>;

    /// clean filled orders and canceled orders
    fn clean_orders(&mut self);

    /// Returns the orders held in the order book.
    fn orders(&self) -> &HashMap<OrderId, L3OrderRef>;
    fn orders_mut(&mut self) -> &mut HashMap<OrderId, L3OrderRef>;
    fn get_orderbook_level(
        &self,
        bid_vec: &mut Vec<(f64, f64, i64)>,
        ask_vec: &mut Vec<(f64, f64, i64)>,
        max_level: usize,
    );
}

pub trait Processor {
    fn initialize_data(&mut self) -> Result<i64, MarketError>;
    fn process_data(&mut self) -> Result<(i64, i64), MarketError>;
    fn submit_order(
        &mut self,
        order_id: OrderId,
        side: Side,
        price: f64,
        qty: f64,
        order_type: OrderType,
        current_timestamp: i64,
    ) -> Result<(), MarketError>;
    fn cancel(&mut self, order_id: OrderId, current_timestamp: i64) -> Result<(), MarketError>;
    fn orders(&self) -> &HashMap<OrderId, OrderRef>;
}

pub trait OrderIter {
    type Item;
    fn next(&mut self) -> Option<(i64, &Self::Item)>;
    fn is_last(&self) -> bool;
}

pub trait KeyOp {
    fn set_key(&mut self, price_tick: i64);
    fn get_key(&self) -> i64;
    fn set_reverse(&mut self, reverse: bool);
}

pub trait ValueOp {
    fn get_reverse(&self) -> bool;
}

pub trait SnapshotOp {
    fn snapshot(&self) -> String;
}

pub trait StatisticsOp {
    fn get_statistics(&self) -> &Statistics;
}

pub trait RecoverOp {
    fn recover(&mut self) -> Result<bool, MarketError>;
}

pub trait PriceLevelOp {
    fn get_level_info(&self) -> (i64, i64, i64);
    fn is_deleted(&self) -> bool;
    fn set_deleted(&mut self);
}
