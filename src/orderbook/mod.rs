pub mod broker;
pub mod dataloader;
pub mod exchange;
pub mod order;
pub mod skiplist_helper;
pub mod skiplist_orderbook;
pub mod types;
use order::OrderRef;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;
use std::usize;
use std::{collections::HashMap, io::Error as IoError};
use thiserror::Error;
use types::*;

/// Represents no best bid in ticks.
pub const INVALID_MIN: i64 = i64::MIN;

/// Represents no best ask in ticks.
pub const INVALID_MAX: i64 = i64::MAX;

pub type OrderId = u64;
/// Represents no best bid in ticks.

#[derive(Error, Debug)]
pub enum MarketError {
    #[error("market side error")]
    MarketSideError,
    #[error("borker for stock already exists")]
    StockBrokerIdExist,
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
    #[error("data error: {0:?}")]
    DataError(#[from] IoError),
}
pub trait MarketDepth {
    fn new_box(mode: ExchangeMode, tick_size: f64, lot_size: f64) -> Box<Self>;

    /// Returns the best bid price.
    /// If there is no best bid, it returns [`f64::NAN`].
    fn best_bid(&self) -> f64;

    /// Returns the best ask price.
    /// If there is no best ask, it returns [`f64::NAN`].
    fn best_ask(&self) -> f64;

    /// Returns the best bid price in ticks.
    /// If there is no best bid, it returns [`INVALID_MIN`].
    fn best_bid_tick(&self) -> i64;

    /// Returns the best ask price in ticks.
    /// If there is no best ask, it returns [`INVALID_MAX`].
    fn best_ask_tick(&self) -> i64;

    /// Returns the tick size.
    fn tick_size(&self) -> f64;

    /// Returns the lot size.
    fn lot_size(&self) -> f64;

    /// Returns the quantity at the bid market depth for a given price in ticks.
    fn bid_vol_at_tick(&self, price_tick: i64) -> i64;

    /// Returns the quantity at the ask market depth for a given price in ticks.
    fn ask_vol_at_tick(&self, price_tick: i64) -> i64;
    fn add(&mut self, order: L3OrderRef) -> Result<i64, MarketError>;
    fn match_order(&mut self, order_ref: L3OrderRef, max_depth: i64) -> Result<i64, MarketError>;
    fn match_bid_depth(
        &mut self,
        order_ref: L3OrderRef,
        max_depth: i64,
    ) -> Result<i64, MarketError>;
    fn match_ask_depth(
        &mut self,
        order_ref: L3OrderRef,
        max_depth: i64,
    ) -> Result<i64, MarketError>;
}

#[derive(Debug, Deserialize, Serialize)]
pub struct L3Order {
    pub source: OrderSourceType,
    pub account: Option<String>,
    pub order_id: OrderId,
    pub side: Side,
    /// 除以tick size后的值
    pub price_tick: i64,
    /// 除以lot_size之后的值，比如股票的lot_size是100，这里就是手
    pub vol: i64,
    ///用于不改变历史时的计算
    pub vol_shadow: i64,
    pub idx: usize,
    pub timestamp: i64,
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
    ) -> Self {
        Self {
            source: source,
            account: account,
            order_id: order_id,
            side: side,
            price_tick: price_tick,
            vol: vol,
            vol_shadow: vol,
            idx: 0,
            timestamp: timestamp,
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
    ) -> L3OrderRef {
        Rc::new(RefCell::new(Self::new(
            source, account, order_id, side, price_tick, vol, timestamp,
        )))
    }
}

pub type L3OrderRef = Rc<RefCell<L3Order>>;

pub trait L3MarketDepth: MarketDepth {
    type Error;

    /// Adds a buy order to the order book and returns a tuple containing (the previous best bid
    /// in ticks, the current best bid in ticks).
    fn add_buy_order(
        &mut self,
        source: OrderSourceType,
        account: Option<String>,
        order_id: OrderId,
        price: f64,
        vol: i64,
        timestamp: i64,
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
    ) -> Result<(i64, i64), Self::Error>;

    /// Deletes the order in the order book.
    fn cancel_order(
        &mut self,
        order_id: OrderId,
        timestamp: i64,
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

    /// Clears the market depth. If the side is [Side::None], both sides are cleared.
    fn clear_orders(&mut self, side: Side);

    /// Returns the orders held in the order book.
    fn orders(&self) -> &HashMap<OrderId, L3OrderRef>;
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
        order_type: OrdType,
        current_timestamp: i64,
    ) -> Result<(), MarketError>;
    fn cancel(&mut self, order_id: OrderId, current_timestamp: i64) -> Result<(), MarketError>;
    fn orders(&self) -> &HashMap<OrderId, OrderRef>;
}

pub trait OrderIter {
    type Item;
    fn next(&mut self) -> Option<Self::Item>;
}
