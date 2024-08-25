use super::{OrdType, OrderSourceType, Side, Status, OrderId};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::{cell::RefCell, rc::Rc};

#[derive(Debug, Serialize, Deserialize)]
pub struct Order {
    pub order_id: OrderId,
    pub stock_code: String,
    /// 交易所接收到订单的时间
    pub local_time: i64,
    /// 交易所处理订单的时间
    pub exch_time: i64,
    pub qty: f64,
    pub price: f64,
    pub price_tick: i64,
    pub order_type: OrdType,
    pub side: Side,
    pub status: Status,
    pub source: Option<OrderSourceType>,
    pub recv_num: i64,
    pub account: Option<String>,

    /// 和盘口成交的数量
    pub filled_qty: f64,
    /// 成交后剩余的数量
    pub left_qty: f64,
}

impl Order {
    pub fn new(
        account: Option<String>,
        stock_code: String,
        price: f64,
        qty: f64,
        side: Side,
        order_type: OrdType,
        timestamp: i64,
        source: Option<OrderSourceType>,
    ) -> Self {
        Self {
            local_time: timestamp,
            exch_time: 0,
            stock_code: stock_code,
            qty: qty,
            price: price,
            price_tick: 0,
            order_id: 0,
            order_type: order_type,
            side: side,
            status: Status::New,
            source: source,
            recv_num: 0,
            account: account,
            filled_qty: 0.0,
            left_qty: qty,
        }
    }

    pub fn new_ref(
        account: Option<String>,
        stock_code: String,
        timestamp: i64,
        price: f64,
        qty: f64,
        bs_flag: &str,
        order_type: OrdType,
        source: Option<OrderSourceType>,
    ) -> OrderRef {
        Rc::new(RefCell::new(Self::new(
            account,
            stock_code,
            price,
            qty,
            Side::from_str(bs_flag).unwrap(),
            order_type,
            timestamp,
            source,
        )))
    }
}

pub type OrderRef = Rc<RefCell<Order>>;
