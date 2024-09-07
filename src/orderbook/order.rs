use super::{OrderType, OrderId, OrderSourceType, OrderStatus, Side};
use serde::{Deserialize, Serialize};
use std::cmp::{Ord, Ordering};
use std::str::FromStr;
use std::{cell::RefCell, rc::Rc};

#[derive(Debug, Deserialize, Serialize)]
/// 表示订单的结构体
/// 包含了订单的基本信息和状态
pub struct Order {
    pub order_id: OrderId,  // 订单 ID
    pub stock_code: String, // 股票代码
    /// 交易所接收到订单的时间
    /// 格式为 `20230801093939123`（年-月-日-时-分-秒-毫秒）
    pub local_time: i64,
    /// 交易所处理订单的时间
    /// 格式为 `20230801093939123`（年-月-日-时-分-秒-毫秒）
    pub exch_time: i64,
    pub qty: f64,            // 订单数量
    pub price: f64,          // 订单价格
    pub price_tick: i64,     // 价格档位
    pub order_type: OrderType, // 订单类型
    pub side: Side,          // 买卖方向
    pub status: OrderStatus, // 订单状态
    #[serde(skip_serializing)]
    pub source: OrderSourceType, // 订单来源类型
    pub account: Option<String>, // 账户信息
    #[serde(skip_serializing)]
    pub seq: i64, // 序列号
    pub queue: f64,          // 持仓量
    /// 和盘口成交的数量
    pub filled_qty: f64,
    /// 成交后剩余的数量
    pub left_qty: f64,
    #[serde(skip_serializing)]
    pub dirty: bool, // 数据是否被修改标志
}

impl Order {
    pub fn new(
        account: Option<String>,
        stock_code: String,
        price: f64,
        qty: f64,
        side: Side,
        order_type: OrderType,
        timestamp: i64,
        source: OrderSourceType,
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
            status: OrderStatus::New,
            source: source,
            account: account,
            filled_qty: 0.0,
            left_qty: qty,
            queue: 0.0,
            seq: 0,
            dirty: false,
        }
    }

    pub fn new_ref(
        account: Option<String>,
        stock_code: String,
        timestamp: i64,
        price: f64,
        qty: f64,
        bs_flag: &str,
        order_type: OrderType,
        source: OrderSourceType,
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

    pub fn update(&mut self) {
        if self.qty != self.filled_qty {
            self.status = OrderStatus::PartiallyFilled;
            self.left_qty = self.qty - self.filled_qty;
        } else {
            self.status = OrderStatus::Filled;
            self.left_qty = 0.0;
        }
    }
}

impl Eq for Order {}

impl Ord for Order {
    fn cmp(&self, other: &Self) -> Ordering {
        self.local_time.cmp(&other.local_time)
    }
}

impl PartialOrd for Order {
    fn partial_cmp(&self, other: &Order) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Order {
    fn eq(&self, other: &Order) -> bool {
        self.local_time == other.local_time
    }
}

pub type OrderRef = Rc<RefCell<Order>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_creation() {
        let order = Order::new(
            Some("account1".to_string()),
            "AAPL".to_string(),
            150.0,
            10.0,
            Side::Buy,
            OrderType::L,
            1234567890,
            OrderSourceType::LocalOrder,
        );

        assert_eq!(order.stock_code, "AAPL");
        assert_eq!(order.price, 150.0);
        assert_eq!(order.qty, 10.0);
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.order_type, OrderType::L);
        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.local_time, 1234567890);
        assert_eq!(order.exch_time, 0);
        assert_eq!(order.source, OrderSourceType::LocalOrder);
        assert_eq!(order.filled_qty, 0.0);
        assert_eq!(order.left_qty, 10.0);
    }

    #[test]
    fn test_order_ref_creation() {
        let order_ref = Order::new_ref(
            Some("account1".to_string()),
            "AAPL".to_string(),
            1234567890,
            150.0,
            10.0,
            "Buy",
            OrderType::L,
            OrderSourceType::LocalOrder,
        );

        let order = order_ref.borrow();
        assert_eq!(order.stock_code, "AAPL");
        assert_eq!(order.price, 150.0);
        assert_eq!(order.qty, 10.0);
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.order_type, OrderType::L);
        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.local_time, 1234567890);
        assert_eq!(order.exch_time, 0);
        assert_eq!(order.source, OrderSourceType::LocalOrder);
        assert_eq!(order.filled_qty, 0.0);
        assert_eq!(order.left_qty, 10.0);
    }

    #[test]
    fn test_order_update_partial_fill() {
        let mut order = Order::new(
            Some("account1".to_string()),
            "AAPL".to_string(),
            150.0,
            10.0,
            Side::Buy,
            OrderType::L,
            1234567890,
            OrderSourceType::LocalOrder,
        );

        order.filled_qty = 5.0;
        order.update();

        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.left_qty, 5.0);
    }

    #[test]
    fn test_order_update_full_fill() {
        let mut order = Order::new(
            Some("account1".to_string()),
            "AAPL".to_string(),
            150.0,
            10.0,
            Side::Buy,
            OrderType::L,
            1234567890,
            OrderSourceType::LocalOrder,
        );

        order.filled_qty = 10.0;
        order.update();

        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.left_qty, 0.0);
    }

    #[test]
    fn test_order_cmp() {
        let order1 = Order::new(
            Some("account1".to_string()),
            "AAPL".to_string(),
            150.0,
            10.0,
            Side::Buy,
            OrderType::L,
            1234567890,
            OrderSourceType::LocalOrder,
        );

        let order2 = Order::new(
            Some("account2".to_string()),
            "GOOG".to_string(),
            100.0,
            5.0,
            Side::Sell,
            OrderType::L,
            1234567891,
            OrderSourceType::LocalOrder,
        );

        assert!(order1 < order2);
        assert!(order2 > order1);
    }

    #[test]
    fn test_order_eq() {
        let order1 = Order::new(
            Some("account1".to_string()),
            "AAPL".to_string(),
            150.0,
            10.0,
            Side::Buy,
            OrderType::L,
            1234567890,
            OrderSourceType::LocalOrder,
        );

        let order2 = Order::new(
            Some("account2".to_string()),
            "GOOG".to_string(),
            100.0,
            5.0,
            Side::Sell,
            OrderType::L,
            1234567890,
            OrderSourceType::LocalOrder,
        );

        assert_eq!(order1, order2);
    }
}
