use serde::{Deserialize, Serialize};
use std::cmp::{Ord, Ordering};
use std::i32;
use std::str::FromStr;

use super::{KeyOp, MarketError};
pub type OrderId = u64;

#[derive(Clone, Copy, Eq, PartialEq, Debug, Deserialize, Serialize)]
#[repr(i8)]
pub enum Side {
    /// 代表买入订单。
    Buy = 1,
    /// 代表卖出订单。
    Sell = 2,
    /// 代表未指定或中性方向。
    None = 0,
    /// 代表不支持的方向。
    Unsupported = 127,
}

impl Side {
    /// 从 `i32` 转换为 `Side`
    ///
    /// # 参数
    /// * `type_num` - 需要转换的 `i32` 数值
    ///
    /// # 返回
    /// * `Ok(Side)` - 成功时返回对应的 `Side`
    /// * `Err(MarketError)` - 如果不支持该 `i32` 数值，返回 `MarketError::SideUnsupported`
    pub fn from_i32(type_num: i32) -> Result<Side, MarketError> {
        match type_num {
            1 => Ok(Side::Buy),
            2 => Ok(Side::Sell),
            0 => Ok(Side::None),
            _ => Err(MarketError::MarketSideError),
        }
    }

    /// 将 `Side` 转换为对应的 `i32` 值
    ///
    /// # 返回
    /// * `i32` - 对应的 `i32` 数值
    pub fn to_i32(self) -> i32 {
        match self {
            Side::Buy => 1,
            Side::Sell => 2,
            Side::None => 0,
            Side::Unsupported => 127,
        }
    }
}

impl FromStr for Side {
    type Err = ();

    fn from_str(input: &str) -> Result<Side, Self::Err> {
        match input.to_lowercase().as_str() {
            "buy" => Ok(Side::Buy),
            "b" => Ok(Side::Buy),
            "sell" => Ok(Side::Sell),
            "s" => Ok(Side::Sell),
            "none" => Ok(Side::None),
            _ => Ok(Side::Unsupported),
        }
    }
}

impl AsRef<str> for Side {
    fn as_ref(&self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
            Side::None => panic!("Side::None"),
            Side::Unsupported => panic!("Side::Unsupported"),
        }
    }
}

/// Order type
#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum OrderType {
    /// 代表普通限价订单。
    L = 0,
    /// 代表最优五档即时成交剩余撤销的市价订单。
    M = 1,
    /// 代表最优五档即时成交剩余转限价的市价订单。
    N = 2,
    /// 代表以本方最优价格申报的市价订单。
    B = 3,
    /// 代表以对手方最优价格申报的市价订单。
    C = 4,
    /// 代表市价全额成交或撤销订单。
    D = 5,
    /// 代表取消委托。
    Cancel = 6,
    /// 用在回测模式时用于完全模拟市场订单的行为
    None = 250,
    /// 代表不支持的订单类型。
    Unsupported = 255,
}

impl OrderType {
    /// 根据整数值创建 `OrderType` 枚举。
    ///
    /// # 参数
    /// - `type_num`: 订单类型的整数表示。
    ///
    /// # 返回
    /// - `Ok(OrderType)`: 对应的订单类型。
    /// - `Err(MarketError)`: 如果类型不被支持，返回错误。
    pub fn from_i32(type_num: i32) -> Result<OrderType, MarketError> {
        match type_num {
            10 | 0 => Ok(OrderType::Cancel),
            1 => Ok(OrderType::C),
            2 => Ok(OrderType::L),
            3 => Ok(OrderType::B),
            _ => Err(MarketError::OrderTypeUnsupported),
        }
    }
    /// 将 `OrderType` 转换为对应的 `i32` 值
    ///
    /// # 返回
    /// * `i32` - 对应的 `i32` 值
    pub fn to_i32(&self) -> i32 {
        match self {
            OrderType::Cancel => 10,
            OrderType::C => 1,
            OrderType::L => 2,
            OrderType::B => 3,
            // 如果有更多的 `OrderType` 变体，请在此补充
            // 其他未处理的情况返回 255
            _ => 255,
        }
    }
}

impl FromStr for OrderType {
    type Err = ();

    fn from_str(input: &str) -> Result<OrderType, Self::Err> {
        match input {
            "L" => Ok(OrderType::L),
            "M" => Ok(OrderType::M),
            "N" => Ok(OrderType::N),
            "B" => Ok(OrderType::B),
            "C" => Ok(OrderType::C),
            "D" => Ok(OrderType::D),
            _ => Ok(OrderType::Unsupported),
        }
    }
}

/// 市场类型的枚举
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum MarketType {
    SH = 0,
    SZ = 1,
    Unknown = 255,
}

impl FromStr for MarketType {
    type Err = MarketError;

    fn from_str(input: &str) -> Result<MarketType, Self::Err> {
        match input.to_lowercase().as_str() {
            "sh" | "shanghai" => Ok(MarketType::SH),
            "sz" | "shenzhen" => Ok(MarketType::SH),
            _ => Err(MarketError::MarketTypeUnknownError),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum OrderSourceType {
    /// 代表本地订单。
    LocalOrder = 0,
    /// 代表用户订单。
    UserOrder = 1,
    /// 代表未知来源。
    Unknown = 255,
}

impl FromStr for OrderSourceType {
    type Err = ();

    fn from_str(input: &str) -> Result<OrderSourceType, Self::Err> {
        match input.to_lowercase().as_str() {
            "localorder" => Ok(OrderSourceType::LocalOrder),
            "userorder" => Ok(OrderSourceType::UserOrder),
            _ => Ok(OrderSourceType::Unknown),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum OrderStatus {
    /// 代表无状态。
    None = 0,
    /// 代表新订单。
    New = 1,
    /// 代表订单过期。
    Expired = 2,
    /// 代表订单已成交。
    Filled = 3,
    /// 代表订单已取消。
    Canceled = 4,
    /// 代表订单部分成交。
    PartiallyFilled = 5,
    /// 代表订单被拒绝。
    Rejected = 6,
    /// 代表不支持的状态。
    Unsupported = 255,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum ExchangeMode {
    /// 代表回测模式。
    Backtest = 0,
    /// 代表实盘模式。
    Live = 1,
    /// 代表不支持的模式。
    Unsupported = 255,
}

impl FromStr for ExchangeMode {
    type Err = ();

    fn from_str(input: &str) -> Result<ExchangeMode, Self::Err> {
        match input.to_lowercase().as_str() {
            "backtest" => Ok(ExchangeMode::Backtest),
            "live" => Ok(ExchangeMode::Live),
            _ => Ok(ExchangeMode::Unsupported),
        }
    }
}

impl AsRef<str> for ExchangeMode {
    fn as_ref(&self) -> &'static str {
        match self {
            ExchangeMode::Backtest => "Backtest",
            ExchangeMode::Live => "Live",
            ExchangeMode::Unsupported => panic!("ExchangeMode::Unsupported"),
        }
    }
}

#[derive(Eq, Debug, Deserialize, Serialize, Clone, Copy)]
pub struct PriceTick {
    /// 价格跳动的整数值。
    pub price_tick: i64,
    /// 是否反转排序。
    #[serde(skip)]
    pub reverse: bool,
}

impl PriceTick {
    pub fn new(price_tick: i64, reverse: bool) -> Self {
        Self {
            price_tick: price_tick,
            reverse: reverse,
        }
    }
}

impl KeyOp for PriceTick {
    fn set_key(&mut self, price_tick: i64) {
        self.price_tick = price_tick;
    }
    fn get_key(&self) -> i64 {
        self.price_tick
    }
    fn set_reverse(&mut self, reverse: bool) {
        self.reverse = reverse;
    }
}

impl Ord for PriceTick {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.reverse {
            true => match self.price_tick.cmp(&other.price_tick) {
                Ordering::Less => Ordering::Greater,
                Ordering::Greater => Ordering::Less,
                Ordering::Equal => Ordering::Equal,
            },
            false => self.price_tick.cmp(&other.price_tick),
        }
    }
}

impl PartialOrd for PriceTick {
    fn partial_cmp(&self, other: &PriceTick) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PriceTick {
    fn eq(&self, other: &PriceTick) -> bool {
        self.price_tick == other.price_tick
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_side_from_str() {
        assert_eq!(Side::from_str("buy").unwrap(), Side::Buy);
        assert_eq!(Side::from_str("sell").unwrap(), Side::Sell);
        assert_eq!(Side::from_str("none").unwrap(), Side::None);
        assert_eq!(Side::from_str("invalid").unwrap(), Side::Unsupported);
    }

    #[test]
    fn test_ord_type_from_i32() {
        assert_eq!(OrderType::from_i32(10).unwrap(), OrderType::Cancel);
        assert_eq!(OrderType::from_i32(2).unwrap(), OrderType::L);
        assert_eq!(OrderType::from_i32(3).unwrap(), OrderType::N);
        assert!(OrderType::from_i32(999).is_err());
    }

    #[test]
    fn test_ord_type_from_str_with_edge_cases() {
        assert_eq!(OrderType::from_str("L").unwrap(), OrderType::L);
        assert_eq!(OrderType::from_str("M").unwrap(), OrderType::M);
        assert_eq!(OrderType::from_str("N").unwrap(), OrderType::N);
        assert_eq!(OrderType::from_str("B").unwrap(), OrderType::B);
        assert_eq!(OrderType::from_str("C").unwrap(), OrderType::C);
        assert_eq!(OrderType::from_str("D").unwrap(), OrderType::D);
        assert_eq!(
            OrderType::from_str("unknown").unwrap(),
            OrderType::Unsupported
        );
    }

    #[test]
    fn test_price_tick() {
        let price_tick1: PriceTick = PriceTick::new(100, true);
        let price_tick2: PriceTick = PriceTick::new(101, true);
        assert_eq!(price_tick1 < price_tick2, false);

        let price_tick1: PriceTick = PriceTick::new(100, false);
        let price_tick2: PriceTick = PriceTick::new(101, false);
        assert_eq!(price_tick1 < price_tick2, true);
    }

    #[test]
    fn test_price_tick_equality() {
        let price_tick1: PriceTick = PriceTick::new(100, true);
        let price_tick2: PriceTick = PriceTick::new(100, true);
        assert_eq!(price_tick1, price_tick2);

        let price_tick1: PriceTick = PriceTick::new(100, true);
        let price_tick2: PriceTick = PriceTick::new(100, false);
        assert_eq!(price_tick1, price_tick2);
    }
}
