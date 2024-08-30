use serde::{Deserialize, Serialize};
use std::cmp::{Ord, Ordering};
use std::i32;
use std::str::FromStr;

use super::MarketError;
pub type OrderId = u64;

#[derive(Clone, Copy, Eq, PartialEq, Debug, Deserialize, Serialize)]
#[repr(i8)]
pub enum Side {
    /// 代表买入订单。
    Buy = 1,
    /// 代表卖出订单。
    Sell = -1,
    /// 代表未指定或中性方向。
    None = 0,
    /// 代表不支持的方向。
    Unsupported = 127,
}

impl AsRef<f64> for Side {
    fn as_ref(&self) -> &f64 {
        match self {
            Side::Buy => &1.0f64,
            Side::Sell => &-1.0f64,
            Side::None => panic!("Side::None"),
            Side::Unsupported => panic!("Side::Unsupported"),
        }
    }
}

impl FromStr for Side {
    type Err = ();

    fn from_str(input: &str) -> Result<Side, Self::Err> {
        match input.to_lowercase().as_str() {
            "buy" => Ok(Side::Buy),
            "sell" => Ok(Side::Sell),
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
pub enum OrdType {
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
    /// 代表不支持的订单类型。
    Unsupported = 255,
}

impl OrdType {
    /// 根据整数值创建 `OrdType` 枚举。
    ///
    /// # 参数
    /// - `type_num`: 订单类型的整数表示。
    ///
    /// # 返回
    /// - `Ok(OrdType)`: 对应的订单类型。
    /// - `Err(MarketError)`: 如果类型不被支持，返回错误。
    pub fn from_i32(type_num: i32) -> Result<OrdType, MarketError> {
        match type_num {
            10 => Ok(OrdType::Cancel),
            2 => Ok(OrdType::L),
            3 => Ok(OrdType::N),
            _ => Err(MarketError::OrderTypeUnsupported),
        }
    }
}

impl FromStr for OrdType {
    type Err = ();

    fn from_str(input: &str) -> Result<OrdType, Self::Err> {
        match input {
            "L" => Ok(OrdType::L),
            "M" => Ok(OrdType::M),
            "N" => Ok(OrdType::N),
            "B" => Ok(OrdType::B),
            "C" => Ok(OrdType::C),
            "D" => Ok(OrdType::D),
            _ => Ok(OrdType::Unsupported),
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
    #[serde(skip_serializing)]
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
        assert_eq!(OrdType::from_i32(10).unwrap(), OrdType::Cancel);
        assert_eq!(OrdType::from_i32(2).unwrap(), OrdType::L);
        assert_eq!(OrdType::from_i32(3).unwrap(), OrdType::N);
        assert!(OrdType::from_i32(999).is_err());
    }

    #[test]
    fn test_ord_type_from_str_with_edge_cases() {
        assert_eq!(OrdType::from_str("L").unwrap(), OrdType::L);
        assert_eq!(OrdType::from_str("M").unwrap(), OrdType::M);
        assert_eq!(OrdType::from_str("N").unwrap(), OrdType::N);
        assert_eq!(OrdType::from_str("B").unwrap(), OrdType::B);
        assert_eq!(OrdType::from_str("C").unwrap(), OrdType::C);
        assert_eq!(OrdType::from_str("D").unwrap(), OrdType::D);
        assert_eq!(OrdType::from_str("unknown").unwrap(), OrdType::Unsupported);
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
