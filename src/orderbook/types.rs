use super::MarketError;
use serde::{Deserialize, Serialize};
use std::cmp::{Ord, Ordering};
use std::str::FromStr;
pub type OrderId = u64;

#[derive(Clone, Copy, Eq, PartialEq, Debug, Deserialize, Serialize)]
#[repr(i8)]
pub enum Side {
    Buy = 1,
    Sell = -1,
    None = 0,
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
        match input {
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
    ///为‘L’表示普通限价订单；
    L = 0,
    ///为‘M’表示最优五档即时成交剩余撤销的市价订单
    M = 1,
    ///为‘N’ 表示最优五档即时成交剩余转限价的市价订单
    N = 2,
    ///为’B’ 表示以本方最优价格申报的市价订单
    B = 3,
    ///为’C’ 表示以对手方最优价格申报的市价订单
    C = 4,
    ///市价全额成交或撤销
    D = 5,

    Unsupported = 255,
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
    LocalOrder = 0,
    UserOrder = 1,
    Unknown = 255,
}

impl FromStr for OrderSourceType {
    type Err = ();

    fn from_str(input: &str) -> Result<OrderSourceType, Self::Err> {
        match input {
            "limit" => Ok(OrderSourceType::LocalOrder),
            "market" => Ok(OrderSourceType::UserOrder),
            _ => Ok(OrderSourceType::Unknown),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum Status {
    None = 0,
    New = 1,
    Expired = 2,
    Filled = 3,
    Canceled = 4,
    PartiallyFilled = 5,
    Rejected = 6,
    Unsupported = 255,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum ExchangeMode {
    Backtest = 0,
    Live = 1,
    Unsupported = 255,
}

impl FromStr for ExchangeMode {
    type Err = ();

    fn from_str(input: &str) -> Result<ExchangeMode, Self::Err> {
        match input {
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

#[derive(Eq, Debug)]
struct PriceTick {
    pub price_tick: i64,
    pub reverse: bool,
}

impl PriceTick {
    fn new(price_tick: i64, reverse: bool) -> Self {
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
    use super::PriceTick;

    #[test]
    fn test_price_tick() {
        let price_tick1: PriceTick = PriceTick::new(100, true);
        let price_tick2: PriceTick = PriceTick::new(101, true);
        assert_eq!(price_tick1 < price_tick2, false);

        let price_tick1: PriceTick = PriceTick::new(100, false);
        let price_tick2: PriceTick = PriceTick::new(101, false);
        assert_eq!(price_tick1 < price_tick2, true);
    }
}
