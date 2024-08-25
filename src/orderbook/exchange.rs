use super::broker::Broker;
use super::order::{Order, OrderRef};
use super::*;
use std::fmt::Debug;
use std::str::FromStr;

pub struct Exchange<MD> {
    pub mode: ExchangeMode,
    pub broker_map: HashMap<String, Broker<MD>>,
    /// YYYY/MM/DD
    pub date: Option<String>,
}

impl<MD> Exchange<MD>
where
    MD: MarketDepth,
{
    pub fn new(mode: &str) -> Self {
        Self {
            mode: ExchangeMode::from_str(&mode).unwrap(),
            broker_map: HashMap::new(),
            date: None,
        }
    }

    pub fn elapse(&mut self, duration: i64) -> Result<bool, MarketError> {
        Ok(true)
    }

    pub fn match_order_util_mdtime(&mut self, order_time: i64) -> Result<String, MarketError> {
        let results: String = String::from("none");
        Ok(results)
    }

    pub fn add_broker(
        &mut self,
        mode: ExchangeMode,
        stock_code: String,
        tick_size: f64,
        lot_size: f64,
    ) -> Result<bool, MarketError> {
        match self.broker_map.contains_key(&stock_code) {
            true => return Err(MarketError::StockBrokerIdExist),
            false => self.broker_map.insert(
                stock_code.clone(),
                Broker::new(mode, stock_code, tick_size, lot_size),
            ),
        };
        Ok(true)
    }

    pub fn get_broker_mut(&mut self, stock_code: &str) -> Option<&mut Broker<MD>>{
        self.broker_map.get_mut(stock_code)
    }

    pub fn send_order(
        &mut self,
        stock_code: &str,
        order_time: i64,
        order_price: f64,
        order_volume: i64,
        bs_flag: &str,
    ) -> Result<OrderId, MarketError> {
        let order_time_str = order_time.to_string();
        if order_time_str.len() != 17 {
            panic!("订单时间只支持17位int, 如20231017093000750");
        }
        let broker = self.broker_map.get_mut(stock_code).unwrap();
        let order_type = OrdType::L;
        let order = Order::new_ref(
            Some("user".to_string()),
            stock_code.to_string(),
            order_time,
            order_price,
            order_volume as f64,
            bs_flag,
            order_type,
            Some(OrderSourceType::UserOrder),
        );
        let mut order_number = broker.submit_order(order).unwrap();
        Ok(order_number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skiplist_orderbook::SkipListMarketDepth;
    #[test]
    fn test_new() {
        let exchange: Exchange<SkipListMarketDepth> = Exchange::new(&"backttest");
    }
}
