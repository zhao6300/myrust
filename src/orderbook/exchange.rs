use dataloader::DataCollator;

use super::broker::Broker;
use super::order::{Order, OrderRef};
use super::*;
use core::borrow;
use std::borrow::Borrow;
use std::str::FromStr;
use std::thread::sleep;

pub struct Exchange<MD> {
    pub mode: ExchangeMode,
    pub broker_map: HashMap<String, Broker<MD>>,
    /// YYYY/MM/DD
    pub date: Option<String>,
    pub latest_seq: i64,
    pub latest_order_id: i64,
}

impl<MD> Exchange<MD>
where
    MD: L3MarketDepth,
    MarketError: From<<MD as L3MarketDepth>::Error>,
{
    pub fn new(mode: &str, data: &str) -> Self {
        Self {
            mode: ExchangeMode::from_str(&mode).unwrap(),
            broker_map: HashMap::new(),
            date: Some(data.to_string()),
            latest_seq: 0,
            latest_order_id: 0,
        }
    }
    /// 将所有经纪商的时间向前推进指定的时间段。
    ///
    /// # 参数
    /// - `duration`: 要推进的时间段（以毫秒为单位）。
    ///
    /// # 返回值
    /// - `Ok(true)`: 如果操作成功。
    /// - `Err(MarketError)`: 如果操作失败，返回错误。
    ///
    /// # 错误
    /// - 错误来自于每个经纪商的 `elapse` 方法。
    pub fn elapse(&mut self, duration: i64) -> Result<bool, MarketError> {
        // 遍历所有经纪商，更新状态
        for (_, broker) in self.broker_map.iter_mut() {
            broker.elapse(duration)?;
            broker.sync_order_info();
        }
        Ok(true)
    }

    pub fn get_orders(
        &mut self,
        stock_code: &str,
        orders: &mut HashMap<OrderId, OrderRef>,
        status: OrderStatus,
    ) {
        if let Some(broker) = self.broker_map.get_mut(stock_code) {
            broker.get_orders(orders, status);
        }
    }
    /// 从经纪商的订单簿中检索最新的订单。
    ///
    /// # 参数
    /// - `stock_code`: 经纪商的股票代码，订单将从此经纪商检索。
    /// - `orders`: 存储最新订单的 `HashMap` 的可变引用。
    ///
    /// # 返回值
    /// 此方法不返回值。它会将最新的订单填充到 `orders` 中。
    pub fn get_latest_orders(&mut self, stock_code: &str, orders: &mut HashMap<OrderId, OrderRef>) {
        if let Some(broker) = self.broker_map.get_mut(stock_code) {
            broker.get_latest_orders(orders);
        }
    }
    pub fn match_order_util_mdtime(&mut self, order_time: i64) -> Result<String, MarketError> {
        let results: String = String::from("none");
        Ok(results)
    }
    /// 向交易所添加一个新的经纪商。
    ///
    /// # 参数
    /// - `mode`: 交易所的模式（例如，实时模式、测试模式）。
    /// - `stock_type`: 股票类型（例如，“stock” 或 “fund”）。
    /// - `stock_code`: 新经纪商的股票代码。
    /// - `lot_size`: 新经纪商的最小交易单位。
    ///
    /// # 返回值
    /// - `Ok(true)`: 如果经纪商成功添加。
    /// - `Err(MarketError)`: 如果无法添加经纪商，返回错误。
    ///
    /// # 错误
    /// - `StockTypeUnSupported`: 如果 `stock_type` 不受支持。
    /// - `StockBrokerIdExist`: 如果给定 `stock_code` 的经纪商已经存在。
    pub fn add_broker(
        &mut self,
        mode: ExchangeMode,
        stock_type: String,
        stock_code: String,
        lot_size: f64,
    ) -> Result<bool, MarketError> {
        // 定义有效的 tick size
        let tick_size = match stock_type.to_lowercase().as_str() {
            "stock" => 0.01,
            "fund" => 0.001,
            _ => return Err(MarketError::StockTypeUnSupported),
        };
        // 检查经纪商是否已存在
        if self.broker_map.contains_key(&stock_code) {
            return Err(MarketError::StockBrokerIdExist);
        }

        // 创建新的 Broker 实例
        let broker = Broker::new(mode, stock_type, stock_code.clone(), tick_size, lot_size);

        // 将新创建的 Broker 插入到 broker_map 中
        self.broker_map.insert(stock_code, broker);

        Ok(true)
    }
    /// 将数据添加到指定经纪商的数据收集器中。
    ///
    /// # 参数
    /// - `stock_code`: 要添加数据的经纪商的股票代码。
    /// - `data`: 要添加到经纪商数据收集器中的数据。
    ///
    /// # 返回值
    /// - `Ok(true)`: 如果数据成功添加。
    /// - `Err(MarketError)`: 如果添加数据失败，返回错误。
    ///
    /// # 错误
    /// - `StockBrokerNotExist`: 如果给定股票代码的经纪商不存在。
    pub fn add_data(&mut self, stock_code: &str, data: DataCollator) -> Result<bool, MarketError> {
        if let Some(broker) = self.broker_map.get_mut(stock_code) {
            broker.add_data(Some(data))?;
            Ok(true)
        } else {
            Err(MarketError::StockBrokerNotExist)
        }
    }

    pub fn get_broker_mut(&mut self, stock_code: &str) -> Option<&mut Broker<MD>> {
        self.broker_map.get_mut(stock_code)
    }

    pub fn get_broker(&self, stock_code: &str) -> Option<&Broker<MD>> {
        self.broker_map.get(stock_code)
    }

    pub fn generate_seq_num(&mut self) -> i64 {
        self.latest_seq += 1;
        self.latest_seq
    }

    pub fn generate_order_num(&mut self) -> i64 {
        self.latest_order_id += 1;
        self.latest_order_id
    }

    fn best_bid(&self, stock_code: &str) -> f64 {
        self.broker_map
            .get(stock_code)
            .unwrap()
            .market_depth
            .as_ref()
            .best_bid()
    }

    fn best_ask(&self, stock_code: &str) -> f64 {
        self.broker_map
            .get(stock_code)
            .unwrap()
            .market_depth
            .as_ref()
            .best_ask()
    }

    fn best_bid_tick(&self, stock_code: &str) -> i64 {
        self.broker_map
            .get(stock_code)
            .unwrap()
            .market_depth
            .as_ref()
            .best_bid_tick()
    }

    fn best_ask_tick(&self, stock_code: &str) -> i64 {
        self.broker_map
            .get(stock_code)
            .unwrap()
            .market_depth
            .as_ref()
            .best_ask_tick()
    }

    fn tick_size(&self, stock_code: &str) -> f64 {
        self.broker_map
            .get(stock_code)
            .unwrap()
            .market_depth
            .as_ref()
            .tick_size()
    }

    fn lot_size(&self, stock_code: &str) -> f64 {
        self.broker_map
            .get(stock_code)
            .unwrap()
            .market_depth
            .as_ref()
            .lot_size()
    }

    fn bid_vol_at_tick(&self, price: f64, stock_code: &str) -> i64 {
        let price_tick = (price / self.tick_size(stock_code)).round() as i64;
        self.broker_map
            .get(stock_code)
            .unwrap()
            .market_depth
            .as_ref()
            .bid_vol_at_tick(price_tick)
    }

    fn ask_vol_at_tick(&self, price: f64, stock_code: &str) -> i64 {
        let price_tick = (price / self.tick_size(stock_code)).round() as i64;
        self.broker_map
            .get(stock_code)
            .unwrap()
            .market_depth
            .as_ref()
            .ask_vol_at_tick(price_tick)
    }
    /// 向指定的股票经纪商发送一个新订单，并返回订单 ID。
    ///
    /// 此方法会：
    /// - 生成一个新的订单 ID。
    /// - 验证订单时间是否为 17 位长度。
    /// - 创建订单并将其提交到指定的经纪商。
    /// - 返回新订单的 ID 或者在失败时返回错误。
    ///
    /// # 参数
    /// - `stock_code`: 目标股票代码，指定订单将被发送到哪个经纪商。
    /// - `order_time`: 订单的下单时间，使用 17 位整数表示，格式应为 YYYYMMDDHHMMSSSSS。
    /// - `order_price`: 订单的价格，以浮点数表示。
    /// - `order_volume`: 订单的数量，以整数表示。
    /// - `bs_flag`: 标记订单是买入还是卖出，具体取值可能依赖于业务逻辑。
    ///
    /// # 返回值
    /// - `Ok(OrderId)`: 如果操作成功，返回新创建的订单 ID。
    /// - `Err(MarketError)`: 如果操作失败，返回错误。可能的错误包括订单时间无效或经纪商不存在。
    ///
    /// # 错误
    /// - `InvalidOrderRequest`: 如果订单时间不是 17 位整数。
    /// - `StockBrokerNotExist`: 如果给定股票代码的经纪商不存在。
    pub fn send_order(
        &mut self,
        acc: &str,
        stock_code: &str,
        order_time: i64,
        order_price: f64,
        order_volume: i64,
        bs_flag: &str,
    ) -> Result<OrderId, MarketError> {
        // 生成新的订单 ID
        let order_id = self.generate_order_num();

        // 验证订单时间是否符合 17 位长度
        let order_time_str = order_time.to_string();
        if order_time_str.len() != 17 {
            return Err(MarketError::InvalidOrderRequest); // 使用自定义错误处理
        }
        // 获取经纪商
        let broker = match self.broker_map.get_mut(stock_code) {
            Some(broker) => broker,
            None => return Err(MarketError::StockBrokerNotExist),
        };
        let account = match acc.to_lowercase().as_str() {
            "none" => None,
            _ => Some(acc.to_string()),
        };
        // 创建订单
        let order_type = OrdType::L; // 默认订单类型
        let order = Order::new_ref(
            account,
            stock_code.to_string(),
            order_time,
            order_price,
            order_volume as f64,
            bs_flag,
            order_type,
            OrderSourceType::UserOrder,
        );

        order.borrow_mut().order_id = order_id;
        // 提交订单
        match broker.submit_order(order) {
            Ok(_) => Ok(order_id),
            Err(err) => Err(err),
        }
    }

    pub fn cancel_order(&mut self, stock_code: &str, order_id: i64) -> Result<bool, MarketError> {
        let broker = match self.broker_map.get_mut(stock_code) {
            Some(broker) => broker,
            None => return Err(MarketError::StockBrokerNotExist),
        };

        broker.cancel_order(order_id);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::skiplist_orderbook::SkipListMarketDepth;

    use super::*;

    #[test]
    fn test_exchange_new() {
        let mut exchange = Exchange::<SkipListMarketDepth>::new("live", "2023/01/01");
        assert_eq!(exchange.mode, ExchangeMode::Live);
        assert!(exchange.broker_map.is_empty());
        assert_eq!(exchange.latest_seq, 0);
        assert_eq!(exchange.latest_order_id, 0);
    }

    #[test]
    fn test_add_broker_success() {
        let mut exchange = Exchange::<SkipListMarketDepth>::new("live", "2023/01/01");
        let result = exchange.add_broker(
            ExchangeMode::Live,
            "stock".to_string(),
            "AAPL".to_string(),
            100.0,
        );
        assert!(result.is_ok());
        assert!(exchange.broker_map.contains_key("AAPL"));
    }

    #[test]
    fn test_add_broker_error_stock_type() {
        let mut exchange = Exchange::<SkipListMarketDepth>::new("live", "2023/01/01");
        let result = exchange.add_broker(
            ExchangeMode::Live,
            "unknown".to_string(),
            "AAPL".to_string(),
            100.0,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_add_broker_error_already_exists() {
        let mut exchange = Exchange::<SkipListMarketDepth>::new("live", "2023/01/01");
        let _ = exchange.add_broker(
            ExchangeMode::Live,
            "stock".to_string(),
            "AAPL".to_string(),
            100.0,
        );
        let result = exchange.add_broker(
            ExchangeMode::Live,
            "stock".to_string(),
            "AAPL".to_string(),
            100.0,
        );
        assert!(result.is_err());
    }
}
