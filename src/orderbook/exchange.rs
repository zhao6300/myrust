use dataloader::DataCollator;
use hook::{Hook, HookType};

use super::broker::Broker;
use super::order::{Order, OrderRef};
use super::*;
use std::marker;
use std::ops::Neg;
use std::str::FromStr;
use std::thread::sleep;

/// `Exchange` 结构体表示一个交易所，用于管理多个经纪商和订单相关的操作。
///
/// # 泛型参数
/// - `MD`: 表示市场深度（`L3MarketDepth`）的类型。
#[derive(Debug, Serialize, Deserialize)]
pub struct Exchange<MD> {
    /// 交易所的模式，例如实时模式或测试模式。
    pub mode: ExchangeMode,
    /// 一个映射，存储了股票代码与对应的经纪商实例。
    pub broker_map: HashMap<String, Broker<MD>>,
    /// 当前日期，格式为 YYYY/MM/DD。
    pub date: Option<String>,
    /// 最新的序列号，用于生成订单的序列号。
    pub latest_seq: i64,
    /// 最新的订单 ID，用于生成订单的唯一标识。
    pub latest_order_id: i64,
}

unsafe impl<MD> Send for Exchange<MD> {}

unsafe impl<MD> Sync for Exchange<MD> {}

impl<'a, MD> Exchange<MD>
where
    MD: L3MarketDepth + Serialize + Deserialize<'a> + RecoverOp + StatisticsOp + SnapshotOp,
    MarketError: From<<MD as L3MarketDepth>::Error>,
{
    /// 创建一个新的 `Exchange` 实例。
    ///
    /// # 参数
    /// - `mode`: 交易所的模式（字符串形式）。
    /// - `data`: 当前日期（字符串形式）。
    ///
    /// # 返回值
    /// 返回一个 `Exchange` 实例。
    pub fn new(mode: &str, data: &str) -> Self {
        Self {
            mode: ExchangeMode::from_str(&mode).unwrap(),
            broker_map: HashMap::new(),
            date: Some(data.to_string()),
            latest_seq: 0,
            latest_order_id: 0,
        }
    }

    pub fn exists_stock(&self, stock_code: &str) -> bool {
        self.broker_map.contains_key(stock_code)
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
    pub fn elapse(&mut self, duration: i64, stock_code: Option<&str>) -> Result<i64, MarketError> {
        // 遍历所有经纪商，更新状态
        let mut total_filled: i64 = 0;

        if stock_code.is_none() {
            for (_, broker) in self.broker_map.iter_mut() {
                let filled = broker.elapse(duration)?;
                total_filled += filled;
                broker.sync_order_info();
            }
        } else {
            let broker = self
                .broker_map
                .get_mut(stock_code.unwrap())
                .ok_or(MarketError::StockBrokerNotExist)?;
            let filled = broker.elapse(duration)?;
            total_filled += filled;
            broker.sync_order_info();
        }

        Ok(total_filled)
    }

    /// 从指定经纪商的订单簿中检索订单，并根据给定的状态筛选订单。
    ///
    /// # 参数
    /// - `stock_code`: 经纪商的股票代码，订单将从此经纪商检索。
    /// - `orders`: 一个可变的 `HashMap`，用于存储符合条件的订单。
    /// - `filter`: 一个包含订单状态的向量，用于筛选符合条件的订单。
    ///
    /// # 返回值
    /// 此方法不返回值。它会根据 `filter` 参数中的状态，将符合条件的订单填充到 `orders` 中。
    pub fn get_orders(
        &mut self,
        orders: &mut HashMap<OrderId, OrderRef>,
        filter: &Vec<OrderStatus>,
        stock_code: Option<&str>,
    ) -> Result<bool, MarketError> {
        if stock_code.is_none() {
            for (_, broker) in self.broker_map.iter_mut() {
                broker.get_orders(orders, filter);
            }
        } else {
            let broker = self
                .broker_map
                .get_mut(stock_code.unwrap())
                .ok_or(MarketError::StockBrokerNotExist)?;
            broker.get_orders(orders, filter);
        }
        Ok(true)
    }
    /// 从经纪商的订单簿中检索最新的订单。
    ///
    /// # 参数
    /// - `stock_code`: 经纪商的股票代码，订单将从此经纪商检索。
    /// - `orders`: 存储最新订单的 `HashMap` 的可变引用。
    ///
    /// # 返回值
    /// 此方法不返回值。它会将最新的订单填充到 `orders` 中。
    pub fn get_latest_orders(
        &mut self,
        orders: &mut HashMap<OrderId, OrderRef>,
        stock_code: Option<&str>,
    ) -> Result<bool, MarketError> {
        if stock_code.is_none() {
            for (_, broker) in self.broker_map.iter_mut() {
                broker.get_latest_orders(orders);
            }
        } else {
            let broker = self
                .broker_map
                .get_mut(stock_code.unwrap())
                .ok_or(MarketError::StockBrokerNotExist)?;
            broker.get_latest_orders(orders);
        }
        Ok(true)
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
        market_type: MarketType,
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
        let mut broker = Broker::new(
            mode,
            market_type,
            stock_type,
            stock_code.clone(),
            tick_size,
            lot_size,
        );
        broker.init();

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

    /// 获取指定股票代码的经纪商的可变引用。
    ///
    /// # 参数
    /// - `stock_code`: 股票代码。
    ///
    /// # 返回值
    /// 返回对应经纪商的可变引用，如果不存在则返回 `None`。
    pub fn get_broker_mut(&mut self, stock_code: &str) -> Option<&mut Broker<MD>> {
        self.broker_map.get_mut(stock_code)
    }

    pub fn get_crurent_time(&self, stock_code: &str) -> Result<i64, MarketError> {
        if let Some(broker) = self.broker_map.get(stock_code) {
            let timestamp = broker.get_crurent_time();
            Ok(timestamp)
        } else {
            Err(MarketError::StockBrokerNotExist)
        }
    }

    /// 获取指定股票代码的经纪商的不可变引用。
    ///
    /// # 参数
    /// - `stock_code`: 股票代码。
    ///
    /// # 返回值
    /// 返回对应经纪商的不可变引用，如果不存在则返回 `None`。
    pub fn get_broker(&self, stock_code: &str) -> Option<&Broker<MD>> {
        self.broker_map.get(stock_code)
    }

    /// 生成一个新的订单序列号。
    ///
    /// # 返回值
    /// 返回生成的订单序列号。
    pub fn generate_seq_num(&mut self) -> i64 {
        self.latest_seq += 1;
        self.latest_seq
    }

    pub fn generate_order_num(&mut self) -> i64 {
        self.latest_order_id += 1;
        self.latest_order_id
    }

    /// 生成一个新的订单 ID。
    ///
    /// # 返回值
    /// 返回生成的订单 ID。
    pub fn best_bid(&self, stock_code: &str, source: &OrderSourceType) -> Result<f64, MarketError> {
        let broker = self
            .broker_map
            .get(stock_code)
            .ok_or(MarketError::StockBrokerNotExist)?;

        Ok(broker.market_depth.best_bid(source))
    }

    /// 获取指定股票代码的最佳买入价。
    ///
    /// # 参数
    /// - `stock_code`: 股票代码。
    /// - `source`: 订单来源类型。
    ///
    /// # 返回值
    /// 返回最佳买入价。
    pub fn best_ask(&self, stock_code: &str, source: &OrderSourceType) -> Result<f64, MarketError> {
        let broker = self
            .broker_map
            .get(stock_code)
            .ok_or(MarketError::StockBrokerNotExist)?;

        Ok(broker.market_depth.best_ask(source))
    }

    pub fn best_bid_tick(
        &self,
        stock_code: &str,
        source: &OrderSourceType,
    ) -> Result<i64, MarketError> {
        let broker = self
            .broker_map
            .get(stock_code)
            .ok_or(MarketError::StockBrokerNotExist)?;

        Ok(broker.market_depth.best_bid_tick(source))
    }

    /// 获取指定股票代码的最佳买入价对应的价格档位。
    ///
    /// # 参数
    /// - `stock_code`: 股票代码。
    /// - `source`: 订单来源类型。
    ///
    /// # 返回值
    /// 返回最佳买入价对应的价格档位（tick）。
    pub fn best_ask_tick(
        &self,
        stock_code: &str,
        source: &OrderSourceType,
    ) -> Result<i64, MarketError> {
        let broker = self
            .broker_map
            .get(stock_code)
            .ok_or(MarketError::StockBrokerNotExist)?;

        Ok(broker.market_depth.best_ask_tick(source))
    }

    /// 获取指定股票代码的最佳卖出价对应的价格档位。
    ///
    /// # 参数
    /// - `stock_code`: 股票代码。
    /// - `source`: 订单来源类型。
    ///
    /// # 返回值
    /// 返回最佳卖出价对应的价格档位（tick）。
    pub fn tick_size(&self, stock_code: &str) -> Result<f64, MarketError> {
        let broker = self
            .broker_map
            .get(stock_code)
            .ok_or(MarketError::StockBrokerNotExist)?;

        Ok(broker.market_depth.tick_size())
    }

    /// 获取指定股票代码的最小价格变动单位。
    ///
    /// # 参数
    /// - `stock_code`: 股票代码。
    ///
    /// # 返回值
    /// 返回最小价格变动单位（tick size）。
    pub fn lot_size(&self, stock_code: &str) -> Result<f64, MarketError> {
        let broker = self
            .broker_map
            .get(stock_code)
            .ok_or(MarketError::StockBrokerNotExist)?;

        Ok(broker.market_depth.lot_size())
    }

    /// 获取指定股票代码的最小交易单位。
    ///
    /// # 参数
    /// - `stock_code`: 股票代码。
    ///
    /// # 返回值
    /// 返回最小交易单位（lot size）。
    pub fn bid_vol_at_tick(&self, price: f64, stock_code: &str) -> Result<i64, MarketError> {
        let price_tick = (price / self.tick_size(stock_code)?).round() as i64;

        let broker = self
            .broker_map
            .get(stock_code)
            .ok_or(MarketError::StockBrokerNotExist)?;

        Ok(broker.market_depth.bid_vol_at_tick(price_tick))
    }

    /// 获取指定价格下的买单量。
    ///
    /// # 参数
    /// - `price`: 指定价格。
    /// - `stock_code`: 股票代码。
    ///
    /// # 返回值
    /// 返回指定价格下的买单量。
    pub fn ask_vol_at_tick(&self, price: f64, stock_code: &str) -> Result<i64, MarketError> {
        let price_tick = (price / self.tick_size(stock_code)?).round() as i64;

        let broker = self
            .broker_map
            .get(stock_code)
            .ok_or(MarketError::StockBrokerNotExist)?;

        Ok(broker.market_depth.ask_vol_at_tick(price_tick))
    }

    pub fn set_prev_close_price(
        &mut self,
        stock_code: &str,
        price: f64,
    ) -> Result<bool, MarketError> {
        let broker = self
            .broker_map
            .get_mut(stock_code)
            .ok_or(MarketError::StockBrokerNotExist)?;
        broker.set_previous_close_price(price);
        Ok(true)
    }

    pub fn register_orderbook_hook(
        &mut self,
        stock_code: &str,
        hook_type: HookType,
        name: &str,
        hook: Hook,
    ) -> Result<bool, MarketError> {
        let mut broker = self
            .broker_map
            .get_mut(stock_code)
            .ok_or(MarketError::StockBrokerNotExist)?;
        broker.register_orderbook_hook(hook_type, name, hook);
        Ok(true)
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
        let order_type = OrderType::L; // 默认订单类型
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

    pub fn snapshot(&self, stock_code: &str) -> String {
        if let Some(broker) = self.broker_map.get(&stock_code.to_string()) {
            serde_json::to_string(broker).unwrap_or("{}".to_string())
        } else {
            String::new()
        }
    }
}

impl<'a, MD> RecoverOp for Exchange<MD>
where
    MD: L3MarketDepth + Serialize + Deserialize<'a> + RecoverOp + StatisticsOp + SnapshotOp,
    MarketError: From<<MD as L3MarketDepth>::Error>,
{
    fn recover(&mut self) -> Result<bool, MarketError> {
        for borker in self.broker_map.values_mut() {
            let _ = borker.recover();
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::skiplist_orderbook::SkipListMarketDepth;

    use super::utils::time_difference_ms_i64;
    use super::*;

    #[test]
    /// 测试 Exchange 的创建。
    /// 验证创建后的模式、经纪商映射、最新序列号和最新订单 ID 是否正确。
    fn test_exchange_new() {
        let mut exchange = Exchange::<SkipListMarketDepth>::new("live", "2023/01/01");
        assert_eq!(exchange.mode, ExchangeMode::Live);
        assert!(exchange.broker_map.is_empty());
        assert_eq!(exchange.latest_seq, 0);
        assert_eq!(exchange.latest_order_id, 0);
    }

    #[test]
    /// 测试成功添加经纪商。
    /// 验证添加经纪商后，`broker_map` 是否包含指定的股票代码。
    fn test_add_broker_success() {
        let mut exchange = Exchange::<SkipListMarketDepth>::new("live", "2023/01/01");
        let result = exchange.add_broker(
            MarketType::SH,
            ExchangeMode::Live,
            "stock".to_string(),
            "AAPL".to_string(),
            100.0,
        );
        assert!(result.is_ok());
        assert!(exchange.broker_map.contains_key("AAPL"));
    }

    #[test]
    /// 测试添加经纪商时，股票类型不支持的错误。
    /// 验证如果提供未知的股票类型，会返回 `StockTypeUnSupported` 错误。
    fn test_add_broker_error_stock_type() {
        let mut exchange = Exchange::<SkipListMarketDepth>::new("live", "2023/01/01");
        let result = exchange.add_broker(
            MarketType::SH,
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
            MarketType::SH,
            ExchangeMode::Live,
            "stock".to_string(),
            "AAPL".to_string(),
            100.0,
        );
        let result = exchange.add_broker(
            MarketType::SH,
            ExchangeMode::Live,
            "stock".to_string(),
            "AAPL".to_string(),
            100.0,
        );
        assert!(result.is_err());
    }

    #[test]
    /// 测试成功发送订单。
    /// 验证订单发送后，返回的订单 ID 是否有效。
    fn test_send_order_success() {
        let mut exchange = Exchange::<SkipListMarketDepth>::new("live", "2023/01/01");
        let _ = exchange.add_broker(
            MarketType::SH,
            ExchangeMode::Live,
            "stock".to_string(),
            "AAPL".to_string(),
            100.0,
        );
        let result = exchange.send_order(
            "none",
            "AAPL",
            20230101123456789, // 17 位时间戳
            150.0,
            10,
            "buy",
        );
        assert!(result.is_ok());
        let order_id = result.unwrap();
        assert!(order_id > 0);
    }

    #[test]
    /// 测试发送订单时订单时间无效的错误。
    /// 验证如果订单时间不是 17 位整数，会返回 `InvalidOrderRequest` 错误。
    fn test_send_order_error_invalid_order_time() {
        let mut exchange = Exchange::<SkipListMarketDepth>::new("live", "2023/01/01");
        let _ = exchange.add_broker(
            MarketType::SH,
            ExchangeMode::Live,
            "stock".to_string(),
            "AAPL".to_string(),
            100.0,
        );
        let result = exchange.send_order(
            "none",
            "AAPL",
            2023010112345678, // 无效的订单时间
            150.0,
            10,
            "buy",
        );
        assert!(result.is_err());
    }

    #[test]
    /// 测试成功取消订单。
    /// 验证取消订单后，返回值是否为 `Ok(true)`。
    fn test_cancel_order_success() {
        let mut exchange = Exchange::<SkipListMarketDepth>::new("live", "2023/01/01");
        let _ = exchange.add_broker(
            MarketType::SH,
            ExchangeMode::Live,
            "stock".to_string(),
            "AAPL".to_string(),
            100.0,
        );
        let _ = exchange
            .send_order(
                "none",
                "AAPL",
                20230101123456789, // 17 位时间戳
                150.0,
                10,
                "buy",
            )
            .unwrap();
        let result = exchange.cancel_order("AAPL", 1); // 使用之前生成的订单 ID
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_snapshot_success() {
        // 创建模拟的交易所
        let mut exchange = Exchange::<SkipListMarketDepth>::new("live", "2023/01/01");

        // 添加一个模拟的经纪商
        let result = exchange.add_broker(
            MarketType::SH,
            ExchangeMode::Live,
            "stock".to_string(),
            "AAPL".to_string(),
            100.0,
        );

        // 确保经纪商添加成功
        assert!(result.is_ok());
        assert!(exchange.broker_map.contains_key("AAPL"));

        // 调用snapshot并检查结果
        let snapshot = exchange.snapshot("AAPL");
        print!("{:?}\n", snapshot);
        // 验证快照内容不为空
        assert!(!snapshot.is_empty(), "Snapshot should not be empty");

        // 反序列化快照并检查经纪商的字段
        let broker: Broker<SkipListMarketDepth> =
            serde_json::from_str(&snapshot).expect("Failed to deserialize snapshot");
        assert_eq!(broker.stock_code, "AAPL");
        assert_eq!(broker.lot_size, 100.0);
        assert_eq!(broker.stock_type, "stock".to_string());
    }

    #[test]
    fn test_elpase() {
        let exchange_mode = "backtest".to_string();
        let stock_code = "688007.SH".to_string();
        let file_type = "local".to_string();
        let data_path = "./data".to_string();
        let date = "20231201".to_string();
        let mode = "L2P";
        let mut exchange =
            Exchange::<SkipListMarketDepth>::new(exchange_mode.as_str(), date.as_str());

        let mut data = DataCollator::new(
            stock_code.clone(),
            file_type.clone(),
            data_path.clone(),
            date.clone(),
            mode.clone(),
        );
        data.init();
        let _ = exchange.add_broker(
            MarketType::SH,
            ExchangeMode::from_str(&exchange_mode.as_str()).unwrap_or(ExchangeMode::Backtest),
            "stock".to_string(),
            stock_code.clone(),
            100.0,
        );

        let _ = exchange.add_data(stock_code.as_str(), data);
        let start: i64 = 20231201092521355;
        let duration = time_difference_ms_i64(
            exchange.get_crurent_time(stock_code.as_str()).unwrap_or(0),
            start,
        )
        .unwrap_or(0);
        let _ = exchange.elapse(duration + 3000, Some(stock_code.as_str()));
        let mut orders = HashMap::new();
        exchange.get_latest_orders(&mut orders, Some(stock_code.as_str()));
        print!("{:?}\n", orders);
        print!("{}\n", exchange.snapshot(stock_code.as_str()));
    }
}
