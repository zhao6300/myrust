use super::dataloader::DataCollator;
use super::*;

use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    fmt::Debug,
};

use super::order::{Order, OrderRef};
/// 交易经纪人结构体
/// `Broker` 结构体管理交易订单、市场深度、以及与订单处理相关的逻辑。
#[derive(Debug)]
pub struct Broker<MD> {
    /// 交易模式，例如回测模式或实时模式
    pub mode: ExchangeMode,
    /// 股票类型，例如普通股或优先股
    pub stock_type: String,
    /// 股票代码
    pub stock_code: String,
    /// 市场深度
    pub market_depth: Box<MD>,
    /// 当前时间待处理订单
    pub pending_orders: VecDeque<OrderRef>,
    /// 未来时间等待处理的订单，按时间排序
    pub waiting_orders: VecDeque<(i64, OrderRef)>,
    /// 当前时间戳
    pub timestamp: i64,
    /// 所有用户的订单
    pub orders: HashMap<OrderId, OrderRef>,
    /// 最新的序列号
    pub latest_seq_number: i64,
    /// 最小价格变动单位
    pub tick_size: f64,
    /// 最小交易单位
    pub lot_size: f64,
    /// 历史数据源
    pub history: Option<DataCollator>,
    /// 脏订单跟踪器
    pub dirty_tracker: Vec<OrderId>,
}

impl<MD> Broker<MD>
where
    MD: L3MarketDepth,
    MarketError: From<<MD as L3MarketDepth>::Error>,
{
    /// 创建一个新的 `Broker` 实例
    ///
    /// # 参数
    ///
    /// * `mode` - 交易模式
    /// * `stock_type` - 股票类型
    /// * `stock_code` - 股票代码
    /// * `tick_size` - 最小价格变动单位
    /// * `lot_size` - 最小交易单位
    ///
    /// # 返回
    ///
    /// 返回一个初始化好的 `Broker` 实例
    pub fn new(
        mode: ExchangeMode,
        stock_type: String,
        stock_code: String,
        tick_size: f64,
        lot_size: f64,
    ) -> Self {
        Self {
            mode: mode,
            stock_type: stock_type,
            stock_code: stock_code,
            market_depth: MD::new_box(mode.clone(), tick_size.clone(), lot_size.clone()),
            pending_orders: VecDeque::new(),
            waiting_orders: VecDeque::new(),
            timestamp: 0,
            orders: HashMap::new(),
            latest_seq_number: 0,
            tick_size: tick_size,
            lot_size: lot_size,
            history: None,
            dirty_tracker: Vec::new(),
        }
    }

    /// 生成并返回下一个序列号。
    /// 每次调用时，最新的序列号递增1。
    ///
    /// # 返回值
    ///
    /// 返回最新的序列号
    pub fn generate_seq_number(&mut self) -> i64 {
        self.latest_seq_number += 1;
        self.latest_seq_number
    }

    /// 设置历史数据，并返回操作是否成功。
    ///
    /// # 参数
    ///
    /// * `history` - 一个可选的 `DataCollator`，用于设置历史数据。
    ///
    /// # 返回值
    ///
    /// * `Ok(true)` 如果操作成功。
    /// * `Err(MarketError)` 如果出现错误。
    pub fn add_data(&mut self, history: Option<DataCollator>) -> Result<bool, MarketError> {
        self.history = history;
        Ok(true)
    }

    pub fn match_order_l(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let filled = self.market_depth.match_order(order_ref.clone(), i64::MAX)?;
        if order_ref.borrow().vol > 0 {
            let best_tick = self.market_depth.add(order_ref)?;
        }

        Ok(filled)
    }

    pub fn match_order_m(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let filled = 0;
        Ok(filled)
    }

    pub fn match_order_n(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let filled = 0;
        Ok(filled)
    }

    pub fn match_order_b(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let mut order = order_ref.borrow_mut();
        if order.side == Side::Buy {
            order.price_tick.price_tick = self.market_depth.best_bid_tick();
        } else {
            order.price_tick.price_tick = self.market_depth.best_ask_tick();
        }
        let filled = self.market_depth.match_order(order_ref.clone(), i64::MAX)?;
        if order_ref.borrow().vol > 0 {
            let best_tick = self.market_depth.add(order_ref.clone())?;
        }
        Ok(filled)
    }

    pub fn match_order_c(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let mut order = order_ref.borrow_mut();
        if order.side == Side::Buy {
            order.price_tick.price_tick = self.market_depth.best_ask_tick();
        } else {
            order.price_tick.price_tick = self.market_depth.best_bid_tick();
        }
        let filled = self.market_depth.match_order(order_ref.clone(), i64::MAX)?;
        if order_ref.borrow().vol > 0 {
            let best_tick = self.market_depth.add(order_ref.clone())?;
        }
        Ok(filled)
    }

    pub fn match_order_d(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let filled = 0;
        Ok(filled)
    }
    /// 处理订单
    ///
    /// 该方法根据订单类型 (`OrdType`) 处理传入的订单，并执行相应的操作。根据不同的订单类型，方法会调用不同的匹配函数来处理订单。
    ///
    /// # 参数
    ///
    /// * `order_type` - 订单类型，指定要处理的订单类型。
    /// * `l3order_ref` - 订单引用，包含要处理的订单的详细信息。
    ///
    /// # 返回
    ///
    /// 返回成功成交的订单量。处理失败则返回 `Err`。
    pub fn process_order(
        &mut self,
        order_type: OrdType,
        l3order_ref: L3OrderRef,
    ) -> Result<i64, MarketError> {
        let result = match order_type {
            // 处理普通限价订单
            OrdType::L => self.match_order_l(l3order_ref),
            // 处理最优五档即时成交剩余撤销的市价订单
            OrdType::M => self.match_order_m(l3order_ref),
            // 处理最优五档即时成交剩余转限价的市价订单
            OrdType::N => self.match_order_n(l3order_ref),
            // 处理以本方最优价格申报的市价订单
            OrdType::B => self.match_order_b(l3order_ref),
            // 处理以对手方最优价格申报的市价订单
            OrdType::C => self.match_order_c(l3order_ref),
            // 处理市价全额成交或撤销订单
            OrdType::D => self.match_order_d(l3order_ref),
            // 处理取消委托
            OrdType::Cancel => self.cancel_order(l3order_ref.borrow().order_id),
            _ => Err(MarketError::OrderTypeUnsupported),
        };

        result
    }
    /// 获取指定状态的订单
    ///
    /// 该方法根据订单的状态从经纪人的订单映射中提取所有匹配的订单，并将它们添加到传入的 `orders` 映射中。
    ///
    /// # 参数
    ///
    /// * `orders` - 用于存储匹配订单的映射。方法将把符合状态的订单添加到这个映射中。
    /// * `status` - 订单状态，用于筛选符合条件的订单。
    pub fn get_orders(&mut self, orders: &mut HashMap<OrderId, OrderRef>, status: OrderStatus) {
        for (k, v) in self
            .orders
            .iter()
            .filter(|&(k, v)| v.borrow().status == status)
        {
            orders.insert(k.clone(), v.clone());
        }
    }
    /// 获取最近的订单
    ///
    /// 获取从上次调用这个方法到现在的最新变动的订单，并将它们添加到传入的 `orders` 中。它会根据 `dirty_tracker` 中记录的脏订单 ID 来筛选和获取订单。之后，会清空 `dirty_tracker`，以准备下一次的订单更新。
    ///
    /// # 参数
    ///
    /// * `orders` - 用于存储最新订单的映射，将从 `dirty_tracker` 中记录的订单添加到此映射中。
    ///
    /// # 返回
    ///
    pub fn get_latest_orders(&mut self, orders: &mut HashMap<OrderId, OrderRef>) {
        // 遍历脏订单跟踪器中的订单 ID
        for order_id in self.dirty_tracker.drain(..) {
            // 从订单映射中获取对应的订单
            if let Some(order_ref) = self.orders.get(&order_id) {
                // 将订单添加到结果映射中
                orders.insert(order_id, order_ref.clone());
            }
            // 如果订单在映射中未找到，则不做任何处理，继续处理下一个订单
        }

        // 清空脏订单跟踪器
        self.dirty_tracker.clear();
    }
    /// 提交一个新的订单到经纪人系统
    ///
    /// 该方法接收一个订单引用，并将其提交到经纪人系统。如果订单的 ID 已经存在，则返回一个错误；如果订单 ID 不存在，则将订单添加到订单队列中，并根据订单的时间信息决定其处理方式。
    ///
    /// # 参数
    ///
    /// * `order_ref` - 包含要提交的订单信息的订单引用。订单引用提供了对订单对象的可变访问。
    ///
    /// # 返回
    ///
    /// 返回一个 `Result<usize, MarketError>`。成功提交订单时，返回订单的队列位置；如果订单 ID 已经存在，则返回 `Err(MarketError::OrderIdExist)`。
    ///
    /// # 错误
    ///
    /// 可能会遇到的错误包括：
    ///
    /// * `MarketError::OrderIdExist` - 如果订单 ID 已经存在于订单映射中。
    pub fn submit_order(&mut self, order_ref: OrderRef) -> Result<usize, MarketError> {
        // 检查订单 ID 是否已存在
        match self.orders.contains_key(&(order_ref.borrow().order_id)) {
            true => return Err(MarketError::OrderIdExist),
            false => self
                .orders
                .insert(order_ref.borrow().order_id.clone(), order_ref.clone()),
        };

        let mut order_mut = RefCell::borrow_mut(&order_ref);

        order_mut.price_tick = (order_mut.price / self.tick_size).round() as i64;
        // 根据订单的本地时间处理订单
        if order_mut.local_time > self.timestamp {
            // 订单在未来时间点处理
            self.waiting_orders
                .push_back((order_mut.local_time, order_ref.clone()));
        } else {
            // 订单立即处理
            order_mut.seq = self.generate_seq_number();
            self.pending_orders.push_back(order_ref.clone());
        }
        // 计算并返回订单在队列中的位置
        let queue_position: usize = self.pending_orders.len() + self.waiting_orders.len();
        Ok(queue_position)
    }
    /// 模拟时间的推移，并处理所有到期的订单
    ///
    /// # 参数
    ///
    /// * `duration` - 模拟的时间段，以时间单位表示。时间推移将基于此时间段来更新当前时间。
    ///
    /// # 返回
    ///
    /// 返回一个 `Result<bool, MarketError>`。如果成功处理了所有订单并推进了时间，则返回 `Ok(true)`；如果时间点达到历史记录的结束，则返回 `Ok(true)`；如果时间点未到达历史记录的结束，则返回 `Ok(false)`。
    ///
    /// # 错误
    ///
    /// 如果处理订单时发生错误（例如匹配订单失败），方法会返回相应的 `MarketError`。
    pub fn elapse(self: &'_ mut Self, duration: i64) -> Result<bool, MarketError> {
        let mut time_point = self.timestamp + duration;
        while !self.pending_orders.is_empty() {
            let order_ref = self.pending_orders.pop_front().unwrap();
            if order_ref.borrow().status == OrderStatus::Canceled {
                continue;
            }
            let mut order = order_ref.borrow_mut();
            let vol = (order.qty / self.lot_size).round() as i64;
            let l3order_ref = L3Order::new_ref(
                order.source.clone(),
                order.account.clone(),
                order.order_id,
                order.side.clone(),
                order.price_tick.clone(),
                vol,
                order.local_time,
            );
            let fillid = self.process_order(order.order_type, l3order_ref)?;
            if fillid > 0 {
                order.filled_qty = fillid as f64 * self.lot_size;
                self.dirty_tracker.push(order.order_id);
                order.update();
            }
        }

        self.waiting_orders.make_contiguous().sort();

        while !self.waiting_orders.is_empty() {
            let timestamp = self.waiting_orders[0].0;
            if timestamp > time_point {
                break;
            }
            let (_, order_ref) = self.waiting_orders.pop_front().unwrap();
            if order_ref.borrow().status == OrderStatus::Canceled {
                continue;
            }
            let _ = self.goto(timestamp.clone());
            let mut order = order_ref.borrow_mut();
            let vol = (order.qty / self.lot_size).round() as i64;
            let l3order_ref = L3Order::new_ref(
                order.source.clone(),
                order.account.clone(),
                order.order_id,
                order.side.clone(),
                order.price_tick.clone(),
                vol,
                order.local_time,
            );
            order.seq = self.generate_seq_number();
            let fillid = self.process_order(order.order_type, l3order_ref)?;

            if fillid > 0 {
                order.filled_qty = fillid as f64 * self.lot_size;
                self.dirty_tracker.push(order.order_id);
                order.update();
            }
        }

        Ok(true)
    }

    /// 同步订单信息，将市场深度中的订单状态与本地订单进行同步。
    /// 如果订单被标记为已处理或取消，将从市场深度中移除并更新本地订单状态。
    pub fn sync_order_info(&mut self) {
        // 获取市场深度中所有订单的信息
        let l30orders = self.market_depth.orders_mut();

        // 用于追踪需要从市场深度中移除的订单 ID
        let mut remove_tracker: Vec<OrderId> = Vec::with_capacity(100);

        for (order_id, l30order) in l30orders.iter_mut() {
            let mut order = self.orders.get(order_id).unwrap().borrow_mut();
            if l30order.borrow().dirty == true {
                // 同步订单的位置信息和数量
                order.position = l30order.borrow().position;
                order.left_qty = l30order.borrow().vol as f64 * self.lot_size;
                order.filled_qty = order.qty - order.left_qty;

                // 根据订单的成交量和方向更新状态
                if l30order.borrow().vol == 0 {
                    remove_tracker.push(order_id.clone());
                    order.status = OrderStatus::Filled;
                } else if l30order.borrow().side == Side::None {
                    remove_tracker.push(order_id.clone());
                    order.status = OrderStatus::Canceled;
                }

                // 将已修改的订单 ID 添加到脏订单追踪器中
                self.dirty_tracker.push(order_id.clone());
            }
        }
        // 从市场深度中移除已处理或取消的订单
        for idx in remove_tracker {
            l30orders.remove(&idx);
        }
    }

    pub fn goto_end_of_day(&mut self) -> Result<bool, MarketError> {
        self.goto(i64::MAX)
    }
    /// 将时间推进到指定的时间点，并处理该时间点之前的所有订单
    ///
    /// 该方法根据提供的时间点 (`time_point`) 继续模拟市场，并处理所有在该时间点之前的订单。它会从历史数据源中获取订单，并根据订单信息调用相应的处理方法。处理完成后，时间戳会更新到处理过的最后一个订单的时间。
    ///
    /// # 参数
    ///
    /// * `time_point` - 要推进到的时间点，指定模拟的结束时间。时间点必须大于或等于当前时间戳。
    ///
    /// # 返回
    ///
    /// 如果成功推进到指定时间点并处理了所有相关订单，返回 `Ok(true)`。如果在处理过程中没有更多的历史数据可供处理，并且时间戳未达到指定时间点，则返回 `Ok(false)`。如果没有历史数据源可用，返回 `Err(MarketError::HistoryIsNone)`。
    ///
    /// # 错误
    ///
    /// 返回以下错误：
    ///
    /// * `MarketError::HistoryIsNone` - 如果历史数据源未设置，将返回此错误。
    ///
    /// # 示例
    ///
    /// ```rust
    /// let mut broker = Broker::new(...);
    /// let result = broker.goto(1627845600);
    /// match result {
    ///     Ok(true) => println!("成功推进到指定时间点并处理所有订单"),
    ///     Ok(false) => println!("未能推进到指定时间点，可能是因为历史数据已用尽"),
    ///     Err(e) => println!("发生错误: {:?}", e),
    /// }
    /// ```
    ///
    /// # 说明
    ///
    /// - 方法首先检查历史数据源是否存在。如果不存在，返回错误。
    /// - 然后，它会遍历历史数据中的订单，直到处理时间戳达到 `time_point`。
    /// - 对于每个订单，根据订单的时间和类型，调用 `process_order` 方法来处理订单。
    /// - 时间戳会更新到当前处理的订单的时间。
    /// - 如果历史数据源已用尽且时间戳未达到 `time_point`，则返回 `Ok(false)`。
    pub fn goto(&mut self, time_point: i64) -> Result<bool, MarketError> {
        if self.history.is_none() {
            return Err(MarketError::HistoryIsNone);
        }

        while self.timestamp <= time_point {
            if self.history.as_ref().unwrap().is_last() {
                return Ok(true);
            }

            let order_ref = self.history.as_ref().unwrap().next().unwrap().clone();

            let order = order_ref.borrow();
            self.timestamp = order.local_time.clone();
            let vol = (order.qty / self.lot_size).round() as i64;
            let l3order_ref = L3Order::new_ref(
                order.source.clone(),
                order.account.clone(),
                order.order_id,
                order.side.clone(),
                order.price_tick.clone(),
                vol,
                order.local_time,
            );
            let filled = self.process_order(order.order_type, l3order_ref)?;
        }

        Ok(false)
    }

    /// 尝试通过订单 ID 取消订单。如果在内部订单列表中找到该订单，
    /// 将其状态标记为已取消。如果未找到，则尝试在市场深度中取消该订单。
    ///
    /// # 参数
    ///
    /// * `order_id` - 要取消的订单的 ID。
    ///
    /// # 返回值
    ///
    /// * 如果操作成功，返回 `Ok(0)`。
    /// * 如果找不到订单或在取消市场深度中的订单时发生错误，返回 `Err(MarketError)`。
    pub fn cancel_order(&mut self, order_id: OrderId) -> Result<i64, MarketError> {
        if let Some(order) = self.orders.get(&order_id) {
            order.borrow_mut().status = OrderStatus::Canceled;
        } else {
            if let Some(order_ref) = self.orders.get_mut(&order_id) {
                self.market_depth.cancel_order(order_id)?;
                order_ref.borrow_mut().status = OrderStatus::Canceled;
            } else {
                return Err(MarketError::OrderNotFound);
            }
        }
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use order::Order;
    use skiplist_orderbook::SkipListMarketDepth;

    #[test]
    fn test_broker_new() {
        let broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::Backtest,
            "STOCK".to_string(),
            "CODE".to_string(),
            0.01,
            100.0,
        );

        assert_eq!(broker.stock_type, "STOCK");
        assert_eq!(broker.stock_code, "CODE");
        assert_eq!(broker.tick_size, 0.01);
        assert_eq!(broker.lot_size, 100.0);
        assert!(broker.pending_orders.is_empty());
        assert!(broker.waiting_orders.is_empty());
        assert_eq!(broker.timestamp, 0);
        assert!(broker.orders.is_empty());
        assert_eq!(broker.latest_seq_number, 0);
        assert!(broker.history.is_none());
        assert!(broker.dirty_tracker.is_empty());
    }

    #[test]
    fn test_generate_seq_number() {
        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::Backtest,
            "STOCK".to_string(),
            "CODE".to_string(),
            0.01,
            100.0,
        );

        assert_eq!(broker.generate_seq_number(), 1);
        assert_eq!(broker.generate_seq_number(), 2);
    }

    #[test]
    fn test_add_data() {
        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::Backtest,
            "STOCK".to_string(),
            "CODE".to_string(),
            0.01,
            100.0,
        );

        assert!(broker.add_data(None).is_ok());
    }

    #[test]
    fn test_get_orders() {
        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::Backtest,
            "STOCK".to_string(),
            "CODE".to_string(),
            0.01,
            100.0,
        );
        let order_ref = Order::new_ref(
            Some("account1".to_string()),
            "AAPL".to_string(),
            1234567890,
            150.0,
            10.0,
            "Buy",
            OrdType::L,
            OrderSourceType::LocalOrder,
        );

        broker.submit_order(order_ref).unwrap();

        let mut orders = HashMap::new();
        broker.get_orders(&mut orders, OrderStatus::New);
        assert_eq!(orders.len(), 1);
    }
    #[test]
    fn test_submit_order() {
        let mode = ExchangeMode::Backtest;
        let stock_code = String::from("stock");
        let account = "user1".to_string();
        let timestamp = 1;
        let price = 11.2;
        let qty = 100.0;
        let bs_flag = "b";
        let order_type = OrdType::L;
        let source = OrderSourceType::LocalOrder;
        let mut broker: Broker<SkipListMarketDepth> =
            Broker::new(mode, "stock".to_string(), "stock".to_string(), 0.01, 100.0);
        let order_ref = Order::new_ref(
            Some(account),
            stock_code,
            timestamp,
            price,
            qty,
            &bs_flag,
            order_type,
            source,
        );
        broker.submit_order(order_ref);
        print!("broker = {:?}\n", broker);
    }
}
