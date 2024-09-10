use utils::should_call_auction_on_close;

use super::dataloader::DataCollator;
use super::*;

use std::{
    cmp,
    collections::{hash_map::Entry, HashMap, VecDeque},
    fmt::Debug,
};

use super::utils::{adjust_timestamp_milliseconds_i64, is_in_call_auction};

use super::hook::{Hook, HookType};
use super::order::{Order, OrderRef};
use super::statistics::StatisticsInfo;
/// 交易经纪人结构体
/// `Broker` 结构体管理交易订单、市场深度、以及与订单处理相关的逻辑。
#[derive(Debug, Serialize, Deserialize)]
pub struct Broker<MD> {
    /// 交易模式，例如回测模式或实时模式
    pub mode: ExchangeMode,
    /// 市场类型，例如股票市场、期货市场等。
    pub market_type: MarketType,
    /// 股票类型，例如普通股或基金
    pub stock_type: String,
    /// 股票代码
    pub stock_code: String,
    ///开盘价
    pub open_tick: i64,
    ///收盘价
    pub close_tick: i64,
    /// 市场深度
    pub market_depth: Box<MD>,
    /// 最新的序列号
    pub latest_seq_number: i64,
    /// 最小价格变动单位
    pub tick_size: f64,
    /// 最小交易单位
    pub lot_size: f64,
    /// 前一交易日的收盘价。
    pub previous_close_price: f64,
    /// 当前时间戳
    pub timestamp: i64,
    /// 历史数据源
    pub history: Option<DataCollator>,
    /// 当前时间待处理订单
    #[serde(skip)]
    pub pending_orders: VecDeque<OrderRef>,
    /// 未来时间等待处理的订单，按时间排序
    #[serde(skip)]
    pub waiting_orders: VecDeque<(i64, OrderRef)>,
    /// 所有用户的订单
    #[serde(skip)]
    pub orders: Option<HashMap<OrderId, OrderRef>>,
    /// 脏订单跟踪器
    #[serde(skip)]
    pub dirty_tracker: Vec<OrderId>,
    /// 钩子（hooks），用于在特定事件发生时执行自定义逻辑。
    /// 这里使用 `HookType` 作为键，`Hook` 表示钩子函数，`String` 用于标识钩子的唯一性
    #[serde(skip)]
    pub hooks: HashMap<HookType, HashMap<String, Hook>>,
}

impl<'a, MD> Broker<MD>
where
    MD: L3MarketDepth + Serialize + Deserialize<'a> + RecoverOp + StatisticsOp + SnapshotOp,
    MarketError: From<<MD as L3MarketDepth>::Error>,
{
    /// 创建一个新的 `Broker` 实例
    ///
    /// # 参数
    ///
    /// * `mode` - 交易模式
    /// * `market_type` - 市场类型
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
        market_type: MarketType,
        stock_type: String,
        stock_code: String,
        tick_size: f64,
        lot_size: f64,
    ) -> Self {
        Self {
            mode: mode,
            market_type,
            stock_type: stock_type,
            stock_code: stock_code,
            market_depth: MD::new_box(mode.clone(), tick_size.clone(), lot_size.clone()),
            pending_orders: VecDeque::new(),
            waiting_orders: VecDeque::new(),
            timestamp: 19700101000000000,
            orders: None,
            latest_seq_number: 0,
            tick_size: tick_size,
            lot_size: lot_size,
            previous_close_price: 0.0,
            history: None,
            dirty_tracker: Vec::new(),
            open_tick: 0,
            close_tick: 0,
            hooks: HashMap::new(),
        }
    }

    pub fn set_previous_close_price(&mut self, previous_close_price: f64) {
        self.previous_close_price = previous_close_price;
        let previous_close_tick = (previous_close_price / self.tick_size).round() as i64;
        self.market_depth
            .set_previous_close_tick(previous_close_tick);
    }

    pub fn register_orderbook_hook(&mut self, hook_type: HookType, name: &str, hook: Hook) {
        self.hooks
            .entry(hook_type)
            .or_insert_with(HashMap::new)
            .insert(name.to_string(), hook);
    }

    pub fn remove_hook(&mut self, name: &str) {
        for hooks in self.hooks.values_mut() {
            hooks.remove(name);
        }
    }

    pub fn init(&mut self) {
        if self.orders.is_none() {
            self.orders = Some(HashMap::new());
        }
    }

    pub fn get_current_time(&self) -> i64 {
        self.timestamp
    }

    pub fn set_current_time(&mut self, timestamp: i64) {
        self.timestamp = timestamp
    }

    pub fn snapshot(&self) -> String {
        serde_json::to_string(self).unwrap_or("{}".to_string())
    }

    pub fn orders(&self) -> &HashMap<OrderId, OrderRef> {
        self.orders.as_ref().unwrap()
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

    pub fn process_local_order(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let mut filled = 0;
        let seq = order_ref.borrow().seq;

        let order_time = order_ref.borrow().timestamp;
        let in_call_auction = is_in_call_auction(order_time, self.market_type)?;
        let auxiliary_info = order_ref
            .borrow_mut()
            .auxiliary_info
            .as_ref()
            .unwrap()
            .clone();

        let match_vol = (auxiliary_info.match_qty / self.lot_size).round() as i64;
        let orderbook_vol = (auxiliary_info.orderbook_qty / self.lot_size).round() as i64;
        let initial_vol = (auxiliary_info.initial_qty / self.lot_size).round() as i64;

        if self.mode == ExchangeMode::Live {
            let price_tick = if match_vol > 0 {
                (auxiliary_info.match_price / self.tick_size).round() as i64
            } else if orderbook_vol > 0 {
                (auxiliary_info.orderbook_price / self.tick_size).round() as i64
            } else {
                (auxiliary_info.initial_price / self.tick_size).round() as i64
            };
            let mut order = order_ref.borrow_mut();
            order.price_tick = price_tick;
            order.vol = initial_vol;
            order.vol_shadow = order.vol;
            drop(order);
            filled = self.market_depth.match_order(order_ref.clone(), i64::MAX)?;
            if filled != initial_vol {
                self.market_depth.add(order_ref.clone());
            }
        } else {
            // print!(" -- order seq = {seq} , {order_ref:?} --\n");
            if auxiliary_info.cancel_seq == seq {
                // print!(
                //     "== before cancel {:?}\n",
                //     self.market_depth.get_bid_level(1)
                // );
                // print!(
                //     "== before cancel {:?}\n",
                //     self.market_depth.get_ask_level(1)
                // );

                let _ = self.cancel_order_from_ref(order_ref.clone());
                // print!("== after cancel {:?}\n", self.market_depth.get_bid_level(1));
                // print!("== after cancel {:?}\n", self.market_depth.get_ask_level(1));
            } else {
                if in_call_auction {
                    let mut order = order_ref.borrow_mut();
                    order.price_tick =
                        (auxiliary_info.initial_price / self.tick_size).round() as i64;
                    order.vol = initial_vol;
                    order.vol_shadow = order.vol;
                    drop(order);
                    let _ = self.market_depth.add(order_ref.clone())?;
                } else {
                    if match_vol > 0 {
                        // print!("== before match {:?}\n", self.market_depth.get_bid_level(1));
                        // print!("== before match {:?}\n", self.market_depth.get_ask_level(1));
                        let mut order = order_ref.borrow_mut();
                        order.price_tick =
                            (auxiliary_info.match_price / self.tick_size).round() as i64;
                        order.vol = initial_vol;
                        order.vol_shadow = order.vol;
                        drop(order);
                        filled = self.market_depth.match_order(order_ref.clone(), i64::MAX)?;

                        if orderbook_vol > 0 {
                            order_ref.borrow_mut().price_tick =
                                (auxiliary_info.orderbook_price / self.tick_size).round() as i64;

                            let _ = self.market_depth.add(order_ref.clone())?;
                        }

                        // if filled != match_vol {
                        //     print!(" ====== filled {filled} shoud be equel to match_vol {match_vol} ======\n");
                        // }
                        // print!("== after match  {:?}\n", self.market_depth.get_bid_level(1));
                        // print!("== after match  {:?}\n", self.market_depth.get_ask_level(1));
                    } else if orderbook_vol > 0 {
                        //尝试去匹配用户订单，因为没有历史成交不代表不可能和用户的订单成交
                        // print!(
                        //     ">> before orderbook {:?}\n",
                        //     self.market_depth.get_bid_level(1)
                        // );
                        // print!(
                        //     ">> befeor orderbook  {:?}\n",
                        //     self.market_depth.get_ask_level(1)
                        // );
                        let mut order = order_ref.borrow_mut();
                        order.price_tick =
                            (auxiliary_info.orderbook_price / self.tick_size).round() as i64;
                        order.vol = initial_vol;
                        order.vol_shadow = order.vol;
                        drop(order);
                        filled = self.market_depth.match_order(order_ref.clone(), i64::MAX)?;
                        let _ = self.market_depth.add(order_ref.clone())?;
                        // if filled > 0 {
                        //     print!("----- orderbook filled {filled}\n");
                        // }
                        // print!(
                        //     ">> after orderbook {:?}\n",
                        //     self.market_depth.get_bid_level(1)
                        // );
                        // print!(
                        //     ">> after orderbook  {:?}\n",
                        //     self.market_depth.get_ask_level(1)
                        // );
                    } else {
                        let mut order = order_ref.borrow_mut();
                        // print!("++ before other {:?}\n", self.market_depth.get_bid_level(1));
                        // print!("++ before other {:?}\n", self.market_depth.get_ask_level(1));
                        order.price_tick =
                            (auxiliary_info.initial_price / self.tick_size).round() as i64;
                        order.vol = (auxiliary_info.initial_qty / self.lot_size).round() as i64;
                        order.vol_shadow = order.vol;
                        drop(order);
                        filled = self.market_depth.match_order(order_ref.clone(), i64::MAX)?;
                        let _ = self.market_depth.add(order_ref.clone())?;
                        // if filled > 0 {
                        //     print!("----- other filled {filled}\n");
                        // }
                        // print!("++ after other {:?}\n", self.market_depth.get_bid_level(1));
                        // print!("++ after other {:?}\n", self.market_depth.get_ask_level(1));
                    }
                }
            }
        }

        Ok(filled)
    }

    /// 根据订单方向和来源获取最佳价格。
    ///
    /// # 参数
    /// - `side`: 订单的买卖方向。
    /// - `source`: 订单的来源。
    ///
    /// # 返回值
    /// - `i64`: 最佳价格。
    fn get_best_tick(&self, side: &Side, source: &OrderSourceType) -> i64 {
        match side {
            Side::Buy => self.market_depth.best_bid_tick(source),
            _ => self.market_depth.best_ask_tick(source),
        }
    }

    pub fn match_order_l(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let filled = self.market_depth.match_order(order_ref.clone(), i64::MAX)?;
        if order_ref.borrow().vol > 0 {
            let best_tick = self.market_depth.add(order_ref)?;
        }

        Ok(filled)
    }

    pub fn match_order_m(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        order_ref.borrow_mut().price_tick = i64::MAX;
        let filled = self.market_depth.match_order(order_ref.clone(), 5)?;
        order_ref.borrow_mut().price_tick = 0;
        Ok(filled)
    }

    pub fn match_order_n(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        order_ref.borrow_mut().price_tick = i64::MAX;
        let source = order_ref.borrow().source;
        let filled = self.market_depth.match_order(order_ref.clone(), 5)?;
        if order_ref.borrow().vol > 0 {
            order_ref.borrow_mut().price_tick = self.market_depth.last_tick(&source);
            let best_tick = self.market_depth.add(order_ref)?;
        }
        Ok(filled)
    }

    /// 处理 `OrderType::B` 订单（以本方最优价格申报的市价订单）。
    ///
    /// 设置订单价格为市场深度中的最佳买价或卖价，并尝试将订单加入市场深度。
    ///
    /// # 参数
    /// - `order_ref`: 订单的引用，用于获取和修改订单信息。
    ///
    /// # 返回值
    /// - `Result<i64, MarketError>`: 操作成功返回 `Ok(0)`，失败返回 `Err(MarketError>`。
    pub fn match_order_b(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let side = order_ref.borrow().side;
        let source = order_ref.borrow().source;
        let vol = order_ref.borrow().vol;
        order_ref.borrow_mut().price_tick = self.get_best_tick(&side, &source);

        if vol > 0 {
            self.market_depth.add(order_ref.clone())?;
        }

        Ok(0)
    }

    /// 处理 `OrderType::C` 订单（以对手方最优价格申报的市价订单）。
    ///
    /// 设置订单价格为市场深度中的最佳卖价或买价，并尝试将订单匹配到市场深度中。
    ///
    /// # 参数
    /// - `order_ref`: 订单的引用，用于获取和修改订单信息。
    ///
    /// # 返回值
    /// - `Result<i64, MarketError>`: 返回实际成交量。
    pub fn match_order_c(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let side = order_ref.borrow().side;
        let source = order_ref.borrow().source;
        let vol = order_ref.borrow().vol;
        order_ref.borrow_mut().price_tick = self.get_best_tick(&side.opposite(), &source);

        let filled = self.market_depth.match_order(order_ref.clone(), i64::MAX)?;

        if vol > 0 {
            self.market_depth.add(order_ref.clone())?;
        }

        Ok(filled)
    }

    pub fn match_order_d(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let filled = 0;
        Ok(filled)
    }

    /// 处理订单
    ///
    /// 该方法根据订单类型 (`OrderType`) 处理传入的订单，并执行相应的操作。根据不同的订单类型，方法会调用不同的匹配函数来处理订单。
    ///
    /// # 参数
    ///
    /// * `order_type` - 订单类型，指定要处理的订单类型。
    /// * `l3order_ref` - 订单引用，包含要处理的订单的详细信息。
    ///
    /// # 返回
    ///
    /// 返回成功成交的订单量。处理失败则返回 `Err`。
    pub fn process_order(&mut self, l3order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let source = l3order_ref.borrow().source;
        let result;
        l3order_ref.borrow_mut().timestamp = self.timestamp;

        if source == OrderSourceType::LocalOrder {
            result = self.process_local_order(l3order_ref.clone());
        } else {
            if is_in_call_auction(self.timestamp, self.market_type).unwrap_or(false) {
                let _ = self.market_depth.add(l3order_ref.clone());
                result = Ok(0);
            } else {
                let order_type = l3order_ref.borrow().order_type;
                let order_id = l3order_ref.borrow().order_id;
                result = match order_type {
                    // 处理普通限价订单
                    OrderType::L => self.match_order_l(l3order_ref.clone()),
                    // 处理最优五档即时成交剩余撤销的市价订单
                    OrderType::M => self.match_order_m(l3order_ref.clone()),
                    // 处理最优五档即时成交剩余转限价的市价订单
                    OrderType::N => self.match_order_n(l3order_ref.clone()),
                    // 处理以本方最优价格申报的市价订单
                    OrderType::B => self.match_order_b(l3order_ref.clone()),
                    // 处理以对手方最优价格申报的市价订单
                    OrderType::C => self.match_order_c(l3order_ref.clone()),
                    // 处理市价全额成交或撤销订单
                    OrderType::D => self.match_order_d(l3order_ref.clone()),
                    // 处理取消委托
                    OrderType::Cancel => self.cancel_order(order_id),
                    _ => Err(MarketError::OrderTypeUnsupported),
                };
            }
        }

        if let Some(hooks) = self.hooks.get_mut(&HookType::Orderbook) {
            for (_, hook) in hooks.iter_mut() {
                let mut info: StatisticsInfo = StatisticsInfo::new();
                let mut bid_orderbook_info: Vec<(f64, f64, i64)> =
                    Vec::with_capacity(hook.max_level);
                let mut ask_orderbook_info: Vec<(f64, f64, i64)> =
                    Vec::with_capacity(hook.max_level);

                info.from_statistics(
                    self.market_depth.get_statistics(),
                    self.tick_size,
                    self.lot_size,
                );
                info.last_price = self.market_depth.last_price(&source);
                info.prev_close_price = self.previous_close_price;
                self.market_depth.get_orderbook_level(
                    &mut bid_orderbook_info,
                    &mut ask_orderbook_info,
                    hook.max_level,
                );
                (hook.handler)(
                    &hook.object,
                    &info,
                    &bid_orderbook_info,
                    &ask_orderbook_info,
                    &l3order_ref,
                );
            }
        }

        result
    }
    // 获取订单信息，并根据给定的状态过滤订单。
    ///
    /// 如果 `filter` 为空，则返回所有订单；如果 `filter` 不为空，则仅返回符合过滤条件的订单。
    ///
    /// # 参数
    /// - `orders`: 一个可变的 `HashMap`，用于存储返回的订单。键为订单 ID，值为订单引用。
    /// - `filter`: 一个包含订单状态的 `Vec`，用于过滤订单。如果为空，则返回所有订单。
    ///
    /// # 备注
    /// - 订单的状态由 `OrderStatus` 枚举定义。
    /// - 如果 `filter` 为空，方法会遍历 `self.orders` 中的所有订单并将其插入到 `orders` 中。
    /// - 如果 `filter` 不为空，方法会根据 `filter` 中的状态来筛选订单，并将符合条件的订单插入到 `orders` 中。
    pub fn get_orders(
        &mut self,
        orders: &mut HashMap<OrderId, OrderRef>,
        filter: &Vec<OrderStatus>,
    ) {
        if filter.is_empty() {
            for (k, v) in self.orders.as_ref().unwrap().iter() {
                orders.insert(k.clone(), v.clone());
            }
        } else {
            // 否则，根据 filter 过滤订单
            for (k, v) in self
                .orders
                .as_ref()
                .unwrap()
                .iter()
                .filter(|&(_, v)| filter.contains(&v.borrow().status))
            {
                orders.insert(k.clone(), v.clone());
            }
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
            if let Some(order_ref) = self.orders.as_ref().unwrap().get(&order_id) {
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
        match self
            .orders
            .as_ref()
            .unwrap()
            .contains_key(&(order_ref.borrow().order_id))
        {
            true => return Err(MarketError::OrderIdExist),
            false => self
                .orders
                .as_mut()
                .unwrap()
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
    /// * `duration` - 模拟的时间段，以时间单位表示。时间推移将基于此时间段来更新当前时间,单位为毫秒。
    ///
    /// # 返回
    ///
    /// 返回一个 `Result<bool, MarketError>`。如果成功处理了所有订单并推进了时间，则返回 `Ok(true)`；如果时间点达到历史记录的结束，则返回 `Ok(true)`；如果时间点未到达历史记录的结束，则返回 `Ok(false)`。
    ///
    /// # 错误
    ///
    /// 如果处理订单时发生错误（例如匹配订单失败），方法会返回相应的 `MarketError`。
    pub fn elapse(self: &'_ mut Self, duration: i64) -> Result<i64, MarketError> {
        let time_point = adjust_timestamp_milliseconds_i64(self.timestamp, duration)?;
        let mut total_filled: i64 = 0;

        //处理pending队列
        while !self.pending_orders.is_empty() {
            let order_ref = self.pending_orders.pop_front().unwrap();
            if order_ref.borrow().status == OrderStatus::Canceled {
                continue;
            }
            let mut order = order_ref.borrow_mut();
            order.exch_time = self.timestamp;
            let l3order_ref = order.to_l3order_ref(self.tick_size, self.lot_size);
            let fillid = self.process_order(l3order_ref)?;
            if fillid > 0 {
                order.filled_qty = fillid as f64 * self.lot_size;
                self.dirty_tracker.push(order.order_id);
                order.update();
            }
            total_filled += fillid;
        }

        self.waiting_orders.make_contiguous().sort();
        //处理waiting队列
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
                order.order_type,
            );
            order.seq = self.generate_seq_number();
            let fillid = self.process_order(l3order_ref)?;
            order.exch_time = self.timestamp;
            if fillid > 0 {
                order.filled_qty = fillid as f64 * self.lot_size;
                self.dirty_tracker.push(order.order_id);
                order.update();
            }
            total_filled += fillid;
        }

        //有可能处理完了waiting队列后，时间还需要继续向前流逝
        let _ = self.goto(time_point);
        Ok(total_filled)
    }

    /// 同步订单信息，将市场深度中的订单状态与本地订单进行同步。
    /// 如果订单被标记为已处理或取消，将从市场深度中移除并更新本地订单状态。
    pub fn sync_order_info(&mut self) {
        // 获取市场深度中所有订单的信息
        let l30orders = self.market_depth.orders_mut();

        // 用于追踪需要从市场深度中移除的订单 ID
        let mut remove_tracker: Vec<OrderId> = Vec::with_capacity(100);

        for (order_id, l30order) in l30orders.iter_mut() {
            let mut order = self
                .orders
                .as_mut()
                .unwrap()
                .get(order_id)
                .unwrap()
                .borrow_mut();
            // print!("{l30order:?}\n");
            if l30order.borrow().dirty == true {
                // 同步订单的位置信息和数量
                order.price = l30order.borrow().price_tick as f64 * self.tick_size;
                order.queue = l30order.borrow().total_vol_before as f64 * self.lot_size;
                order.left_qty = l30order.borrow().vol as f64 * self.lot_size;
                order.filled_qty = order.qty - order.left_qty;
                order.exch_time = self.timestamp;
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
        info!("goto time_point {time_point}");
        let mut end_of_history = false;
        if self.history.is_none() {
            return Err(MarketError::HistoryIsNone);
        }

        while self.timestamp <= time_point {
            if self.history.as_ref().unwrap().is_last() {
                end_of_history = true;
                break;
            }

            let (seq, order_ref) = self.history.as_mut().unwrap().next().unwrap();
            order_ref.borrow_mut().seq = seq;
            debug!("history order info {order_ref:?}");

            self.timestamp = order_ref.borrow().timestamp.clone();
            let order_ref_arg = order_ref.clone();
            if !is_in_call_auction(self.timestamp, self.market_type).unwrap_or(false)
                && self.open_tick == 0
            {
                (self.open_tick, _) = self.market_depth.call_auction().unwrap_or((0, 0));
            }

            let filled = self.process_order(order_ref_arg)?;
        }
        self.timestamp = time_point;
        if should_call_auction_on_close(self.timestamp, self.market_type)? && self.close_tick == 0 {
            let (close_tick, _) = self.market_depth.call_auction().unwrap_or((0, 0));
            self.close_tick = close_tick;
        }
        Ok(end_of_history)
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
        let _ = self.market_depth.cancel_order(order_id);

        Ok(0)
    }

    pub fn cancel_order_from_ref(&mut self, order_ref: L3OrderRef) -> Result<i64, MarketError> {
        let _ = self.market_depth.cancel_order_from_ref(order_ref);

        Ok(0)
    }
}

impl<'a, MD> RecoverOp for Broker<MD>
where
    MD: L3MarketDepth + Serialize + Deserialize<'a> + RecoverOp + StatisticsOp + SnapshotOp,
    MarketError: From<<MD as L3MarketDepth>::Error>,
{
    fn recover(&mut self) -> Result<bool, MarketError> {
        self.init();
        if self.history.is_some() {
            self.history.as_mut().unwrap().init();
        }

        Ok(true)
    }
}
#[cfg(test)]
mod tests {
    use core::borrow;
    use std::str::FromStr;

    use super::utils::time_difference_ms_i64;
    use super::*;
    use order::Order;
    use skiplist_orderbook::SkipListMarketDepth;

    #[test]
    fn test_broker_new() {
        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::Backtest,
            MarketType::SH,
            "STOCK".to_string(),
            "CODE".to_string(),
            0.01,
            100.0,
        );
        broker.init();
        assert_eq!(broker.stock_type, "STOCK");
        assert_eq!(broker.stock_code, "CODE");
        assert_eq!(broker.tick_size, 0.01);
        assert_eq!(broker.lot_size, 100.0);
        assert!(broker.pending_orders.is_empty());
        assert!(broker.waiting_orders.is_empty());
        assert_eq!(broker.timestamp, 19700101000000000);
        assert!(broker.orders().is_empty());
        assert_eq!(broker.latest_seq_number, 0);
        assert!(broker.history.is_none());
        assert!(broker.dirty_tracker.is_empty());
    }

    #[test]
    fn test_generate_seq_number() {
        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::Backtest,
            MarketType::SH,
            "STOCK".to_string(),
            "CODE".to_string(),
            0.01,
            100.0,
        );
        broker.init();
        assert_eq!(broker.generate_seq_number(), 1);
        assert_eq!(broker.generate_seq_number(), 2);
    }

    #[test]
    fn test_add_data() {
        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::Backtest,
            MarketType::SH,
            "STOCK".to_string(),
            "CODE".to_string(),
            0.01,
            100.0,
        );
        broker.init();
        assert!(broker.add_data(None).is_ok());
    }

    #[test]
    fn test_get_orders() {
        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::Backtest,
            MarketType::SH,
            "STOCK".to_string(),
            "CODE".to_string(),
            0.01,
            100.0,
        );
        broker.init();
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

        broker.submit_order(order_ref).unwrap();

        let mut orders = HashMap::new();
        broker.get_orders(&mut orders, &vec![OrderStatus::New]);
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
        let order_type = OrderType::L;
        let source = OrderSourceType::LocalOrder;
        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            mode,
            MarketType::SH,
            "stock".to_string(),
            "stock".to_string(),
            0.01,
            100.0,
        );
        broker.init();
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

        let result = broker.submit_order(order_ref);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1); // Assuming this is the expected queue position
    }

    #[test]
    fn test_cancel_order() {
        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::Backtest,
            MarketType::SH,
            "STOCK".to_string(),
            "CODE".to_string(),
            0.01,
            100.0,
        );
        broker.init();
        // Create and submit a test order
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
        broker.submit_order(order_ref.clone()).unwrap();

        let order_id = order_ref.borrow().order_id;
        // Cancel the order
        let result = broker.cancel_order(order_id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);

        // Verify the order status is canceled
        let order = broker
            .orders
            .as_ref()
            .unwrap()
            .get(&order_ref.borrow().order_id)
            .unwrap();
        assert_eq!(order.borrow().status, OrderStatus::Canceled);
    }

    #[test]
    fn test_get_orders_multiple_statuses() {
        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::Backtest,
            MarketType::SH,
            "STOCK".to_string(),
            "CODE".to_string(),
            0.01,
            100.0,
        );
        broker.init();
        // 创建多个订单，具有不同的状态
        let new_order_ref = Order::new_ref(
            Some("account1".to_string()),
            "AAPL".to_string(),
            1234567890,
            150.0,
            10.0,
            "Buy",
            OrderType::L,
            OrderSourceType::UserOrder,
        );

        let filled_order_ref = Order::new_ref(
            Some("account2".to_string()),
            "AAPL".to_string(),
            1234567891,
            155.0,
            15.0,
            "Sell",
            OrderType::B,
            OrderSourceType::UserOrder,
        );

        let canceled_order_ref = Order::new_ref(
            Some("account3".to_string()),
            "AAPL".to_string(),
            1234567892,
            160.0,
            20.0,
            "Buy",
            OrderType::C,
            OrderSourceType::UserOrder,
        );

        let new_order_id = 1234567890;
        let filled_order_id = 1234567891;
        let canceled_order_id = 1234567892;

        new_order_ref.borrow_mut().order_id = new_order_id;
        filled_order_ref.borrow_mut().order_id = filled_order_id;
        canceled_order_ref.borrow_mut().order_id = canceled_order_id;
        // 提交订单
        broker.submit_order(new_order_ref.clone()).unwrap();
        broker.submit_order(filled_order_ref.clone()).unwrap();
        broker.submit_order(canceled_order_ref.clone()).unwrap();

        // 将状态修改为不同状态以便测试
        broker
            .orders
            .as_mut()
            .unwrap()
            .get_mut(&1234567890)
            .unwrap()
            .borrow_mut()
            .status = OrderStatus::New;
        broker
            .orders
            .as_mut()
            .unwrap()
            .get_mut(&1234567891)
            .unwrap()
            .borrow_mut()
            .status = OrderStatus::Filled;
        broker
            .orders
            .as_mut()
            .unwrap()
            .get_mut(&1234567892)
            .unwrap()
            .borrow_mut()
            .status = OrderStatus::Canceled;

        // 测试获取新订单
        let mut orders = HashMap::new();
        broker.get_orders(&mut orders, &vec![OrderStatus::New]);
        assert_eq!(orders.len(), 1);
        assert!(orders.contains_key(&1234567890));

        // 清空映射并测试获取已完成订单
        orders.clear();
        broker.get_orders(&mut orders, &vec![OrderStatus::Filled]);
        assert_eq!(orders.len(), 1);
        assert!(orders.contains_key(&1234567891));

        // 清空映射并测试获取已取消订单
        orders.clear();
        broker.get_orders(&mut orders, &vec![OrderStatus::Canceled]);
        assert_eq!(orders.len(), 1);
        assert!(orders.contains_key(&1234567892));

        // 清空映射并测试获取多个状态的订单
        orders.clear();
        broker.get_orders(&mut orders, &vec![OrderStatus::New, OrderStatus::Filled]);
        assert_eq!(orders.len(), 2);
        assert!(orders.contains_key(&1234567890));
        assert!(orders.contains_key(&1234567891));
    }

    #[test]
    fn test_broker_snapshot() {
        // 创建一个 Broker 实例
        // 使用 Backtest 模式，股票类型为 "STOCK"，股票代码为 "CODE"，
        // 最小价格变动单位为 0.01，最小交易单位为 100.0
        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::Backtest,
            MarketType::SH,
            "STOCK".to_string(),
            "CODE".to_string(),
            0.01,
            100.0,
        );
        broker.init();
        // 调用 snapshot 方法，获取 Broker 实例的 JSON 序列化表示
        let snapshot = broker.snapshot();
        print!("{:?}\n", snapshot);
        print!("{:?}\n", serde_json::to_string(&broker));
        // 验证 snapshot 返回的 JSON 字符串是否包含期望的字段及其值
        // 确保交易模式被正确序列化
        assert!(snapshot.contains(r#""mode":"Backtest""#));
        // 确保股票类型被正确序列化
        assert!(snapshot.contains(r#""stock_type":"STOCK""#));
        // 确保股票代码被正确序列化
        assert!(snapshot.contains(r#""stock_code":"CODE""#));
        // 确保最小价格变动单位被正确序列化
        assert!(snapshot.contains(r#""tick_size":0.01"#));
        // 确保最小交易单位被正确序列化
        assert!(snapshot.contains(r#""lot_size":100.0"#));
        // 确保当前时间戳被正确序列化
        assert!(snapshot.contains(r#""timestamp":0"#));
        // 确保最新的序列号被正确序列化
        assert!(snapshot.contains(r#""latest_seq_number":0"#));

        // 验证 snapshot 返回的 JSON 字符串不包含被跳过序列化的字段
        // pending_orders、waiting_orders、orders、history 和 dirty_tracker
        // 被标记为 #[serde(skip)]，因此不应包含在序列化输出中
        assert!(!snapshot.contains(r#""pending_orders":[]"#));
        assert!(!snapshot.contains(r#""waiting_orders":[]"#));
        assert!(!snapshot.contains(r#""dirty_tracker":[]"#));
    }

    #[test]
    fn test_broker_add_dataloader() {
        let exchange_mode = "backtest".to_string();
        let stock_code = "688007.SH".to_string();
        let file_type = "local".to_string();
        let data_path = "./data".to_string();
        let date = "20231201".to_string();
        let mode = "L2P";

        let mut data = DataCollator::new(
            stock_code.clone(),
            file_type.clone(),
            data_path.clone(),
            date.clone(),
            mode.clone(),
        );
        data.init();

        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::from_str(&exchange_mode.as_str()).unwrap(),
            MarketType::SH,
            "stock".to_string(),
            stock_code.clone(),
            0.01,
            1.0,
        );
        broker.init();
        let start: i64 = 20231201092521355;
        let duration = time_difference_ms_i64(broker.timestamp, start).unwrap_or(0);
        broker.add_data(Some(data));
        broker.elapse(duration + 10000);
        print!("{:?}\n", broker.snapshot());
    }

    #[test]
    fn test_broker_live_mode() {
        let exchange_mode = "live";
        let stock_code = "688007.SH".to_string();
        let file_type = "local".to_string();
        let data_path = "./data".to_string();
        let date = "20231201".to_string();
        let mode = "L2P";

        let mut data = DataCollator::new(
            stock_code.clone(),
            file_type.clone(),
            data_path.clone(),
            date.clone(),
            mode.clone(),
        );
        data.init();

        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::from_str(&exchange_mode).unwrap(),
            MarketType::SH,
            "stock".to_string(),
            stock_code.clone(),
            0.01,
            1.0,
        );
        broker.init();
        let start: i64 = 20231201092521355;
        let duration = time_difference_ms_i64(broker.timestamp, start).unwrap_or(0);
        broker.add_data(Some(data));
        broker.elapse(duration + 24 * 3600 * 1000);
        print!("{:?}\n", broker.snapshot());
    }

    #[test]
    fn test_process_user_order() {
        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::Live,
            MarketType::SH,
            "STOCK".to_string(),
            "CODE".to_string(),
            0.01,
            1.0,
        );
        broker.init();
        let timestamp = 20231201093021355;
        broker.set_current_time(timestamp);
        // Create and submit a local order
        let buy_order_ref = Order::new_ref(
            None,
            "AAPL".to_string(),
            timestamp,
            150.0,
            10.0,
            "Buy",
            OrderType::L,
            OrderSourceType::UserOrder,
        );
        buy_order_ref.borrow_mut().order_id = 1;
        let sell_order_ref = Order::new_ref(
            None,
            "AAPL".to_string(),
            timestamp,
            150.0,
            10.0,
            "Sell",
            OrderType::L,
            OrderSourceType::UserOrder,
        );
        sell_order_ref.borrow_mut().order_id = 2;
        broker.submit_order(buy_order_ref.clone()).unwrap();
        broker.submit_order(sell_order_ref.clone()).unwrap();
        // Process the local order

        broker.elapse(1000);
        broker.sync_order_info();

        // print!("{buy_order_ref:?}\n,{sell_order_ref:?}\n");

        // print!("{:?}\n", broker.market_depth.get_ask_level(2));
        // print!("{:?}\n", broker.market_depth.get_bid_level(2));
        // Verify the order status
        assert_eq!(buy_order_ref.borrow().status, OrderStatus::Filled);
    }

    #[test]
    fn test_process_cancel_order() {
        let mut broker: Broker<SkipListMarketDepth> = Broker::new(
            ExchangeMode::Live,
            MarketType::SH,
            "STOCK".to_string(),
            "CODE".to_string(),
            0.01,
            1.0,
        );
        broker.init();

        let timestamp = 20231201093021355;
        broker.set_current_time(timestamp);

        // Create and submit a limit order
        let order_ref = Order::new_ref(
            None,
            "AAPL".to_string(),
            timestamp,
            150.0,
            10.0,
            "Buy",
            OrderType::L,
            OrderSourceType::UserOrder,
        );
        order_ref.borrow_mut().order_id = 1;
        broker.submit_order(order_ref.clone()).unwrap();

        // Process the order to ensure it is added
        broker.elapse(1000);

        broker.cancel_order(order_ref.borrow_mut().order_id);
        // print!("{:?}\n", broker.market_depth.orders);
        broker.sync_order_info();

        assert_eq!(order_ref.borrow().status, OrderStatus::Canceled);
    }
}
