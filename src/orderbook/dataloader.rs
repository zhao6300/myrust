use std::collections::VecDeque;
use std::str::FromStr;

use super::dataapi::DataApi;
use super::utils::is_in_call_auction;
use super::*;
use polars::export::num::ToPrimitive;
use polars::prelude::*;
use rayon::prelude::*;

/// `DataCollator` 结构体用于聚合和处理交易所和股票的订单和交易数据。
///
/// # 字段
///
/// * `exchange_code` - 交易所代码，如 "SH" 或 "SZ"。
/// * `stock_code` - 股票代码，如 "600519"。
/// * `file_type` - 数据文件类型，可以是 "local" 或 "hdfs"。
/// * `data_path` - 数据文件所在的路径。
/// * `source` - 订单的来源类型，使用 `OrderSourceType` 枚举表示。
/// * `df_order` - 包含订单数据的 `DataFrame` 对象。
/// * `df_trade` - 包含交易数据的 `DataFrame` 对象。
/// * `last_df_order_idx` - 上一次处理的订单数据的索引。
/// * `last_df_trade_idx` - 上一次处理的交易数据的索引。
/// * `is_last` - 表示是否已经处理完所有数据的标志。
/// * `orders` - 订单的哈希映射，键为订单 ID，值为 `OrderRef`。
/// * `index_by_seq` - 按照订单序号排序的队列，包含订单序号和订单 ID 的元组。
/// * `current_idx` - 当前正在处理的订单索引。
/// * `len` - 当前订单队列的长度。
/// * `da_api` - 数据接口，用于加载订单和交易数据。
#[derive(Debug, Serialize, Deserialize)]
pub struct DataCollator {
    pub date: String,
    pub exchange_code: String,   // 交易所代码
    pub stock_code: String,      // 股票代码
    pub file_type: String,       // 数据文件类型（如本地或HDFS）
    pub data_path: String,       // 数据路径
    pub source: OrderSourceType, // 订单来源类型
    #[serde(skip)]
    pub df_order: Option<DataFrame>, // 订单数据框架
    #[serde(skip)]
    pub df_trade: Option<DataFrame>, // 交易数据框架
    #[serde(skip)]
    pub is_last: bool, // 是否是最后一个数据
    #[serde(skip)]
    pub orders: Option<HashMap<OrderId, L3OrderRef>>, // 订单映射
    /// 按照 order_seq 排序的队列，其中包含 order_seq 和 order_id，如果是撤单，第三个值为 true。
    #[serde(skip)]
    pub index_by_seq: Option<VecDeque<(i64, i64)>>,
    pub current_idx: usize, // 当前处理的索引
    #[serde(skip)]
    pub len: usize, // 队列长度
    #[serde(skip)]
    pub da_api: Option<DataApi>, // 数据 API 对象
    mode: String,
}

impl DataCollator {
    /// 创建一个新的 `DataCollator` 实例。
    ///
    /// # 参数
    /// * `exchange_code` - 交易所代码，通常为 "SH" 或 "SZ"。
    /// * `stock_code` - 股票代码。
    /// * `file_type` - 文件类型，可以是 "local" 或 "hdfs"。
    /// * `data_path` - 数据路径，用于存储和加载数据。
    /// * `date` - 数据日期，格式为 `%Y%m%d`。
    /// * `mode` - 模式类型，支持 "ORDER" 或 "L2P"。
    ///
    /// # 返回值
    /// 返回一个新的 `DataCollator` 实例。
    pub fn new(
        stock_code: String,
        file_type: String,
        data_path: String,
        date: String, //%Y%m%d
        mode: &str,
    ) -> Self {
        // 校验模式是否合法
        let mode_upper = mode.to_uppercase();
        if !["ORDER", "L2P"].contains(&mode_upper.as_str()) {
            panic!("撮合模式只有 ORDER, L2P 两种，请重新输入！");
        }
        let exchange_code = "shanghai".to_string();
        Self {
            date: date,
            exchange_code,
            stock_code: stock_code.clone().to_string(),
            file_type: file_type,
            data_path: data_path,
            source: OrderSourceType::LocalOrder,
            df_order: None,
            df_trade: None,
            is_last: false,
            orders: None,
            index_by_seq: None,
            current_idx: 0,
            len: 0,
            da_api: None,
            mode: mode_upper,
        }
    }

    /// 初始化 `DataCollator`，根据交易所类型加载数据。
    pub fn init(&mut self) {
        let restrict_aggressive_order =
            !self.stock_code.is_empty() && self.stock_code.chars().nth(0) == Some('3');
        // 检查并设置交易所代码
        let exchange_code = if self.stock_code.ends_with("SH") {
            "SH".to_string()
        } else {
            "SZ".to_string()
        };

        self.exchange_code = exchange_code.clone();

        let mut da_api = DataApi::new(
            self.date.clone(),
            self.file_type.clone().to_string(),
            self.mode.clone(),
            self.data_path.clone().to_string(),
        );

        // 加载订单和交易数据（根据文件类型判断是否加载）
        let (df_order, df_trade) = if self.file_type == "local" || self.file_type == "hdfs" {
            (
                da_api.load_order_data(&self.stock_code, false),
                da_api.load_transaction_data(&self.stock_code, false),
            )
        } else {
            (DataFrame::default(), DataFrame::default())
        };

        self.df_order = Some(df_order);
        self.df_trade = Some(df_trade);
        self.orders = Some(HashMap::new());
        self.index_by_seq = Some(VecDeque::new());
        self.da_api = Some(da_api);

        if self.exchange_code.to_lowercase() == "sz" {
            self.init_sz();
        } else {
            self.init_sh();
        }
    }

    pub fn get_next_timestamp(&self) -> Option<i64> {
        if self.is_last() {
            return None;
        }
        let (_, order_id) = self.index_by_seq.as_ref().unwrap()[self.current_idx];
        Some(
            self.orders
                .as_ref()
                .unwrap()
                .get(&order_id)
                .as_ref()
                .unwrap()
                .borrow()
                .timestamp
                .clone(),
        )
    }
    fn load_order_sz(&mut self) {
        let order_no_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("OrderNO")
            .unwrap()
            .i64()
            .unwrap();
        let order_bs_flag_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("OrderBSFlag")
            .unwrap()
            .i32()
            .unwrap();
        let order_type_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("OrderType")
            .unwrap()
            .i32()
            .unwrap();
        let order_price_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("OrderPrice")
            .unwrap()
            .f64()
            .unwrap();
        let order_qty_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("OrderQty")
            .unwrap()
            .f64()
            .unwrap();
        let recv_time_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("ReceiveDateTime")
            .unwrap()
            .i64()
            .unwrap();
        let seq_num_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("ApplSeqNum")
            .unwrap()
            .i64()
            .unwrap();

        for idx in 0..self.df_order.as_ref().unwrap().height() {
            let order_no = order_no_col.get(idx).unwrap();
            let seq_num = seq_num_col.get(idx).unwrap();
            let side = if order_bs_flag_col.get(idx).unwrap() == 1 {
                "B"
            } else {
                "S"
            };

            let order_type = OrderType::from_i32(order_type_col.get(idx).unwrap()).unwrap();
            let is_cancel = order_type == OrderType::Cancel;
            let qty = order_qty_col.get(idx).unwrap();

            let order_ref = L3Order::new_ref(
                OrderSourceType::LocalOrder,
                None,
                order_no,
                Side::from_str(&side).unwrap(),
                0,
                0,
                recv_time_col.get(idx).unwrap(),
                order_type,
            );

            if !is_cancel {
                self.orders
                    .as_mut()
                    .unwrap()
                    .insert(order_no, order_ref.clone());
                let mut order = order_ref.borrow_mut();
                let auxiliary_info = order.auxiliary_info.as_mut().unwrap();

                auxiliary_info.initial_price = order_price_col.get(idx).unwrap();
                auxiliary_info.initial_seq = seq_num;
                auxiliary_info.initial_qty = qty;
            }
        }
    }
    /// 加载订单数据，并将其存储在 `orders` 和 `index_by_seq` 中。
    fn load_order_sh(&mut self) {
        // 提取 `df_order` 数据框中的各列
        let order_no_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("OrderNO")
            .unwrap()
            .i64()
            .unwrap();
        let order_bs_flag_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("OrderBSFlag")
            .unwrap()
            .i32()
            .unwrap();
        let order_type_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("OrderType")
            .unwrap()
            .i32()
            .unwrap();
        let order_price_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("OrderPrice")
            .unwrap()
            .f64()
            .unwrap();
        let order_qty_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("OrderQty")
            .unwrap()
            .f64()
            .unwrap();
        let md_time_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("MDTime")
            .unwrap()
            .i64()
            .unwrap();
        let seq_num_col = self
            .df_order
            .as_ref()
            .unwrap()
            .column("ApplSeqNum")
            .unwrap()
            .i64()
            .unwrap();

        for idx in 0..self.df_order.as_ref().unwrap().height() {
            let order_no = order_no_col.get(idx).unwrap();
            let seq_num = seq_num_col.get(idx).unwrap();
            let md_time = md_time_col.get(idx).unwrap();
            let side = if order_bs_flag_col.get(idx).unwrap() == 1 {
                "B"
            } else {
                "S"
            };

            let order_type = OrderType::from_i32(order_type_col.get(idx).unwrap()).unwrap();
            let is_cancel = order_type == OrderType::Cancel;
            let qty = order_qty_col.get(idx).unwrap();

            if !is_cancel {
                let order_ref = L3Order::new_ref(
                    OrderSourceType::LocalOrder,
                    None,
                    order_no,
                    Side::from_str(&side).unwrap(),
                    0,
                    0,
                    md_time,
                    order_type,
                );

                let mut order = order_ref.borrow_mut();
                let auxiliary_info = order.auxiliary_info.as_mut().unwrap();
                auxiliary_info.initial_price = order_price_col.get(idx).unwrap();
                auxiliary_info.initial_qty = qty;
                auxiliary_info.initial_seq = seq_num;

                self.orders
                    .as_mut()
                    .unwrap()
                    .insert(order_no, order_ref.clone());

                print!("== load order ==  {order:?}\n");
            } else {
                let order_ref = self.orders.as_mut().unwrap().get(&order_no).unwrap();
                let mut order = order_ref.borrow_mut();
                let auxiliary_info = order.auxiliary_info.as_mut().unwrap();
                auxiliary_info.cancel_seq = seq_num;
                self.index_by_seq
                    .as_mut()
                    .unwrap()
                    .push_back((seq_num, order_no));
                print!("== load cancel ==  {order:?}\n");
            }
        }
    }
    /// 加载深圳交易所的交易数据，并更新订单信息。
    fn load_trade_sz(&mut self) {
        let bs_flag_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("TradeBSFlag")
            .unwrap()
            .i32()
            .unwrap();
        let buy_no_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("TradeBuyNo")
            .unwrap()
            .i64()
            .unwrap();
        let sell_no_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("TradeSellNo")
            .unwrap()
            .i64()
            .unwrap();
        let trade_type_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("TradeType")
            .unwrap()
            .i32()
            .unwrap();
        let trade_price_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("TradePrice")
            .unwrap()
            .f64()
            .unwrap();
        let trade_qty_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("TradeQty")
            .unwrap()
            .f64()
            .unwrap();
        let md_time_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("MDTime")
            .unwrap()
            .i64()
            .unwrap();
        let seq_num_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("ApplSeqNum")
            .unwrap()
            .i64()
            .unwrap();

        for idx in 0..self.df_trade.as_ref().unwrap().height() {
            let buy_order_id = buy_no_col.get(idx).unwrap();
            let sell_order_id = sell_no_col.get(idx).unwrap();
            let qty = trade_qty_col.get(idx).unwrap();
            let trade_price = trade_price_col.get(idx).unwrap();
            let trade_type = OrderType::from_i32(trade_type_col.get(idx).unwrap()).unwrap();
            let md_time = md_time_col.get(idx).unwrap();
            let seq_num = seq_num_col.get(idx).unwrap();

            // 根据买卖标志确定订单编号和方向

            let order_type = OrderType::from_i32(trade_type_col.get(idx).unwrap()).unwrap();
            let is_cancel = order_type == OrderType::Cancel;

            // 根据买卖标志确定订单编号和方向
            let (order_id, side) = if bs_flag_col.get(idx).unwrap() == 1 {
                (buy_no_col.get(idx).unwrap(), "B")
            } else {
                (sell_no_col.get(idx).unwrap(), "S")
            };

            if !is_cancel {
                let buy_order_ref = self
                    .orders
                    .as_mut()
                    .unwrap()
                    .get_mut(&buy_order_id)
                    .unwrap()
                    .clone();

                let mut buy_order = buy_order_ref.borrow_mut();
                let buy_auxiliary_info = buy_order.auxiliary_info.as_mut().unwrap();

                let sell_order_ref = self
                    .orders
                    .as_mut()
                    .unwrap()
                    .get_mut(&sell_order_id)
                    .unwrap()
                    .clone();

                let mut sell_order = sell_order_ref.borrow_mut();
                let sell_auxiliary_info = sell_order.auxiliary_info.as_mut().unwrap();

                if side == "B" {
                    buy_auxiliary_info.match_price = trade_price;
                    buy_auxiliary_info.match_qty += qty;
                    buy_auxiliary_info.match_seq = seq_num;
                    buy_auxiliary_info.match_count += 1;

                    sell_auxiliary_info.orderbook_price = trade_price;
                    sell_auxiliary_info.orderbook_qty += qty;
                    sell_auxiliary_info.orderbook_seq = seq_num;
                } else {
                    sell_auxiliary_info.match_price = trade_price;
                    sell_auxiliary_info.match_qty += qty;
                    sell_auxiliary_info.match_seq = seq_num;
                    sell_auxiliary_info.match_count += 1;

                    buy_auxiliary_info.orderbook_price = trade_price;
                    buy_auxiliary_info.orderbook_qty += qty;
                    buy_auxiliary_info.orderbook_seq = seq_num;
                }
            } else {
                let order_ref = self.orders.as_mut().unwrap().get(&order_id).unwrap();
                let mut order = order_ref.borrow_mut();
                let auxiliary_info = order.auxiliary_info.as_mut().unwrap();
                auxiliary_info.cancel_seq = seq_num;

                self.index_by_seq
                    .as_mut()
                    .unwrap()
                    .push_back((seq_num, order_id));
            }
        }
    }

    /// 加载上海交易所的交易数据，并更新订单信息。
    ///
    /// 该方法处理上海交易所的交易数据。对于每一笔交易，方法会根据买卖订单编号
    /// 更新对应订单的成交数量。如果订单在 `orders` 中尚未存在，则会创建新的订单
    /// 并添加到 `orders` 和 `index_by_seq` 中。
    fn load_trade_sh(&mut self) {
        let bs_flag_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("TradeBSFlag")
            .unwrap()
            .i32()
            .unwrap();
        let buy_no_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("TradeBuyNo")
            .unwrap()
            .i64()
            .unwrap();
        let sell_no_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("TradeSellNo")
            .unwrap()
            .i64()
            .unwrap();
        let trade_type_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("TradeType")
            .unwrap()
            .i32()
            .unwrap();
        let trade_price_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("TradePrice")
            .unwrap()
            .f64()
            .unwrap();
        let trade_qty_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("TradeQty")
            .unwrap()
            .f64()
            .unwrap();
        let md_time_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("MDTime")
            .unwrap()
            .i64()
            .unwrap();
        let seq_num_col = self
            .df_trade
            .as_ref()
            .unwrap()
            .column("ApplSeqNum")
            .unwrap()
            .i64()
            .unwrap();

        for idx in 0..self.df_trade.as_ref().unwrap().height() {
            let buy_order_id = buy_no_col.get(idx).unwrap();
            let sell_order_id = sell_no_col.get(idx).unwrap();
            let qty = trade_qty_col.get(idx).unwrap();
            let trade_price = trade_price_col.get(idx).unwrap();
            let trade_type = OrderType::from_i32(trade_type_col.get(idx).unwrap()).unwrap();
            let md_time = md_time_col.get(idx).unwrap();
            let seq_num = seq_num_col.get(idx).unwrap();

            let side = if bs_flag_col.get(idx).unwrap() == 1 {
                "B"
            } else {
                "S"
            };

            match self.orders.as_mut().unwrap().get_mut(&buy_order_id) {
                Some(order_ref) => {
                    let mut order = order_ref.borrow_mut();
                    let timestamp = order.timestamp.clone();
                    let auxiliary_info = order.auxiliary_info.as_mut().unwrap();

                    if side == "B" {
                        auxiliary_info.match_price = trade_price;
                        auxiliary_info.match_qty += qty;
                        if auxiliary_info.match_seq == i64::MAX {
                            auxiliary_info.match_seq = seq_num;
                        }
                        auxiliary_info.match_count += 1;
                        if !is_in_call_auction(timestamp, MarketType::SH).unwrap_or(false) {
                            auxiliary_info.initial_qty += qty;
                        }
                    } else {
                        auxiliary_info.orderbook_price = trade_price;
                        auxiliary_info.orderbook_qty += qty;
                        auxiliary_info.orderbook_seq = seq_num;
                    }
                    print!("== buy some side = {side} , seq = {seq_num} ,  ==  {order:?}\n");
                }
                None => {
                    let order_ref = L3Order::new_ref(
                        OrderSourceType::LocalOrder,
                        None,
                        buy_order_id,
                        Side::Buy,
                        0,
                        0,
                        md_time,
                        OrderType::None,
                    );

                    let mut order = order_ref.borrow_mut();
                    let auxiliary_info = order.auxiliary_info.as_mut().unwrap();

                    auxiliary_info.match_price = trade_price;
                    auxiliary_info.match_qty += qty;
                    auxiliary_info.match_seq = seq_num;
                    auxiliary_info.match_count += 1;

                    auxiliary_info.initial_qty += qty;

                    self.orders
                        .as_mut()
                        .unwrap()
                        .insert(buy_order_id, order_ref.clone());
                    print!("== buy none side = {side} , seq = {seq_num} ,  == {order:?}\n");
                }
            }

            match self.orders.as_mut().unwrap().get_mut(&sell_order_id) {
                Some(order_ref) => {
                    let mut order = order_ref.borrow_mut();
                    let timestamp = order.timestamp.clone();
                    let auxiliary_info = order.auxiliary_info.as_mut().unwrap();

                    if side == "S" {
                        auxiliary_info.match_price = trade_price;
                        auxiliary_info.match_qty += qty;
                        if auxiliary_info.match_seq == i64::MAX {
                            auxiliary_info.match_seq = seq_num;
                        }
                        auxiliary_info.match_count += 1;

                        if !is_in_call_auction(timestamp, MarketType::SZ).unwrap_or(false) {
                            auxiliary_info.initial_qty += qty;
                        }
                    } else {
                        auxiliary_info.orderbook_price = trade_price;
                        auxiliary_info.orderbook_qty += qty;
                        auxiliary_info.orderbook_seq = seq_num;
                    }
                    print!("== sell some == side = {side} , seq = {seq_num} , {order:?}\n");
                }
                None => {
                    let order_ref = L3Order::new_ref(
                        OrderSourceType::LocalOrder,
                        None,
                        sell_order_id,
                        Side::Sell,
                        0,
                        0,
                        md_time,
                        OrderType::None,
                    );

                    let mut order = order_ref.borrow_mut();
                    let auxiliary_info = order.auxiliary_info.as_mut().unwrap();

                    auxiliary_info.match_price = trade_price;
                    auxiliary_info.match_qty += qty;
                    auxiliary_info.match_seq = seq_num;
                    auxiliary_info.match_count += 1;

                    auxiliary_info.initial_qty += qty;

                    self.orders
                        .as_mut()
                        .unwrap()
                        .insert(sell_order_id, order_ref.clone());
                    print!("== sell none side = {side} , seq = {seq_num} , == {order:?}\n");
                }
            }
        }
    }

    fn init_sz(&mut self) {
        self.load_order_sz();
        self.load_trade_sz();
        self.post_init();
    }

    fn init_sh(&mut self) {
        self.load_order_sh();
        self.load_trade_sh();
        self.post_init();
    }

    fn post_init(&mut self) {
        for (order_id, order_ref) in self.orders.as_ref().unwrap().iter() {
            let seq = order_ref
                .borrow()
                .auxiliary_info
                .as_ref()
                .unwrap()
                .orderbook_seq();
            order_ref.borrow_mut().seq = seq;
            self.index_by_seq
                .as_mut()
                .unwrap()
                .push_back((seq, order_id.clone()));
        }
        self.index_by_seq.as_mut().unwrap().make_contiguous().sort();
        self.len = self.index_by_seq.as_ref().unwrap().len();
    }
}

impl OrderIter for DataCollator {
    type Item = L3OrderRef;

    fn next(&mut self) -> Option<(i64, &Self::Item)> {
        if self.is_last() {
            return None;
        }
        let (idx, order_id) = self.index_by_seq.as_ref().unwrap()[self.current_idx];
        self.current_idx += 1;
        Some((idx, self.orders.as_ref().unwrap().get(&order_id).unwrap()))
    }

    fn is_last(&self) -> bool {
        self.current_idx == self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // 创建一个测试用的 DataCollator 实例
    fn create_test_collator() -> DataCollator {
        DataCollator::new(
            "600519".to_string(),
            "local".to_string(),
            "path/to/data".to_string(),
            "20240830".to_string(),
            "ORDER",
        )
    }

    // 测试 DataCollator 的新建
    #[test]
    fn test_new() {
        let collator = create_test_collator();
        assert_eq!(collator.exchange_code, "SH");
        assert_eq!(collator.stock_code, "600519");
        assert_eq!(collator.file_type, "local");
        assert_eq!(collator.data_path, "path/to/data");
        assert_eq!(collator.source, OrderSourceType::LocalOrder);
    }

    #[test]
    fn test_new2() {
        let exchange_mode = "backtest".to_string();
        let stock_code = "688007.SH".to_string();
        let file_type = "local".to_string();
        let data_path = "./data".to_string();
        let date = "20231201".to_string();
        let mode = "L2P";

        let mut data = DataCollator::new(stock_code, file_type, data_path, date, mode);
        data.init();
        print!("data len = {}\n", data.len);
        for i in 1..=data.len {
            print!("{:?}\n", data.next());
        }
        print!("data current_idx = {}\n", data.current_idx)
    }

    // // 测试初始化
    // #[test]
    // fn test_init() {
    //     let mut collator = create_test_collator();
    //     collator.init();
    //     assert!(!collator.is_last);
    //     assert_eq!(collator.len, collator.index_by_seq.len());
    // }

    // // 测试 load_order
    // #[test]
    // fn test_load_order() {
    //     // 设置测试数据
    //     let df_order = DataFrame::new(vec![
    //         Series::new("OrderNO", &[1001, 1002]),
    //         Series::new("OrderBSFlag", &[1, 0]),
    //         Series::new("OrderType", &[1, 2]), // 假设 1 对应 L，2 对应 M
    //         Series::new("OrderPrice", &[10.5, 20.5]),
    //         Series::new("OrderQty", &[100.0, 200.0]),
    //         Series::new("ReceiveDateTime", &[1234567890, 1234567900]),
    //         Series::new("ApplSeqNum", &[1, 2]),
    //     ])
    //     .unwrap();

    //     let mut collator = create_test_collator();
    //     collator.df_order = df_order;
    //     collator.load_order();

    //     assert_eq!(collator.orders.len(), 2);
    //     assert_eq!(collator.index_by_seq.len(), 2);
    // }
}
