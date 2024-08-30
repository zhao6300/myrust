use std::collections::VecDeque;

use super::dataapi::DataApi;
use super::*;
use order::{Order, OrderRef};
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
#[derive(Debug)]
pub struct DataCollator {
    pub exchange_code: String,              // 交易所代码
    pub stock_code: String,                 // 股票代码
    pub file_type: String,                  // 数据文件类型（如本地或HDFS）
    pub data_path: String,                  // 数据路径
    pub source: OrderSourceType,            // 订单来源类型
    pub df_order: DataFrame,                // 订单数据框架
    pub df_trade: DataFrame,                // 交易数据框架
    pub last_df_order_idx: usize,           // 上次处理的订单数据索引
    pub last_df_trade_idx: usize,           // 上次处理的交易数据索引
    pub is_last: bool,                      // 是否是最后一个数据
    pub orders: HashMap<OrderId, OrderRef>, // 订单映射
    /// 按照 order_seq 排序的队列，其中包含 order_seq 和 order_id，如果是撤单，第三个值为 true。
    pub index_by_seq: VecDeque<(i64, i64)>,
    pub current_idx: usize, // 当前处理的索引
    pub len: usize,         // 队列长度
    pub da_api: DataApi,    // 数据 API 对象
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
        exchange_code: String,
        stock_code: String,
        file_type: String,
        data_path: String,
        date: String, //%Y%m%d
        mode: &str,
    ) -> Self {
        let restrict_aggressive_order =
            !stock_code.is_empty() && stock_code.chars().nth(0) == Some('3');
        // 检查并设置交易所代码
        let exchange_code = if stock_code.ends_with("SH") {
            "SH".to_string()
        } else {
            "SZ".to_string()
        };

        // 校验模式是否合法
        let mode_upper = mode.to_uppercase();
        if !["ORDER", "L2P"].contains(&mode_upper.as_str()) {
            panic!("撮合模式只有 ORDER, L2P 两种，请重新输入！");
        }

        let da_api = DataApi::new(
            date.clone(),
            file_type.to_string(),
            mode_upper,
            data_path.to_string(),
        );
        
        // 加载订单和交易数据（根据文件类型判断是否加载）
        let (df_order, df_trade) = if file_type == "local" || file_type == "hdfs" {
            (
                da_api.load_order_data(&stock_code, false),
                da_api.load_transaction_data(&stock_code, false),
            )
        } else {
            (DataFrame::default(), DataFrame::default())
        };

        // 计算数据总量（订单和交易）
        let size = df_order.shape().0 + df_trade.shape().0;

        Self {
            exchange_code,
            stock_code: stock_code.clone().to_string(),
            file_type: file_type,
            data_path: data_path,
            source: OrderSourceType::LocalOrder,
            df_order: df_order,
            df_trade: df_trade,
            last_df_order_idx: 0,
            last_df_trade_idx: 0,
            is_last: false,
            orders: HashMap::new(),
            index_by_seq: VecDeque::new(),
            current_idx: 0,
            len: 0,
            da_api: da_api,
        }
    }
    /// 初始化 `DataCollator`，根据交易所类型加载数据。
    pub fn init(&mut self) {
        if self.exchange_code.to_lowercase() == "sz" {
            self.init_sz();
        } else {
            self.init_sh();
        }
    }
    /// 加载订单数据，并将其存储在 `orders` 和 `index_by_seq` 中。
    fn load_order(&mut self) {
        let _bs = self.df_order.column("OrderBSFlag").unwrap().i32().unwrap();
        let _no = self.df_order.column("OrderNO").unwrap().i64().unwrap();
        let _type = self.df_order.column("OrderType").unwrap().i32().unwrap();
        let _p = self.df_order.column("OrderPrice").unwrap().f64().unwrap();
        let _v = self.df_order.column("OrderQty").unwrap().f64().unwrap();
        let _mdtime = self.df_order.column("MDTime").unwrap().i64().unwrap();
        let _recvtime = self
            .df_order
            .column("ReceiveDateTime")
            .unwrap()
            .i64()
            .unwrap();
        let _seq = self.df_order.column("ApplSeqNum").unwrap().i64().unwrap();

        for idx in 0..self.df_order.height() {
            let mut order_no = _no.get(idx).unwrap();
            let seq = _seq.get(idx).unwrap();

            let side: String;

            if _bs.get(idx).unwrap() == 1 {
                side = "B".to_string();
            } else {
                side = "S".to_string();
            }
            let order_type = OrdType::from_i32(_type.get(idx).unwrap()).unwrap();
            if order_type != OrdType::Cancel {
                order_no = -order_no;
            }
            let order_ref = Order::new_ref(
                None,
                self.stock_code.clone(),
                _recvtime.get(idx).unwrap(),
                _p.get(idx).unwrap(),
                _v.get(idx).unwrap(),
                side.as_str(),
                order_type,
                OrderSourceType::LocalOrder,
            );
            order_ref.borrow_mut().order_id = order_no;
            order_ref.borrow_mut().seq = seq;
            self.orders.insert(order_no, order_ref);

            self.index_by_seq.push_back((seq, order_no));
        }
    }
    /// 加载深圳交易所的交易数据，并更新订单信息。
    fn load_trade_sz(&mut self) {
        let _bs = self.df_trade.column("TradeBSFlag").unwrap().i32().unwrap();
        let _buy_no = self.df_trade.column("TradeBuyNo").unwrap().i64().unwrap();
        let _sell_no = self.df_trade.column("TradeSellNo").unwrap().i64().unwrap();
        let _type = self.df_trade.column("TradeType").unwrap().i32().unwrap();
        let _p = self.df_trade.column("TradePrice").unwrap().f64().unwrap();
        let _v = self.df_trade.column("TradeQty").unwrap().f64().unwrap();
        let _mdtime = self.df_trade.column("MDTime").unwrap().i64().unwrap();
        let _recvtime = self
            .df_trade
            .column("ReceiveDateTime")
            .unwrap()
            .i64()
            .unwrap();
        let _seq = self.df_trade.column("ApplSeqNum").unwrap().i64().unwrap();

        for idx in 0..self.df_trade.height() {
            let seq = _seq.get(idx).unwrap();
            let mut order_no: i64;
            let mut side: &str;
            if _bs.get(idx).unwrap() == 1 {
                order_no = _buy_no.get(idx).unwrap();
                side = "B";
            } else {
                order_no = _sell_no.get(idx).unwrap();
                side = "S";
            }

            let order_type = OrdType::from_i32(_type.get(idx).unwrap()).unwrap();
            if order_type != OrdType::Cancel {
                order_no = -order_no;
                let order_ref = Order::new_ref(
                    None,
                    self.stock_code.clone(),
                    _recvtime.get(idx).unwrap(),
                    _p.get(idx).unwrap(),
                    _v.get(idx).unwrap(),
                    side,
                    OrdType::from_i32(_type.get(idx).unwrap()).unwrap(),
                    OrderSourceType::LocalOrder,
                );
                order_ref.borrow_mut().order_id = order_no;
                order_ref.borrow_mut().seq = seq;
                self.orders.insert(order_no, order_ref);
                self.index_by_seq.push_back((seq, order_no));
            }
        }
    }

    /// 加载上海交易所的交易数据，并更新订单信息。
    ///
    /// 该方法处理上海交易所的交易数据。对于每一笔交易，方法会根据买卖订单编号
    /// 更新对应订单的成交数量。如果订单在 `orders` 中尚未存在，则会创建新的订单
    /// 并添加到 `orders` 和 `index_by_seq` 中。
    fn load_trade_sh(&mut self) {
        let _bs = self.df_trade.column("TradeBSFlag").unwrap().i32().unwrap();
        let _buy_no = self.df_trade.column("TradeBuyNo").unwrap().i64().unwrap();
        let _sell_no = self.df_trade.column("TradeSellNo").unwrap().i64().unwrap();
        let _type = self.df_trade.column("TradeType").unwrap().i32().unwrap();
        let _p = self.df_trade.column("TradePrice").unwrap().f64().unwrap();
        let _v = self.df_trade.column("TradeQty").unwrap().f64().unwrap();
        let _mdtime = self.df_trade.column("MDTime").unwrap().i64().unwrap();
        let _recvtime = self
            .df_trade
            .column("ReceiveDateTime")
            .unwrap()
            .i64()
            .unwrap();
        let _seq = self.df_trade.column("ApplSeqNum").unwrap().i64().unwrap();

        for idx in 0..self.df_trade.height() {
            let buy_order_id = _buy_no.get(idx).unwrap();
            let sell_order_id = _sell_no.get(idx).unwrap();
            let qty = _v.get(idx).unwrap();
            let seq = _seq.get(idx).unwrap();
            match self.orders.get_mut(&buy_order_id) {
                Some(order) => order.borrow_mut().qty += qty,
                None => {
                    let order_ref = Order::new_ref(
                        None,
                        self.stock_code.clone(),
                        _recvtime.get(idx).unwrap(),
                        _p.get(idx).unwrap(),
                        _v.get(idx).unwrap(),
                        "B",
                        OrdType::from_i32(_type.get(idx).unwrap()).unwrap(),
                        OrderSourceType::LocalOrder,
                    );
                    order_ref.borrow_mut().order_id = buy_order_id;
                    order_ref.borrow_mut().seq = seq;
                    self.orders.insert(buy_order_id, order_ref);
                    self.index_by_seq.push_back((seq, buy_order_id));
                }
            }

            match self.orders.get_mut(&sell_order_id) {
                Some(order) => order.borrow_mut().qty += qty,
                None => {
                    let order_ref = Order::new_ref(
                        None,
                        self.stock_code.clone(),
                        _recvtime.get(idx).unwrap(),
                        _p.get(idx).unwrap(),
                        _v.get(idx).unwrap(),
                        "S",
                        OrdType::from_i32(_type.get(idx).unwrap()).unwrap(),
                        OrderSourceType::LocalOrder,
                    );
                    order_ref.borrow_mut().order_id = sell_order_id;
                    order_ref.borrow_mut().seq = seq;
                    self.orders.insert(sell_order_id, order_ref);
                    self.index_by_seq.push_back((seq, sell_order_id));
                }
            }
        }
    }

    fn init_sz(&mut self) {
        self.load_order();
        self.load_trade_sz();
        self.post_init();
    }

    fn init_sh(&mut self) {
        self.load_order();
        self.load_trade_sh();
        self.post_init();
    }

    fn post_init(&mut self) {
        self.index_by_seq.make_contiguous().sort();
        self.len = self.index_by_seq.len();
    }
}

impl OrderIter for DataCollator {
    type Item = OrderRef;

    fn next(&self) -> Option<&Self::Item> {
        if self.is_last() {
            return None;
        }
        let (_, order_id) = self.index_by_seq[self.current_idx];

        self.orders.get(&order_id)
    }

    fn is_last(&self) -> bool {
        self.current_idx == self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init() {}
}
