use libc::EEXIST;
use polars::prelude::DataFrame;
use polars::prelude::*;
use pyo3::{self, basic::getattr, prelude::*};
#[warn(unused_imports)]
mod depth;

use pyo3::types::{PyDict, PyList};

use depth::dataloader::DataCollator;
use depth::exchange::Exchange;
use depth::skiplist_orderbook::SkipListMarketDepth;
use depth::types::{ExchangeMode, OrderStatus};
use depth::utils::time_difference_ms_i64;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;

/// TradeMockerRS 是一个用于模拟交易的 Rust 结构体，通过 PyO3 与 Python 进行交互。
#[pyclass(subclass)]
pub struct TradeMockerRS {
    pub exchange: Arc<Mutex<Exchange<SkipListMarketDepth>>>,
    pub stock_code: Option<String>,
    pub exchange_mode: String,
    pub file_type: String,
    pub data_path: String,
    pub date: String,
    pub mode: String,
}

#[pymethods]
impl TradeMockerRS {
    /// 创建一个新的 TradeMockerRS 实例
    ///
    /// # 参数
    /// - `mode`: 模式，表示运行环境，如 `backtest` 或 `live`。
    /// - `date`: 日期，格式为 `YYYY-MM-DD`。
    /// - `need_output`: 是否需要输出结果。
    /// - `orderbook_level`: 订单簿的深度等级。
    /// - `file_type`: 数据文件类型，如 `hdfs`。
    /// - `data_path`: 数据路径。
    /// - `exchange_mode`: 交易模式，如 `backtest`。
    /// - `verbose`: 日志输出等级。
    ///
    /// # 返回
    /// - 返回一个 TradeMockerRS 实例。
    #[staticmethod]
    pub fn new(
        mode: &str,
        date: &str,
        need_output: bool,
        orderbook_level: i32,
        file_type: &str,
        data_path: &str,
        exchange_mode: &str,
        verbose: i32,
    ) -> Self {
        let exchange = Exchange::new(exchange_mode, date);
        Self {
            exchange: Arc::new(Mutex::new(exchange)),
            stock_code: None,
            exchange_mode: exchange_mode.to_string(),
            file_type: file_type.to_string(),
            data_path: data_path.to_string(),
            date: date.to_string(),
            mode: mode.to_string(),
        }
    }

    /// 发送订单
    ///
    /// # 参数
    /// - `stock_code`: 股票代码。
    /// - `order_time`: 订单时间。
    /// - `order_price`: 订单价格。
    /// - `order_volume`: 订单数量。
    /// - `bs_flag`: 买卖标识，`buy` 或 `sell`。
    ///
    /// # 返回
    /// - 返回订单 ID，如果失败返回 -1。
    ///
    pub fn init(&mut self, stock_code: &str) -> bool {
        if self.stock_code.is_none() {
            self.stock_code = Some(stock_code.to_string());

            let mut data = DataCollator::new(
                self.exchange_mode.clone(),
                stock_code.to_string().clone(),
                self.file_type.clone(),
                self.data_path.clone(),
                self.date.clone(),
                self.mode.as_str(),
            );
            data.init();
            let stock_type = data.da_api.as_mut().unwrap()._stock_type.borrow().clone();
            let exchange_mode = ExchangeMode::from_str(self.exchange_mode.as_str())
                .unwrap_or(ExchangeMode::Backtest);
            let mut exchange = self.exchange.lock().unwrap();
            if let Err(e) = exchange.add_broker(
                exchange_mode,
                stock_type,
                self.stock_code.as_ref().unwrap().clone(),
                1.0,
            ) {
                eprintln!("Failed to add broker: {}", e);
                false // 返回错误代码，表示添加经纪商失败
            } else {
                let _ = exchange.add_data(self.stock_code.as_ref().unwrap(), data);
                true
            }
        } else {
            true
        }
    }
    pub fn send_order(
        &mut self,
        stock_code: &str,
        order_time: i64,
        order_price: f64,
        order_volume: i64,
        bs_flag: &str,
    ) -> i64 {
        if !self.init(stock_code) {
            return -1;
        }

        match self.exchange.lock().unwrap().send_order(
            "none",
            stock_code,
            order_time,
            order_price,
            order_volume,
            bs_flag,
        ) {
            Ok(order_id) => order_id,
            Err(_) => -1,
        }
    }

    /// 撤销订单
    ///
    /// # 参数
    /// - `order_number`: 订单编号。
    ///
    /// # 返回
    /// - 成功撤销返回 `true`。
    pub fn cancel_order(&mut self, order_number: i64) -> bool {
        self.exchange
            .lock()
            .unwrap()
            .cancel_order(self.stock_code.as_ref().unwrap().as_str(), order_number)
            .is_ok()
    }

    /// 获取待处理订单
    ///
    /// # 返回
    /// - 返回以 JSON 格式表示的待处理订单列表。
    pub fn get_pending_orders(&self) -> String {
        let mut orders = HashMap::new();
        self.exchange.lock().unwrap().get_orders(
            self.stock_code.as_ref().unwrap().as_str(),
            &mut orders,
            &vec![OrderStatus::New, OrderStatus::PartiallyFilled],
        );
        serde_json::to_string(&orders).unwrap()
    }

    pub fn get_crurent_time(&self) -> i64 {
        self.exchange
            .lock()
            .unwrap()
            .get_crurent_time(self.stock_code.as_ref().unwrap().as_str())
            .unwrap_or(-1)
    }

    /// 获取已取消订单
    ///
    /// # 返回
    /// - 返回以 JSON 格式表示的已取消订单列表。
    pub fn get_cancel_orders(&self) -> String {
        let mut orders = HashMap::new();
        self.exchange.lock().unwrap().get_orders(
            self.stock_code.as_ref().unwrap().as_str(),
            &mut orders,
            &vec![OrderStatus::Canceled],
        );
        serde_json::to_string(&orders).unwrap()
    }

    pub fn get_finished_order(&self) -> String {
        let mut orders = HashMap::new();
        self.exchange.lock().unwrap().get_orders(
            self.stock_code.as_ref().unwrap().as_str(),
            &mut orders,
            &vec![OrderStatus::Filled],
        );
        serde_json::to_string(&orders).unwrap()
    }

    pub fn elapse(&self, start: i64, duration: i64) -> i64{
        let current_timepoint = self
            .exchange
            .lock()
            .unwrap()
            .get_crurent_time(self.stock_code.as_ref().unwrap().as_str())
            .unwrap_or(0);
        let expected_duration =
            time_difference_ms_i64(current_timepoint, start).unwrap_or(0) + duration;
        let filled = self.exchange.lock().unwrap().elapse(expected_duration);

    }

    pub fn get_latest_orders(&self) -> String {
        let mut orders = HashMap::new();
        self.exchange
            .lock()
            .unwrap()
            .get_latest_orders(self.stock_code.as_ref().unwrap().as_str(), &mut orders);
        serde_json::to_string(&orders).unwrap()
    }

    pub fn elapse_with_orders(&self, start: i64, duration: i64) -> String {
        let current_timepoint = self
            .exchange
            .lock()
            .unwrap()
            .get_crurent_time(self.stock_code.as_ref().unwrap().as_str())
            .unwrap_or(0);
        let expected_duration =
            time_difference_ms_i64(current_timepoint, start).unwrap_or(0) + duration;
        let filled = self.exchange.lock().unwrap().elapse(expected_duration);
        let mut orders = HashMap::new();
        self.exchange
            .lock()
            .unwrap()
            .get_latest_orders(self.stock_code.as_ref().unwrap().as_str(), &mut orders);
        serde_json::to_string(&orders).unwrap()
    }

    pub fn match_order_util_mdtime(&mut self, mkt_clock_time: i64) -> String {
        let filled = self.exchange.lock().unwrap().elapse(mkt_clock_time);
        let mut orders = HashMap::new();
        self.exchange
            .lock()
            .unwrap()
            .get_latest_orders(self.stock_code.as_ref().unwrap().as_str(), &mut orders);
        serde_json::to_string(&orders).unwrap()
    }

    pub fn match_order_util_recvtime(&mut self, mkt_clock_time: i64) -> String {
        let filled = self.exchange.lock().unwrap().elapse(mkt_clock_time);
        let mut orders = HashMap::new();
        self.exchange
            .lock()
            .unwrap()
            .get_latest_orders(self.stock_code.as_ref().unwrap().as_str(), &mut orders);
        serde_json::to_string(&orders).unwrap()
    }

    pub fn restore_real_orderbook(&mut self) -> bool {
        // let result = self.exchange.restore_real_orderbook();
        // result
        true
    }

    pub fn add_order_data(
        &mut self,
        stock_code: &str,
        order_bs_flag_slice: Vec<i32>,
        order_no_slice: Vec<i64>,
        order_type_slice: Vec<i32>,
        order_price_slice: Vec<i32>,
        order_qty_slice: Vec<i32>,
        md_time_slice: Vec<i64>,
        receive_date_time_slice: Vec<i64>,
        appl_seq_num_slice: Vec<i64>,
    ) {
        // Create DataFrame
        let order_df = df!(
            "OrderBSFlag" => order_bs_flag_slice,
            "OrderNO" => order_no_slice,
            "OrderType" => order_type_slice,
            "OrderPrice" => order_price_slice,
            "OrderQty" => order_qty_slice,
            "MDTime" => md_time_slice,
            "ReceiveDateTime" => receive_date_time_slice,
            "ApplSeqNum" => appl_seq_num_slice
        )
        .unwrap();

        // self.exchange.add_order_data(stock_code, order_df);
    }

    pub fn add_trade_data(
        &mut self,
        stock_code: &str,
        trade_bs_flag_slice: Vec<i32>,
        trade_buy_no_slice: Vec<i64>,
        trade_sell_no_slice: Vec<i64>,
        trade_type_slice: Vec<i32>,
        trade_price_slice: Vec<i32>,
        trade_qty_slice: Vec<i32>,
        md_time_slice: Vec<i64>,
        receive_date_time_slice: Vec<i64>,
        appl_seq_num_slice: Vec<i64>,
    ) {
        // Create DataFrame
        let trade_df = df!(
            "TradeBSFlag" => trade_bs_flag_slice,
            "TradeBuyNo" => trade_buy_no_slice,
            "TradeSellNo" => trade_sell_no_slice,
            "TradeType" => trade_type_slice,
            "TradePrice" => trade_price_slice,
            "TradeQty" => trade_qty_slice,
            "MDTime" => md_time_slice,
            "ReceiveDateTime" => receive_date_time_slice,
            "ApplSeqNum" => appl_seq_num_slice
        )
        .unwrap();

        // self.exchange.add_trade_data(stock_code, trade_df);
    }

    pub fn get_current_l3_snapshot(&self, stock_code: &str) -> String {
        let result_json = self.exchange.lock().unwrap().snapshot(stock_code);
        result_json
    }

    pub fn presist_l3_data(&mut self, stock_code: &str, clean_up: Option<bool>) -> bool {
        // let clean_up = match clean_up {
        //     Some(n) => n,
        //     None => true,
        // };
        // let result = self.exchange.presist_l3_data(stock_code, clean_up);
        // result
        true
    }
}

#[pyfunction]
fn trade_mocker_instance(
    py: Python,
    mode: &str,
    date: &str,
    need_output: bool,
    orderbook_level: Option<i32>,
    file_type: Option<&str>,
    data_path: Option<&str>,
    exchange_mode: Option<&str>,
    verbose: Option<i32>,
) -> PyResult<PyObject> {
    let file_type = match file_type {
        Some(n) => n,
        None => "hdfs",
    };
    let data_path = match data_path {
        Some(n) => n,
        None => "/root/mdc_data/",
    };
    let orderbook_level = match orderbook_level {
        Some(n) => n as i32,
        None => 50 as i32,
    };

    let exchange_mode = match exchange_mode {
        Some(mode) => mode,
        None => "backtest",
    };

    let verbose = match verbose {
        Some(n) => n as i32,
        None => 0 as i32,
    };

    let my_class = TradeMockerRS::new(
        mode,
        date,
        need_output,
        orderbook_level,
        file_type,
        data_path,
        exchange_mode,
        verbose,
    );
    let my_class_py = my_class.into_py(py);
    Ok(my_class_py)
}

/// 实现 Python 模块 `trade_mocker_rust`
///
/// 该模块将 Rust 中的 `TradeMockerRS` 结构体暴露为 Python 类，并提供一个创建 `TradeMockerRS` 实例的工厂函数。
///
/// # 参数
/// - `_py`: Python 解释器的上下文对象
/// - `m`: 该模块的 Python 模块对象
///
/// # 返回
/// 返回 `PyResult<()>`，表示模块的初始化状态。
///
/// # 详细说明
/// - 该模块中注册了 `TradeMockerRS` 类，使其可以在 Python 中直接使用。
/// - 同时，注册了 `trade_mocker_instance` 工厂函数，使用户可以方便地通过 Python 调用此函数来创建 `TradeMockerRS` 的实例。
#[pymodule]
fn trade_mocker_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    // 将 `TradeMockerRS` 类注册为 Python 模块中的类。
    m.add_class::<TradeMockerRS>()?;

    // 将 `trade_mocker_instance` 函数注册为 Python 模块中的函数。
    m.add_wrapped(wrap_pyfunction!(trade_mocker_instance))?;
    Ok(())
}
s