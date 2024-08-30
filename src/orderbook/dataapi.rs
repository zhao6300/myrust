use core::panic;
use std::io::Write;

use hdrs::Client;
use hdrs::ClientBuilder;
use parquet2::read::{deserialize_metadata, read_metadata};
use polars::export::num::ToPrimitive;
use polars::prelude::*;
use std::cell::RefCell;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::Cursor;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DataApi {
    pub _date: String,
    pub _file_type: String,
    pub _stock_type: RefCell<String>,
    pub _price_unit: RefCell<f64>, //100表示保留2位小数
    pub _broker_mod: String,
    pub _data_path: String,
    pub fs: Option<Arc<Client>>,
}

impl DataApi {
    pub fn new(date: String, file_type: String, broker_mod: String, data_path: String) -> Self {
        let _date = date;
        let _file_type = file_type;
        let _stock_type = RefCell::new("unknow".to_string());
        let _price_unit = RefCell::new(100.0);
        let file_type_list = vec![
            "hdfs".to_string(),
            "local".to_string(),
            "vector".to_string(),
        ];
        if !file_type_list.contains(&_file_type) {
            panic!("file_type只能选择hdfs (hdfs文件) 或local (本地文件)或vector (内存vector)!");
        }
        let _broker_mod = broker_mod;
        let _data_path = data_path;

        env::set_var("JAVA_HOME", "/usr/java/latest/");
        env::set_var("JAVA_TOOL_OPTIONS", "-Xss1280K");
        env::set_var(
            "ARROW_LIBHDFS_DIR",
            "/opt/cloudera/parcels/CDH/lib/impala/lib/",
        );
        env::set_var("HADOOP_HOME", "/opt/cloudera/parcels/CDH/lib/hadoop");
        env::set_var(
            "HADOOP_CONF_DIR",
            "/opt/cloudera/parcels/CDH/lib/hadoop/etc/hadoop",
        );
        env::set_var(
            "YARN_CONF_DIR",
            "/opt/cloudera/parcels/CDH/lib/hadoop/etc/hadoop",
        );
        env::set_var(
            "LD_LIBRARY_PATH",
            "$LD_LIBRARY_PATH:/usr/java/latest/jre/lib/amd64/server/",
        );

        let fs = if _file_type == "hdfs" {
            Some(Arc::new(ClientBuilder::new("default").connect().unwrap()))
        } else {
            None
        };

        Self {
            _date,
            _file_type,
            _stock_type,
            _price_unit,
            _broker_mod,
            _data_path,
            fs,
        }
    }

    fn gen_bs_for_trans(&self, trade_bs_flag: &Series, trade_type: &Series) -> Series {
        let mut res: Vec<&str> = vec![];
        let s_len = trade_bs_flag.len();
        let mut i = 0;
        loop {
            let trade_type_i = trade_type.i32().unwrap().get(i).unwrap();
            let trade_bs_flag_i = trade_bs_flag.i32().unwrap().get(i).unwrap();
            if trade_type_i == 1 {
                res.push("");
            } else if trade_bs_flag_i == 1 {
                res.push("B");
            } else {
                res.push("S");
            }
            i += 1;
            if i == s_len {
                break;
            }
        }
        let bs_flag = Series::new("BSFlags", &res);
        return bs_flag;
    }

    fn gen_cancel(&self, trade_type: &Series) -> Series {
        let mut res: Vec<&str> = vec![];
        let s_len = trade_type.len();
        let mut i = 0;
        loop {
            let trade_type_i = trade_type.i32().unwrap().get(i).unwrap();
            if trade_type_i == 1 {
                res.push("C");
            } else {
                res.push("0");
            }
            i += 1;
            if i == s_len {
                break;
            }
        }
        let cancel = Series::new("FunctionCode", &res);
        return cancel;
    }

    fn transform_trans_data(&self, df_mdc: DataFrame) -> DataFrame {
        let df_mdc = df_mdc
            .select([
                "MDDate",
                "MDTime",
                "TradeBSFlag",
                "TradeType",
                "TradePrice",
                "TradeQty",
                "TradeSellNo",
                "TradeBuyNo",
            ])
            .unwrap();
        let mut df_mdc = df_mdc
            .lazy()
            .filter(col("MDDate").is_in(lit(&*self._date)))
            .collect()
            .unwrap();
        let trade_bsflag = df_mdc.column("TradeBSFlag").unwrap();
        let trade_type = df_mdc.column("TradeType").unwrap();
        let bsflags = self.gen_bs_for_trans(trade_bsflag, trade_type);
        let function_code = self.gen_cancel(trade_type);
        let df_mdc = df_mdc.with_column(bsflags).unwrap();
        let df_mdc = df_mdc.with_column(function_code).unwrap();
        let mddate = df_mdc
            .column("MDDate")
            .unwrap()
            .cast(&polars::prelude::DataType::Int64)
            .unwrap();
        let df_mdc = df_mdc.with_column(mddate).unwrap();
        // 暂未找到批量修改列名的方法
        let df_mdc = df_mdc.rename("MDDate", "Date").unwrap();
        let df_mdc = df_mdc.rename("MDTime", "Timestamp").unwrap();
        let df_mdc = df_mdc.rename("TradePrice", "Price").unwrap();
        let df_mdc = df_mdc.rename("TradeQty", "Volume").unwrap();
        let df_mdc = df_mdc.rename("TradeSellNo", "AskOrder").unwrap();
        let df_mdc = df_mdc.rename("TradeBuyNo", "BidOrder").unwrap();

        let df_trans = df_mdc
            .select([
                "Date",
                "Timestamp",
                "BSFlags",
                "Price",
                "Volume",
                "AskOrder",
                "BidOrder",
                "FunctionCode",
            ])
            .unwrap();
        return df_trans;
    }

    fn load_marketdata(&self, symbol: &str, data_type: &str) -> DataFrame {
        if (*self._stock_type.borrow()) == "unknow" {
            match self.load_marketdata_by_type(symbol, data_type, "Stock") {
                Ok(df) => return df,
                Err(err) => match self.load_marketdata_by_type(symbol, data_type, "Fund") {
                    Ok(df) => return df,
                    Err(error_msg) => panic!("{}", error_msg.as_str()),
                },
            }
        } else {
            let stock_type = (*self._stock_type.borrow()).clone();
            self.load_marketdata_by_type(symbol, data_type, stock_type.as_str())
                .unwrap()
        }
    }

    fn load_marketdata_by_type(
        &self,
        symbol: &str,
        data_type: &str,
        stock_type: &str,
    ) -> Result<DataFrame, String> {
        // 根据标的获取SZ或SH
        let exchange_code = &symbol[symbol.len() - 2..];
        let date_month = &self._date[0..6];
        let mut sub_path = "".to_string();
        let mut df_mdc: DataFrame;
        let mut stock_type_str;
        let mut data_type_str;
        if stock_type.to_uppercase() == "STOCK".to_string() {
            stock_type_str = "Stock";
        } else {
            stock_type_str = "Fund";
        }
        if data_type.to_uppercase() == "TRANSACTION".to_string() {
            data_type_str = "Transaction";
        } else {
            data_type_str = "Order";
        }
        if exchange_code == "SZ" {
            sub_path = format!(
                "XSHE_{}_{}_Auction_Month/month={}/XSHE_{}_{}_Auction_{}_{}.parquet",
                stock_type_str,
                data_type_str,
                date_month,
                stock_type_str,
                data_type_str,
                symbol,
                date_month
            );
        } else {
            sub_path = format!(
                "XSHG_{}_{}_Auction_Month/month={}/XSHG_{}_{}_Auction_{}_{}.parquet",
                stock_type_str,
                data_type_str,
                date_month,
                stock_type_str,
                data_type_str,
                symbol,
                date_month
            );
        }
        dbg!(&sub_path);
        if self._file_type == "local" {
            let base_path = Path::new(&self._data_path);
            let file_path = base_path.join(sub_path);
            let error_msg: String = format!("行情文件不存在：{}！", file_path.to_str().unwrap());
            if *self._stock_type.borrow() == "unknow" {
                if std::fs::metadata(&file_path).is_err() {
                    return Err(error_msg.to_string());
                } else {
                    // 内部可变性
                    let mut variable1 = self._stock_type.borrow_mut();
                    *variable1 = stock_type_str.to_string();
                    if stock_type_str == "Fund" {
                        let mut variable2 = self._price_unit.borrow_mut();
                        *variable2 = 10.0;
                    }
                }
            }
            let mut file = match std::fs::File::open(file_path) {
                Ok(f) => f,
                Err(err) => return Err(error_msg.to_string()),
            };
            df_mdc = ParquetReader::new(&mut file).finish().unwrap();
        } else {
            let fs = match self.fs.as_ref() {
                Some(value) => value,
                None => panic!("no value"),
            };
            let base_path = Path::new(&"/htdata/mdc/MDCProvider/");
            let file_path = base_path.join(sub_path);
            let error_msg: String = format!("行情文件不存在：{}！", file_path.to_str().unwrap());
            if *self._stock_type.borrow() == "unknow" {
                if fs.metadata(&file_path.to_str().unwrap()).is_err() {
                    return Err(error_msg.to_string());
                } else {
                    // 内部可变性
                    let mut variable = self._stock_type.borrow_mut();
                    *variable = stock_type_str.to_string();
                    if stock_type_str == "Fund" {
                        let mut variable2 = self._price_unit.borrow_mut();
                        *variable2 = 10.0;
                    }
                }
            }
            let mut f = match fs.open_file().read(true).open(&file_path.to_str().unwrap()) {
                Ok(file) => file,
                Err(err) => return Err(error_msg.to_string()),
            };
            let mut buf: Vec<u8> = Vec::new();
            let n = f.read_to_end(&mut buf).unwrap();
            let reader = Cursor::new(&buf);
            df_mdc = ParquetReader::new(reader).finish().unwrap();
        }
        Ok(df_mdc)
    }

    pub fn load_transaction_data(&self, symbol: &str, transform: bool) -> DataFrame {
        let mut df_mdc = self.load_marketdata(symbol, "Transaction");
        df_mdc = df_mdc
            .lazy()
            .filter(col("MDDate").eq(lit(self._date.to_string())))
            .collect()
            .unwrap();
        let date_int = self._date.to_string().parse::<i64>().unwrap() * 1000000000;
        let price_unit = *self._price_unit.borrow();
        df_mdc = df_mdc
            .lazy()
            .with_columns([col("MDTime").cast(DataType::Int64) + lit(date_int)])
            .filter(col("MDTime").lt(lit(date_int + 150000000)))
            .collect()
            .unwrap();
        if transform {
            let df_trans = self.transform_trans_data(df_mdc);
            return df_trans;
        } else {
            df_mdc = df_mdc
                .lazy()
                .filter(col("MDDate").is_in(lit(&*self._date)))
                .collect()
                .unwrap();
            return df_mdc;
        }
    }

    fn gen_bs_for_order(&self, order_bsflag: &Series) -> Series {
        let mut res: Vec<&str> = vec![];
        let s_len = order_bsflag.len();
        let mut i = 0;

        loop {
            let bsflag_i = order_bsflag.i32().unwrap().get(i).unwrap();
            if bsflag_i == 1 {
                res.push("B");
            } else {
                res.push("S");
            }
            i += 1;
            if i == s_len {
                break;
            }
        }
        let bsflag = Series::new("FunctionCode", &res);
        return bsflag;
    }

    fn gen_kind_for_order(&self, ordr_type: &Series) -> Series {
        let mut res: Vec<&str> = vec![];
        let s_len = ordr_type.len();
        let mut i = 0;

        loop {
            let ordr_type_i = ordr_type.i32().unwrap().get(i).unwrap();
            if ordr_type_i == 2 {
                res.push("0");
            } else if ordr_type_i == 1 {
                res.push("1");
            } else if ordr_type_i == 3 {
                res.push("U");
            } else if ordr_type_i == 10 {
                res.push("C");
            }
            i += 1;
            if i == s_len {
                break;
            }
        }
        let kind = Series::new("OrderKind", &res);
        return kind;
    }

    fn transform_order_data(&self, df_mdc: DataFrame) -> DataFrame {
        let df_mdc = df_mdc
            .select([
                "MDDate",
                "MDTime",
                "OrderBSFlag",
                "OrderType",
                "OrderPrice",
                "OrderQty",
                "OrderNO",
            ])
            .unwrap();
        let mut df_mdc = df_mdc
            .lazy()
            .filter(col("MDDate").is_in(lit(&*self._date)))
            .collect()
            .unwrap();
        let order_bsflag = df_mdc.column("OrderBSFlag").unwrap();
        let order_type = df_mdc.column("OrderType").unwrap();
        let bsflags = self.gen_bs_for_order(order_bsflag);
        let order_kind = self.gen_kind_for_order(order_type);

        let df_mdc = df_mdc.with_column(bsflags).unwrap();
        let df_mdc = df_mdc.with_column(order_kind).unwrap();
        let mddate = df_mdc
            .column("MDDate")
            .unwrap()
            .cast(&polars::prelude::DataType::Int64)
            .unwrap();
        let df_mdc = df_mdc.with_column(mddate).unwrap();

        // 暂未找到批量修改列名的方法
        let df_mdc = df_mdc.rename("MDDate", "Date").unwrap();
        let df_mdc = df_mdc.rename("MDTime", "Timestamp").unwrap();
        let df_mdc = df_mdc.rename("OrderPrice", "Price").unwrap();
        let df_mdc = df_mdc.rename("OrderQty", "Volume").unwrap();
        let df_mdc = df_mdc.rename("OrderNO", "OrderNumber").unwrap();

        let df_order = df_mdc
            .select([
                "Date",
                "Timestamp",
                "FunctionCode",
                "Price",
                "Volume",
                "OrderNumber",
                "OrderKind",
            ])
            .unwrap();
        return df_order;
    }

    fn _load_order_data(&self, symbol: &str, transform: bool) -> DataFrame {
        let exchange_code = &symbol[symbol.len() - 2..];
        let mut df_mdc = self.load_marketdata(symbol, "Order");
        let column_vec = df_mdc.get_column_names_owned();
        for colume in column_vec {
            if colume == "SecurityStatus" {
                df_mdc = df_mdc
                    .lazy()
                    .filter(col("SecurityStatus").is_null())
                    .collect()
                    .unwrap();
            }
        }
        df_mdc = df_mdc
            .lazy()
            .filter(col("MDDate").eq(lit(self._date.to_string())))
            .collect()
            .unwrap();

        let date_int = self._date.to_string().parse::<i64>().unwrap() * 1000000000;
        let price_unit = *self._price_unit.borrow();
        df_mdc = df_mdc
            .lazy()
            .with_columns([col("MDTime").cast(DataType::Int64) + lit(date_int)])
            .filter(col("MDTime").lt(lit(date_int + 150000000)))
            .collect()
            .unwrap();

        if exchange_code == "SZ" {
            //深交所的OrderIndex既是表示时间顺序的技术编号，又是订单编号（用于建立成交、撤单对应关系）。
            //而上交的OrderIndex只表示时间顺序的技术编号，还有额外的OrderNO字段表示订单编号。上交所的OrderIndex和ApplSeqNum不同，AqqlSeqNum是逐笔委托和成交一起编号。
            df_mdc = df_mdc
                .lazy()
                .with_column(col("OrderIndex").alias("OrderNO"))
                .collect()
                .unwrap();
        }
        if transform {
            let df_order = self.transform_order_data(df_mdc);
            return df_order;
        } else {
            df_mdc = df_mdc
                .lazy()
                .filter(col("MDDate").is_in(lit(&*self._date)))
                .collect()
                .unwrap();
            return df_mdc;
        }
    }

    fn process_trans_data(&self, _df_trans: &DataFrame) -> DataFrame {
        // 处理逐笔成交数据
        let _c1 = "C";
        let df_trans_1 = _df_trans
            .clone()
            .lazy()
            .filter(not(col("FunctionCode").is_in(lit(_c1))))
            .collect()
            .unwrap();
        // 小于等于：lt_eq 大于等于：gt_eq
        let df_trans_ = df_trans_1
            .clone()
            .lazy()
            .filter(col("Timestamp").gt_eq(93000000))
            .collect()
            .unwrap();
        let mut ask_order = df_trans_
            .select(["AskOrder", "Date", "Price", "Timestamp", "Volume"])
            .unwrap();
        let func_code1 = Series::new("FunctionCode", &[String::from("S")]);
        let ask_order = ask_order.with_column(func_code1).unwrap();
        let ask_order = ask_order.rename("AskOrder", "OrderNumber").unwrap();

        let trans_volume_1 = ask_order
            .clone()
            .lazy()
            .group_by([col("OrderNumber"), col("FunctionCode")])
            .agg([col("Volume").sum()])
            .collect()
            .unwrap();
        let trans_price_1 = ask_order
            .clone()
            .lazy()
            .group_by([col("OrderNumber"), col("FunctionCode")])
            .agg([col("Price").min()])
            .collect()
            .unwrap();
        let trans_timestamp_1 = ask_order
            .clone()
            .lazy()
            .group_by([col("OrderNumber"), col("FunctionCode")])
            .agg([col("Timestamp").min()])
            .collect()
            .unwrap();
        let ask_order2_1 = trans_volume_1
            .join(
                &trans_price_1,
                ["OrderNumber", "FunctionCode"],
                ["OrderNumber", "FunctionCode"],
                JoinArgs::new(JoinType::Full),
            )
            .unwrap();
        let ask_order2 = ask_order2_1
            .join(
                &trans_timestamp_1,
                ["OrderNumber", "FunctionCode"],
                ["OrderNumber", "FunctionCode"],
                JoinArgs::new(JoinType::Full),
            )
            .unwrap();

        let mut bid_order_1 = df_trans_
            .select(["BidOrder", "Date", "Price", "Timestamp", "Volume"])
            .unwrap();
        let func_code2 = Series::new("FunctionCode", &[String::from("B")]);
        let bid_order = bid_order_1.with_column(func_code2).unwrap();
        let bid_order = bid_order.rename("BidOrder", "OrderNumber").unwrap();

        let trans_volume_2 = bid_order
            .clone()
            .lazy()
            .group_by([col("OrderNumber"), col("FunctionCode")])
            .agg([col("Volume").sum()])
            .collect()
            .unwrap();
        let trans_price_2 = bid_order
            .clone()
            .lazy()
            .group_by([col("OrderNumber"), col("FunctionCode")])
            .agg([col("Price").max()])
            .collect()
            .unwrap();
        let trans_timestamp_2 = bid_order
            .clone()
            .lazy()
            .group_by([col("OrderNumber"), col("FunctionCode")])
            .agg([col("Timestamp").min()])
            .collect()
            .unwrap();
        let bid_order2_1 = trans_volume_2
            .join(
                &trans_price_2,
                ["OrderNumber", "FunctionCode"],
                ["OrderNumber", "FunctionCode"],
                JoinArgs::new(JoinType::Full),
            )
            .unwrap();
        let bid_order2 = bid_order2_1
            .join(
                &trans_timestamp_2,
                ["OrderNumber", "FunctionCode"],
                ["OrderNumber", "FunctionCode"],
                JoinArgs::new(JoinType::Full),
            )
            .unwrap();

        let mut df_order_ = concat([ask_order2.lazy(), bid_order2.lazy()], UnionArgs{..Default::default()}, )
            .unwrap()
            .collect()
            .unwrap();
        let ok_ = Series::new("OrderKind", &[String::from("0")]);
        let df_order_ = df_order_.with_column(ok_).unwrap();
        let date = self._date.parse::<i64>().unwrap();
        let date_ = Series::new("Date", &[date]);
        let df_order_ = df_order_.with_column(date_).unwrap();
        let df_order_ = df_order_
            .sort(["Timestamp", "OrderNumber"], SortMultipleOptions::new())
            .unwrap();
        // println!("{:?}", df_order_);
        return df_order_;
    }

    pub fn load_order_data(&self, symbol: &str, transform: bool) -> DataFrame {
        if self._broker_mod == "ORDER".to_string()
            || self._broker_mod == "L2P".to_string().to_uppercase()
        {
            let df_order = self._load_order_data(symbol, transform);
            return df_order;
        } else {
            let df_trans_ = self.load_transaction_data(symbol, true);
            let df_order = self.process_trans_data(&df_trans_);
            return df_order;
        }
    }
}

#[test]
fn test_load_transaction_data_1() {
    let data_api = DataApi::new(
        "20230726".to_string(),
        "local".to_string(),
        "ORDER".to_string(),
        "/root/mdc_data".to_string(),
    );
    let df_trans = data_api.load_transaction_data("000001.SZ", true);
    println!("{:?}", df_trans);
}

#[test]
fn test_load_transaction_data_2() {
    let data_api = DataApi::new(
        "20230206".to_string(),
        "local".to_string(),
        "ORDER".to_string(),
        "/root/mdc_data".to_string(),
    );
    let df_trans = data_api.load_transaction_data("600000.SH", true);
    println!("{:?}", df_trans);
}

#[test]
fn test_load_order_data_1() {
    let data_api = DataApi::new(
        "20230726".to_string(),
        "local".to_string(),
        "ORDER".to_string(),
        "/root/mdc_data".to_string(),
    );
    let df_order = data_api.load_order_data("000001.SZ", true);
    println!("{:?}", df_order);
}

#[test]
fn test_load_order_data_2() {
    let data_api = DataApi::new(
        "20230206".to_string(),
        "local".to_string(),
        "ORDER".to_string(),
        "/root/mdc_data".to_string(),
    );
    let df_order = data_api.load_order_data("600000.SH", true);
    println!("{:?}", df_order);
}

#[test]
fn test_load_order_data_3() {
    let data_api = DataApi::new(
        "20230726".to_string(),
        "local".to_string(),
        "TRANS".to_string(),
        "/root/mdc_data".to_string(),
    );
    let df_order = data_api.load_order_data("000001.SZ", true);
    println!("{:?}", df_order);
}

#[test]
fn test_load_order_data_4() {
    let data_api = DataApi::new(
        "20230206".to_string(),
        "local".to_string(),
        "TRANS".to_string(),
        "/root/mdc_data".to_string(),
    );
    let df_order = data_api.load_order_data("600000.SH", true);
    println!("{:?}", df_order);
}
