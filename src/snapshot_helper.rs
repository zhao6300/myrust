use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::orderbook::types::{OrderType, Side};

use super::orderbook::hook::Hook;
use super::orderbook::statistics::StatisticsInfo;
use super::orderbook::L3OrderRef;
use polars::export::num::ToPrimitive;
use polars::prelude::*;
use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::time;
use std::{any, fmt};
use std::{cell::RefCell, rc::Rc};

pub trait BigArray<'de>: Sized {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer;
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>;
}

macro_rules! big_array {
    ($($len:expr,)+) => {
        $(
            impl<'de, T> BigArray<'de> for [T; $len]
                where T: Default + Copy + Serialize + Deserialize<'de>
            {
                fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                    where S: Serializer
                {
                    let mut seq = serializer.serialize_tuple(self.len())?;
                    for elem in &self[..] {
                        seq.serialize_element(elem)?;
                    }
                    seq.end()
                }

                fn deserialize<D>(deserializer: D) -> Result<[T; $len], D::Error>
                    where D: Deserializer<'de>
                {
                    struct ArrayVisitor<T> {
                        element: PhantomData<T>,
                    }

                    impl<'de, T> Visitor<'de> for ArrayVisitor<T>
                        where T: Default + Copy + Deserialize<'de>
                    {
                        type Value = [T; $len];

                        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                            formatter.write_str(concat!("an array of length ", $len))
                        }

                        fn visit_seq<A>(self, mut seq: A) -> Result<[T; $len], A::Error>
                            where A: SeqAccess<'de>
                        {
                            let mut arr = [T::default(); $len];
                            for i in 0..$len {
                                arr[i] = seq.next_element()?
                                    .ok_or_else(|| Error::invalid_length(i, &self))?;
                            }
                            Ok(arr)
                        }
                    }

                    let visitor = ArrayVisitor { element: PhantomData };
                    deserializer.deserialize_tuple($len, visitor)
                }
            }
        )+
    }
}

big_array! {
    40, 48, 50, 56, 64, 72, 96, 100, 128, 160, 192, 200, 224, 256, 384, 512,
    768, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
}

const LEVELNUM: usize = 50;
type F64ArrLvl = [f64; LEVELNUM];
type I32ArrLvl = [i32; LEVELNUM];

#[derive(Serialize)]
pub struct OrderBookSnapshot {
    symbol: String,
    date: String,
    recvtime: i64,
    mdtime: i64,
    finished_time: i64,
    last_seq_num: i64,
    last_price: f64,
    high_price: f64,
    low_price: f64,
    total_turnover: f64,
    total_volume: i32,
    prev_close_price: f64,

    #[serde(with = "BigArray")]
    asks_p: F64ArrLvl,
    #[serde(with = "BigArray")]
    bids_p: F64ArrLvl,
    #[serde(with = "BigArray")]
    asks_vol: I32ArrLvl,
    #[serde(with = "BigArray")]
    bids_vol: I32ArrLvl,
    #[serde(with = "BigArray")]
    asks_num: I32ArrLvl,
    #[serde(with = "BigArray")]
    bids_num: I32ArrLvl,
    // volume: i32,
    // turnover: f64,
    // trade_num: i32,
    total_trade_num: i32,
    avg_ask_price: f64,
    avg_bid_price: f64,
    // ask_num: i32,
    // bid_num: i32,
    // ask_qty: i32,
    // bid_qty: i32,
    // ask_price_num: i32,
    // bid_price_num: i32,
    // order_or_trade: i32,
    msg_buy_no: i64,
    msg_sell_no: i64,
    msg_trade_type: i32,
    msg_order_type: i32,
    msg_bsflag: i32,
    msg_price: f64,
    msg_qty: i32,
    msg_amt: f64,

    #[serde(skip_serializing)]
    vec_recvtime: Vec<i64>,
    #[serde(skip_serializing)]
    vec_mdtime: Vec<i64>,
    #[serde(skip_serializing)]
    vec_finished_time: Vec<i64>,
    #[serde(skip_serializing)]
    vec_last_seq_num: Vec<i64>,
    #[serde(skip_serializing)]
    vec_last_price: Vec<f64>,
    #[serde(skip_serializing)]
    vec_high_price: Vec<f64>,
    #[serde(skip_serializing)]
    vec_low_price: Vec<f64>,
    #[serde(skip_serializing)]
    vec_total_turnover: Vec<f64>,
    #[serde(skip_serializing)]
    vec_total_volume: Vec<i32>,
    #[serde(skip_serializing)]
    vec_prev_close_price: Vec<f64>,
    #[serde(skip_serializing, with = "BigArray")]
    vec_asks_p: Vec<F64ArrLvl>,
    #[serde(skip_serializing, with = "BigArray")]
    vec_bids_p: Vec<F64ArrLvl>,
    #[serde(skip_serializing, with = "BigArray")]
    vec_asks_vol: Vec<I32ArrLvl>,
    #[serde(skip_serializing, with = "BigArray")]
    vec_bids_vol: Vec<I32ArrLvl>,
    #[serde(skip_serializing, with = "BigArray")]
    vec_asks_num: Vec<I32ArrLvl>,
    #[serde(skip_serializing, with = "BigArray")]
    vec_bids_num: Vec<I32ArrLvl>,
    // #[serde(skip_serializing)]
    // vec_volume: Vec<i32>,
    // #[serde(skip_serializing)]
    // vec_turnover: Vec<f64>,
    // #[serde(skip_serializing)]
    // vec_trade_num: Vec<i32>,
    #[serde(skip_serializing)]
    vec_total_trade_num: Vec<i32>,
    #[serde(skip_serializing)]
    vec_avg_ask_price: Vec<f64>,
    #[serde(skip_serializing)]
    vec_avg_bid_price: Vec<f64>,
    // #[serde(skip_serializing)]
    // vec_ask_num: Vec<i32>,
    // #[serde(skip_serializing)]
    // vec_bid_num: Vec<i32>,
    // #[serde(skip_serializing)]
    // vec_ask_qty: Vec<i32>,
    // #[serde(skip_serializing)]
    // vec_bid_qty: Vec<i32>,
    // #[serde(skip_serializing)]
    // vec_ask_price_num: Vec<i32>,
    // #[serde(skip_serializing)]
    // vec_bid_price_num: Vec<i32>,
    // #[serde(skip_serializing)]
    // vec_order_or_trade: Vec<i32>,
    #[serde(skip_serializing)]
    vec_msg_buy_no: Vec<i64>,
    #[serde(skip_serializing)]
    vec_msg_sell_no: Vec<i64>,
    #[serde(skip_serializing)]
    vec_msg_trade_type: Vec<i32>,
    #[serde(skip_serializing)]
    vec_msg_order_type: Vec<i32>,
    #[serde(skip_serializing)]
    vec_msg_bsflag: Vec<i32>,
    #[serde(skip_serializing)]
    vec_msg_price: Vec<f64>,
    #[serde(skip_serializing)]
    vec_msg_qty: Vec<i32>,
    #[serde(skip_serializing)]
    vec_msg_amt: Vec<f64>,
    #[serde(skip_serializing)]
    need_output: bool,
}

impl OrderBookSnapshot {
    pub fn new(symbol: String, date: String, size: usize) -> Self {
        Self {
            symbol,
            date,
            recvtime: 0,
            mdtime: 0,
            finished_time: 0,
            last_seq_num: 0,
            last_price: 0.0,
            high_price: 0.0,
            low_price: 0.0,
            total_turnover: 0.0,
            total_volume: 0,
            prev_close_price: 0.0,
            asks_p: [0.0; LEVELNUM],
            bids_p: [0.0; LEVELNUM],
            asks_vol: [0; LEVELNUM],
            bids_vol: [0; LEVELNUM],
            asks_num: [0; LEVELNUM],
            bids_num: [0; LEVELNUM],
            // volume: 0,
            // turnover: 0.0,
            // trade_num: 0,
            total_trade_num: 0,
            avg_ask_price: 0.0,
            avg_bid_price: 0.0,
            // ask_num: 0,
            // bid_num: 0,
            // ask_qty: 0,
            // bid_qty: 0,
            // ask_price_num: 0,
            // bid_price_num: 0,
            // order_or_trade: 0,
            msg_buy_no: 0,
            msg_sell_no: 0,
            msg_trade_type: 0,
            msg_order_type: 0,
            msg_bsflag: 0,
            msg_price: 0.0,
            msg_qty: 0,
            msg_amt: 0.0,
            vec_recvtime: Vec::<i64>::with_capacity(size),
            vec_mdtime: Vec::<i64>::with_capacity(size),
            vec_finished_time: Vec::<i64>::with_capacity(size),
            vec_last_seq_num: Vec::<i64>::with_capacity(size),
            vec_last_price: Vec::<f64>::with_capacity(size),
            vec_high_price: Vec::<f64>::with_capacity(size),
            vec_low_price: Vec::<f64>::with_capacity(size),
            vec_total_turnover: Vec::<f64>::with_capacity(size),
            vec_total_volume: Vec::<i32>::with_capacity(size),
            vec_prev_close_price: Vec::<f64>::with_capacity(size),
            vec_asks_p: Vec::<F64ArrLvl>::with_capacity(size),
            vec_bids_p: Vec::<F64ArrLvl>::with_capacity(size),
            vec_asks_vol: Vec::<I32ArrLvl>::with_capacity(size),
            vec_bids_vol: Vec::<I32ArrLvl>::with_capacity(size),
            vec_asks_num: Vec::<I32ArrLvl>::with_capacity(size),
            vec_bids_num: Vec::<I32ArrLvl>::with_capacity(size),
            // vec_volume: Vec::<i32>::with_capacity(size),
            // vec_turnover: Vec::<f64>::with_capacity(size),
            // vec_trade_num: Vec::<i32>::with_capacity(size),
            vec_total_trade_num: Vec::<i32>::with_capacity(size),
            vec_avg_ask_price: Vec::<f64>::with_capacity(size),
            vec_avg_bid_price: Vec::<f64>::with_capacity(size),
            // vec_ask_num: Vec::<i32>::with_capacity(size),
            // vec_bid_num: Vec::<i32>::with_capacity(size),
            // vec_ask_qty: Vec::<i32>::with_capacity(size),
            // vec_bid_qty: Vec::<i32>::with_capacity(size),
            // vec_ask_price_num: Vec::<i32>::with_capacity(size),
            // vec_bid_price_num: Vec::<i32>::with_capacity(size),
            // vec_order_or_trade: Vec::<i32>::with_capacity(size),
            vec_msg_buy_no: Vec::<i64>::with_capacity(size),
            vec_msg_sell_no: Vec::<i64>::with_capacity(size),
            vec_msg_trade_type: Vec::<i32>::with_capacity(size),
            vec_msg_order_type: Vec::<i32>::with_capacity(size),
            vec_msg_bsflag: Vec::<i32>::with_capacity(size),
            vec_msg_price: Vec::<f64>::with_capacity(size),
            vec_msg_qty: Vec::<i32>::with_capacity(size),
            vec_msg_amt: Vec::<f64>::with_capacity(size),
            need_output: false,
        }
    }

    pub fn snapshot_once(
        &mut self,
        recvtime: i64,
        mdtime: i64,
        finished_time: i64,
        last_seq_num: i64,
        last_price: f64,
        high_price: f64,
        low_price: f64,
        total_turnover: f64,
        total_volume: i32,
        prev_close_price: f64,
        asks_p: F64ArrLvl,
        bids_p: F64ArrLvl,
        asks_vol: I32ArrLvl,
        bids_vol: I32ArrLvl,
        asks_num: I32ArrLvl,
        bids_num: I32ArrLvl,
        // volume: i32,
        // turnover: f64,
        // trade_num: i32,
        total_trade_num: i32,
        avg_ask_price: f64,
        avg_bid_price: f64,
        msg_buy_no: i64,
        msg_sell_no: i64,
        msg_trade_type: i32,
        msg_order_type: i32,
        msg_bsflag: i32,
        msg_price: f64,
        msg_qty: i32,
        msg_amt: f64,
        modified: bool,
        need_output: bool,
    ) {
        self.recvtime = recvtime;
        self.mdtime = mdtime;
        self.finished_time = finished_time;
        self.last_seq_num = last_seq_num;
        self.last_price = last_price;
        self.high_price = high_price;
        self.low_price = low_price;
        self.total_turnover = total_turnover;
        self.total_volume = total_volume;
        self.prev_close_price = prev_close_price;
        self.asks_p = asks_p.clone();
        self.bids_p = bids_p.clone();
        self.asks_vol = asks_vol.clone();
        self.bids_vol = bids_vol.clone();
        self.asks_num = asks_num.clone();
        self.bids_num = bids_num.clone();
        // self.volume = volume;
        // self.turnover = turnover;
        // self.trade_num = trade_num;
        self.total_trade_num = total_trade_num;
        self.avg_ask_price = avg_ask_price;
        self.avg_bid_price = avg_bid_price;
        // self.ask_num = ask_num;
        // self.bid_num = bid_num;
        // self.ask_qty = ask_qty;
        // self.bid_qty = bid_qty;
        // self.ask_price_num = ask_price_num;
        // self.bid_price_num = bid_price_num;
        // self.order_or_trade = order_or_trade;
        self.msg_buy_no = msg_buy_no;
        self.msg_sell_no = msg_sell_no;
        self.msg_trade_type = msg_trade_type;
        self.msg_order_type = msg_order_type;
        self.msg_bsflag = msg_bsflag;
        self.msg_price = msg_price;
        self.msg_qty = msg_qty;
        self.msg_amt = msg_amt;
        if need_output {
            // 将传入的参数添加到对应的 Vec 变量中
            self.vec_recvtime.push(recvtime);
            self.vec_mdtime.push(mdtime);
            self.vec_finished_time.push(finished_time);
            self.vec_last_seq_num.push(last_seq_num);
            self.vec_last_price.push(last_price);
            self.vec_high_price.push(high_price);
            self.vec_low_price.push(low_price);
            self.vec_total_turnover.push(total_turnover);
            self.vec_total_volume.push(total_volume);
            self.vec_prev_close_price.push(prev_close_price);
            self.vec_asks_p.push(asks_p);
            self.vec_bids_p.push(bids_p);
            self.vec_asks_vol.push(asks_vol);
            self.vec_bids_vol.push(bids_vol);
            self.vec_asks_num.push(asks_num);
            self.vec_bids_num.push(bids_num);
            // self.vec_volume.push(volume);
            // self.vec_turnover.push(turnover);
            // self.vec_trade_num.push(trade_num);
            self.vec_total_trade_num.push(total_trade_num);
            self.vec_avg_ask_price.push(avg_ask_price);
            self.vec_avg_bid_price.push(avg_bid_price);
            // self.vec_ask_num.push(ask_num);
            // self.vec_bid_num.push(bid_num);
            // self.vec_ask_qty.push(ask_qty);
            // self.vec_bid_qty.push(bid_qty);
            // self.vec_ask_price_num.push(ask_price_num);
            // self.vec_bid_price_num.push(bid_price_num);
            // self.vec_order_or_trade.push(order_or_trade);
            self.vec_msg_buy_no.push(msg_buy_no);
            self.vec_msg_sell_no.push(msg_sell_no);
            self.vec_msg_trade_type.push(msg_trade_type);
            self.vec_msg_order_type.push(msg_order_type);
            self.vec_msg_bsflag.push(msg_bsflag);
            self.vec_msg_price.push(msg_price);
            self.vec_msg_qty.push(msg_qty);
            self.vec_msg_amt.push(msg_amt);
        }
    }

    pub fn presist(&self) -> bool {
        let sy_time_init: time::SystemTime = time::SystemTime::now();
        let sr_mdtime: Series = Series::new("mdtime", &self.vec_mdtime);
        let sr_recvtime: Series = Series::new("recvtime", &self.vec_recvtime);
        let sr_finished_time = Series::new("finished_time", &self.vec_finished_time);
        let sr_last_seq_num = Series::new("last_seq_num", &self.vec_last_seq_num);
        let sr_last_price = Series::new("last_price", &self.vec_last_price);
        let sr_high_price = Series::new("high_price", &self.vec_high_price);
        let sr_low_price = Series::new("low_price", &self.vec_low_price);
        let sr_total_turnover = Series::new("ttl_turn_over", &self.vec_total_turnover);
        let sr_total_volume = Series::new("ttl_volume", &self.vec_total_volume);
        let sr_prev_close_price = Series::new("prev_close_price", &self.vec_prev_close_price);

        let capacity = self.vec_bids_vol.capacity();
        let value_capacity = self.vec_bids_vol.capacity() * 5;
        let mut chunked_array_asks_p: ListPrimitiveChunkedBuilder<Float64Type> =
            ListPrimitiveChunkedBuilder::new(
                "chunked_array_asks_p",
                capacity,
                value_capacity,
                DataType::Float64,
            );
        for x in self.vec_asks_p.iter() {
            chunked_array_asks_p.append_slice(x);
        }
        // self.vec_asks_p.par_iter()
        //     .for_each(|chunk| chunked_array_asks_p.append_slice(chunk));
        // self.vec_bids_p.par_iter()
        //     .map(|&value| value * 2.0) // Example processing: double each value
        //     .collect_into_vec(&mut chunked_array_bids_p);
        let mut chunked_array_bids_p: ListPrimitiveChunkedBuilder<Float64Type> =
            ListPrimitiveChunkedBuilder::new(
                "chunked_array_bids_p",
                capacity,
                value_capacity,
                DataType::Float64,
            );
        for x in self.vec_bids_p.iter() {
            chunked_array_bids_p.append_slice(x);
        }

        let mut chunked_array_asks_vol: ListPrimitiveChunkedBuilder<Int32Type> =
            ListPrimitiveChunkedBuilder::new(
                "chunked_array_asks_vol",
                capacity,
                value_capacity,
                DataType::Int32,
            );
        for x in self.vec_asks_vol.iter() {
            chunked_array_asks_vol.append_slice(x);
        }

        let mut chunked_array_bids_vol: ListPrimitiveChunkedBuilder<Int32Type> =
            ListPrimitiveChunkedBuilder::new(
                "chunked_array_bids_vol",
                capacity,
                value_capacity,
                DataType::Int32,
            );
        for x in self.vec_bids_vol.iter() {
            chunked_array_bids_vol.append_slice(x);
        }

        let mut chunked_array_asks_num: ListPrimitiveChunkedBuilder<Int32Type> =
            ListPrimitiveChunkedBuilder::new(
                "chunked_array_asks_num",
                capacity,
                value_capacity,
                DataType::Int32,
            );
        for x in self.vec_asks_num.iter() {
            chunked_array_asks_num.append_slice(x);
        }

        let mut chunked_array_bids_num: ListPrimitiveChunkedBuilder<Int32Type> =
            ListPrimitiveChunkedBuilder::new(
                "chunked_array_bids_num",
                capacity,
                value_capacity,
                DataType::Int32,
            );
        for x in self.vec_bids_num.iter() {
            chunked_array_bids_num.append_slice(x);
        }

        let sr_asks_p = Series::new("asks_price", chunked_array_asks_p.finish());
        let sr_bids_p = Series::new("bids_price", chunked_array_bids_p.finish());
        let sr_asks_vol = Series::new("asks_qty", chunked_array_asks_vol.finish());
        let sr_bids_vol = Series::new("bids_qty", chunked_array_bids_vol.finish());
        let sr_asks_num: Series = Series::new("asks_count", chunked_array_asks_num.finish());
        let sr_bids_num: Series = Series::new("bids_count", chunked_array_bids_num.finish());

        // //将数组拼接成一整个字符串
        // let str_asks_p: Vec<String> = self
        //     .vec_asks_p
        //     .iter()
        //     .map(|x| {
        //         x.iter()
        //             .map(|sub_x| sub_x.to_string())
        //             .collect::<Vec<String>>()
        //             .join("|")
        //     })
        //     .collect();
        // let str_bids_p: Vec<String> = self
        //     .vec_bids_p
        //     .iter()
        //     .map(|x| {
        //         x.iter()
        //             .map(|sub_x| sub_x.to_string())
        //             .collect::<Vec<String>>()
        //             .join("|")
        //     })
        //     .collect();
        // let str_asks_vol: Vec<String> = self
        //     .vec_asks_vol
        //     .iter()
        //     .map(|x| {
        //         x.iter()
        //             .map(|sub_x| sub_x.to_string())
        //             .collect::<Vec<String>>()
        //             .join("|")
        //     })
        //     .collect();
        // let str_bids_vol: Vec<String> = self
        //     .vec_bids_vol
        //     .iter()
        //     .map(|x| {
        //         x.iter()
        //             .map(|sub_x| sub_x.to_string())
        //             .collect::<Vec<String>>()
        //             .join("|")
        //     })
        //     .collect();
        // let str_asks_num: Vec<String> = self
        //     .vec_asks_num
        //     .iter()
        //     .map(|x| {
        //         x.iter()
        //             .map(|sub_x| sub_x.to_string())
        //             .collect::<Vec<String>>()
        //             .join("|")
        //     })
        //     .collect();
        // let str_bids_num: Vec<String> = self
        //     .vec_bids_num
        //     .iter()
        //     .map(|x| {
        //         x.iter()
        //             .map(|sub_x| sub_x.to_string())
        //             .collect::<Vec<String>>()
        //             .join("|")
        //     })
        //     .collect();

        // let sr_asks_p = Series::new("asks_price", str_asks_p);
        // let sr_bids_p = Series::new("bids_price", str_bids_p);
        // let sr_asks_vol = Series::new("asks_qty", str_asks_vol);
        // let sr_bids_vol = Series::new("bids_qty", str_bids_vol);
        // let sr_asks_num = Series::new("asks_count", str_asks_num);
        // let sr_bids_num = Series::new("bids_count", str_bids_num);

        // let sr_volume = Series::new("volume", &self.vec_volume);
        // let sr_turnover = Series::new("turn_over", &self.vec_turnover);
        // let sr_trade_num = Series::new("trade_num", &self.vec_trade_num);
        let sr_total_trade_num = Series::new("ttl_trade_num", &self.vec_total_trade_num);
        let sr_avg_ask_price = Series::new("avg_ask_price", &self.vec_avg_ask_price);
        let sr_avg_bid_price = Series::new("avg_bid_price", &self.vec_avg_bid_price);
        // let sr_ask_num = Series::new("ask_num", &self.vec_ask_num);
        // let sr_bid_num = Series::new("bid_num", &self.vec_bid_num);
        // let sr_ask_qty = Series::new("ask_qty", &self.vec_ask_qty);
        // let sr_bid_qty = Series::new("bid_qty", &self.vec_bid_qty);
        // let sr_ask_price_num = Series::new("ask_price_num", &self.vec_ask_price_num);
        // let sr_bid_price_num = Series::new("bid_price_num", &self.vec_bid_price_num);
        // let sr_order_or_trade = Series::new("order_or_trade", &self.vec_order_or_trade);
        let sr_msg_buy_no = Series::new("msg_buy_no", &self.vec_msg_buy_no);
        let sr_msg_sell_no = Series::new("msg_sell_no", &self.vec_msg_sell_no);
        let sr_msg_trade_type = Series::new("msg_trade_type", &self.vec_msg_trade_type);
        let sr_msg_order_type = Series::new("msg_order_type", &self.vec_msg_order_type);
        let sr_msg_bsflag = Series::new("msg_bsflag", &self.vec_msg_bsflag);
        let sr_msg_price = Series::new("msg_price", &self.vec_msg_price);
        let sr_msg_qty = Series::new("msg_qty", &self.vec_msg_qty);
        let sr_msg_amt = Series::new("msg_amt", &self.vec_msg_amt);

        // let sr_modified = Series::new("modified", &self.vec_modified);
        let mut df = DataFrame::new(vec![
            sr_mdtime,
            sr_last_price,
            sr_asks_p,
            sr_bids_p,
            sr_asks_vol,
            sr_bids_vol,
            sr_asks_num,
            sr_bids_num,
            sr_high_price,
            sr_low_price,
            sr_prev_close_price,
            sr_total_volume,
            sr_total_turnover,
            sr_total_trade_num,
            // sr_volume,
            // sr_turnover,
            // sr_trade_num,
            sr_avg_ask_price,
            sr_avg_bid_price,
            // sr_ask_num,
            // sr_bid_num,
            // sr_ask_qty,
            // sr_bid_qty,
            // sr_ask_price_num,
            // sr_bid_price_num,
            sr_recvtime,
            sr_finished_time,
            sr_msg_trade_type,
            sr_msg_order_type,
            sr_msg_bsflag,
            sr_msg_price,
            sr_msg_qty,
            sr_msg_amt,
            sr_msg_buy_no,
            sr_msg_sell_no,
            sr_last_seq_num,
            // sr_order_or_trade,
            // sr_modified,
        ])
        .unwrap();
        df = df
            .lazy()
            .with_columns([
                lit(self.symbol.to_string()).alias("code_str"),
                col("msg_price").cast(DataType::Float64),
            ])
            .collect()
            .unwrap();
        let mut file =
            std::fs::File::create(format!("{}_{}.parquet", self.symbol, self.date)).unwrap();
        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Snappy)
            .finish(&mut df)
            .unwrap();
        println!(
            "presist l2p: {} save parquet spend: {:?} us",
            self.symbol,
            time::SystemTime::now()
                .duration_since(sy_time_init)
                .unwrap()
                .as_micros()
        );
        true
    }
}

pub type OrderBookSnapshotRef = Rc<RefCell<OrderBookSnapshot>>;

pub fn get_hook(ob_snapshot: OrderBookSnapshotRef) -> Hook {
    Hook {
        object: ob_snapshot,
        handler: handler,
        max_level: 50,
    }
}

pub fn handler(
    snapshot_ref: &Rc<RefCell<dyn Any>>,
    info: &StatisticsInfo,          // aggregated info
    bid_vec: &Vec<(f64, f64, i64)>, // bid orderbook
    ask_vec: &Vec<(f64, f64, i64)>, // ask orderbook
    order_info: &L3OrderRef,        // current order info
) -> bool {
    if let Some(snapshot) = snapshot_ref
        .borrow_mut()
        .downcast_mut::<OrderBookSnapshot>()
    {
        let order = order_info.borrow();
        let timestamp = order.timestamp;
        let last_seq_num = order.seq;
        let last_price = info.last_price;
        let high_price = info.high;
        let low_price = info.low;
        let total_turnover = ((info.total_bid + info.total_ask)*1000.0).round()/1000.0;
        let total_volume = (info.total_bid_qty + info.total_ask_qty).round() as i32;
        let prev_close_price = info.prev_close_price;
        let mut sub_asks_p: F64ArrLvl = [0.0; LEVELNUM];
        let mut sub_asks_vol: I32ArrLvl = [0; LEVELNUM];
        let mut sub_asks_num: I32ArrLvl = [0; LEVELNUM];
        let mut sub_bids_p: F64ArrLvl = [0.0; LEVELNUM];
        let mut sub_bids_vol: I32ArrLvl = [0; LEVELNUM];
        let mut sub_bids_num: I32ArrLvl = [0; LEVELNUM];

        sub_bids_p
            .iter_mut()
            .zip(sub_bids_vol.iter_mut())
            .zip(sub_bids_num.iter_mut())
            .zip(bid_vec.iter())
            .for_each(|(((p, vol), num), &(price, qty, count))| {
                *p = (price * 1000.0).round() / 1000.0;
                *vol = qty.round() as i32;
                *num = count as i32;
            });

        sub_asks_p
            .iter_mut()
            .zip(sub_asks_vol.iter_mut())
            .zip(sub_asks_num.iter_mut())
            .zip(ask_vec.iter())
            .for_each(|(((p, vol), num), &(price, qty, count))| {
                *p = (price * 1000.0).round() / 1000.0;
                *vol = qty.round() as i32;
                *num = count as i32;
            });

        let msg_buy_no = order.order_id;
        let msg_sell_no = order.order_id;
        let msg_trade_type = order.side.to_i32();
        let msg_order_type = order.order_type.to_i32();
        let msg_bsflag = order.side.to_i32();
        let msg_price = order.price_tick as f64 * info.tick_size;
        let msg_qty = (order.vol as f64 * info.lot_size).round() as i32;
        let msg_amt = (msg_price * (order.vol as f64 * info.lot_size) * 1000.0).round() / 1000.0;
        let modified = true;
        let total_trade_num = (info.total_bid_order + info.total_ask_order) as i32;
        let avg_ask_price = ((info.total_ask / info.total_ask_qty) * 1000.0).round() / 1000.0;
        let avg_bid_price = ((info.total_bid / info.total_bid_qty) * 1000.0).round() / 1000.0;
        let need_output = snapshot.need_output;
        snapshot.snapshot_once(
            timestamp,
            timestamp,
            timestamp,
            last_seq_num,
            last_price,
            high_price,
            low_price,
            total_turnover,
            total_volume,
            prev_close_price,
            sub_asks_p,
            sub_bids_p,
            sub_asks_vol,
            sub_bids_vol,
            sub_asks_num,
            sub_bids_num,
            total_trade_num,
            avg_ask_price,
            avg_bid_price,
            msg_buy_no,
            msg_sell_no,
            msg_trade_type,
            msg_order_type,
            msg_bsflag,
            msg_price,
            msg_qty,
            msg_amt,
            modified,
            need_output,
        );
        true
    } else {
        false
    }
}
