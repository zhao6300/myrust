from trade_mocker_rust import trade_mocker_rust as tmr
import pandas as pd
import json
import pandas

mode = "L2P"
order_time = 20231201093939000
trade_date = str(int(order_time/1000000000))
# 是否需要输出L3增强行情
need_output = True

# 初始化并加载数据
tmk = tmr.trade_mocker_instance(
    mode, trade_date, need_output, file_type='local', data_path='./data', exchange_mode='backtest')
stock_code = "688007.SH"
order_price = 140.70
order_volume = 4000
bs_flag = "B"

# 1. 模拟下单
order_number1 = tmk.send_order(stock_code=stock_code, order_time=order_time,
                               order_price=order_price, order_volume=order_volume, bs_flag=bs_flag)
order_number2 = tmk.send_order(stock_code=stock_code, order_time=order_time,
                               order_price=order_price, order_volume=order_volume, bs_flag=bs_flag)

# 2. 撮合订单至指定时间
orders = tmk.elapse_with_orders(order_time, 10000)
print(orders)

# 3. 撮合订单并返回成交订单量
filled_orders = tmk.elapse(20000)
print(f"Filled orders: {filled_orders}")


# 4. 获取已成交订单
finished_orders = tmk.get_finished_order(stock_code=stock_code)
print(f"Finished orders for {stock_code}: {finished_orders}")

# 5. 获取未成交订单
pending_orders = tmk.get_pending_orders()
print(f"Pending orders: {pending_orders}")

# 6. 获取当前时间戳
current_time = tmk.get_crurent_time()
print(f"Current time: {current_time}")

# 7. 获取最新的订单信息
latest_orders = tmk.get_latest_orders()
print(f"Latest orders: {latest_orders}")


# 8. 获取指定股票的当前 L3 快照数据
snapshot = tmk.get_current_l3_snapshot(stock_code)
snapshot_json = json.loads(snapshot)
print(f"L3 Snapshot for {stock_code}: {snapshot_json}")

# 9. 撤销订单
cancel_status = tmk.cancel_order(order_number1)
print(f"Cancel order status: {cancel_status}")

# 10. 获取所有的订单
orders = tmk.get_all_orders()
print(f"order : {orders}")

def order_data():
    file = "data/XSHG_Stock_Order_Auction_Month/month=202312/XSHG_Stock_Order_Auction_688007.SH_202312.parquet"

    df = pandas.read_parquet(file)
    import pdb
    pdb.set_trace()
    print(df[['OrderIndex', 'OrderType', 'OrderPrice',
          'OrderQty', 'OrderBSFlag', 'OrderNO']].head())


def trade_data():
    file = "data/XSHG_Stock_Transaction_Auction_Month/month=202312/XSHG_Stock_Transaction_Auction_688007.SH_202312.parquet"

    df = pandas.read_parquet(file)
    print(df.info(verbose=True))
    import pdb
    pdb.set_trace()
    print(df[['OrderIndex', 'OrderType', 'OrderPrice',
          'OrderQty', 'OrderBSFlag', 'OrderNO']].head())


# order_data()
# trade_data()
