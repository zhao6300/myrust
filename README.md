# TradeMocker Rust (`trade_mocker_rust`)

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2021-orange.svg)](https://www.rust-lang.org)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org)

一个基于 Rust 实现的高性能 L3 订单簿（Order Book）撮合与交易模拟回测引擎。项目通过 **PyO3** 提供了无缝的 Python 接口绑定，专为量化交易、高频策略回测以及微观市场结构研究而设计。

---

## 📖 目录

- [核心特性](#-核心特性)
- [系统架构](#-系统架构)
- [关键概念与支持类型](#-关键概念与支持类型)
- [安装与编译](#-安装与编译)
- [Python 快速上手](#-python-快速上手)
- [数据格式与持久化](#-数据格式与持久化)
- [技术栈](#-技术栈)
- [开源协议](#-开源协议)

---

## ✨ 核心特性

- ⚡ **高性能撮合引擎**：基于跳表（SkipList）的高效价格档位存储，实现订单的高速插入、撤单与深度撮合。
- 🐍 **原生 Python 绑定**：基于 PyO3 构建，让 Python 策略能够以接近 C/Rust 的极速性能进行订单提交、行情轮询和时间推进。
- 📊 **L3 (Level 3) 深度模拟**：不仅支持价格与数量的变动，还能完整维护订单队列（包含每个订单的排队顺序与状态变更）。
- 🕰️ **精确的时间推演**：支持通过 `elapse` 手动推进模拟时钟，实现毫秒级的时间片回测，精准模拟市场延迟和订单排队。
- 📂 **回放与持久化**：集成 Polars 读取本地或 HDFS 上的 Parquet 历史订单与成交数据，并能将模拟生成的 L3 深度数据持久化为标准的 Parquet 文件。

---

## 🏗️ 系统架构

项目的核心目录结构如下：

```text
├── Cargo.toml              # Rust 项目配置文件及依赖
├── src/
│   ├── lib.rs              # 库入口，包含基础线程池实现
│   ├── libpy.rs            # PyO3 绑定层，定义暴露给 Python 的接口类 TradeMockerRS
│   ├── snapshot_helper.rs  # 订单簿 L3 快照序列化与 Parquet 写入辅助器
│   ├── t1.py               # Python 调用示例脚本
│   └── orderbook/          # 撮合引擎核心逻辑
│       ├── mod.rs          # 订单簿模块定义与错误类型
│       ├── types.rs        # 交易指令、买卖方向、订单状态等核心枚举
│       ├── order.rs        # 订单结构定义与内存管理
│       ├── broker.rs       # 经纪商管理，维护个股的订单队列与事件挂钩 (Hooks)
│       ├── exchange.rs     # 交易所管理，协同多只股票的撮合与全局时钟推进
│       ├── skiplist_orderbook.rs  # 基于跳表的高效市场深度数据结构
│       ├── dataloader.rs   # 历史行情数据装载器 (DataCollator)
│       └── statistics.rs   # 撮合统计信息收集（成交量、成交额等）
```

---

## 🗂️ 关键概念与支持类型

### 1. 交易模式 (`ExchangeMode`)
* **Backtest (回测模式)**：使用历史成交及订单流数据，完整模拟市场订单的行为，计算策略排队。
* **Live (实盘模式)**：支持实时的、基于事件驱动的撮合机制。

### 2. 订单类型 (`OrderType`)
引擎支持多种主流交易所的申报类型：
* `L`：普通限价委托（Limit）
* `M`：最优五档即时成交剩余撤销
* `N`：最优五档即时成交剩余转限价
* `B`：本方最优价格申报
* `C`：对手方最优价格申报
* `D`：即时全部成交或撤销委托（FOK）
* `Cancel`：撤单委托

### 3. 买卖方向 (`Side`)
* `Buy` (1)：买入
* `Sell` (2)：卖出

---

## 🔧 安装与编译

### 依赖环境
* **Rust**: 1.70.0+ (支持 2021 Edition)
* **Python**: 3.8+
* **Maturin**: 用于构建和发布 PyO3 绑定的 Rust 包。可通过 pip 安装：
  ```bash
  pip install maturin patchelf
  ```

### 编译 Python 模块
在项目根目录下执行以下命令，将 Rust 项目编译并安装为当前 Python 环境的包：

```bash
# 开发模式安装（直接在当前 Python 环境生成链接，便于调试）
maturin develop

# 生产模式编译（生成高摩擦优化的 release 包）
maturin build --release
```

编译完成后，您可以在 Python 中直接通过 `import trade_mocker_rust` 导入。

---

## 🚀 Python 快速上手

下面展示了如何在 Python 中初始化撮合引擎、发送订单、推进时间以及获取市场深度快照。更多用法请参考 [t1.py](src/t1.py)。

```python
import json
from trade_mocker_rust import trade_mocker_rust as tmr

# 1. 初始化交易模拟器
# 参数：运行模式，日期，是否需要输出，文件类型，数据路径，交易模式
tmk = tmr.trade_mocker_instance(
    "L2P", 
    "20231201", 
    need_output=True, 
    file_type="local", 
    data_path="./data", 
    exchange_mode="backtest"
)

stock_code = "688007.SH"
order_time = 20231201093939000

# 2. 模拟向交易所发送买入限价单
order_id1 = tmk.send_order(
    stock_code=stock_code,
    order_time=order_time,
    order_price=140.70,
    order_volume=4000,
    bs_flag="B"
)
print(f"Submitted Order 1 ID: {order_id1}")

# 3. 推进全局模拟时钟（推进 10,000 毫秒 / 10 秒）
# 该方法会触发历史行情的回放，并在后台对您的订单进行排队与撮合
affected_orders_json = tmk.elapse_with_orders(order_time, 10000)
print("Affected orders:", json.loads(affected_orders_json))

# 4. 查询订单状态
pending_orders = tmk.get_pending_orders()
print(f"Pending orders: {pending_orders}")

finished_orders = tmk.get_finished_order(stock_code=stock_code)
print(f"Finished orders: {finished_orders}")

# 5. 获取当前 L3 级别的订单簿快照
l3_snapshot_json = tmk.get_current_l3_snapshot(stock_code)
print("L3 Snapshot:", json.loads(l3_snapshot_json))

# 6. 撤销未成交的订单
success = tmk.cancel_order(order_id1)
print(f"Cancel status: {success}")
```

---

## 💾 数据格式与持久化

### 输入数据
回测模式下，系统能够通过 `DataCollator` 加载逐笔委托（Order）和逐笔成交（Transaction）历史数据。
* 数据源支持：本地目录（`local`）或分布式文件系统（`hdfs`）。
* 数据格式：推荐使用经过压缩的 Parquet 文件，按 `month=YYYYMM` 归档。

### 输出快照
模拟过程中可以通过 Hook 自动导出 L3 快照，快照定义在 `OrderBookSnapshot` 结构体中。它包含：
* **十档/五十档深度**：`asks_p`/`bids_p` (价格) 与 `asks_vol`/`bids_vol` (挂单量)。
* **委托笔数**：`asks_num`/`bids_num` (各档位的委托总笔数)。
* **统计信息**：高开低收价格、成交总量、总成交额、平均买卖价格等。

你可以调用 `presist_l3_data` 将生成的仿真行情数据快速导出为 Parquet 格式，供后续 Alpha 因子挖掘或策略训练使用。

---

## 🛠️ 技术栈

* **开发语言**：Rust & Python
* **并行计算**：[Rayon](https://github.com/rayon-rs/rayon) (用于多股多经纪商的并发事件回放)
* **数据处理**：[Polars](https://github.com/pola-rs/polars) (用于高性能 I/O 及 DataFrame 操作)
* **序列化/反序列化**：[Serde](https://serde.rs/) & [serde_json](https://github.com/serde-rs/json)
* **错误处理**：[thiserror](https://github.com/dtolnay/thiserror)

---

## 📄 开源协议

本项目采用 [MIT License](LICENSE) 协议开源。
