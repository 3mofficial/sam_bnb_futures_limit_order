# 导入必要的模块
import xlsxwriter  # 用于创建和写入 Excel 文件，处理账户指标数据
import json  # 用于处理 JSON 数据格式，保存和加载账户信息
import logging  # 用于日志记录，跟踪程序运行状态和错误
from datetime import datetime, timedelta  # 用于处理日期和时间，管理交易时间戳
import time  # 用于时间相关的操作，如等待和超时控制
from typing import Dict, List, Any, Tuple  # 类型提示，增强代码可读性和类型检查
import pandas as pd  # 用于数据处理和 Excel/CSV 文件操作
import os  # 用于文件和路径操作，如检查文件存在或创建目录
from .binance_client import BinanceFuturesClient  # 导入自定义的 Binance 期货客户端模块，处理交易所交互
from .data_processor import DataProcessor  # 导入自定义的数据处理模块，处理交易数据
from .logger import setup_logger  # 导入自定义的日志设置模块，初始化日志记录器
from typing import Set, Dict  # 导入 Set 和 Dict 类型提示，用于黑名单和持仓数据

class TradingEngine:
    """交易引擎类，负责执行交易逻辑"""

    def __init__(self, client: BinanceFuturesClient, config: Dict[str, str], logger: logging.Logger):
        """初始化交易引擎"""
        self.client = client  # 保存 Binance 期货客户端实例，用于交易所操作
        self.config = config  # 保存配置字典，包含交易参数和 API 密钥
        self.logger = logger  # 保存日志记录器实例，用于记录运行日志
        self.leverage = int(config["leverage"])  # 从配置中获取杠杆倍数并转换为整数
        self.basic_funds = float(config["basic_funds"])  # 从配置中获取基础资金并转换为浮点数
        self.num_long_pos = int(config["num_long_pos"])  # 从配置中获取多头持仓数量并转换为整数
        self.num_short_pos = int(config["num_short_pos"])  # 从配置中获取空头持仓数量并转换为整数
        self.trade_time = config["trade_time"]  # 从配置中获取交易时间
        self.max_wait_time = float(config.get("max_wait_time", 30))  # 获取最大等待时间（分钟），默认30分钟
        self.account_metrics = {}  # 初始化账户指标字典，存储交易相关指标
        self.pending_orders = []  # 初始化挂单列表，存储未成交的订单
        self.failed_depth_tickers = set()  # 初始化失败交易对集合，记录获取深度失败的交易对
        self.error_reasons = {}  # 初始化错误原因字典，记录交易失败原因，格式为 {ticker: reason}
        self.is_first_run = self._check_first_run()  # 检查是否为首次运行，决定初始化逻辑
        self.positions_file = 'data/positions_output.csv'  # 设置持仓文件路径
        if not os.path.exists(self.positions_file):  # 检查持仓文件是否存在
            os.makedirs(os.path.dirname(self.positions_file), exist_ok=True)  # 创建文件所在目录
            df = pd.DataFrame(columns=['调仓日期', '交易对', '持仓数量', '入场价格'])  # 创建空的持仓数据框架
            df.to_csv(self.positions_file, index=False, encoding='utf-8')  # 保存空持仓数据到 CSV 文件
            self.logger.info(f"初始化空持仓文件: {self.positions_file}")  # 记录持仓文件初始化日志

    def _check_first_run(self) -> bool:
        """检查是否为首次运行"""
        account_metrics_file = "data/account_metrics.xlsx"  # 设置账户指标文件路径
        if not os.path.exists(account_metrics_file):  # 如果账户指标文件不存在
            return True  # 返回 True，表示首次运行
        try:
            df = pd.read_excel(account_metrics_file, sheet_name='Account_Metrics')  # 读取账户指标 Excel 文件
            # 检查是否存在 before_trade_balance 或 after_trade_balance 数据
            has_balance_data = df['Metric'].isin(['before_trade_balance', 'after_trade_balance']).any()  # 检查是否包含余额数据
            return df.empty or not has_balance_data  # 如果文件为空或无余额数据，返回 True
        except Exception as e:
            self.logger.warning(f"检查首次运行状态失败: {str(e)}，假设为首次运行")  # 记录检查失败日志
            return True  # 发生异常时，假设为首次运行

    def get_postonly_price(self, symbol: str, side: str, depth_level: int = 0) -> float:
        """获取 postOnly 挂单价格，直接使用买一档或卖一档价格"""
        try:
            depth = self.client.client.futures_order_book(symbol=symbol, limit=20)  # 获取指定交易对的订单簿数据，限制20档
            price = float(depth['bids'][0][0]) if side == "BUY" else float(depth['asks'][0][0])  # 根据交易方向选择买一或卖一价格
            exchange_info = self.client.client.futures_exchange_info()  # 获取交易所信息
            symbol_info = next(s for s in exchange_info["symbols"] if s["symbol"] == symbol)  # 查找指定交易对信息
            price_filter = next(f for f in symbol_info["filters"] if f["filterType"] == "PRICE_FILTER")  # 获取价格过滤规则
            tick_size = float(price_filter["tickSize"])  # 获取价格最小变动单位
            adjusted_price = round(price / tick_size) * tick_size  # 调整价格以符合交易所精度要求
            self.logger.debug(f"{symbol} {side} 挂单价格: 原始={price}, 调整后={adjusted_price}, tickSize={tick_size}")  # 记录价格调整日志
            return adjusted_price  # 返回调整后的价格
        except Exception as e:
            self.logger.error(f"获取 {symbol} postOnly 挂单价格失败: {str(e)}")  # 记录价格获取失败日志
            self.error_reasons[symbol] = f"获取价格失败: {str(e)}"  # 记录失败原因
            return 0.0  # 获取失败时返回 0.0

    def get_current_positions(self) -> Dict[str, float]:
        """获取当前持仓向量（适配修改后的get_stable_positions）"""
        positions = self.get_stable_positions()  # 调用 get_stable_positions 获取持仓数据

        # 现在positions已经是字典，直接返回
        if not isinstance(positions, dict):  # 检查持仓数据是否为字典格式
            self.logger.error(f"持仓数据格式错误，预期 dict，实际: {type(positions)}")  # 记录格式错误日志
            return {}  # 返回空字典

        self.logger.info(f"当前持仓: {positions}")  # 记录当前持仓信息
        return positions  # 返回持仓字典

    def calculate_position_size(self, balance: float, price: float) -> float:
        """计算单笔交易的持仓数量"""
        per_share = balance / (self.num_long_pos + self.num_short_pos)  # 计算每笔交易的资金份额
        margin = per_share * self.leverage  # 计算杠杆后的保证金
        return margin / price  # 返回持仓数量（保证金除以价格）

    def adjust_quantity(self, symbol: str, quantity: float, price: float, is_close: bool = False) -> float:
        """根据交易所规则调整交易数量，平仓跳过最小名义价值检查"""
        try:
            exchange_info = self.client.client.futures_exchange_info()  # 获取交易所信息
            symbol_info = next(s for s in exchange_info["symbols"] if s["symbol"] == symbol)  # 查找指定交易对信息
            filters = {f["filterType"]: f for f in symbol_info["filters"]}  # 提取交易对的过滤规则
            quantity_precision = symbol_info["quantityPrecision"]  # 获取数量精度
            step_size = float(filters["LOT_SIZE"]["stepSize"])  # 获取数量最小变动单位
            min_qty = float(filters["LOT_SIZE"]["minQty"])  # 获取最小交易数量
            max_qty = float(filters["LOT_SIZE"]["maxQty"])  # 获取最大交易数量
            min_notional = float(filters["MIN_NOTIONAL"]["notional"]) if not is_close else 0.0  # 获取最小名义价值，平仓时忽略

            adjusted_qty = round(round(quantity / step_size) * step_size, quantity_precision)  # 调整数量以符合精度和步长
            if adjusted_qty < min_qty:  # 如果调整后数量小于最小值
                adjusted_qty = 0  # 设置数量为 0
                self.logger.warning(f"{symbol} 数量调整为 0 (原值: {adjusted_qty} 小于最小值: {min_qty})")  # 记录警告日志
                self.error_reasons[symbol] = f"数量过小: {quantity} 小于最小值 {min_qty}"  # 记录错误原因
            elif adjusted_qty > max_qty:  # 如果调整后数量大于最大值
                adjusted_qty = max_qty  # 设置为最大值
                self.logger.warning(f"{symbol} 数量调整为最大值: {max_qty} (原值: {adjusted_qty} 大于最大值: {max_qty})")  # 记录警告日志
                self.error_reasons[symbol] = f"数量过大: {quantity} 大于最大值 {max_qty}"  # 记录错误原因

            if not is_close:  # 如果不是平仓操作
                notional = adjusted_qty * price  # 计算名义价值
                if notional < min_notional:  # 如果名义价值小于最小要求
                    adjusted_qty = 0  # 设置数量为 0
                    self.logger.warning(f"{symbol} 数量调整为 0 (因名义价值 {notional} 小于最小值 {min_notional})")  # 记录警告日志
                    self.error_reasons[symbol] = f"名义价值过低: {notional} 小于最小值 {min_notional}"  # 记录错误原因

            self.logger.info(
                f"{symbol} {'平仓' if is_close else ''}数量调整: {quantity} -> {adjusted_qty}, 最小值: {min_qty}, 最大值: {max_qty}, "
                f"名义价值={adjusted_qty * price}, 真实保证金={(adjusted_qty * price) / self.leverage:.6f}, 数量精度={quantity_precision}"
            )  # 记录数量调整详情日志
            return adjusted_qty  # 返回调整后的数量
        except Exception as e:
            self.logger.error(f"调整 {symbol} 数量失败: {str(e)}")  # 记录数量调整失败日志
            self.error_reasons[symbol] = f"数量调整失败: {str(e)}"  # 记录错误原因
            return 0.0  # 调整失败时返回 0.0

    def execute_trade(self, symbol: str, side: str, quantity: float, is_close: bool = False) -> bool:
        """执行交易操作"""
        try:
            # 检查交易对状态
            exchange_info = self.client.get_exchange_info()  # 获取交易所信息
            symbol_info = next((s for s in exchange_info["symbols"] if s["symbol"] == symbol), None)  # 查找交易对信息
            if not symbol_info or symbol_info["status"] != "TRADING":  # 如果交易对不存在或不可交易
                error_msg = f"交易对 {symbol} 不可交易"  # 设置错误信息
                self.logger.error(error_msg)  # 记录错误日志
                self.error_reasons[symbol] = error_msg  # 记录错误原因
                return False  # 返回交易失败

            # 设置杠杆
            self.client.set_leverage(symbol, self.leverage)  # 设置交易对的杠杆倍数
            price = self.client.get_symbol_price(symbol)  # 获取交易对当前价格
            if price == 0.0:  # 如果价格获取失败
                error_msg = f"{symbol} 获取价格失败"  # 设置错误信息
                self.logger.error(error_msg)  # 记录错误日志
                self.error_reasons[symbol] = error_msg  # 记录错误原因
                return False  # 返回交易失败

            # 检查账户余额
            account_info = self.client.get_account_info()  # 获取账户信息
            available_balance = float(account_info["availableBalance"])  # 获取可用余额
            margin_required = (quantity * price) / self.leverage  # 计算所需保证金
            if available_balance < margin_required:  # 如果可用余额不足
                error_msg = f"{symbol} 余额不足: 可用 {available_balance} < 需求 {margin_required}"  # 设置错误信息
                self.logger.error(error_msg)  # 记录错误日志
                self.error_reasons[symbol] = error_msg  # 记录错误原因
                return False  # 返回交易失败

            # 检查是否为反向开仓
            try:
                current_positions = self.get_current_positions()  # 获取当前持仓
            except Exception as e:
                error_msg = f"获取持仓失败: {str(e)}"  # 设置错误信息
                self.logger.error(error_msg, exc_info=True)  # 记录错误日志并包含异常详情
                self.error_reasons[symbol] = error_msg  # 记录错误原因
                return False  # 返回交易失败

            target_qty = self.final_target.get(symbol, 0)  # 获取目标持仓数量
            is_reverse_open = (
                (side == "BUY" and current_positions.get(symbol, 0) <= 0 and target_qty > 0) or
                (side == "SELL" and current_positions.get(symbol, 0) >= 0 and target_qty < 0)
            )  # 判断是否为反向开仓（从空头到多头或从多头到空头）
            adjusted_qty = (
                self.adjust_quantity(symbol, quantity, price, is_close=True)
                if (is_reverse_open or is_close) and price != 0.0
                else self.adjust_quantity(symbol, quantity, price, is_close=False) if price != 0.0 else 0
            )  # 根据是否平仓或反向开仓调整数量
            if adjusted_qty == 0:  # 如果调整后数量为 0
                error_msg = f"{symbol} 调整数量为 0"  # 设置错误信息
                self.logger.warning(error_msg)  # 记录警告日志
                self.error_reasons[symbol] = error_msg  # 记录错误原因
                return False  # 返回交易失败

            success = False  # 初始化交易成功标志
            order_id = 0  # 初始化订单 ID
            trade_details = {"total_quote": 0.0, "total_qty": 0.0, "price": 0.0}  # 初始化交易详情字典
            if adjusted_qty != 0:  # 如果调整后数量不为 0
                self.logger.debug(
                    f"准备下市价单: symbol={symbol}, side={side}, quantity={adjusted_qty}, reduce_only={is_close}")  # 记录准备下市价单的调试日志
                success, order, trade_details, order_id = self.client.place_market_order(
                    symbol, side, adjusted_qty, reduce_only=is_close
                )  # 下市价单并获取结果
                self.logger.debug(
                    f"市价单返回: success={success}, order={order}, trade_details={trade_details}, order_id={order_id}")  # 记录市价单返回结果

            if success:  # 如果交易成功
                trade_key = f"trade_{symbol}_{side}_{datetime.now().strftime('%Y-%m-%d')}_{order_id}"  # 生成交易记录键
                self.account_metrics[trade_key] = {
                    "value": trade_details,  # 保存交易详情
                    "description": (
                        f"{side} {symbol} 成交数量 {trade_details['total_qty']} "
                        f"成交均价 {trade_details['price']} "
                        f"杠杆后成交金额 {trade_details['total_quote']} "
                        f"真实成交金额 {trade_details['total_quote'] / self.leverage} "
                        f"订单ID {order_id}"
                    ),  # 设置交易描述
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录交易时间
                }
                self.record_trade(0, symbol, side, order_id, adjusted_qty)  # 记录交易到文件
                self.logger.info(
                    f"交易 {symbol} {side} 成功: 订单ID={order_id}, 成交数量={trade_details['total_qty']}, "
                    f"成交均价={trade_details['price']}, 杠杆后成交金额={trade_details['total_quote']}, "
                    f"真实成交金额={trade_details['total_quote'] / self.leverage}, is_close={is_close}"
                )  # 记录交易成功日志
                return True  # 返回交易成功
            else:
                error_msg = (
                    order.get("error", str(order)) if isinstance(order, dict)
                    else str(order) if order else "未知错误"
                )  # 获取订单错误信息
                self.logger.warning(f"交易 {symbol} {side} 下单失败，订单ID: {order_id}, 错误: {error_msg}")  # 记录交易失败日志
                self.error_reasons[symbol] = f"下单失败: {error_msg}"  # 记录错误原因
                return False  # 返回交易失败
        except Exception as e:
            error_msg = f"交易 {symbol} {side} 失败: {str(e)}"  # 设置错误信息
            self.logger.error(error_msg, exc_info=True)  # 记录错误日志并包含异常详情
            self.error_reasons[symbol] = error_msg  # 记录错误原因
            return False  # 返回交易失败

    def write_to_excel(self, filename="data/account_metrics.xlsx", run_id=None):
        """将账户指标写入 Excel 文件"""
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)  # 确保文件目录存在
            current_date = datetime.now().strftime('%Y-%m-%d')  # 获取当前日期
            required_metrics = [
                "position_file", "before_trade_balance", "before_available_balance",
                "after_trade_balance", "balance_loss", "balance_loss_rate",
                "after_available_balance", "btc_usdt_price",
                f"trade_commission_summary_{current_date}",
                f"trade_commission_summary_ratio_{current_date}",
                f"trade_realized_pnl_summary_{current_date}",
                f"trade_realized_pnl_summary_ratio_{current_date}",
                "pre_rebalance_return", "post_rebalance_return"
            ]  # 定义所需记录的指标列表

            # 获取账户信息以填充缺失指标
            try:
                account_info = self.client.get_account_info()  # 获取账户信息
                total_balance = float(account_info["totalMarginBalance"])  # 获取总保证金余额
                available_balance = float(account_info["availableBalance"])  # 获取可用余额
            except Exception as e:
                self.logger.error(f"获取账户信息失败，无法填充缺失指标: {str(e)}")  # 记录账户信息获取失败日志
                total_balance = 0.0  # 设置默认总余额
                available_balance = 0.0  # 设置默认可用余额

            # 填充 after_trade_balance 和 after_available_balance
            if "after_trade_balance" not in self.account_metrics:  # 如果缺少调仓后总余额
                self.account_metrics["after_trade_balance"] = {
                    "value": total_balance,  # 保存总余额
                    "description": "调仓后账户总保证金余额(totalMarginBalance)",  # 设置描述
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                }
            if "after_available_balance" not in self.account_metrics:  # 如果缺少调仓后可用余额
                self.account_metrics["after_available_balance"] = {
                    "value": available_balance,  # 保存可用余额
                    "description": "调仓后可用保证金余额(available_balance)",  # 设置描述
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                }

            # 计算 balance_loss 和 balance_loss_rate
            if "before_trade_balance" in self.account_metrics and "after_trade_balance" in self.account_metrics:  # 如果存在调仓前后余额
                before_balance = float(self.account_metrics["before_trade_balance"]["value"])  # 获取调仓前余额
                after_balance = float(self.account_metrics["after_trade_balance"]["value"])  # 获取调仓后余额
                self.account_metrics["balance_loss"] = {
                    "value": before_balance - after_balance,  # 计算余额损失
                    "description": "调仓前后账户余额变化",  # 设置描述
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                }
                self.account_metrics["balance_loss_rate"] = {
                    "value": f"{((before_balance - after_balance) / before_balance * 100) if before_balance != 0 else 0:.6f}%",  # 计算余额损失率
                    "description": "调仓前后账户余额变化率(%)",  # 设置描述
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                }

            # 获取 BTC/USDT 价格
            btc_usdt_price = self.client.get_symbol_price("BTCUSDT")  # 获取 BTC/USDT 当前价格
            if "btc_usdt_price" not in self.account_metrics:  # 如果缺少 BTC/USDT 价格记录
                self.account_metrics["btc_usdt_price"] = {
                    "value": btc_usdt_price,  # 保存价格
                    "description": f"当前 BTC/USDT 价格: {btc_usdt_price}",  # 设置描述
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                }

            # 初始化缺失的指标（首次运行或异常情况）
            for metric in required_metrics:  # 遍历所需指标
                if metric not in self.account_metrics:  # 如果指标缺失
                    if metric.startswith("trade_commission_summary_"):  # 如果是手续费总和指标
                        self.account_metrics[metric] = {
                            "value": 0.0,  # 默认值 0.0
                            "description": f"{current_date} 买卖交易手续费总和（未计算）",  # 设置描述
                            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                        }
                    elif metric.startswith("trade_commission_summary_ratio_"):  # 如果是手续费占比指标
                        self.account_metrics[metric] = {
                            "value": "0.000000%",  # 默认值 0%
                            "description": f"{current_date} 买卖交易总手续费占比（未计算）",  # 设置描述
                            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                        }
                    elif metric.startswith("trade_realized_pnl_summary_"):  # 如果是已实现盈亏总和指标
                        self.account_metrics[metric] = {
                            "value": 0.0,  # 默认值 0.0
                            "description": f"{current_date} 买卖交易已实现盈亏总和（未计算）",  # 设置描述
                            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                        }
                    elif metric.startswith("trade_realized_pnl_summary_ratio_"):  # 如果是盈亏占比指标
                        self.account_metrics[metric] = {
                            "value": "0.000000%",  # 默认值 0%
                            "description": f"{current_date} 买卖交易总盈亏占比（未计算）",  # 设置描述
                            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                        }
                    elif metric == "pre_rebalance_return":  # 如果是调仓前回报率
                        self.account_metrics[metric] = {
                            "value": "0.000000%",  # 默认值 0%
                            "description": f"调仓前回报率: 首次运行或缺少历史数据",  # 设置描述
                            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                        }
                    elif metric == "post_rebalance_return":  # 如果是调仓后回报率
                        self.account_metrics[metric] = {
                            "value": "0.000000%",  # 默认值 0%
                            "description": f"调仓后回报率: 首次运行或缺少历史数据",  # 设置描述
                            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                        }

            # 检查缺失指标并记录
            missing_metrics = [m for m in required_metrics if m not in self.account_metrics]  # 查找缺失的指标
            if missing_metrics:  # 如果存在缺失指标
                self.logger.warning(f"缺失指标: {missing_metrics}，已初始化默认值")  # 记录警告日志

            data = []  # 初始化数据列表，用于存储指标记录
            record_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 获取当前记录时间
            run_id = run_id or f"{record_time.replace(' ', '_').replace(':', '')}"  # 生成运行 ID
            for metric in required_metrics:  # 遍历所需指标
                if metric in self.account_metrics:  # 如果指标存在
                    entry = {
                        "Metric": metric,  # 指标名称
                        "Value": self.account_metrics[metric]["value"],  # 指标值
                        "Description": self.account_metrics[metric]["description"],  # 指标描述
                        "Date": self.account_metrics[metric]["date"],  # 记录时间
                        "Record_Time": record_time,  # 记录时间戳
                        "Run_ID": run_id  # 运行 ID
                    }
                    data.append(entry)  # 添加到数据列表

            # 如果无数据，写入空文件
            if not data:  # 如果数据列表为空
                self.logger.warning("无有效账户指标数据，生成空 account_metrics.xlsx")  # 记录警告日志
                df_new = pd.DataFrame(columns=["Metric", "Value", "Description", "Date", "Record_Time", "Run_ID"])  # 创建空数据框架
            else:
                df_new = pd.DataFrame(data)  # 将数据列表转换为数据框架

            # 写入 Excel
            if os.path.exists(filename):  # 如果文件已存在
                try:
                    df_existing = pd.read_excel(filename, sheet_name='Account_Metrics')  # 读取现有 Excel 文件
                    concat_list = [df for df in [df_existing, df_new] if not df.empty and not df.isna().all().all()]  # 准备合并数据
                    if concat_list:  # 如果有有效数据
                        df_combined = pd.concat(concat_list, ignore_index=True)  # 合并数据
                        df_combined = df_combined.drop_duplicates(subset=["Metric", "Run_ID"], keep="last")  # 去重
                        with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:  # 使用 openpyxl 写入
                            df_combined.to_excel(writer, index=False, sheet_name='Account_Metrics')  # 保存到 Excel
                    else:
                        self.logger.warning("无有效数据可拼接，写入空文件")  # 记录警告日志
                        df_new.to_excel(filename, index=False, sheet_name='Account_Metrics')  # 写入空文件
                except ValueError as e:
                    self.logger.warning(f"工作表 'Account_Metrics' 不存在，创建新文件: {str(e)}")  # 记录工作表不存在日志
                    df_new.to_excel(filename, index=False, sheet_name='Account_Metrics')  # 创建新文件
            else:
                df_new.to_excel(filename, index=False, sheet_name='Account_Metrics')  # 写入新文件

            self.logger.info(f"成功写入 {len(data)} 条指标到 {filename}，Run_ID={run_id}")  # 记录写入成功日志
        except Exception as e:
            self.logger.error(f"写入 account_metrics.xlsx 失败: {str(e)}")  # 记录写入失败日志

    def datetime_to_timestamp(self, date_time_str: str) -> int:
        """将日期时间字符串转换为Unix时间戳（毫秒）"""
        try:
            dt = datetime.strptime(date_time_str, "%Y-%m-%d_%H:%M:%S")  # 尝试解析日期时间字符串
            return int(dt.timestamp() * 1000)  # 转换为毫秒时间戳
        except ValueError:
            try:
                dt = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")  # 尝试解析另一种格式
                return int(dt.timestamp() * 1000)  # 转换为毫秒时间戳
            except ValueError as e:
                self.logger.error(f"日期时间格式错误: {str(e)}，请使用 YYYY-MM-DD_HH:MM:SS 或 YYYY-MM-DD HH:MM:SS 格式")  # 记录格式错误日志
                raise  # 抛出异常

    def process_trade_commissions(self):
        """通过API获取指定日期的手续费总和"""
        current_date = datetime.now().strftime('%Y-%m-%d')  # 获取当前日期
        commission_key = f"trade_commission_summary_{current_date}"  # 生成手续费总和键

        # 获取调仓时间范围
        start_time_str = self.account_metrics.get("before_trade_balance", {}).get("date")  # 获取调仓前时间
        end_time_str = self.account_metrics.get("after_trade_balance", {}).get("date")  # 获取调仓后时间

        if not start_time_str or not end_time_str:  # 如果缺少时间信息
            self.logger.warning(f"缺少调仓时间信息，无法获取手续费: start={start_time_str}, end={end_time_str}")  # 记录警告日志
            self.account_metrics[commission_key] = {
                "value": 0.0,  # 设置默认值
                "description": f"{current_date} 买卖交易手续费总和（缺少时间信息）",  # 设置描述
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
            }
            return self.account_metrics[commission_key]  # 返回手续费记录

        try:
            start_time = self.datetime_to_timestamp(start_time_str)  # 转换为开始时间戳
            end_time = self.datetime_to_timestamp(end_time_str)  # 转换为结束时间戳
            if start_time >= end_time:  # 如果时间范围无效
                self.logger.warning(f"无效时间范围: 开始时间 {start_time_str} 不早于结束时间 {end_time_str}")  # 记录警告日志
                self.account_metrics[commission_key] = {
                    "value": 0.0,  # 设置默认值
                    "description": f"{current_date} 买卖交易手续费总和（无效时间范围）",  # 设置描述
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                }
                return self.account_metrics[commission_key]  # 返回手续费记录

            records = self.client.get_commission_history(start_time, end_time)  # 获取手续费记录
            total_commission = self.client.calculate_total_commission(records)  # 计算总手续费

            self.account_metrics[commission_key] = {
                "value": total_commission,  # 保存总手续费
                "description": f"{current_date} 买卖交易手续费总和（API获取，{len(records)}条记录）",  # 设置描述
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
            }
            self.logger.info(f"手续费汇总 ({commission_key}): {total_commission} USDT, 记录数: {len(records)}")  # 记录手续费汇总日志
        except Exception as e:
            self.logger.error(f"获取手续费失败: {str(e)}")  # 记录获取失败日志
            self.account_metrics[commission_key] = {
                "value": 0.0,  # 设置默认值
                "description": f"{current_date} 买卖交易手续费总和（获取失败: {str(e)}）",  # 设置描述
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
            }
        return self.account_metrics[commission_key]  # 返回手续费记录

    def process_trade_realized_pnl(self):
        """计算特定日期买卖交易的已实现盈亏总和"""
        current_date = datetime.now().strftime('%Y-%m-%d')  # 获取当前日期
        trade_keys = [key for key in self.account_metrics.keys() if
                      ("SELL" in key or "BUY" in key) and current_date in key]  # 筛选当天的交易记录键
        total_realized_pnl = 0.0  # 初始化总盈亏
        for trade_key in trade_keys:  # 遍历交易记录
            trades = self.account_metrics[trade_key]["value"]  # 获取交易详情
            if not trades:  # 如果交易详情为空
                self.logger.warning(f"{trade_key} 无成交记录，跳过盈亏计算")  # 记录警告日志
                continue
            try:
                total_realized_pnl += sum(float(trade["realizedPnl"]) for trade in trades)  # 计算总盈亏
            except Exception as e:
                self.logger.error(f"{trade_key} 计算盈亏失败: {str(e)}")  # 记录计算失败日志
                continue
        pnl_key = f"trade_realized_pnl_summary_{current_date}"  # 生成盈亏总和键
        self.account_metrics[pnl_key] = {
            "value": total_realized_pnl,  # 保存总盈亏
            "description": f"{current_date} 买卖交易已实现盈亏总和",  # 设置描述
            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
        }
        self.logger.info(f"盈亏汇总 ({pnl_key}): {total_realized_pnl}")  # 记录盈亏汇总日志
        return self.account_metrics[pnl_key]  # 返回盈亏记录

    def cancel_all_open_orders(self):
        """撤销所有未成交挂单"""
        try:
            open_orders = self.client.client.futures_get_open_orders()  # 获取所有未成交订单
            if not open_orders:  # 如果没有未成交订单
                self.logger.info("当前没有未成交的挂单")  # 记录信息日志
                return  # 直接返回
            self.logger.info(f"发现 {len(open_orders)} 个未成交挂单，开始撤单...")  # 记录发现的挂单数量
            for order in open_orders:  # 遍历所有未成交订单
                ticker = order["symbol"]  # 获取交易对
                order_id = order["orderId"]  # 获取订单 ID
                side = order["side"]  # 获取交易方向
                try:
                    self.client.cancel_order(ticker, order_id)  # 撤销订单
                    self.logger.info(f"成功撤单: {ticker} {side}，订单ID={order_id}")  # 记录成功日志
                except Exception as e:
                    self.logger.error(f"撤单失败: {ticker} {side}，订单ID={order_id}，错误: {str(e)}")  # 记录错误日志
        except Exception as e:
            self.logger.error(f"获取未成交挂单失败: {str(e)}")  # 记录获取挂单失败的错误

    def get_price_precision(self, symbol: str) -> int:
        """获取交易对的价格精度"""
        try:
            exchange_info = self.client.client.futures_exchange_info()  # 获取交易所信息
            symbol_info = next(s for s in exchange_info["symbols"] if s["symbol"] == symbol)  # 查找指定交易对信息
            return symbol_info["pricePrecision"]  # 返回价格精度
        except Exception as e:
            self.logger.error(f"获取 {symbol} 价格精度失败: {str(e)}")  # 记录错误日志
            return 8  # 默认返回精度 8

    def adjust_or_open_positions(self, long_candidates: List[Dict], short_candidates: List[Dict], run_id: str,
                                 date_str: str):
        """持续限价单调仓，直到完成或超时后市价单补齐，非目标持仓通过限价单平仓"""
        try:
            # 获取账户信息并缓存
            account_info = self.client.get_account_info()  # 获取账户信息
            total_balance = float(account_info["totalMarginBalance"]) - self.basic_funds  # 计算可用总余额（扣除基础资金）
            available_balance = float(account_info["availableBalance"])  # 获取可用余额
            self.logger.info(
                f"=====账户信息获取成功: totalMarginBalance = {total_balance + self.basic_funds}, "
                f"available_balance = {available_balance}====="
            )  # 记录账户信息日志

            # 记录调仓前账户信息
            self.account_metrics["before_trade_balance"] = {
                "value": total_balance + self.basic_funds,  # 保存调仓前总余额
                "description": "调仓前账户总保证金余额(totalMarginBalance)",  # 设置描述
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
            }
            self.account_metrics["before_available_balance"] = {
                "value": available_balance,  # 保存调仓前可用余额
                "description": "调仓前可用保证金余额(available_balance)",  # 设置描述
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
            }

            # 计算每笔交易的资金份额
            per_share = total_balance / (self.num_long_pos + self.num_short_pos)  # 计算每笔交易的资金份额
            self.logger.info(f"每笔交易资金份额: per_share={per_share}")  # 记录资金份额日志
            self.per_share = per_share  # 保存资金份额到实例变量

            # 初始化目标持仓
            self.final_target = {}  # 初始化目标持仓字典
            current_positions = self.get_stable_positions()  # 获取当前持仓
            is_first_day = not bool(current_positions)  # 判断是否为第一天（无持仓）
            non_zero_positions = {k: v for k, v in current_positions.items() if v != 0}  # 筛选非零持仓
            self.logger.info(f"当前持仓: {non_zero_positions}, 是否第一天: {is_first_day}")  # 记录持仓和是否第一天日志
            long_count, short_count = 0, 0  # 初始化多头和空头计数器

            # 提前检查交易对状态
            exchange_info = self.client.get_exchange_info()  # 获取交易所信息
            valid_symbols = {s["symbol"] for s in exchange_info["symbols"] if s["status"] == "TRADING"}  # 筛选可交易的交易对

            # 处理多头候选
            for candidate in long_candidates:  # 遍历多头候选列表
                if long_count >= self.num_long_pos:  # 如果多头数量达到上限
                    break  # 退出循环
                ticker = candidate["ticker"]  # 获取交易对
                if ticker not in valid_symbols:  # 如果交易对不可交易
                    self.logger.warning(f"交易对 {ticker} 不可交易，跳过")  # 记录警告日志
                    self.error_reasons[ticker] = f"交易对不可交易"  # 记录错误原因
                    continue  # 跳过当前交易对
                price = self.client.get_symbol_price(ticker)  # 获取当前价格
                if price == 0.0:  # 如果价格获取失败
                    self.logger.warning(f"{ticker} 获取价格失败，跳过")  # 记录警告日志
                    self.error_reasons[ticker] = "获取价格失败"  # 记录错误原因
                    continue  # 跳过当前交易对
                qty = self.adjust_quantity(ticker, self.calculate_position_size(total_balance, price), price)  # 计算并调整持仓数量
                if qty > 0:  # 如果调整后数量有效
                    self.final_target[ticker] = qty  # 添加到目标持仓
                    long_count += 1  # 多头计数器加 1
                    self.logger.info(f"添加多头目标: {ticker}, 数量={qty}")  # 记录添加多头目标日志
                else:
                    self.error_reasons[ticker] = self.error_reasons.get(ticker, "数量调整为 0")  # 记录错误原因
                    self.logger.warning(f"{ticker} 数量调整为 0，跳过")  # 记录警告日志

            # 处理空头候选
            for candidate in short_candidates:  # 遍历空头候选列表
                if short_count >= self.num_short_pos:  # 如果空头数量达到上限
                    break  # 退出循环
                ticker = candidate["ticker"]  # 获取交易对
                if ticker not in valid_symbols:  # 如果交易对不可交易
                    self.logger.warning(f"交易对 {ticker} 不可交易，跳过")  # 记录警告日志
                    self.error_reasons[ticker] = f"交易对不可交易"  # 记录错误原因
                    continue  # 跳过当前交易对
                price = self.client.get_symbol_price(ticker)  # 获取当前价格
                if price == 0.0:  # 如果价格获取失败
                    self.logger.warning(f"{ticker} 获取价格失败，跳过")  # 记录警告日志
                    self.error_reasons[ticker] = "获取价格失败"  # 记录错误原因
                    continue  # 跳过当前交易对
                qty = self.adjust_quantity(ticker, self.calculate_position_size(total_balance, price), price)  # 计算并调整持仓数量
                if qty > 0:  # 如果调整后数量有效
                    self.final_target[ticker] = -qty  # 添加到目标持仓（空头为负值）
                    short_count += 1  # 空头计数器加 1
                    self.logger.info(f"添加空头目标: {ticker}, 数量=-{qty}")  # 记录添加空头目标日志
                else:
                    self.error_reasons[ticker] = self.error_reasons.get(ticker, "数量调整为 0")  # 记录错误原因
                    self.logger.warning(f"{ticker} 数量调整为 0，跳过")  # 记录警告日志

            self.logger.info(f"初始化目标持仓: {self.final_target}")  # 记录目标持仓日志

            # 计算所需调整的持仓
            current_positions = self.get_stable_positions()  # 获取当前持仓
            if not isinstance(current_positions, dict):  # 检查持仓数据格式
                self.logger.error("get_stable_positions 返回格式错误，预期 Dict[str, float]")  # 记录格式错误日志
                raise ValueError("持仓数据格式错误")  # 抛出异常
            require = {k: self.final_target.get(k, 0) - current_positions.get(k, 0) for k in
                       set(self.final_target) | set(current_positions)}  # 计算需要调整的持仓差额
            all_orders = {ticker: qty for ticker, qty in require.items() if qty != 0}  # 筛选非零调整订单
            adjusted_to_zero = set()  # 初始化调整为零的交易对集合

            # 获取交易所信息中的 tick_size
            tick_sizes = {
                s["symbol"]: float(next(f for f in s["filters"] if f["filterType"] == "PRICE_FILTER")["tickSize"])
                for s in exchange_info["symbols"]
            }  # 获取所有交易对的 tick_size

            self.logger.info("=" * 20 + " 开始持续限价单调仓 " + "=" * 20)  # 记录开始限价单调仓日志
            round_num = 0  # 初始化轮次计数器
            start_time = time.time()  # 记录开始时间
            max_wait_seconds = self.max_wait_time * 60  # 将最大等待时间转换为秒

            while True:  # 开始限价单调仓循环
                round_num += 1  # 轮次计数器加 1
                self.logger.info(f"开始第 {round_num} 轮调仓...")  # 记录轮次开始日志
                pending_orders = []  # 初始化挂单列表
                # 实时更新账户信息
                try:
                    account_info = self.client.get_account_info()  # 获取账户信息
                    available_balance = float(account_info["availableBalance"])  # 获取可用余额
                    self.logger.info(f"第 {round_num} 轮开始时可用余额: {available_balance}")  # 记录轮次开始时余额
                except Exception as e:
                    self.logger.error(f"第 {round_num} 轮获取账户信息失败: {str(e)}, 使用缓存余额: {available_balance}")  # 记录账户信息获取失败日志

                # 检查未成交订单
                try:
                    open_orders_info = self.client.client.futures_get_open_orders()  # 获取未成交订单
                    for order in open_orders_info:  # 遍历未成交订单
                        ticker = order['symbol']  # 获取交易对
                        executed_qty = float(order['executedQty'])  # 获取已成交数量
                        if executed_qty > 0 and order['status'] == 'PARTIALLY_FILLED':  # 如果订单部分成交
                            self.logger.info(
                                f"发现部分成交订单: {ticker}, 已成交数量: {executed_qty}, 订单ID: {order['orderId']}")  # 记录部分成交日志
                            side = order['side']  # 获取交易方向
                            current_positions[ticker] = current_positions.get(ticker, 0) + (
                                executed_qty if side == 'BUY' else -executed_qty)  # 更新持仓
                except Exception as e:
                    self.logger.error(f"检查未成交订单失败: {str(e)}")  # 记录检查订单失败日志

                # 检查是否超时
                elapsed_time = time.time() - start_time  # 计算已用时间
                if elapsed_time >= max_wait_seconds:  # 如果超过最大等待时间
                    self.logger.info(
                        f"限价单调仓已运行 {elapsed_time:.2f} 秒，超过最大等待时间 {max_wait_seconds} 秒，进入市价单补齐")  # 记录超时日志
                    break  # 退出循环

                processed_tickers = set()  # 初始化已处理交易对集合

                # 处理平仓订单
                close_orders = {
                    ticker: qty for ticker, qty in all_orders.items()
                    if ticker in current_positions and (
                            (current_positions[ticker] != 0 and self.final_target.get(ticker, 0) == 0) or
                            (current_positions[ticker] > 0 and self.final_target.get(ticker, 0) > 0 and qty < 0) or
                            (current_positions[ticker] < 0 and self.final_target.get(ticker, 0) < 0 and qty > 0) or
                            (qty > 0 and current_positions[ticker] < 0) or
                            (qty < 0 and current_positions[ticker] > 0)
                    ) and ticker not in processed_tickers
                }  # 筛选需要平仓的订单
                for ticker, qty in close_orders.items():  # 遍历平仓订单
                    if qty == 0 or ticker in adjusted_to_zero:  # 如果数量为 0 或已调整为零
                        self.logger.info(f"第 {round_num} 轮: 跳过 {ticker} 平仓，原因：数量为0或已调整为零")  # 记录跳过日志
                        continue  # 跳过
                    side = "BUY" if qty > 0 or current_positions[ticker] < 0 else "SELL"  # 确定平仓方向
                    qty_to_close = abs(qty) if abs(qty) < abs(current_positions[ticker]) else abs(
                        current_positions[ticker])  # 计算平仓数量
                    self.logger.info(f"第 {round_num} 轮: 处理平仓 {ticker} {side}, 需求数量={qty_to_close}")  # 记录平仓需求日志
                    price = self.get_postonly_price(ticker, side)  # 获取限价单价格
                    if price == 0.0:  # 如果价格获取失败
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 获取一档价格失败，跳过")  # 记录警告日志
                        self.error_reasons[ticker] = "获取价格失败"  # 记录错误原因
                        continue  # 跳过
                    adjusted_qty = self.adjust_quantity(ticker, qty_to_close, price, is_close=True)  # 调整平仓数量
                    self.logger.info(f"第 {round_num} 轮: {ticker} 平仓调整后数量={adjusted_qty}")  # 记录调整后数量
                    if adjusted_qty <= 0:  # 如果调整后数量无效
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 调整数量为 0，跳过")  # 记录警告日志
                        self.error_reasons[ticker] = "数量调整为 0"  # 记录错误原因
                        continue  # 跳过
                    # 平仓无需检查余额，因为释放保证金
                    self.logger.info(
                        f"第 {round_num} 轮: {ticker} 为平仓操作，跳过余额检查")  # 记录跳过余额检查日志
                    self.logger.info(
                        f"第 {round_num} 轮: 准备平仓挂单 {ticker} {side}, 数量={adjusted_qty}, 价格={price}, is_close=True")  # 记录准备挂单日志
                    success, order, order_id, trade_details = self.client.place_postonly_order(ticker, side,
                                                                                               adjusted_qty, price,
                                                                                               is_close=True)  # 下平仓限价单
                    self.logger.info(
                        f"第 {round_num} 轮: {ticker} 平仓挂单结果 success={success}, order_id={order_id}, order={order}")  # 记录挂单结果
                    if success:  # 如果挂单成功
                        pending_orders.append((ticker, order_id, side, adjusted_qty))  # 添加到挂单列表
                        self.logger.info(
                            f"第 {round_num} 轮: {ticker} {side} postOnly 平仓挂单成功，订单ID={order_id}, is_close=True")  # 记录成功日志
                        processed_tickers.add(ticker)  # 添加到已处理交易对
                        current_positions[ticker] = current_positions.get(ticker, 0) + (
                            adjusted_qty if side == "BUY" else -adjusted_qty)  # 更新持仓
                        all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(ticker, 0)  # 更新订单需求
                    else:
                        error_msg = str(order) if order else "未知错误"  # 获取错误信息
                        self.error_reasons[ticker] = f"下单失败: {error_msg}"  # 记录错误原因
                        self.handle_postonly_error(round_num, ticker, side, qty_to_close, error_msg, pending_orders)  # 处理挂单失败

                # 处理开仓订单
                open_orders = {ticker: qty for ticker, qty in all_orders.items() if ticker not in adjusted_to_zero}  # 筛选开仓订单
                self.logger.info(f"第 {round_num} 轮开仓前可用余额: {available_balance}, 开仓需求: {open_orders}")  # 记录开仓前信息
                for ticker, qty in open_orders.items():  # 遍历开仓订单
                    if qty == 0 or ticker in processed_tickers:  # 如果数量为 0 或已处理
                        self.logger.info(f"第 {round_num} 轮: 跳过 {ticker} 开仓，原因：数量为0或已处理")  # 记录跳过日志
                        continue  # 跳过
                    side = "BUY" if qty > 0 else "SELL"  # 确定开仓方向
                    self.logger.info(f"第 {round_num} 轮: 处理开仓 {ticker} {side}, 需求数量={qty}")  # 记录开仓需求日志
                    price = self.get_postonly_price(ticker, side)  # 获取限价单价格
                    if price == 0.0:  # 如果价格获取失败
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 获取一档价格失败，跳过")  # 记录警告日志
                        self.error_reasons[ticker] = "获取价格失败"  # 记录错误原因
                        continue  # 跳过
                    # 修改 is_reverse_open 条件，包含平仓情况 (final_target[ticker] == 0)
                    is_reverse_open = (
                        ticker in current_positions and (
                            (qty > 0 and current_positions[ticker] <= 0 and self.final_target.get(ticker, 0) >= 0) or
                            (qty < 0 and current_positions[ticker] >= 0 and self.final_target.get(ticker, 0) <= 0)
                        )
                    )  # 判断是否为反向开仓或平仓
                    adjusted_qty = self.adjust_quantity(ticker, abs(qty), price,
                                                        is_close=True if is_reverse_open else False)  # 调整开仓数量
                    self.logger.info(
                        f"第 {round_num} 轮: {ticker} 开仓调整后数量={adjusted_qty}, is_reverse_open={is_reverse_open}")  # 记录调整后数量
                    if adjusted_qty <= 0:  # 如果调整后数量无效
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 调整数量为 0，跳过")  # 记录警告日志
                        self.error_reasons[ticker] = "数量调整为 0"  # 记录错误原因
                        continue  # 跳过
                    # 对于反向开仓（包括平仓），跳过余额检查
                    if not is_reverse_open:  # 如果不是反向开仓
                        margin_required = (adjusted_qty * price) / self.leverage  # 计算所需保证金
                        self.logger.info(
                            f"第 {round_num} 轮: {ticker} 开仓保证金需求={margin_required}, 可用余额={available_balance}")  # 记录保证金需求
                        if available_balance < margin_required:  # 如果余额不足
                            self.logger.info(
                                f"第 {round_num} 轮: 可用余额 {available_balance} < 需求 {margin_required}，跳过 {ticker}")  # 记录余额不足日志
                            self.error_reasons[ticker] = f"余额不足: 可用 {available_balance} < 需求 {margin_required}"  # 记录错误原因
                            continue  # 跳过
                    else:
                        self.logger.info(f"第 {round_num} 轮: {ticker} 为反向开仓或平仓，跳过余额检查")  # 记录跳过余额检查日志
                    self.logger.info(
                        f"第 {round_num} 轮: 准备开仓挂单 {ticker} {side}, 数量={adjusted_qty}, 价格={price}, is_close={is_reverse_open}")  # 记录准备开仓挂单日志
                    success, order, order_id, trade_details = self.client.place_postonly_order(ticker, side,
                                                                                               adjusted_qty, price,
                                                                                               is_close=is_reverse_open)  # 下开仓限价单
                    self.logger.info(
                        f"第 {round_num} 轮: {ticker} 开仓挂单结果 success={success}, order_id={order_id}, order={order}")  # 记录挂单结果
                    if success:  # 如果挂单成功
                        pending_orders.append((ticker, order_id, side, adjusted_qty))  # 添加到挂单列表
                        self.logger.info(
                            f"第 {round_num} 轮: {ticker} {side} postOnly 开仓挂单成功，订单ID={order_id}, is_close={is_reverse_open}")  # 记录成功日志
                        processed_tickers.add(ticker)  # 添加到已处理交易对
                        current_positions[ticker] = current_positions.get(ticker, 0) + (
                            adjusted_qty if side == "BUY" else -adjusted_qty)  # 更新持仓
                        all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(ticker, 0)  # 更新订单需求
                    else:
                        error_msg = str(order) if order else "未知错误"  # 获取错误信息
                        self.error_reasons[ticker] = f"下单失败: {error_msg}"  # 记录错误原因
                        self.handle_postonly_error(round_num, ticker, side, qty, error_msg, pending_orders)  # 处理挂单失败

                # 处理挂单状态
                if pending_orders:  # 如果存在挂单
                    self.logger.info(f"第 {round_num} 轮挂单等待，最多 30 秒...")  # 记录挂单等待日志
                    wait_start = time.time()  # 记录等待开始时间
                    while time.time() - wait_start < 30:  # 等待最多30秒
                        still_pending = []  # 初始化仍未成交的挂单列表
                        for ticker, order_id, side, qty in pending_orders:  # 遍历挂单
                            try:
                                order_info = self.client.client.futures_get_order(symbol=ticker, orderId=order_id)  # 获取订单状态
                                if order_info["status"] == "NEW":  # 如果订单仍未成交
                                    still_pending.append((ticker, order_id, side, qty))  # 添加到未成交列表
                                elif order_info["status"] == "FILLED":  # 如果订单完全成交
                                    executed_qty = float(order_info["executedQty"])  # 获取成交数量
                                    self.record_trade(round_num, ticker, side, order_id, executed_qty)  # 记录成交
                                    self.logger.info(
                                        f"第 {round_num} 轮: {ticker} {side} 已完全成交，订单ID={order_id}, 成交数量={executed_qty}")  # 记录完全成交日志
                                    current_positions[ticker] = current_positions.get(ticker, 0) + (
                                        executed_qty if side == "BUY" else -executed_qty)  # 更新持仓
                                    all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(
                                        ticker, 0)  # 更新订单需求
                                elif order_info["status"] == "PARTIALLY_FILLED":  # 如果订单部分成交
                                    executed_qty = float(order_info["executedQty"])  # 获取已成交数量
                                    self.record_trade(round_num, ticker, side, order_id, executed_qty)  # 记录成交
                                    still_pending.append((ticker, order_id, side, qty - executed_qty))  # 更新未成交数量
                                    self.logger.info(
                                        f"第 {round_num} 轮: {ticker} {side} 部分成交，订单ID={order_id}, 已成交数量={executed_qty}")  # 记录部分成交日志
                                    current_positions[ticker] = current_positions.get(ticker, 0) + (
                                        executed_qty if side == "BUY" else -executed_qty)  # 更新持仓
                                    all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(
                                        ticker, 0)  # 更新订单需求
                                else:
                                    self.logger.info(
                                        f"第 {round_num} 轮: {ticker} {side} 订单已取消或失效，状态={order_info['status']}, 订单ID={order_id}")  # 记录订单取消或失效日志
                                    self.error_reasons[ticker] = f"订单取消或失效: 状态 {order_info['status']}"  # 记录错误原因
                                    trades = self.client.client.futures_account_trades(symbol=ticker, orderId=order_id)  # 获取交易记录
                                    if trades:  # 如果有成交记录
                                        executed_qty = sum(float(trade['qty']) for trade in trades)  # 计算总成交数量
                                        self.record_trade(round_num, ticker, side, order_id, executed_qty)  # 记录成交
                                        self.logger.info(
                                            f"第 {round_num} 轮: {ticker} {side} 订单状态为 {order_info['status']}，但发现成交记录，记录成交数量={executed_qty}")  # 记录成交日志
                                        current_positions[ticker] = current_positions.get(ticker, 0) + (
                                            executed_qty if side == "BUY" else -executed_qty)  # 更新持仓
                                        all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(
                                            ticker, 0)  # 更新订单需求
                            except Exception as e:
                                self.logger.error(
                                    f"第 {round_num} 轮: 检查 {ticker} 订单 {order_id} 状态失败: {str(e)}")  # 记录检查订单状态失败日志
                                if "APIError(code=-2011)" in str(e):  # 如果订单不存在
                                    trades = self.client.client.futures_account_trades(symbol=ticker, orderId=order_id)  # 获取交易记录
                                    if trades:  # 如果有成交记录
                                        executed_qty = sum(float(trade['qty']) for trade in trades)  # 计算总成交数量
                                        self.record_trade(round_num, ticker, side, order_id, executed_qty)  # 记录成交
                                        self.logger.info(
                                            f"第 {round_num} 轮: {ticker} {side} 订单不存在但发现成交记录，记录成交数量={executed_qty}, 订单ID={order_id}")  # 记录成交日志
                                        current_positions[ticker] = current_positions.get(ticker, 0) + (
                                            executed_qty if side == "BUY" else -executed_qty)  # 更新持仓
                                        all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(
                                            ticker, 0)  # 更新订单需求
                                    else:
                                        self.logger.warning(
                                            f"第 {round_num} 轮: {ticker} {side} 订单不存在且无成交记录，订单ID={order_id}")  # 记录订单不存在日志
                                        self.error_reasons[ticker] = f"订单不存在且无成交记录: {str(e)}"  # 记录错误原因
                                else:
                                    still_pending.append((ticker, order_id, side, qty))  # 添加到未成交列表
                                    self.error_reasons[ticker] = f"检查订单状态失败: {str(e)}"  # 记录错误原因
                        pending_orders = still_pending  # 更新挂单列表
                        if not pending_orders:  # 如果没有未成交订单
                            self.logger.info(f"第 {round_num} 轮所有挂单已处理完成，提前结束等待")  # 记录提前结束等待日志
                            break  # 退出等待循环
                        time.sleep(2)  # 等待2秒后继续检查

                    if pending_orders:  # 如果仍有未成交订单
                        self.logger.info(f"第 {round_num} 轮结束，取消所有未成交挂单...")  # 记录取消挂单日志
                        for ticker, order_id, side, qty in pending_orders:  # 遍历未成交订单
                            try:
                                self.client.cancel_order(ticker, order_id)  # 取消订单
                                self.logger.info(
                                    f"第 {round_num} 轮: 成功取消 {ticker} {side} 挂单，订单ID={order_id}")  # 记录取消成功日志
                            except Exception as e:
                                self.logger.error(
                                    f"第 {round_num} 轮: 取消 {ticker} {side} 挂单失败，订单ID={order_id}，错误: {str(e)}")  # 记录取消失败日志
                                if "APIError(code=-2011)" in str(e):  # 如果订单不存在
                                    trades = self.client.client.futures_account_trades(symbol=ticker, orderId=order_id)  # 获取交易记录
                                    if trades:  # 如果有成交记录
                                        executed_qty = sum(float(trade['qty']) for trade in trades)  # 计算总成交数量
                                        self.record_trade(round_num, ticker, side, order_id, executed_qty)  # 记录成交
                                        self.logger.info(
                                            f"第 {round_num} 轮: {ticker} {side} 撤单失败但发现成交记录，记录成交数量={executed_qty}, 订单ID={order_id}")  # 记录成交日志
                                        current_positions[ticker] = current_positions.get(ticker, 0) + (
                                            executed_qty if side == "BUY" else -executed_qty)  # 更新持仓
                                        all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(
                                            ticker, 0)  # 更新订单需求
                                    else:
                                        self.logger.warning(
                                            f"第 {round_num} 轮: {ticker} {side} 撤单失败且无成交记录，订单ID={order_id}")  # 记录撤单失败日志
                                        self.error_reasons[ticker] = f"撤单失败且无成交记录: {str(e)}"  # 记录错误原因
                                else:
                                    self.error_reasons[ticker] = f"取消订单失败: {str(e)}"  # 记录错误原因
                        pending_orders = []  # 清空挂单列表

                # 更新当前持仓和订单需求
                current_positions = self.get_stable_positions()  # 获取最新持仓
                all_orders = {
                    k: self.final_target.get(k, 0) - current_positions.get(k, 0)
                    for k in set(self.final_target) | set(current_positions)
                    if self.final_target.get(k, 0) != current_positions.get(k, 0)
                }  # 更新订单需求
                # 处理非首日的小额调整
                if not is_first_day:  # 如果不是第一天
                    for ticker, qty in all_orders.items():  # 遍历订单需求
                        price = self.client.get_symbol_price(ticker)  # 获取当前价格
                        if price != 0.0:  # 如果价格有效
                            adjusted_qty = self.adjust_quantity(ticker, abs(qty), price, is_close=True)  # 调整数量
                            is_open = ticker in self.final_target and self.final_target[ticker] != 0  # 判断是否为开仓
                            notional = adjusted_qty * price  # 计算名义价值
                            if is_open and (adjusted_qty == 0 or notional < 5):  # 如果是小额调整
                                self.logger.info(
                                    f"第 {round_num} 轮: {ticker} 差值 {qty} 为小额调整（名义价值 {notional} < 5 或数量过小），跳过后续轮次"
                                )  # 记录小额调整日志
                                self.final_target[ticker] = current_positions.get(ticker, 0)  # 更新目标持仓
                                adjusted_to_zero.add(ticker)  # 添加到调整为零集合
                                self.error_reasons[ticker] = f"小额调整忽略: 名义价值 {notional} < 5 或数量过小"  # 记录错误原因
                    all_orders = {ticker: qty for ticker, qty in all_orders.items() if ticker not in adjusted_to_zero}  # 更新订单需求

                self.logger.info(f"第 {round_num} 轮完成，剩余需求: {all_orders}")  # 记录轮次完成日志
                if not all_orders:  # 如果没有剩余订单需求
                    self.logger.info(f"第 {round_num} 轮后所有目标已达成，结束限价单调仓")  # 记录调仓结束日志
                    break  # 退出循环

            # 市价单补齐逻辑
            self.logger.info("=" * 20 + f" 市价单补齐：{'第一天' if is_first_day else '后续调仓'} " + "=" * 20)  # 记录市价单补齐开始日志
            current_positions = self.get_stable_positions()  # 获取最新持仓
            remaining_require = {k: self.final_target.get(k, 0) - current_positions.get(k, 0) for k in
                                 set(self.final_target) | set(current_positions)}  # 计算剩余需求
            if not any(qty != 0 for qty in remaining_require.values()):  # 如果没有剩余需求
                self.logger.info("市价单补齐：所有目标已达成，无需补齐")  # 记录无需补齐日志
            else:
                self.logger.info(f"市价单补齐前可用余额: {available_balance}")  # 记录补齐前余额
                force_adjustments = remaining_require  # 获取需要强制调整的订单
                if force_adjustments:  # 如果有需要调整的订单
                    if not is_first_day:  # 如果不是第一天
                        self.logger.info("市价单补齐：处理需要强制调整的交易对，先平仓释放资金...")  # 记录平仓释放资金日志
                        for ticker, qty in force_adjustments.items():  # 遍历需要调整的订单
                            if ticker in current_positions and current_positions[ticker] != 0 and self.final_target.get(
                                    ticker, 0) == 0:  # 如果需要完全平仓
                                side = "BUY" if current_positions[ticker] < 0 else "SELL"  # 确定平仓方向
                                qty_to_close = abs(current_positions[ticker])  # 计算平仓数量
                                self.logger.info(f"市价单补齐: 准备平仓 {ticker} {side}, 数量={qty_to_close}")  # 记录准备平仓日志
                                success = self.execute_trade(ticker, side, qty_to_close, is_close=True)  # 执行平仓
                                if success:  # 如果平仓成功
                                    self.logger.info(
                                        f"市价单补齐平仓: {ticker} {side} 数量={qty_to_close}, is_close=True")  # 记录平仓成功日志
                                else:
                                    self.logger.warning(f"市价单补齐平仓失败: {ticker} {side}")  # 记录平仓失败日志
                    # 更新可用余额
                    account_info = self.client.get_account_info()  # 获取最新账户信息
                    available_balance = float(account_info["availableBalance"])  # 获取可用余额
                    self.logger.info(f"市价单补齐开仓前可用余额: {available_balance}")  # 记录开仓前余额
                    for ticker, qty in force_adjustments.items():  # 遍历需要调整的订单
                        if qty == 0:  # 如果数量为 0
                            self.logger.info(f"市价单补齐: 跳过 {ticker}，原因：数量为0")  # 记录跳过日志
                            continue  # 跳过
                        if ticker not in valid_symbols:  # 如果交易对不可交易
                            self.logger.warning(f"市价单补齐: 交易对 {ticker} 不可交易，跳过")  # 记录警告日志
                            self.error_reasons[ticker] = f"交易对不可交易"  # 记录错误原因
                            continue  # 跳过
                        side = "BUY" if qty > 0 else "SELL"  # 确定交易方向
                        price = self.client.get_symbol_price(ticker)  # 获取当前价格
                        if price == 0.0:  # 如果价格获取失败
                            self.logger.warning(f"市价单补齐: {ticker} 获取价格失败，跳过")  # 记录警告日志
                            self.error_reasons[ticker] = "获取价格失败"  # 记录错误原因
                            continue  # 跳过
                        is_open = ticker in self.final_target and self.final_target[ticker] != 0  # 判断是否为开仓
                        adjusted_qty = self.adjust_quantity(ticker, abs(qty), price, is_close=not is_open)  # 调整数量
                        self.logger.info(f"市价单补齐: {ticker} 调整后数量={adjusted_qty}, is_open={is_open}")  # 记录调整后数量
                        if adjusted_qty == 0:  # 如果调整后数量无效
                            self.logger.warning(f"市价单补齐: {ticker} 调整数量为 0，跳过")  # 记录警告日志
                            self.error_reasons[ticker] = "数量调整为 0"  # 记录错误原因
                            continue  # 跳过
                        margin_required = (adjusted_qty * price) / self.leverage  # 计算所需保证金
                        self.logger.info(
                            f"市价单补齐: {ticker} 保证金需求={margin_required},可用余额={available_balance}")
                        if available_balance < margin_required:
                            self.logger.info(
                                f"市价单补齐: 可用余额 {available_balance} < 需求 {margin_required}，跳过 {ticker}"
                            )
                            self.error_reasons[ticker] = f"余额不足: 可用 {available_balance} < 需求 {margin_required}"
                            continue
                        self.logger.info(
                            f"市价单补齐: 准备执行 {ticker} {side}, 数量={adjusted_qty}, is_close={not is_open}")
                        success = self.execute_trade(ticker, side, adjusted_qty, is_close=not is_open)
                        if success:
                            self.logger.info(
                                f"市价单补齐: {ticker} {side} 成功，数量={adjusted_qty}, is_close={not is_open}"
                            )
                            current_positions[ticker] = current_positions.get(ticker, 0) + (
                                adjusted_qty if side == "BUY" else -adjusted_qty)
                            available_balance -= margin_required  # 更新可用余额
                        else:
                            self.logger.warning(f"市价单补齐: {ticker} {side} 失败，查看 error_reasons 详情")

                # 记录最终持仓
                current_positions = self.get_stable_positions()
                positions_list = [
                    {
                        "symbol": ticker,
                        "positionAmt": qty,
                        "entryPrice": self.client.get_symbol_price(ticker)
                    }
                    for ticker, qty in current_positions.items() if qty != 0
                ]
                if positions_list:
                    self.save_positions_to_csv(positions_list, run_id)
                    self.logger.info(f"持仓记录已保存，Run_ID: {run_id}")
                else:
                    self.logger.info("无非零持仓数据可保存")

                # 记录调仓后账户信息并计算所有指标
                account_info = self.client.get_account_info()
                self.account_metrics["after_trade_balance"] = {
                    "value": float(account_info["totalMarginBalance"]),
                    "description": "调仓后账户总保证金余额(totalMarginBalance)",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                }
                self.account_metrics["after_available_balance"] = {
                    "value": float(account_info["availableBalance"]),
                    "description": "调仓后可用保证金余额(available_balance)",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                }

                # 计算指标并保存
                self.record_post_trade_metrics(total_balance=total_balance)
                self.calculate_and_append_returns()
                self.write_to_excel(run_id=run_id)
                self.logger.info("调仓完成，账户指标已记录")
        except Exception as e:
            self.logger.error(f"调仓失败: {str(e)}", exc_info=True)
            self.error_reasons["global"] = f"调仓失败: {str(e)}"
            # 保存当前持仓以避免数据丢失
            try:
                current_positions = self.get_stable_positions()
                positions_list = [
                    {
                        "symbol": ticker,
                        "positionAmt": qty,
                        "entryPrice": self.client.get_symbol_price(ticker)
                    }
                    for ticker, qty in current_positions.items() if qty != 0
                ]
                if positions_list:
                    self.save_positions_to_csv(positions_list, run_id)
            except Exception as save_e:
                self.logger.error(f"保存持仓失败: {str(save_e)}")
            self.write_to_excel(run_id=run_id)

    def adjust_quantity_for_close(self, symbol: str, quantity: float, price: float) -> float:
        """调整平仓交易数量，跳过最小名义价值检查"""
        try:
            exchange_info = self.client.client.futures_exchange_info()
            symbol_info = next(s for s in exchange_info["symbols"] if s["symbol"] == symbol)
            filters = {f["filterType"]: f for f in symbol_info["filters"]}
            quantity_precision = symbol_info["quantityPrecision"]
            step_size = float(filters["LOT_SIZE"]["stepSize"])
            min_qty = float(filters["LOT_SIZE"]["minQty"])
            max_qty = float(filters["LOT_SIZE"]["maxQty"])

            adjusted_qty = round(round(quantity / step_size) * step_size, quantity_precision)
            if adjusted_qty < min_qty:
                adjusted_qty = 0
                self.logger.warning(f"{symbol} 数量调整为 0 (原值: {adjusted_qty} 小于最小值: {min_qty})")
            elif adjusted_qty > max_qty:
                adjusted_qty = max_qty
                self.logger.warning(
                    f"{symbol} 数量调整为最大值: {max_qty} (原值: {adjusted_qty} 大于最大值: {max_qty})")

            self.logger.info(
                f"{symbol} 平仓数量调整: {quantity} -> {adjusted_qty}, 最小值: {min_qty}, 最大值: {max_qty}, "
                f"数量精度={quantity_precision}"
            )
            return adjusted_qty
        except Exception as e:
            self.logger.error(f"调整 {symbol} 平仓数量失败: {str(e)}")
            return 0.0

    def handle_postonly_error(self, round_num: int, ticker: str, side: str, qty: float, error_msg: str,
                              pending_orders: List[Tuple[str, int, str, float]]):
        """处理 postOnly 挂单失败的错误"""
        success = False  # 初始化为失败状态
        order_id = None

        self.logger.error(f"第 {round_num} 轮: {ticker} {side} postOnly 挂单失败，错误: {error_msg}")

        if "APIError(code=-5022)" in error_msg:  # 特定错误才重试
            max_wait_seconds = 60  # 最大等待60秒
            start_time = time.time()

            while not success and (time.time() - start_time) < max_wait_seconds:
                try:
                    new_price = self.get_postonly_price(ticker, side)
                    if new_price == 0.0:
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 获取新价格失败，等待重试")
                        time.sleep(1)  # 价格获取失败时等待1秒
                        continue

                    adjusted_qty = self.adjust_quantity(ticker, abs(qty), new_price)
                    if adjusted_qty <= 0:
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 调整数量为 0，等待重试")
                        time.sleep(1)  # 数量调整失败时等待1秒
                        continue

                    success, order, order_id, trade_details = self.client.place_postonly_order(
                        ticker, side, adjusted_qty, new_price
                    )
                    if success:
                        pending_orders.append((ticker, order_id, side, adjusted_qty))
                        self.logger.info(
                            f"第 {round_num} 轮: {ticker} {side} postOnly 挂单重试成功，订单ID={order_id}"
                        )
                        break
                    else:
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 重试挂单失败: {order}")
                        time.sleep(1)  # 挂单失败时等待1秒

                except Exception as retry_error:
                    self.logger.error(f"第 {round_num} 轮: {ticker} 重试失败: {str(retry_error)}")
                    time.sleep(1)  # 异常时等待1秒

        if not success:
            self.logger.error(f"第 {round_num} 轮: {ticker} {side} 挂单最终失败")
            self.error_reasons[ticker] = f"postOnly 挂单失败: {error_msg}"

    def record_trade(self, round_num: int, ticker: str, side: str, order_id: int, qty: float):
        """记录成交订单到 account_metrics，使用标准格式"""
        trades = self.client.client.futures_account_trades(symbol=ticker, orderId=order_id)
        if not trades:
            self.logger.warning(f"第 {round_num} 轮: {ticker} {side} 订单ID={order_id} 无成交记录")
            return

        # 计算汇总信息
        total_qty = sum(float(trade['qty']) for trade in trades)
        total_quote = sum(float(trade['qty']) * float(trade['price']) for trade in trades)
        avg_price = total_quote / total_qty if total_qty > 0 else 0
        total_commission = sum(float(trade['commission']) for trade in trades)
        total_realized_pnl = sum(float(trade.get('realizedPnl', 0)) for trade in trades)

        # 构建交易记录
        trade_key = f"trade_{ticker}_{side}_{datetime.now().strftime('%Y-%m-%d')}_{order_id}"

        # 创建符合标准格式的交易记录
        trade_record = {
            'side': side,
            'symbol': ticker,
            'total_qty': total_qty,
            'price': avg_price,
            'total_quote': total_quote,
            'real_quote': total_quote / self.leverage,
            'order_id': order_id,
            'commission': total_commission,
            'realized_pnl': total_realized_pnl
        }

        self.account_metrics[trade_key] = {
            "value": trade_record,  # 使用单个对象而不是列表
            "description": (
                f"{side} {ticker} 成交数量 {total_qty} "
                f"成交均价 {avg_price} "
                f"杠杆后成交金额 {total_quote} "
                f"真实成交金额 {total_quote / self.leverage} "
                f"订单ID {order_id}"
            ),
            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        }

        self.logger.info(
            f"第 {round_num} 轮: {ticker} {side} 单已成交，订单ID={order_id}, "
            f"成交数量={total_qty}, 均价={avg_price}"
        )

    def record_post_trade_metrics(self, total_balance: float = None):
        """记录调仓后的账户信息、手续费和盈亏"""
        account_info = self.client.get_account_info()
        after_trade_balance = float(account_info["totalMarginBalance"])
        after_available_balance = float(account_info["availableBalance"])

        # 如果未提供 total_balance，则使用当前账户总余额减去基本资金
        if total_balance is None:
            total_balance = after_trade_balance - self.basic_funds
            self.logger.info(f"未提供 total_balance，使用账户余额计算: {total_balance}")

        # 确保 before_trade_balance 存在
        if "before_trade_balance" not in self.account_metrics:
            self.account_metrics["before_trade_balance"] = {
                "value": total_balance + self.basic_funds,
                "description": "调仓前账户总保证金余额（未记录，使用当前余额估算）",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }
            self.logger.warning("缺少 before_trade_balance，使用估算值")

        # 更新账户指标
        self.account_metrics.update({
            "after_trade_balance": {
                "value": after_trade_balance,
                "description": "调仓后账户总保证金余额",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            },
            "after_available_balance": {
                "value": after_available_balance,
                "description": "调仓后可用余额",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            },
            "balance_loss": {
                "value": self.account_metrics["before_trade_balance"]["value"] - after_trade_balance,
                "description": "调仓前后余额损失金额",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            },
            "balance_loss_rate": {
                "value": f"{((self.account_metrics['before_trade_balance']['value'] - after_trade_balance) / self.account_metrics['before_trade_balance']['value'] * 100) if self.account_metrics['before_trade_balance']['value'] != 0 else 0:.6f}%",
                "description": "调仓前后余额损失率 (%)",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }
        })

        self.logger.info(
            f"调仓后: totalMarginBalance={after_trade_balance}, "
            f"available_balance={after_available_balance}, "
            f"balance_loss={self.account_metrics['balance_loss']['value']}"
        )

        current_date = datetime.now().strftime('%Y-%m-%d')
        commission_key = f"trade_commission_summary_{current_date}"
        commission_ratio = f"trade_commission_summary_ratio_{current_date}"
        pnl_key = f"trade_realized_pnl_summary_{current_date}"
        pnl_ratio_key = f"trade_realized_pnl_summary_ratio_{current_date}"

        # 计算手续费和盈亏
        if commission_key not in self.account_metrics:
            self.process_trade_commissions()
        if pnl_key not in self.account_metrics:
            self.process_trade_realized_pnl()

        # 计算手续费和盈亏占比
        before_balance = float(self.account_metrics["before_trade_balance"]["value"])
        if commission_key in self.account_metrics:
            commission_value = float(self.account_metrics[commission_key]["value"])
            self.account_metrics[commission_ratio] = {
                "value": f"{(commission_value / before_balance * 100) if before_balance != 0 else 0:.6f}%",
                "description": f"{current_date} 买卖交易总手续费占比",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }
        else:
            self.account_metrics[commission_ratio] = {
                "value": "0.000000%",
                "description": f"{current_date} 买卖交易总手续费占比（未计算）",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }

        if pnl_key in self.account_metrics:
            pnl_value = float(self.account_metrics[pnl_key]["value"])
            self.account_metrics[pnl_ratio_key] = {
                "value": f"{(pnl_value / before_balance * 100) if before_balance != 0 else 0:.6f}%",
                "description": f"{current_date} 买卖交易总盈亏占比",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }
        else:
            self.account_metrics[pnl_ratio_key] = {
                "value": "0.000000%",
                "description": f"{current_date} 买卖交易总盈亏占比（未计算）",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }

        btc_usdt_price = self.client.get_symbol_price("BTCUSDT")
        self.account_metrics["btc_usdt_price"] = {
            "value": btc_usdt_price,
            "description": f"当前 BTC/USDT 价格: {btc_usdt_price}",
            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        }

    def balance_long_short(self, long_candidates: List[Dict], short_candidates: List[Dict], total_balance: float):
        """调整多空平衡"""
        current_positions = self.get_current_positions()  # 获取当前持仓
        long_count = sum(1 for qty in current_positions.values() if qty > 0)  # 计算多头数量
        short_count = sum(1 for qty in current_positions.values() if qty < 0)  # 计算空头数量
        total_count = long_count + short_count  # 计算总持仓数量
        self.logger.info(f"当前总持仓数:{total_count}, 多头数:{long_count}, 空头数:{short_count}")  # 记录持仓统计

        if total_count != self.num_long_pos + self.num_short_pos or long_count != self.num_long_pos or short_count != self.num_short_pos:  # 如果持仓不平衡
            self.logger.info(f"持仓验证失败，开始多空平衡调整...")  # 记录调整开始
            if long_count > short_count:  # 如果多头数量大于空头
                to_close = (long_count - short_count) // 2  # 计算需要平仓的多头数量
                current_longs = {ticker for ticker, qty in current_positions.items() if qty > 0}  # 获取当前多头交易对
                long_candidate_tickers = {c['ticker'] for c in long_candidates}  # 获取多头候选交易对
                long_candidates_sorted = sorted(long_candidates, key=lambda x: x['id'], reverse=True)  # 按 ID 降序排序多头候选
                closed = 0  # 初始化已平仓计数器
                for ticker, qty in current_positions.items():  # 遍历当前持仓
                    if qty > 0 and ticker not in long_candidate_tickers and closed < to_close:  # 如果是多头且不在候选列表中
                        if self.execute_trade(ticker, "SELL", abs(qty)):  # 执行卖出平仓
                            closed += 1  # 已平仓计数器加 1
                if closed < to_close:  # 如果仍需平仓
                    for candidate in long_candidates_sorted:  # 遍历排序后的多头候选
                        ticker = candidate['ticker']  # 获取交易对
                        if ticker in current_positions and current_positions[
                            ticker] > 0 and closed < to_close:  # 如果有持仓且需平仓
                            qty = current_positions[ticker]  # 获取持仓数量
                            if self.execute_trade(ticker, "SELL", abs(qty)):  # 执行卖出平仓
                                closed += 1  # 已平仓计数器加 1

                short_candidates_sorted = sorted(short_candidates, key=lambda x: x['id'], reverse=True)  # 按 ID 降序排序空头候选
                current_shorts = {ticker for ticker, qty in current_positions.items() if qty < 0}  # 获取当前空头交易对
                opened = 0  # 初始化已开仓计数器
                for candidate in short_candidates_sorted:  # 遍历空头候选
                    if candidate['ticker'] not in current_shorts and opened < to_close:  # 如果不在当前空头且需开仓
                        price = self.client.get_symbol_price(candidate['ticker'])  # 获取价格
                        qty = self.adjust_quantity(candidate['ticker'],
                                                   self.calculate_position_size(total_balance, price),
                                                   price) if price != 0.0 else 0  # 计算并调整数量
                        if qty != 0 and self.execute_trade(candidate['ticker'], "SELL", qty):  # 执行卖出开仓
                            opened += 1  # 已开仓计数器加 1
            elif short_count > long_count:  # 如果空头数量大于多头
                to_close = (short_count - long_count) // 2  # 计算需要平仓的空头数量
                current_shorts = {ticker for ticker, qty in current_positions.items() if qty < 0}  # 获取当前空头交易对
                short_candidate_tickers = {c['ticker'] for c in short_candidates}  # 获取空头候选交易对
                short_candidates_sorted = sorted(short_candidates, key=lambda x: x['id'])  # 按 ID 升序排序空头候选
                closed = 0  # 初始化已平仓计数器
                for ticker, qty in current_positions.items():  # 遍历当前持仓
                    if qty < 0 and ticker not in short_candidate_tickers and closed < to_close:  # 如果是空头且不在候选列表中
                        if self.execute_trade(ticker, "BUY", abs(qty)):  # 执行买入平仓
                            closed += 1  # 已平仓计数器加 1
                if closed < to_close:  # 如果仍需平仓
                    for candidate in short_candidates_sorted:  # 遍历排序后的空头候选
                        ticker = candidate['ticker']  # 获取交易对
                        if ticker in current_positions and current_positions[
                            ticker] < 0 and closed < to_close:  # 如果有持仓且需平仓
                            qty = current_positions[ticker]  # 获取持仓数量
                            if self.execute_trade(ticker, "BUY", abs(qty)):  # 执行买入平仓
                                closed += 1  # 已平仓计数器加 1

                long_candidates_sorted = sorted(long_candidates, key=lambda x: x['id'])  # 按 ID 升序排序多头候选
                current_longs = {ticker for ticker, qty in current_positions.items() if qty > 0}  # 获取当前多头交易对
                opened = 0  # 初始化已开仓计数器
                for candidate in long_candidates_sorted:  # 遍历多头候选
                    if candidate['ticker'] not in current_longs and opened < to_close:  # 如果不在当前多头且需开仓
                        price = self.client.get_symbol_price(candidate['ticker'])  # 获取价格
                        qty = self.adjust_quantity(candidate['ticker'],
                                                   self.calculate_position_size(total_balance, price),
                                                   price) if price != 0.0 else 0  # 计算并调整数量
                        if qty != 0 and self.execute_trade(candidate['ticker'], "BUY", qty):  # 执行买入开仓
                            opened += 1  # 已开仓计数器加 1

    def save_positions_to_csv(self, positions: List[Dict], run_id: str):
        try:
            date_str = datetime.now().strftime("%Y-%m-%d")
            run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data = []
            for pos in positions:
                qty = float(pos["positionAmt"])
                if qty != 0:
                    data.append({
                        "调仓日期": date_str,
                        "交易对": pos["symbol"],
                        "持仓数量": qty,
                        "入场价格": float(pos["entryPrice"]),
                        "运行时间": run_time,
                        "Run_ID": run_id  # 确保 Run_ID 始终写入
                    })
            if data:
                df_new = pd.DataFrame(data)
                if os.path.exists(self.positions_file):
                    try:
                        df_existing = pd.read_csv(self.positions_file)
                        # 确保现有文件有 Run_ID 列
                        if 'Run_ID' not in df_existing.columns:
                            df_existing['Run_ID'] = ''
                        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                        df_combined = df_combined.drop_duplicates(subset=["Run_ID", "交易对"], keep="last")
                        df_combined.to_csv(self.positions_file, index=False, encoding='utf-8')
                    except Exception as e:
                        self.logger.error(f"合并持仓数据失败: {str(e)}")
                        df_new.to_csv(self.positions_file, index=False, encoding='utf-8')
                else:
                    df_new.to_csv(self.positions_file, index=False, encoding='utf-8')
                self.logger.info(f"持仓数据已保存到 {self.positions_file}，记录数: {len(data)}，Run_ID: {run_id}")
                # 验证写入的数据
                df_verify = pd.read_csv(self.positions_file)
                run_id_records = df_verify[df_verify['Run_ID'] == run_id]
                self.logger.info(f"验证: {self.positions_file} 中 Run_ID={run_id} 的记录数: {len(run_id_records)}")
            else:
                self.logger.info("无有效持仓数据可保存")
        except Exception as e:
            self.logger.error(f"保存持仓到CSV失败: {str(e)}")

    def _load_account_metrics(self) -> pd.DataFrame:
        """加载 account_metrics.xlsx 文件"""
        account_metrics_file = "data/account_metrics.xlsx"
        try:
            if not os.path.exists(account_metrics_file):
                self.logger.info("account_metrics.xlsx 不存在，返回空 DataFrame")
                return pd.DataFrame()
            df = pd.read_excel(account_metrics_file, sheet_name='Account_Metrics')
            # 规范化 Date 列
            df['Date'] = pd.to_datetime(df['Date'].str.split('_').str[0], errors='coerce')
            return df if not df.empty else pd.DataFrame()
        except Exception as e:
            self.logger.error(f"加载 account_metrics.xlsx 失败: {str(e)}")
            return pd.DataFrame()

    def calculate_and_append_returns(self):
        """计算并追加调仓前/后回报率"""
        try:
            # 获取当前 run_id，优先从 after_trade_balance 获取
            run_id = self.account_metrics.get("after_trade_balance", {}).get("Run_ID",
                                                                             datetime.now().strftime("%Y%m%d%H%M%S"))
            current_date = pd.to_datetime(datetime.now().strftime('%Y-%m-%d'))

            # 加载历史数据
            df = self._load_account_metrics()
            if df.empty:
                self.logger.info("account_metrics.xlsx 为空或不存在，回报率设为 0")
                self.account_metrics["pre_rebalance_return"] = {
                    "value": "0.000000%",
                    "description": "调仓前回报率: 无历史数据",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                    "Run_ID": run_id
                }
                self.account_metrics["post_rebalance_return"] = {
                    "value": "0.000000%",
                    "description": "调仓后回报率: 无历史数据",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                    "Run_ID": run_id
                }
                return

            # 处理日期格式，兼容字符串和 datetime 类型
            def normalize_date(date_val):
                if pd.isna(date_val):
                    return pd.NaT
                if isinstance(date_val, (pd.Timestamp, datetime)):
                    return date_val
                if isinstance(date_val, str):
                    # 尝试处理字符串格式（YYYY-MM-DD_HH:MM:SS 或 YYYY-MM-DD）
                    try:
                        return pd.to_datetime(date_val.split('_')[0], errors='coerce')
                    except ValueError:
                        return pd.to_datetime(date_val, errors='coerce')
                return pd.NaT

            df['Date'] = df['Date'].apply(normalize_date)
            df = df.dropna(subset=['Date'])  # 移除无效日期记录

            # 筛选当前日期和前一天的数据
            prev_date = current_date - timedelta(days=1)
            current_day_data = df[df['Date'].dt.date == current_date.date()]
            prev_day_data = df[df['Date'].dt.date == prev_date.date()]

            # 获取当前余额数据，宽松匹配 Run_ID
            current_before = current_day_data[current_day_data['Metric'] == 'before_trade_balance']
            current_after = current_day_data[current_day_data['Metric'] == 'after_trade_balance']
            if not current_before.empty:
                current_before = current_before.sort_values('Record_Time').iloc[-1:]  # 取最新记录
            if not current_after.empty:
                current_after = current_after.sort_values('Record_Time').iloc[-1:]

            # 获取前一天的最后一条记录
            prev_before = prev_day_data[prev_day_data['Metric'] == 'before_trade_balance']
            prev_after = prev_day_data[prev_day_data['Metric'] == 'after_trade_balance']
            if not prev_before.empty:
                prev_before = prev_before.sort_values('Record_Time').iloc[-1:]
            if not prev_after.empty:
                prev_after = prev_after.sort_values('Record_Time').iloc[-1:]

            # 计算 Pre-rebalance return
            if not current_before.empty and not prev_after.empty:
                current_before_value = float(current_before['Value'].iloc[0])
                prev_after_value = float(prev_after['Value'].iloc[0])
                pre_rebalance_return = ((current_before_value - prev_after_value) / prev_after_value) * 100
                self.account_metrics["pre_rebalance_return"] = {
                    "value": f"{pre_rebalance_return:.6f}%",
                    "description": f"调仓前回报率: ({current_before_value} - {prev_after_value}) / {prev_after_value} * 100",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                    "Run_ID": run_id
                }
                self.logger.info(f"Pre-rebalance return: {pre_rebalance_return:.6f}%")
            else:
                self.account_metrics["pre_rebalance_return"] = {
                    "value": "0.000000%",
                    "description": f"调仓前回报率: 缺少 {current_date.date()} 的 before_trade_balance 或 {prev_date.date()} 的 after_trade_balance 数据",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                    "Run_ID": run_id
                }
                self.logger.warning(
                    f"无法计算 Pre-rebalance return: 缺少 {current_date.date()} 的 before_trade_balance 或 {prev_date.date()} 的 after_trade_balance 数据"
                )

            # 计算 Post-rebalance return
            if not current_after.empty and not prev_after.empty:
                current_after_value = float(current_after['Value'].iloc[0])
                prev_after_value = float(prev_after['Value'].iloc[0])
                post_rebalance_return = ((current_after_value - prev_after_value) / prev_after_value) * 100
                self.account_metrics["post_rebalance_return"] = {
                    "value": f"{post_rebalance_return:.6f}%",
                    "description": f"调仓后回报率: ({current_after_value} - {prev_after_value}) / {prev_after_value} * 100",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                    "Run_ID": run_id
                }
                self.logger.info(f"Post-rebalance return: {post_rebalance_return:.6f}%")
            else:
                self.account_metrics["post_rebalance_return"] = {
                    "value": "0.000000%",
                    "description": f"调仓后回报率: 缺少 {current_date.date()} 的 after_trade_balance 或 {prev_date.date()} 的 after_trade_balance 数据",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                    "Run_ID": run_id
                }
                self.logger.warning(
                    f"无法计算 Post-rebalance return: 缺少 {current_date.date()} 的 after_trade_balance 或 {prev_date.date()} 的 after_trade_balance 数据"
                )

        except Exception as e:
            self.logger.error(f"计算回报率失败: {str(e)}")
            self.account_metrics["pre_rebalance_return"] = {
                "value": "0.000000%",
                "description": f"调仓前回报率: 计算失败 ({str(e)})",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                "Run_ID": run_id
            }
            self.account_metrics["post_rebalance_return"] = {
                "value": "0.000000%",
                "description": f"调仓后回报率: 计算失败 ({str(e)})",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                "Run_ID": run_id
            }

    def save_to_json(self, date_str: str, run_id: str):
        """将账户信息保存为JSON文件"""
        try:
            # 通过API获取最新的账户信息
            account_info = self.client.get_account_info()
            positions = self.client.get_position_info()

            # 构建与原来相同格式的数据
            json_data = {
                "account_info": account_info,
                "positions": positions,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "run_id": run_id
            }

            # 确保data目录存在
            os.makedirs("data", exist_ok=True)

            # 生成文件名
            filename = f"data/account_info_{date_str.replace('-', '')}_{run_id.split('_')[-1]}.json"

            # 写入文件
            with open(filename, "w") as f:
                json.dump(json_data, f, indent=4)

            self.logger.info(f"账户信息已保存为JSON文件: {filename}")
        except Exception as e:
            self.logger.error(f"保存账户信息到JSON文件失败: {str(e)}")

    def run(self, date_str: str, run_id: str) -> Dict[str, str]:
        """
        执行交易引擎主逻辑
        Args:
            date_str: 日期字符串 (格式: YYYY-MM-DD)
            run_id: 本次运行的唯一标识符
        Returns:
            错误原因字典 (key为错误类型，value为详情)
        """
        self.error_reasons = {}
        start_time = int(time.time() * 1000)  # 记录调仓开始时间（毫秒）
        try:
            # ==================== 初始化阶段 ====================
            self.logger.info(f"🚀 开始执行交易引擎 | Date: {date_str} | RunID: {run_id}")
            self.cancel_all_open_orders()

            # ==================== 数据加载阶段 ====================
            funding_file = f'data/pos{date_str.replace("-", "")}_v3.csv'
            if not os.path.exists(funding_file):
                msg = f"资金费率文件不存在: {funding_file}"
                self.logger.error(msg)
                self.error_reasons["file_not_found"] = msg
                return self.error_reasons

            # 设置监控指标
            self.account_metrics["position_file"] = {
                "value": os.path.basename(funding_file),
                "description": "调仓文件",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }

            # ==================== 黑名单处理阶段 ====================
            df = pd.read_csv(funding_file)
            blacklist = self.load_blacklist()
            blacklisted_tickers = sorted(set(df['ticker']) & blacklist)

            if blacklisted_tickers:
                tickers_str = ", ".join(blacklisted_tickers)
                self.error_reasons["blacklisted_tickers"] = tickers_str
                self.logger.info(
                    "📋 黑名单过滤结果\n"
                    f"├─ 被过滤币种 ({len(blacklisted_tickers)}个): {tickers_str}\n"
                    f"└─ 剩余候选池: {len(df) - len(blacklisted_tickers)}/{len(df)}"
                )

            # ==================== 候选列表生成 ====================
            long_candidates = df[
                (df['fundingRate'] < 0) &
                (~df['ticker'].isin(blacklist))
                ][['ticker', 'fundingRate', 'id']].to_dict('records')

            short_candidates = df[
                (df['fundingRate'] > 0) &
                (~df['ticker'].isin(blacklist))
                ][['ticker', 'fundingRate', 'id']].to_dict('records')

            short_candidates.sort(key=lambda x: x['id'], reverse=True)

            self.logger.info(
                "🔍 有效候选列表\n"
                f"├─ 多头候选: {len(long_candidates)}个\n"
                f"└─ 空头候选: {len(short_candidates)}个"
            )

            # ==================== 记录调仓前余额 ====================
            account_info = self.client.get_account_info()
            before_trade_balance = float(account_info["totalMarginBalance"])
            before_available_balance = float(account_info["availableBalance"])
            self.account_metrics["before_trade_balance"] = {
                "value": before_trade_balance,
                "description": "调仓前账户总保证金余额(totalMarginBalance)",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }
            self.account_metrics["before_available_balance"] = {
                "value": before_available_balance,
                "description": "调仓前可用保证金余额(available_balance)",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }

            # ==================== 仓位调整阶段 ====================
            try:
                self.adjust_or_open_positions(long_candidates, short_candidates, run_id, date_str)
            except Exception as e:
                error_msg = f"仓位调整失败: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                self.error_reasons["position_adjustment_failed"] = error_msg

            # ==================== 记录调仓后余额和交易记录 ====================
            end_time = int(time.time() * 1000)  # 记录调仓结束时间（毫秒）
            account_info = self.client.get_account_info()
            after_trade_balance = float(account_info["totalMarginBalance"])
            after_available_balance = float(account_info["availableBalance"])

            # 计算余额损失
            balance_loss = after_trade_balance - before_trade_balance
            balance_loss_rate = (balance_loss / before_trade_balance * 100) if before_trade_balance != 0 else 0

            # 获取 BTC/USDT 价格
            btc_price = float(self.client.get_symbol_price('BTCUSDT'))

            # 获取所有交易对的交易历史
            all_symbols = list(set([c['ticker'] for c in long_candidates + short_candidates]))
            trade_records = []
            total_commission = 0
            total_realized_pnl = 0

            for symbol in all_symbols:
                trades = self.client.get_trade_history(symbol, start_time, end_time)
                for trade in trades:
                    real_quote = trade['quoteQty'] / self.leverage  # 假设杠杆为 self.leverage
                    trade_record = {
                        'side': trade['side'],
                        'symbol': trade['symbol'],
                        'total_qty': trade['quantity'],
                        'price': trade['price'],
                        'total_quote': trade['quoteQty'],
                        'real_quote': real_quote,
                        'order_id': trade['orderId']
                    }
                    trade_records.append((trade['symbol'], trade['side'], trade['orderId'], trade_record))
                    total_commission += trade['commission']
                    total_realized_pnl += trade['realizedPnl']

            # 计算手续费和盈亏占比
            commission_ratio = (total_commission / before_trade_balance * 100) if before_trade_balance != 0 else 0
            realized_pnl_ratio = (total_realized_pnl / before_trade_balance * 100) if before_trade_balance != 0 else 0

            # 格式化 account_info JSON
            current_date = datetime.now().strftime('%Y-%m-%d')
            account_info_data = {
                'position_file': {
                    'value': os.path.basename(funding_file),
                    'description': '调仓文件',
                    'date': datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                },
                'before_trade_balance': {
                    'value': before_trade_balance,
                    'description': '调仓前账户总保证金余额(totalMarginBalance)',
                    'date': datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                },
                'before_available_balance': {
                    'value': before_available_balance,
                    'description': '调仓前可用保证金余额(available_balance)',
                    'date': datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                },
                'after_trade_balance': {
                    'value': after_trade_balance,
                    'description': '调仓后账户总保证金余额(totalMarginBalance)',
                    'date': datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                },
                'balance_loss': {
                    'value': balance_loss,
                    'description': '调仓前后余额损失金额',
                    'date': datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                },
                'balance_loss_rate': {
                    'value': f"{balance_loss_rate:.6f}%",
                    'description': '调仓前后余额损失率 (%)',
                    'date': datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                },
                'after_available_balance': {
                    'value': after_available_balance,
                    'description': '调仓后可用余额',
                    'date': datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                },
                'btc_usdt_price': {
                    'value': btc_price,
                    'description': f'当前btc_usdt_price:{btc_price}',
                    'date': datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                },
                f'trade_commission_summary_{current_date}': {
                    'value': total_commission,
                    'description': f"{current_date} 买卖交易手续费总和",
                    'date': datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                },
                f'trade_commission_summary_ratio_{current_date}': {
                    'value': f"{commission_ratio:.6f}%",
                    'description': f"{current_date} 买卖交易总手续费占比",
                    'date': datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                },
                f'trade_realized_pnl_summary_{current_date}': {
                    'value': total_realized_pnl,
                    'description': f"{current_date} 买卖交易已实现盈亏总和",
                    'date': datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                },
                f'trade_realized_pnl_summary_ratio_{current_date}': {
                    'value': f"{realized_pnl_ratio:.6f}%",
                    'description': f"{current_date} 买卖交易总盈亏占比",
                    'date': datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                }
            }

            # 添加交易记录 - 直接从account_metrics中获取所有交易记录
            for key, value in self.account_metrics.items():
                if key.startswith("trade_") and current_date in key:
                    account_info_data[key] = value

            # 保存 account_info 到 JSON 文件
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'data/account_info_{timestamp}.json'
            os.makedirs("data", exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(account_info_data, f, ensure_ascii=False, indent=4)
            self.logger.info(f"账户信息已保存为JSON文件: {output_file}")

            # ==================== 结果持久化 ====================
            self.calculate_and_append_returns()  # 添加回报率计算
            self.write_to_excel(run_id=run_id)
            # 移除对save_to_json的调用，避免生成额外的JSON文件
            # self.save_to_json(date_str, run_id)  # 这行被注释掉
            self.logger.info(f"✅ 交易引擎执行完成 | RunID: {run_id}")

        except Exception as e:
            error_msg = f"交易引擎执行异常: {str(e)}"
            self.logger.critical(error_msg, exc_info=True)
            # 仅在必要时保存持仓
            if "position_file" not in self.account_metrics:
                try:
                    positions = self.client.client.futures_position_information()
                    self.save_positions_to_csv(positions, run_id)
                except Exception as e:
                    self.logger.error(f"持仓信息保存失败: {str(e)}")
            self.write_to_excel(run_id=run_id)
            # 移除对save_to_json的调用，避免生成额外的JSON文件
            # self.save_to_json(date_str, run_id)  # 这行被注释掉
            self.error_reasons["system_error"] = error_msg

        return self.error_reasons

    def load_blacklist(self) -> Set[str]:
        """加载黑名单列表"""
        blacklist_path = "data/blacklist.csv"
        try:
            if os.path.exists(blacklist_path):
                return set(pd.read_csv(blacklist_path)['ticker'].tolist())
            self.logger.info("未检测到黑名单文件，跳过过滤")
            return set()
        except Exception as e:
            self.logger.error(f"黑名单加载异常: {str(e)}")
            return set()

    def get_stable_positions(self) -> Dict[str, float]:
        """获取稳定的持仓信息（统一返回字典格式）"""
        try:
            positions = self.client.get_position_info()

            # 如果返回的是列表，转换为字典
            if isinstance(positions, list):
                return {
                    item["symbol"]: float(item["positionAmt"])
                    for item in positions
                    if float(item["positionAmt"]) != 0
                }

            # 如果已经是字典格式，直接返回
            elif isinstance(positions, dict):
                return {k: float(v) for k, v in positions.items()}

            else:
                raise ValueError(f"未知的持仓数据格式: {type(positions)}")

        except Exception as e:
            self.logger.error(f"获取稳定持仓失败: {str(e)}")
            return {}  # 返回空字典def get_current_positions(self) -> Dict[str, float]:


if __name__ == "__main__":
    logger = setup_logger("../logs/trading.log")  # 设置日志记录器
    from config_loader import ConfigLoader  # 导入配置加载器

    config_loader = ConfigLoader()  # 创建配置加载器实例
    api_config = config_loader.get_api_config()  # 获取 API 配置
    trading_config = config_loader.get_trading_config()  # 获取交易配置
    paths_config = config_loader.get_paths_config()  # 获取路径配置
    config = {**api_config, **trading_config, **paths_config}  # 合并所有配置
    client = BinanceFuturesClient(api_config["api_key"], api_config["api_secret"], api_config["test_net"] == "True",
                                  logger)  # 创建 Binance 客户端实例
    engine = TradingEngine(client, config, logger)  # 创建交易引擎实例
    engine.run("20250324")  # 运行交易引擎，指定日期为 2025-03-24