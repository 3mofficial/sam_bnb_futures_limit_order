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
        self.account_metrics = {}  # 初始化账户指标字典
        self.trade_start_time = None  # 初始化交易开始时间戳
        self.trade_end_time = None  # 初始化交易结束时间戳
        self.pending_orders = []  # 初始化挂单列表，存储未成交的订单
        self.failed_depth_tickers = set()  # 初始化失败交易对集合，记录获取深度失败的交易对
        self.error_reasons = {}  # 初始化错误原因字典，记录交易失败原因，格式为 {ticker: reason}
        self.is_first_run = self._check_first_run()  # 检查是否为首次运行，决定初始化逻辑
        self.positions_file = 'data/positions_output.csv'  # 设置持仓文件路径
        if not os.path.exists(self.positions_file):  # 检查持仓文件是否存在
            os.makedirs(os.path.dirname(self.positions_file), exist_ok=True)  # 创建文件所在目录
            df = pd.DataFrame(columns=['调仓日期', '交易对', '持仓数量', '入场价格', 'Run_ID', '运行时间'])  # 创建空的持仓数据框架
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

            data = []
            record_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 标准格式
            run_id = run_id or f"{record_time.replace(' ', '_').replace(':', '')}"
            for metric in required_metrics:
                if metric in self.account_metrics:
                    date_str = self.account_metrics[metric]["date"]
                    try:
                        normalized_date = self.normalize_date_time(date_str).strftime('%Y-%m-%d')
                    except Exception as e:
                        self.logger.warning(f"规范化日期失败: {date_str}, 使用原始值: {str(e)}")
                        normalized_date = date_str
                    entry = {
                        "Metric": metric,
                        "Value": self.account_metrics[metric]["value"],
                        "Description": self.account_metrics[metric]["description"],
                        "Date": normalized_date,  # 使用规范化后的日期
                        "Record_Time": record_time,
                        "Run_ID": run_id
                    }
                    data.append(entry)

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
                        
                        # 统一格式化Date列为YYYY-MM-DD格式
                        try:
                            df_combined['Date'] = pd.to_datetime(df_combined['Date'], format='mixed', errors='coerce').dt.strftime('%Y-%m-%d')
                            # 确保 Record_Time 列保留完整的时间信息
                            if 'Record_Time' in df_combined.columns:
                                df_combined['Record_Time'] = pd.to_datetime(df_combined['Record_Time'], format='mixed', errors='coerce')
                        except Exception as e:
                            self.logger.warning(f"格式化Date列失败: {str(e)}")
                            
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

    def datetime_to_timestamp(self, date_time_str) -> int:
        """将日期时间字符串转换为Unix时间戳（毫秒）"""
        try:
            if date_time_str is None:
                self.logger.error("日期时间字符串为None")
                raise ValueError("日期时间字符串不能为None")
                
            # 添加详细日志，显示原始输入
            self.logger.debug(f"尝试转换日期时间: '{date_time_str}', 类型: {type(date_time_str)}")
            
            if isinstance(date_time_str, (int, float)):
                # 如果已经是数字，假设是秒级时间戳
                return int(date_time_str) * 1000
                
            if isinstance(date_time_str, datetime):
                # 如果是datetime对象
                timestamp = int(date_time_str.timestamp() * 1000)
                self.logger.debug(f"datetime对象转换为时间戳: {timestamp}，对应时间: {date_time_str}")
                return timestamp
                
            # 尝试多种格式解析字符串
            formats = [
                '%Y-%m-%d_%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%Y/%m/%d %H:%M:%S',
                '%Y-%m-%d',
                '%Y/%m/%d',
                '%Y-%m-%m-%d_%H:%M:%S'  # 添加错误格式以兼容现有数据
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_time_str, fmt)
                    timestamp = int(dt.timestamp() * 1000)
                    self.logger.debug(f"成功转换时间戳，格式: {fmt}, 结果: {timestamp}, 对应时间: {dt}")
                    return timestamp
                except ValueError:
                    continue
                    
            # 尝试使用pandas解析
            try:
                dt = pd.to_datetime(date_time_str)
                timestamp = int(dt.timestamp() * 1000)
                self.logger.debug(f"使用pandas成功转换时间戳，结果: {timestamp}, 对应时间: {dt}")
                return timestamp
            except Exception as e:
                self.logger.warning(f"pandas转换失败: {str(e)}")
                
            # 所有方法都失败，记录详细错误
            self.logger.error(f"无法解析日期时间格式: '{date_time_str}'，尝试了所有支持的格式")
            # 返回当前时间戳作为后备方案，而不是抛出异常
            current_timestamp = int(time.time() * 1000)
            self.logger.warning(f"使用当前时间戳作为后备方案: {current_timestamp}")
            return current_timestamp
        except Exception as e:
            self.logger.error(f"时间戳转换失败: {str(e)}，输入: '{date_time_str}'")
            # 返回当前时间戳作为后备方案，而不是抛出异常
            current_timestamp = int(time.time() * 1000)
            self.logger.warning(f"使用当前时间戳作为后备方案: {current_timestamp}")
            return current_timestamp

    def process_trade_commissions(self):
        """通过API获取指定日期的手续费总和"""
        try:
            current_date = datetime.now().strftime('%Y-%m-%d')  # 获取当前日期
            commission_key = f"trade_commission_summary_{current_date}"  # 生成手续费总和键
            
            # 使用交易的实际开始和结束时间戳，而不是依赖account_metrics中的date字段
            if hasattr(self, 'trade_start_time') and hasattr(self, 'trade_end_time'):
                start_time = self.trade_start_time
                end_time = self.trade_end_time
                self.logger.info(f"使用实际交易时间戳: start_time={start_time}, end_time={end_time}")
                
                # 添加调试日志，显示对应的日期时间
                start_dt = datetime.fromtimestamp(start_time/1000)
                end_dt = datetime.fromtimestamp(end_time/1000)
                self.logger.debug(f"交易时间戳对应的时间: start={start_dt} - end={end_dt}")
            else:
                # 如果没有实际交易时间戳，回退到使用account_metrics中的date字段
                start_time_str = self.account_metrics.get("before_trade_balance", {}).get("date")  # 获取调仓前时间
                end_time_str = self.account_metrics.get("after_trade_balance", {}).get("date")  # 获取调仓后时间

                # 添加调试日志，显示原始时间字符串
                self.logger.debug(f"手续费计算时间范围原始字符串: start_time_str='{start_time_str}', end_time_str='{end_time_str}'")

                if not start_time_str or not end_time_str:  # 如果缺少时间信息
                    self.logger.warning(f"缺少调仓时间信息，无法获取手续费: start={start_time_str}, end={end_time_str}")  # 记录警告日志
                    self.account_metrics[commission_key] = {
                        "value": 0.0,  # 设置默认值
                        "description": f"{current_date} 买卖交易手续费总和（缺少时间信息）",  # 设置描述
                        "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                    }
                    return self.account_metrics[commission_key]  # 返回手续费记录

                try:
                    # 转换时间戳前记录原始值
                    self.logger.info(f"尝试转换时间戳: start_time_str='{start_time_str}', end_time_str='{end_time_str}'")
                    
                    start_time = self.datetime_to_timestamp(start_time_str)  # 转换为开始时间戳
                    end_time = self.datetime_to_timestamp(end_time_str)  # 转换为结束时间戳
                    
                    # 添加调试日志，显示转换后的时间戳和对应的日期时间
                    start_dt = datetime.fromtimestamp(start_time/1000)
                    end_dt = datetime.fromtimestamp(end_time/1000)
                    self.logger.debug(f"转换后的时间戳: start={start_time} ({start_dt}) - end={end_time} ({end_dt})")
                    self.logger.info(f"获取手续费记录，时间范围: {start_time} ({start_dt}) - {end_time} ({end_dt})")
                    
                    if start_time >= end_time:  # 如果时间范围无效
                        self.logger.warning(f"无效时间范围: 开始时间 '{start_time_str}' ({start_time}) 不早于结束时间 '{end_time_str}' ({end_time})")
                        self.account_metrics[commission_key] = {
                            "value": 0.0,  # 设置默认值
                            "description": f"{current_date} 买卖交易手续费总和（无效时间范围）",  # 设置描述
                            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                        }
                        return self.account_metrics[commission_key]  # 返回手续费记录
                except Exception as e:
                    self.logger.error(f"时间戳转换失败: {str(e)}")
                    self.account_metrics[commission_key] = {
                        "value": 0.0,  # 设置默认值
                        "description": f"{current_date} 买卖交易手续费总和（时间戳转换失败）",  # 设置描述
                        "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录时间
                    }
                    return self.account_metrics[commission_key]  # 返回手续费记录

            # 使用新的API获取手续费记录，支持分页
            records = []
            has_more = True
            current_start_time = start_time
            max_retries = 3
            
            while has_more and current_start_time < end_time:
                for retry in range(max_retries):
                    try:
                        self.logger.debug(f"请求手续费记录: start_time={current_start_time}, end_time={end_time}")
                        batch_records = self.client.get_commission_history(start_time_ms=current_start_time, end_time_ms=end_time, limit=1000)
                        self.logger.debug(f"API响应: 获取到 {len(batch_records)} 条记录")
                        
                        if not batch_records:
                            self.logger.info(f"API返回空记录，可能该时间段内没有交易")
                            has_more = False
                            break
                            
                        records.extend(batch_records)
                        
                        # 更新时间戳，从最后一条记录的时间开始
                        if batch_records:
                            last_record_time = int(batch_records[-1].get('time', current_start_time))
                            current_start_time = last_record_time + 1
                            if len(batch_records) < 1000:
                                has_more = False
                        else:
                            has_more = False
                        break  # 成功获取数据，跳出重试循环
                    except Exception as e:
                        self.logger.warning(f"获取手续费记录批次失败 (尝试 {retry+1}/{max_retries}): {str(e)}")
                        if retry == max_retries - 1:  # 最后一次重试
                            self.logger.error(f"多次尝试后仍无法获取手续费记录: {str(e)}")
                            self.logger.exception("详细错误信息:")
                            has_more = False
                        else:
                            time.sleep(1)  # 等待1秒后重试
            
            self.logger.info(f"获取到 {len(records)} 条手续费记录")
            
            if len(records) > 0:
                self.logger.debug(f"手续费记录示例: {records[0]}")
            
            total_commission = self.client.calculate_total_commission(records)  # 计算总手续费

            self.account_metrics[commission_key] = {
                "value": total_commission,  # 保存总手续费
                "description": "买卖交易手续费总和",  # 设置描述
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),  # 记录时间
                "records_count": len(records)  # 记录数量
            }
            self.logger.info(f"手续费汇总 ({commission_key}): {total_commission} USDT, 记录数: {len(records)}")  # 记录手续费汇总日志
            return self.account_metrics[commission_key]  # 返回手续费记录
        except Exception as e:
            self.logger.error(f"获取手续费失败: {str(e)}")
            self.logger.exception("详细错误信息:")
            self.account_metrics[commission_key] = {
                "value": 0.0,  # 设置默认值
                "description": f"{current_date} 买卖交易手续费总和（未计算）",  # 设置描述
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),  # 记录时间
                "error": str(e)  # 错误信息
            }
            return self.account_metrics[commission_key]  # 返回手续费记录


    def process_trade_realized_pnl(self):
        try:
            current_date_str = datetime.now().strftime("%Y-%m-%d")
            pnl_key = f'trade_realized_pnl_summary_{current_date_str}'
            
            # 使用交易的实际开始和结束时间戳，而不是依赖当天的时间范围
            if hasattr(self, 'trade_start_time') and hasattr(self, 'trade_end_time'):
                start_time_ms = self.trade_start_time
                end_time_ms = self.trade_end_time
                self.logger.info(f"使用实际交易时间戳: start_time={start_time_ms}, end_time={end_time_ms}")
                
                # 添加调试日志，显示对应的日期时间
                start_dt = datetime.fromtimestamp(start_time_ms/1000)
                end_dt = datetime.fromtimestamp(end_time_ms/1000)
                self.logger.debug(f"交易时间戳对应的时间: start={start_dt} - end={end_dt}")
            else:
                # 如果没有实际交易时间戳，回退到使用当天的时间范围
                today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                start_time_ms = int(today.timestamp() * 1000)
                end_time_ms = int((today.replace(hour=23, minute=59, second=59) + timedelta(days=1)).timestamp() * 1000)
                self.logger.info(f"使用当天时间范围: start_time={start_time_ms}, end_time={end_time_ms}")
            
            self.logger.info(f"获取已实现盈亏记录，时间范围: {start_time_ms} ({datetime.fromtimestamp(start_time_ms/1000)}) - {end_time_ms} ({datetime.fromtimestamp(end_time_ms/1000)})")
            
            # 获取已实现盈亏记录
            pnl_history = self.client.get_realized_pnl_history(start_time_ms=start_time_ms, end_time_ms=end_time_ms)
            self.logger.info(f"获取到 {len(pnl_history)} 条已实现盈亏记录")
            
            if len(pnl_history) > 0:
                self.logger.debug(f"已实现盈亏记录示例: {pnl_history[0]}")
            
            # 计算总已实现盈亏
            total_realized_pnl = self.client.calculate_total_realized_pnl(pnl_history, 'USDT')
            self.account_metrics[pnl_key] = {
                "value": total_realized_pnl,
                "description": "买卖交易已实现盈亏总和",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                "records_count": len(pnl_history)
            }
            self.logger.info(f"Processed realized PnL for {current_date_str}. Total: {total_realized_pnl} USDT, Records: {len(pnl_history)}")
            return self.account_metrics[pnl_key]
        except Exception as e:
            self.logger.error(f"Error in process_trade_realized_pnl: {e}")
            self.logger.exception("详细错误信息:")
            self.account_metrics[pnl_key] = {
                "value": 0,
                "description": f"{current_date_str} 买卖交易已实现盈亏总和（未计算）",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                "error": str(e)
            }
            return self.account_metrics[pnl_key]

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

            # 从self.account_metrics获取调仓前余额信息，而不是直接使用变量
            before_trade_balance = self.account_metrics.get("before_trade_balance", {}).get("value", total_balance)
            before_available_balance = self.account_metrics.get("before_available_balance", {}).get("value", available_balance)
            
            # 记录调仓前账户信息
            self.account_metrics["before_trade_balance"] = {
                "value": before_trade_balance,  # 保存调仓前总余额
                "description": "调仓前账户总保证金余额(totalMarginBalance)",  # 设置描述
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 修复日期格式
            }
            self.account_metrics["before_available_balance"] = {
                "value": before_available_balance,  # 保存调仓前可用余额
                "description": "调仓前可用保证金余额(available_balance)",  # 设置描述
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 修复日期格式
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
                }
                for ticker, qty in close_orders.items():  # 遍历平仓订单
                    if qty == 0 or ticker in adjusted_to_zero:  # 如果数量为 0 或已调整为零
                        self.logger.info(f"第 {round_num} 轮: 跳过 {ticker} 平仓，原因：数量为0或已调整为零")
                        continue  # 跳过
                    side = "BUY" if qty > 0 or current_positions[ticker] < 0 else "SELL"  # 确定平仓方向
                    qty_to_close = abs(qty) if abs(qty) < abs(current_positions[ticker]) else abs(
                        current_positions[ticker])  # 计算平仓数量
                    self.logger.info(f"第 {round_num} 轮: 处理平仓 {ticker} {side}, 需求数量={qty_to_close}")
                    price = self.get_postonly_price(ticker, side)  # 获取限价单价格
                    if price == 0.0:  # 如果价格获取失败
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 获取一档价格失败，跳过")
                        self.error_reasons[ticker] = "获取价格失败"
                        continue  # 跳过
                    adjusted_qty = self.adjust_quantity(ticker, qty_to_close, price, is_close=True)  # 调整平仓数量
                    self.logger.info(f"第 {round_num} 轮: {ticker} 平仓调整后数量={adjusted_qty}")
                    if adjusted_qty <= 0:  # 如果调整后数量无效
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 调整数量为 0，跳过")
                        self.error_reasons[ticker] = "数量调整为 0"
                        continue  # 跳过
                    # 平仓无需检查余额，因为释放保证金
                    self.logger.info(f"第 {round_num} 轮: {ticker} 为平仓操作，跳过余额检查")

                    # 在下单前检查是否存在未成交订单
                    if self.check_existing_orders(ticker, side):
                        self.logger.info(f"第 {round_num} 轮: {ticker} {side} 已存在未成交订单，跳过平仓下单")
                        continue  # 如果存在未成交订单，跳过下单

                    self.logger.info(
                        f"第 {round_num} 轮: 准备平仓挂单 {ticker} {side}, 数量={adjusted_qty}, 价格={price}, is_close=True")
                    success, order, order_id, trade_details = self.client.place_postonly_order(ticker, side,
                                                                                               adjusted_qty, price,
                                                                                               is_close=True)  # 下平仓限价单
                    self.logger.info(
                        f"第 {round_num} 轮: {ticker} 平仓挂单结果 success={success}, order_id={order_id}, order={order}")
                    if success:  # 如果挂单成功
                        pending_orders.append((ticker, order_id, side, adjusted_qty))  # 添加到挂单列表
                        self.logger.info(
                            f"第 {round_num} 轮: {ticker} {side} postOnly 平仓挂单成功，订单ID={order_id}, is_close=True")
                        processed_tickers.add(ticker)  # 添加到已处理交易对
                        current_positions[ticker] = current_positions.get(ticker, 0) + (
                            adjusted_qty if side == "BUY" else -adjusted_qty)  # 更新持仓
                        all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(ticker,
                                                                                                      0)  # 更新订单需求
                    else:
                        error_msg = str(order) if order else "未知错误"  # 获取错误信息
                        self.error_reasons[ticker] = f"下单失败: {error_msg}"  # 记录错误原因
                        self.handle_postonly_error(round_num, ticker, side, qty_to_close, error_msg,
                                                   pending_orders, is_close=True)  # 处理挂单失败

                # 处理开仓订单
                open_orders = {ticker: qty for ticker, qty in all_orders.items() if
                               ticker not in adjusted_to_zero}  # 筛选开仓订单
                self.logger.info(f"第 {round_num} 轮开仓前可用余额: {available_balance}, 开仓需求: {open_orders}")
                for ticker, qty in open_orders.items():  # 遍历开仓订单
                    if qty == 0 or ticker in processed_tickers:  # 如果数量为 0 或已处理
                        self.logger.info(f"第 {round_num} 轮: 跳过 {ticker} 开仓，原因：数量为0或已处理")
                        continue  # 跳过
                    side = "BUY" if qty > 0 else "SELL"  # 确定开仓方向
                    self.logger.info(f"第 {round_num} 轮: 处理开仓 {ticker} {side}, 需求数量={qty}")
                    price = self.get_postonly_price(ticker, side)  # 获取限价单价格
                    if price == 0.0:  # 如果价格获取失败
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 获取一档价格失败，跳过")
                        self.error_reasons[ticker] = "获取价格失败"
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
                        f"第 {round_num} 轮: {ticker} 开仓调整后数量={adjusted_qty}, is_reverse_open={is_reverse_open}")
                    if adjusted_qty <= 0:  # 如果调整后数量无效
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 调整数量为 0，跳过")
                        self.error_reasons[ticker] = "数量调整为 0"
                        continue  # 跳过
                    # 对于反向开仓（包括平仓），跳过余额检查
                    if not is_reverse_open:  # 如果不是反向开仓
                        margin_required = (adjusted_qty * price) / self.leverage  # 计算所需保证金
                        self.logger.info(
                            f"第 {round_num} 轮: {ticker} 开仓保证金需求={margin_required}, 可用余额={available_balance}")
                        if available_balance < margin_required:  # 如果余额不足
                            self.logger.info(
                                f"第 {round_num} 轮: 可用余额 {available_balance} < 需求 {margin_required}，跳过 {ticker}")
                            self.error_reasons[ticker] = f"余额不足: 可用 {available_balance} < 需求 {margin_required}"
                            continue  # 跳过
                    else:
                        self.logger.info(f"第 {round_num} 轮: {ticker} 为反向开仓或平仓，跳过余额检查")

                    # 在下单前检查是否存在未成交订单
                    if self.check_existing_orders(ticker, side):
                        self.logger.info(f"第 {round_num} 轮: {ticker} {side} 已存在未成交订单，跳过开仓下单")
                        continue  # 如果存在未成交订单，跳过下单

                    self.logger.info(
                        f"第 {round_num} 轮: 准备开仓挂单 {ticker} {side}, 数量={adjusted_qty}, 价格={price}, is_close={is_reverse_open}")
                    success, order, order_id, trade_details = self.client.place_postonly_order(
                        ticker, side, adjusted_qty, price, is_close=is_reverse_open)  # 下开仓限价单
                    self.logger.info(
                        f"第 {round_num} 轮: {ticker} 开仓挂单结果 success={success}, order_id={order_id}, order={order}")
                    if success:  # 如果挂单成功
                        pending_orders.append((ticker, order_id, side, adjusted_qty))  # 添加到挂单列表
                        self.logger.info(
                            f"第 {round_num} 轮: {ticker} {side} postOnly 开仓挂单成功，订单ID={order_id}, is_close={is_reverse_open}")
                        processed_tickers.add(ticker)  # 添加到已处理交易对
                        current_positions[ticker] = current_positions.get(ticker, 0) + (
                            adjusted_qty if side == "BUY" else -adjusted_qty)  # 更新持仓
                        all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(ticker,
                                                                                                      0)  # 更新订单需求
                        if not is_reverse_open:  # 如果不是反向开仓
                            available_balance -= (adjusted_qty * price) / self.leverage  # 更新可用余额
                            self.logger.info(
                                f"第 {round_num} 轮: {ticker} 开仓后更新余额: {available_balance}")
                    else:
                        error_msg = str(order) if order else "未知错误"  # 获取错误信息
                        self.error_reasons[ticker] = f"下单失败: {error_msg}"  # 记录错误原因
                        self.handle_postonly_error(round_num, ticker, side, abs(qty), error_msg,
                                                   pending_orders, is_close=is_reverse_open)  # 处理挂单失败

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
                        # 首先获取交易所中所有未成交订单
                        try:
                            open_orders = self.client.client.futures_get_open_orders()  # 获取所有未成交订单
                            open_order_ids = {order['orderId'] for order in open_orders}  # 提取订单ID集合
                            self.logger.info(f"第 {round_num} 轮: 发现 {len(open_orders)} 个未成交订单")
                        except Exception as e:
                            self.logger.error(f"第 {round_num} 轮: 获取未成交订单失败: {str(e)}")
                            open_order_ids = set()

                        # 取消程序记录的挂单
                        for ticker, order_id, side, qty in pending_orders:
                            if order_id in open_order_ids:  # 仅取消仍存在于交易所的订单
                                try:
                                    self.client.cancel_order(ticker, order_id)  # 取消订单
                                    self.logger.info(
                                        f"第 {round_num} 轮: 成功取消 {ticker} {side} 挂单，订单ID={order_id}")
                                except Exception as e:
                                    self.logger.error(
                                        f"第 {round_num} 轮: 取消 {ticker} 挂单失败，订单ID={order_id}，错误: {str(e)}")
                            else:
                                self.logger.info(
                                    f"第 {round_num} 轮: 订单 {ticker} {side} (ID={order_id}) 已不在未成交列表中，跳过取消")

                        # 取消其他未记录的挂单（以防遗漏）
                        for order in open_orders:
                            order_id = order['orderId']
                            ticker = order['symbol']
                            side = order['side']
                            if order_id not in {o[1] for o in pending_orders}:  # 如果订单不在 pending_orders 中
                                try:
                                    self.client.cancel_order(ticker, order_id)
                                    self.logger.info(
                                        f"第 {round_num} 轮: 成功取消未记录的 {ticker} {side} 挂单，订单ID={order_id}")
                                except Exception as e:
                                    self.logger.error(
                                        f"第 {round_num} 轮: 取消未记录的 {ticker} 挂单失败，订单ID={order_id}，错误: {str(e)}")

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
                            # 判断是否为平仓操作
                            is_close_order = (ticker in current_positions and current_positions[ticker] != 0 and 
                                             (ticker not in self.final_target or self.final_target.get(ticker, 0) == 0 or 
                                              (self.final_target.get(ticker, 0) * current_positions[ticker] < 0)))
                            
                            adjusted_qty = self.adjust_quantity(ticker, abs(qty), price, is_close=is_close_order)  # 调整数量
                            is_open = ticker in self.final_target and self.final_target[ticker] != 0  # 判断是否为开仓
                            notional = adjusted_qty * price  # 计算名义价值
                            
                            # 只对开仓订单进行小额调整检查，平仓订单不检查
                            if is_open and not is_close_order and (adjusted_qty == 0 or notional < 5):  # 如果是小额开仓调整
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

    def check_existing_orders(self, ticker, side):
        """
        检查是否存在未成交订单，避免重复下单
        :param ticker: 交易对 (如 XRPUSDT)
        :param side: 方向 (BUY 或 SELL)
        :return: 如果存在未成交订单，返回 True；否则返回 False
        """
        try:
            self.logger.debug(f"检查 {ticker} {side} 未成交订单: 开始查询 pending_orders 列表 (当前大小: {len(self.pending_orders)})...")
            # 检查 pending_orders 列表
            for i, (pending_ticker, order_id, pending_side, qty) in enumerate(self.pending_orders):
                if pending_ticker == ticker and pending_side == side:
                    # 进一步确认该订单是否仍在交易所活跃
                    try:
                        order_status = self.client.get_order_status(ticker, order_id)
                        if order_status in ['NEW', 'PARTIALLY_FILLED']:
                            self.logger.info(f"发现 {ticker} {side} 的未成交订单 (ID={order_id}, Qty={qty}, Status={order_status}) 在 pending_orders 列表中且状态活跃，将复用.")
                            return True
                        else:
                            self.logger.info(f"订单 (ID={order_id}, Status={order_status}) 在 pending_orders 但已非活跃状态，将从pending_orders移除.")
                            self.pending_orders.pop(i) # 从列表中移除不活跃的订单
                            # 继续检查API，因为这个订单虽然在pending_orders里，但已经不活跃了
                    except Exception as e:
                        self.logger.warning(f"检查 pending_orders 中订单 {order_id} 状态失败: {e}, 假设其可能已失效，将尝试从API获取最新状态.")
                        self.pending_orders.pop(i) # 无法确认状态，也移除，依赖API
                        
            self.logger.debug(f"检查 {ticker} {side} 未成交订单: pending_orders 中未找到活跃订单，开始查询交易所API...")
            # 通过 Binance API 查询未成交订单
            open_orders_from_api = self.client.get_open_orders(ticker)
            if open_orders_from_api:
                self.logger.info(f"交易所API返回 {len(open_orders_from_api)} 条 {ticker} 的未成交订单.")
            else:
                self.logger.debug(f"交易所API未返回 {ticker} 的未成交订单.")

            for order in open_orders_from_api:
                if order['side'] == side and order['symbol'] == ticker:
                    self.logger.info(f"发现 {ticker} {side} 的未成交订单 (ID={order['orderId']}, Qty={order['origQty']}) 来自交易所API，状态为 {order['status']}. 将添加到 pending_orders.")
                    # 避免重复添加，先检查是否已存在（理论上前面已处理，但作为保险）
                    if not any(o_id == order['orderId'] for _, o_id, _, _ in self.pending_orders):
                        self.pending_orders.append((ticker, order['orderId'], side, float(order['origQty'])))
                    return True
            self.logger.debug(f"检查 {ticker} {side} 未成交订单: 交易所API中也未找到匹配的活跃订单.")
            return False
        except Exception as e:
            self.logger.error(f"检查 {ticker} {side} 未成交订单时发生错误: {e}")
            return False # 出错时保守处理，允许下单

    def handle_postonly_error(self, round_num: int, ticker: str, side: str, qty: float, error_msg: str,
                              pending_orders: List[Tuple[str, int, str, float]], is_close: bool = False):
        """处理 postOnly 挂单失败的错误"""
        success = False  # 初始化为失败状态
        order_id = None

        self.logger.error(f"第 {round_num} 轮: {ticker} {side} postOnly 挂单失败，错误: {error_msg}")

        if "APIError(code=-5022)" in error_msg:  # 特定错误才重试
            max_wait_seconds = 60  # 最大等待60秒
            start_time = time.time()

            while not success and (time.time() - start_time) < max_wait_seconds:
                try:
                    if self.check_existing_orders(ticker, side):
                        self.logger.info(f"第 {round_num} 轮: {ticker} {side} 已存在未成交订单，停止重试")
                        break
                    new_price = self.get_postonly_price(ticker, side)
                    if new_price == 0.0:
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 获取新价格失败，等待重试")
                        time.sleep(1)  # 价格获取失败时等待1秒
                        continue

                    adjusted_qty = self.adjust_quantity(ticker, abs(qty), new_price, is_close=is_close)
                    if adjusted_qty <= 0:
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 调整数量为 0，等待重试")
                        time.sleep(1)  # 数量调整失败时等待1秒
                        continue

                    success, order, order_id, trade_details = self.client.place_postonly_order(
                        ticker, side, adjusted_qty, new_price, is_close=is_close
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
            'side': side,  # 记录交易方向（BUY 或 SELL）
            'symbol': ticker,  # 记录交易对符号
            'total_qty': total_qty,  # 记录总成交数量
            'price': avg_price,  # 记录成交均价
            'total_quote': total_quote,  # 记录杠杆后的总成交金额
            'real_quote': total_quote / self.leverage,  # 记录真实成交金额（除以杠杆）
            'order_id': order_id,  # 记录订单ID
            'commission': total_commission,  # 记录总手续费
            'realized_pnl': total_realized_pnl  # 记录已实现盈亏
        }

        self.account_metrics[trade_key] = {
            "value": trade_record,  # 将交易记录对象存储到账户指标字典
            "description": (
                f"{side} {ticker} 成交数量 {total_qty} "  # 描述交易方向和交易对
                f"成交均价 {avg_price} "  # 描述成交均价
                f"杠杆后成交金额 {total_quote} "  # 描述杠杆后的成交金额
                f"真实成交金额 {total_quote / self.leverage} "  # 描述真实成交金额
                f"订单ID {order_id}"  # 描述订单ID
            ),
            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录当前时间，格式为 YYYY-MM-DD_HH:MM:SS
        }

        self.logger.info(
            f"第 {round_num} 轮: {ticker} {side} 单已成交，订单ID={order_id}, "  # 记录交易成交日志，包含轮次、交易对、方向
            f"成交数量={total_qty}, 均价={avg_price}"  # 记录成交数量和均价
        )

    def record_post_trade_metrics(self, total_balance: float = None):
        """记录调仓后的账户信息、手续费和盈亏"""
        account_info = self.client.get_account_info()
        after_trade_balance = float(account_info["totalMarginBalance"])
        self.logger.debug(f"记录调仓后余额: after_trade_balance={after_trade_balance}")
        if after_trade_balance == 0:
            self.logger.warning("after_trade_balance 为 0，可能导致回报率计算失败")
        after_available_balance = float(account_info["availableBalance"])

        if total_balance is None:
            total_balance = after_trade_balance - self.basic_funds
            self.logger.info(f"未提供 total_balance，使用账户余额计算: {total_balance}")

        if "before_trade_balance" not in self.account_metrics:
            self.account_metrics["before_trade_balance"] = {
                "value": total_balance + self.basic_funds,
                "description": "调仓前账户总保证金余额（未记录，使用当前余额估算）",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }
            self.logger.warning("缺少 before_trade_balance，使用估算值")

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

        # 强制计算手续费 - 已禁用
        # self.logger.debug(f"强制调用 process_trade_commissions for {commission_key}")
        # commission_record = self.process_trade_commissions()
        # self.logger.info(f"手续费计算结果: {commission_record}")

        # if pnl_key not in self.account_metrics:
        #     self.process_trade_realized_pnl()

        before_balance = float(self.account_metrics["before_trade_balance"]["value"])
        if commission_key in self.account_metrics:
            commission_value = float(self.account_metrics[commission_key]["value"])
            self.account_metrics[commission_ratio] = {
                "value": f"{(commission_value / before_balance * 100) if before_balance != 0 else 0:.6f}%",
                "description": "买卖交易总手续费占比",
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
                "description": "买卖交易总盈亏占比",
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
        current_positions = self.get_current_positions()  # 获取当前持仓信息
        long_count = sum(1 for qty in current_positions.values() if qty > 0)  # 统计多头持仓数量
        short_count = sum(1 for qty in current_positions.values() if qty < 0)  # 统计空头持仓数量
        total_count = long_count + short_count  # 计算总持仓数量
        self.logger.info(f"当前总持仓数:{total_count}, 多头数:{long_count}, 空头数:{short_count}")  # 记录持仓统计信息

        if total_count != self.num_long_pos + self.num_short_pos or long_count != self.num_long_pos or short_count != self.num_short_pos:  # 检查持仓是否平衡
            self.logger.info(f"持仓验证失败，开始多空平衡调整...")  # 记录持仓不平衡，开始调整
            if long_count > short_count:  # 如果多头数量大于空头
                to_close = (long_count - short_count) // 2  # 计算需要平仓的多头数量
                current_longs = {ticker for ticker, qty in current_positions.items() if qty > 0}  # 获取当前多头交易对
                long_candidate_tickers = {c['ticker'] for c in long_candidates}  # 获取多头候选交易对
                long_candidates_sorted = sorted(long_candidates, key=lambda x: x['id'], reverse=True)  # 按 ID 降序排序多头候选
                closed = 0  # 初始化已平仓计数器
                for ticker, qty in current_positions.items():  # 遍历当前持仓
                    if qty > 0 and ticker not in long_candidate_tickers and closed < to_close:  # 如果是多头且不在候选列表
                        if self.execute_trade(ticker, "SELL", abs(qty)):  # 执行卖出平仓
                            closed += 1  # 增加已平仓计数
                if closed < to_close:  # 如果仍需平仓更多多头
                    for candidate in long_candidates_sorted:  # 遍历排序后的多头候选
                        ticker = candidate['ticker']  # 获取交易对
                        if ticker in current_positions and current_positions[
                            ticker] > 0 and closed < to_close:  # 如果持仓为多头且需平仓
                            qty = current_positions[ticker]  # 获取持仓数量
                            if self.execute_trade(ticker, "SELL", abs(qty)):  # 执行卖出平仓
                                closed += 1  # 增加已平仓计数

                short_candidates_sorted = sorted(short_candidates, key=lambda x: x['id'], reverse=True)  # 按 ID 降序排序空头候选
                current_shorts = {ticker for ticker, qty in current_positions.items() if qty < 0}  # 获取当前空头交易对
                opened = 0  # 初始化已开仓计数器
                for candidate in short_candidates_sorted:  # 遍历空头候选
                    if candidate['ticker'] not in current_shorts and opened < to_close:  # 如果不在当前空头且需开仓
                        price = self.client.get_symbol_price(candidate['ticker'])  # 获取交易对当前价格
                        qty = self.adjust_quantity(candidate['ticker'],
                                                   self.calculate_position_size(total_balance, price),
                                                   price) if price != 0.0 else 0  # 计算并调整持仓数量
                        if qty != 0 and self.execute_trade(candidate['ticker'], "SELL", qty):  # 执行卖出开仓
                            opened += 1  # 增加已开仓计数
            elif short_count > long_count:  # 如果空头数量大于多头
                to_close = (short_count - long_count) // 2  # 计算需要平仓的空头数量
                current_shorts = {ticker for ticker, qty in current_positions.items() if qty < 0}  # 获取当前空头交易对
                short_candidate_tickers = {c['ticker'] for c in short_candidates}  # 获取空头候选交易对
                short_candidates_sorted = sorted(short_candidates, key=lambda x: x['id'])  # 按 ID 升序排序空头候选
                closed = 0  # 初始化已平仓计数器
                for ticker, qty in current_positions.items():  # 遍历当前持仓
                    if qty < 0 and ticker not in short_candidate_tickers and closed < to_close:  # 如果是空头且不在候选列表
                        if self.execute_trade(ticker, "BUY", abs(qty)):  # 执行买入平仓
                            closed += 1  # 增加已平仓计数
                if closed < to_close:  # 如果仍需平仓更多空头
                    for candidate in short_candidates_sorted:  # 遍历排序后的空头候选
                        ticker = candidate['ticker']  # 获取交易对
                        if ticker in current_positions and current_positions[
                            ticker] < 0 and closed < to_close:  # 如果持仓为空头且需平仓
                            qty = current_positions[ticker]  # 获取持仓数量
                            if self.execute_trade(ticker, "BUY", abs(qty)):  # 执行买入平仓
                                closed += 1  # 增加已平仓计数

                long_candidates_sorted = sorted(long_candidates, key=lambda x: x['id'])  # 按 ID 升序排序多头候选
                current_longs = {ticker for ticker, qty in current_positions.items() if qty > 0}  # 获取当前多头交易对
                opened = 0  # 初始化已开仓计数器
                for candidate in long_candidates_sorted:  # 遍历多头候选
                    if candidate['ticker'] not in current_longs and opened < to_close:  # 如果不在当前多头且需开仓
                        price = self.client.get_symbol_price(candidate['ticker'])  # 获取交易对当前价格
                        qty = self.adjust_quantity(candidate['ticker'],
                                                   self.calculate_position_size(total_balance, price),
                                                   price) if price != 0.0 else 0  # 计算并调整持仓数量
                        if qty != 0 and self.execute_trade(candidate['ticker'], "BUY", qty):  # 执行买入开仓
                            opened += 1  # 增加已开仓计数

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
                        "Run_ID": run_id
                    })
            self.logger.info(f"准备保存 {len(data)} 条持仓记录，Run_ID={run_id}, 调仓日期={date_str}")
            if not data:
                self.logger.warning("无有效持仓数据可保存，检查 positions 数据是否为空")
                return  # 提前退出
            if data:
                df_new = pd.DataFrame(data)
                if os.path.exists(self.positions_file):
                    try:
                        df_existing = pd.read_csv(self.positions_file)
                        if 'Run_ID' not in df_existing.columns:
                            df_existing['Run_ID'] = ''
                        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                        df_combined = df_combined.drop_duplicates(['Run_ID', '交易对'], keep="last")
                        df_combined.to_csv(self.positions_file, index=False, encoding='utf-8')
                        self.logger.info(f"持仓数据已保存到 {self.positions_file}，合并后记录数: {len(df_combined)}")
                    except Exception as e:
                        self.logger.error(f"合并持仓数据失败: {str(e)}")
                        df_new.to_csv(self.positions_file, index=False, encoding='utf-8')
                        self.logger.info(f"直接保存新持仓数据到 {self.positions_file}，记录数: {len(df_new)}")
                else:
                    df_new.to_csv(self.positions_file, index=False, encoding='utf-8')
                    self.logger.info(f"首次保存持仓数据到 {self.positions_file}，记录数: {len(df_new)}")
                # 验证保存结果
                df_verify = pd.read_csv(self.positions_file)
                run_id_records = df_verify[df_verify['Run_ID'] == run_id]
                self.logger.info(f"验证: {self.positions_file} 中 Run_ID={run_id} 的记录数: {len(run_id_records)}")
                if run_id_records.empty:
                    self.logger.error(f"保存后验证失败: 未找到 Run_ID={run_id} 的记录")
            else:
                self.logger.info("无有效持仓数据可保存")
        except Exception as e:
            self.logger.error(f"保存持仓到CSV失败: {str(e)}")

    def _load_account_metrics(self) -> pd.DataFrame:
        """加载 account_metrics.xlsx 文件"""
        try:
            account_metrics_file = "data/account_metrics.xlsx"
            if not os.path.exists(account_metrics_file):
                self.logger.info("account_metrics.xlsx 不存在，返回空DataFrame")
                return pd.DataFrame()
            df = pd.read_excel(account_metrics_file, sheet_name='Account_Metrics')
            df['Date'] = df['Date'].apply(self.normalize_date_time)
            df['Record_Time'] = df['Record_Time'].apply(self.normalize_date_time)
            invalid_rows = df[df['Record_Time'].isna()]
            if not invalid_rows.empty:
                self.logger.warning(
                    f"发现 {len(invalid_rows)} 条无效 Record_Time 记录: {invalid_rows[['Metric', 'Record_Time']].to_dict('records')}")
            df = df.dropna(subset=['Record_Time'])
            self.logger.info(f"加载 account_metrics.xlsx 成功，记录数: {len(df)}")
            return df if not df.empty else pd.DataFrame()
        except Exception as e:
            self.logger.error(f"加载 account_metrics.xlsx 失败: {str(e)}")
            return pd.DataFrame()

    def normalize_date_time(self, time_val):
        """规范化时间值，支持多种格式并返回 pd.Timestamp"""
        if pd.isna(time_val) or time_val is None:
            self.logger.warning("时间值为空或无效，返回 pd.NaT")
            return pd.NaT
        if isinstance(time_val, (pd.Timestamp, datetime)):
            return time_val
        if isinstance(time_val, str):
            time_val = time_val.strip()  # 去除首尾空格
            formats = [
                '%Y-%m-%d %H:%M:%S',  # 标准格式
                '%Y-%m-%d_%H:%M:%S',  # 下划线分隔
                '%Y%m%d %H:%M:%S',  # 无分隔符
                '%Y-%m-%d',  # 仅日期
                '%H:%M:%S',  # 仅时间
            ]
            for fmt in formats:
                try:
                    parsed_time = pd.to_datetime(time_val, format=fmt, errors='coerce')
                    if not pd.isna(parsed_time):
                        return parsed_time
                except ValueError:
                    continue
            # 回退到 mixed 格式解析
            try:
                parsed_time = pd.to_datetime(time_val, errors='coerce')
                if not pd.isna(parsed_time):
                    self.logger.info(f"使用默认格式成功解析时间: {time_val}")
                    return parsed_time
            except ValueError:
                self.logger.warning(f"无法解析时间格式: {time_val}，尝试的格式: {formats} + 默认格式")
            return pd.NaT
        self.logger.warning(f"时间值类型不支持: {type(time_val)}，值: {time_val}")
        return pd.NaT

    def calculate_and_append_returns(self):
        try:
            run_id = self.account_metrics.get("after_trade_balance", {}).get("Run_ID",
                                                                             datetime.now().strftime("%Y%m%d%H%M%S"))
            current_date = pd.to_datetime(datetime.now().strftime('%Y-%m-%d'))

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

            df['Record_Time'] = df['Record_Time'].apply(self.normalize_date_time)
            df['Date'] = df['Date'].apply(self.normalize_date_time)
            df = df.dropna(subset=['Record_Time'])

            current_before = self.account_metrics.get("before_trade_balance", {}).get("value", 0)
            current_after = self.account_metrics.get("after_trade_balance", {}).get("value", 0)
            self.logger.debug(f"当前余额: before={current_before}, after={current_after}")
            if current_after == 0:
                self.logger.warning("current_after 为 0，检查 after_trade_balance 是否正确赋值")

            current_record_time_str = self.account_metrics.get("after_trade_balance", {}).get("date",
                                                                                              datetime.now().strftime(
                                                                                                  '%Y-%m-%d_%H:%M:%S'))
            current_record_time = self.normalize_date_time(current_record_time_str)
            if current_record_time is pd.NaT:
                self.logger.warning(f"无法解析当前记录时间: {current_record_time_str}，使用当前时间")
                current_record_time = pd.to_datetime(datetime.now())

            # 修改这部分代码，查找最后一条记录
            previous_data = df[(df['Metric'] == 'after_trade_balance')]
            self.logger.info(f"找到 {len(previous_data)} 条 after_trade_balance 记录")
            
            previous_after_value = None
            previous_record_time = None
            
            # 修改后的解决方案：排除当前 Run_ID，然后按时间排序获取最新记录
            if not previous_data.empty:
                # 获取当前 Run_ID
                current_run_id = self.account_metrics.get("after_trade_balance", {}).get("Run_ID")
                
                # 如果有当前 Run_ID，排除它
                if current_run_id:
                    previous_data = previous_data[previous_data['Run_ID'] != current_run_id]
                    self.logger.info(f"排除当前 Run_ID={current_run_id} 后，剩余 {len(previous_data)} 条记录")
                
                # 如果还有记录，按时间排序并获取最新的一条
                if not previous_data.empty:
                    previous_data = previous_data.sort_values('Record_Time', ascending=False)
                    previous_after_value = float(previous_data.iloc[0]['Value'])
                    previous_record_time = previous_data.iloc[0]['Record_Time']
                    previous_run_id = previous_data.iloc[0]['Run_ID']
                    self.logger.info(
                        f"找到上一次记录: Record_Time={previous_record_time}, Run_ID={previous_run_id}, after_trade_balance={previous_after_value}")
                else:
                    self.logger.warning(f"排除当前 Run_ID 后未找到上一次 after_trade_balance 记录")
            else:
                self.logger.warning(f"未找到任何 after_trade_balance 记录")

            if current_before and previous_after_value and previous_after_value != 0:
                pre_rebalance_return = ((float(current_before) - previous_after_value) / previous_after_value) * 100
                self.account_metrics["pre_rebalance_return"] = {
                    "value": f"{pre_rebalance_return:.6f}%",
                    "description": f"调仓前回报率: ({current_before} - {previous_after_value}) / {previous_after_value} * 100",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                    "Run_ID": run_id
                }
                self.logger.info(f"Pre-rebalance return: {pre_rebalance_return:.6f}%")
            else:
                self.account_metrics["pre_rebalance_return"] = {
                    "value": "0.000000%",
                    "description": f"调仓前回报率: 缺少数据 (current_before={current_before}, previous_after_value={previous_after_value})",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                    "Run_ID": run_id
                }
                self.logger.warning(
                    f"无法计算 Pre-rebalance return: current_before={current_before}, previous_after_value={previous_after_value}")

            if current_after and previous_after_value and previous_after_value != 0:
                post_rebalance_return = ((float(current_after) - previous_after_value) / previous_after_value) * 100
                self.account_metrics["post_rebalance_return"] = {
                    "value": f"{post_rebalance_return:.6f}%",
                    "description": f"调仓后回报率: ({current_after} - {previous_after_value}) / {previous_after_value} * 100",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                    "Run_ID": run_id
                }
                self.logger.info(f"Post-rebalance return: {post_rebalance_return:.6f}%")
            else:
                self.account_metrics["post_rebalance_return"] = {
                    "value": "0.000000%",
                    "description": f"调仓后回报率: 缺少数据 (current_after={current_after}, previous_after_value={previous_after_value})",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S'),
                    "Run_ID": run_id
                }
                self.logger.warning(
                    f"无法计算 Post-rebalance return: current_after={current_after}, previous_after_value={previous_after_value}")

        except Exception as e:
            self.logger.error(f"计算回报率失败: {str(e)}", exc_info=True)
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
        """将账户信息保存为JSON文件 - 已禁用"""
        try:
            # 不再生成account_info_YYYYMMDD_HHMMSS.json文件
            self.logger.info(f"账户信息JSON文件生成已禁用")
        except Exception as e:
            self.logger.error(f"保存账户信息到JSON文件失败: {str(e)}")  # 记录保存失败的错误日志

    def run(self, date_str: str, run_id: str) -> Dict[str, str]:
        """
        执行交易引擎主逻辑
        Args:
            date_str: 日期字符串 (格式: YYYY-MM-DD)
            run_id: 本次运行的唯一标识符
        Returns:
            错误原因字典 (key为错误类型，value为详情)
        """
        self.error_reasons = {}  # 初始化错误原因字典
        start_time = int(time.time() * 1000)  # 记录调仓开始时间（毫秒时间戳）

        # 保存交易开始时间戳用于手续费计算
        self.trade_start_time = start_time
        try:
            # ==================== 初始化阶段 ====================
            self.logger.info(f"🚀 开始执行交易引擎 | Date: {date_str} | RunID: {run_id}")  # 记录交易引擎启动日志
            self.cancel_all_open_orders()  # 撤销所有未成交的挂单

            # ==================== 数据加载阶段 ====================
            funding_file = f'data/pos{date_str.replace("-", "")}_v3.csv'  # 构造资金费率文件路径
            if not os.path.exists(funding_file):  # 检查资金费率文件是否存在
                msg = f"资金费率文件不存在: {funding_file}"  # 设置错误信息
                self.logger.error(msg)  # 记录文件不存在的错误日志
                self.error_reasons["file_not_found"] = msg  # 将错误原因添加到字典
                return self.error_reasons  # 返回错误原因字典

            # 设置监控指标
            self.account_metrics["position_file"] = {
                "value": os.path.basename(funding_file),  # 保存资金费率文件名
                "description": "调仓文件",  # 设置描述
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录当前时间
            }

            # ==================== 黑名单处理阶段 ====================
            df = pd.read_csv(funding_file)  # 读取资金费率数据文件
            blacklist = self.load_blacklist()  # 加载黑名单列表
            blacklisted_tickers = sorted(set(df['ticker']) & blacklist)  # 获取在黑名单中的交易对

            if blacklisted_tickers:  # 如果存在黑名单中的交易对
                tickers_str = ", ".join(blacklisted_tickers)  # 将黑名单中的交易对转换为字符串
                self.error_reasons["blacklisted_tickers"] = tickers_str  # 记录黑名单交易对
                self.logger.info(
                    "📋 黑名单过滤结果\n" +
                    f"├─ 被过滤币种 ({len(blacklisted_tickers)}个): {tickers_str}\n" +
                    f"└─ 剩余候选池: {len(df) - len(blacklisted_tickers)}/{len(df)}"
                )  # 记录黑名单过滤结果日志

            # ==================== 候选列表生成 ====================
            long_candidates = df[
                (df['fundingRate'] < 0) &  # 筛选资金费率为负的交易对（多头候选）
                (~df['ticker'].isin(blacklist))  # 排除黑名单中的交易对
                ][['ticker', 'fundingRate', 'id']].to_dict('records')  # 转换为字典列表

            short_candidates = df[
                (df['fundingRate'] > 0) &  # 筛选资金费率为正的交易对（空头候选）
                (~df['ticker'].isin(blacklist))  # 排除黑名单中的交易对
                ][['ticker', 'fundingRate', 'id']].to_dict('records')  # 转换为字典列表

            short_candidates.sort(key=lambda x: x['id'], reverse=True)  # 按ID降序排序空头候选

            self.logger.info(
                "🔍 有效候选列表\n" +
                f"├─ 多头候选: {len(long_candidates)}个\n" +
                f"└─ 空头候选: {len(short_candidates)}个"

            )  # 记录有效候选列表的统计信息

            # ==================== 记录调仓前余额 ===================
            account_info = self.client.get_account_info()  # 获取账户信息
            before_trade_balance = float(account_info["totalMarginBalance"])  # 获取调仓前总保证金余额
            before_available_balance = float(account_info["availableBalance"])  # 获取调仓前可用余额
            self.account_metrics["before_trade_balance"] = {
                "value": before_trade_balance,  # 保存调仓前总余额
                "description": "调仓前账户总保证金余额(totalMarginBalance)",  # 设置描述
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 记录当前时间
            }
            self.account_metrics["before_available_balance"] = {
                "value": before_available_balance,  # 保存调仓前可用余额
                "description": "调仓前可用保证金余额(available_balance)",  # 设置描述
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # 修复日期格式，删除多余的-m
            }

            # ==================== 仓位调整阶段 ====================
            try:
                self.adjust_or_open_positions(long_candidates, short_candidates, run_id, date_str)  # 执行仓位调整
                # 确保在调仓后保存持仓
                positions = self.client.client.futures_position_information()
                self.save_positions_to_csv(positions, run_id)
            except Exception as e:
                error_msg = f"仓位调整失败: {str(e)}"  # 设置错误信息
                self.logger.error(error_msg, exc_info=True)  # 记录错误日志，包含异常详情
                self.error_reasons["position_adjustment_failed"] = error_msg  # 记录错误原因

            # ==================== 记录调仓后余额和交易记录 ===================
            self.trade_end_time = int(time.time() * 1000)  # 记录调仓结束时间（毫秒）
            end_time = self.trade_end_time  # 兼容原有代码
            end_time = self.trade_end_time  # 兼容原有代码
            account_info = self.client.get_account_info()  # 获取最新账户信息
            after_trade_balance = float(account_info["totalMarginBalance"])  # 获取调仓后总余额
            after_available_balance = float(account_info["availableBalance"])  # 获取调仓后可用余额

            # 计算余额损失
            balance_loss = after_trade_balance - before_trade_balance  # 计算调仓前后余额差
            balance_loss_rate = (
                        balance_loss / before_trade_balance * 100) if before_trade_balance != 0 else 0  # 计算余额损失率

            # 获取 BTC/USDT 的价格
            btc_price = float(self.client.get_symbol_price('BTCUSDT'))  # 获取 BTC/USDT 当前价格

            # 获取所有交易对的交易历史
            all_symbols = list(set([c['ticker'] for c in long_candidates + short_candidates]))  # 获取所有候选交易对
            trade_records = []  # 初始化交易记录列表
            total_commission = 0  # 初始化总手续费
            total_realized_pnl = 0  # 初始化总已实现盈亏

            for symbol in all_symbols:  # 遍历所有交易对
                trades = self.client.get_trade_history(symbol=symbol, start_time=start_time,
                                                       end_time=end_time)  # 获取交易历史
                for trade in trades:  # 遍历交易记录
                    real_quote = trade['quoteQty'] / self.leverage  # 计算真实成交金额（除以杠杆）
                    trade_record = {
                        'side': trade['side'],  # 记录交易方向
                        'symbol': trade['symbol'],  # 记录交易对
                        'total_qty': trade['qty'],  # 记录交易数量
                        'price': trade['price'],  # 记录成交价格
                        'total_quote': trade['quoteQty'],  # 记录杠杆后成交金额
                        'real_quote': real_quote,  # 记录真实成交金额
                        'order_id': trade['orderId']  # 记录订单ID
                    }
                    trade_records.append((trade['ticker'], trade['side'], trade['orderId'], trade_record))  # 添加到交易记录列表
                    total_commission += trade['commission']  # 累加手续费
                    total_realized_pnl += trade['realizedPnl']  # 累加已实现盈亏

            # 计算手续费和盈亏占比
            commission_ratio = (total_commission / before_trade_balance * 100) if before_trade_balance != 0 else 0
            realized_pnl_ratio = (total_realized_pnl / before_trade_balance * 100) if before_trade_balance != 0 else 0

            # 格式化 account_info JSON 数据
            current_date = datetime.now().strftime('%Y-%m-%d')  # 获取当前日期
            account_info_data = {
                'position_file': {
                    'value': os.path.basename(funding_file),  # 保存资金费率文件名
                    'description': '调仓文件',  # 设置描述
                    'date': datetime.now().strftime('%Y-%m-%d')  # 记录时间（只保留日期）
                },
                'before_trade_balance': {
                    'value': before_trade_balance,  # 保存调仓前总余额
                    'description': '调仓前账户总保证金余额(totalMarginBalance)',  # 设置描述
                    'date': datetime.now().strftime('%Y-%m-%d')  # 记录时间（只保留日期）
                },
                'before_available_balance': {
                    'value': before_available_balance,  # 保存调仓前可用余额
                    'description': '调仓前可用保证金余额(available_balance)',  # 设置描述
                    'date': datetime.now().strftime('%Y-%m-%d')  # 记录时间（只保留日期）
                },
                'after_trade_balance': {
                    'value': after_trade_balance,  # 保存调仓后总余额
                    'description': '调仓后账户总保证金余额(totalMarginBalance)',  # 设置描述
                    'date': datetime.now().strftime('%Y-%m-%d')  # 记录时间（只保留日期）
                },
                'balance_loss': {
                    'value': balance_loss,  # 保存余额损失金额
                    'description': '调仓前后余额损失金额',  # 设置描述
                    'date': datetime.now().strftime('%Y-%m-%d')  # 记录时间（只保留日期）
                },
                'balance_loss_rate': {
                    'value': f"{balance_loss_rate:.6f}%",  # 保存余额损失率，保留6位小数
                    'description': '调仓前后余额损失率 (%)',  # 设置描述
                    'date': datetime.now().strftime('%Y-%m-%d')  # 记录时间（只保留日期）
                },
                'after_available_balance': {
                    'value': after_available_balance,  # 保存调仓后可用余额
                    'description': '调仓后可用余额',  # 设置描述
                    'date': datetime.now().strftime('%Y-%m-%d')  # 记录时间（只保留日期）
                },
                'btc_usdt_price': {
                    'value': btc_price,  # 保存 BTC/USDT 价格
                    'description': f'当前btc_usdt_price:{btc_price}',  # 设置描述
                    'date': datetime.now().strftime('%Y-%m-%d')  # 记录时间（只保留日期）
                },
                f'trade_commission_summary_{current_date}': {
                    'value': total_commission,  # 保存当天的总手续费
                    'description': "买卖交易手续费总和",  # 设置描述
                    'date': datetime.now().strftime('%Y-%m-%d')  # 记录时间（只保留日期）
                },
                f'trade_commission_summary_ratio_{current_date}': {
                    'value': f"{commission_ratio:.6f}%",  # 保存当天的手续费占比
                    'description': "买卖交易总手续费占比",  # 设置描述
                    'date': datetime.now().strftime('%Y-%m-%d')  # 记录时间（只保留日期）
                },
                f'trade_realized_pnl_summary_{current_date}': {
                    'value': total_realized_pnl,  # 保存当天的总盈亏
                    'description': "买卖交易已实现盈亏总和",  # 设置描述
                    'date': datetime.now().strftime('%Y-%m-%d')  # 记录时间（只保留日期）
                },
                f'trade_realized_pnl_summary_ratio_{current_date}': {
                    'value': f"{realized_pnl_ratio:.6f}%",  # 保存当天的盈亏占比
                    'description': "买卖交易总盈亏占比",  # 设置描述
                    'date': datetime.now().strftime('%Y-%m-%d')  # 记录时间（只保留日期）
                }
            }

            # 添加交易记录 - 直接从account_metrics中获取所有交易记录
            for key, value in self.account_metrics.items():  # 遍历账户指标
                if key.startswith("trade_") and current_date in key:  # 筛选当天的交易记录
                    account_info_data[key] = value  # 添加到账户信息数据中

            # 保存 account_info 到 JSON 文件之前，更新到self.account_metrics
            self.account_metrics.update(account_info_data)  # 将account_info_data更新到self.account_metrics

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')  # 生成时间戳，格式为 YYYYMMDD_HHMMSS
            output_file = f'data/account_info_{timestamp}.json'  # 构造输出文件名
            os.makedirs("data", exist_ok=True)  # 确保data目录存在
            with open(output_file, 'w', encoding='utf-8') as f:  # 打开文件以写入
                json.dump(account_info_data, f, ensure_ascii=False, indent=4)  # 写入JSON数据，格式化缩进为4
            self.logger.info(f"账户信息已保存为JSON文件: {output_file}")  # 记录保存成功的日志

            # ==================== 结果持久化 ===================
            self.logger.info("开始计算手续费和已实现盈亏...")
            try:
                current_date = datetime.now().strftime('%Y-%m-%d')
                commission_key = f"trade_commission_summary_{current_date}"
                pnl_key = f"trade_realized_pnl_summary_{current_date}"
                
                # 只有在没有数据或数据为0时才重新计算
                if commission_key not in self.account_metrics or float(self.account_metrics[commission_key]["value"]) == 0:
                    commission_result = self.process_trade_commissions()
                    self.logger.info(f"手续费计算结果: {commission_result}")
                else:
                    self.logger.info(f"已有手续费数据，跳过计算: {self.account_metrics[commission_key]['value']}")

                if pnl_key not in self.account_metrics or float(self.account_metrics[pnl_key]["value"]) == 0:
                    pnl_result = self.process_trade_realized_pnl()
                    self.logger.info(f"已实现盈亏计算结果: {pnl_result}")
                else:
                    self.logger.info(f"已有盈亏数据，跳过计算: {self.account_metrics[pnl_key]['value']}")
                
                # 重新计算占比
                before_balance = float(self.account_metrics["before_trade_balance"]["value"])
                commission_ratio_key = f"trade_commission_summary_ratio_{current_date}"
                pnl_ratio_key = f"trade_realized_pnl_summary_ratio_{current_date}"
                
                if commission_key in self.account_metrics:
                    commission_value = float(self.account_metrics[commission_key]["value"])
                    self.account_metrics[commission_ratio_key] = {
                        "value": f"{(commission_value / before_balance * 100) if before_balance != 0 else 0:.6f}%",
                        "description": "买卖交易总手续费占比",
                        "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                    }
                
                if pnl_key in self.account_metrics:
                    pnl_value = float(self.account_metrics[pnl_key]["value"])
                    self.account_metrics[pnl_ratio_key] = {
                        "value": f"{(pnl_value / before_balance * 100) if before_balance != 0 else 0:.6f}%",
                        "description": "买卖交易总盈亏占比",
                        "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                    }
            except Exception as e:
                self.logger.error(f"计算手续费或已实现盈亏失败: {str(e)}")
                self.logger.exception("详细错误信息:")

            self.logger.info(f"✅ 交易引擎执行完成 | RunID: {run_id}")  # 记录交易引擎执行完成的日志

        except Exception as e:
            error_msg = f"交易引擎执行异常: {str(e)}"  # 设置错误信息
            self.logger.critical(error_msg, exc_info=True)  # 记录严重错误日志，包含异常详情
            # 仅在必要时保存持仓
            if "position_file" not in self.account_metrics:  # 如果未记录调仓文件
                try:
                    positions = self.client.client.futures_position_information()  # 获取当前持仓信息
                    self.save_positions_to_csv(positions, run_id)  # 保存持仓到CSV文件
                except Exception as e:
                    self.logger.error(f"持仓信息保存失败: {str(e)}")  # 记录持仓保存失败的错误日志

            self.error_reasons["system_error"] = error_msg  # 记录系统错误原因

        finally:
            # ==================== 结果持久化（移到 finally 块中确保只执行一次） ===================
            # 保存账户信息到JSON文件
            self.save_to_json(date_str, run_id)

            # 计算并追加回报率
            self.calculate_and_append_returns()

            # 写入Excel文件
            self.write_to_excel(run_id=run_id)

        return self.error_reasons  # 返回错误原因字典

    def load_blacklist(self) -> Set[str]:
        """加载黑名单列表"""
        blacklist_path = "data/blacklist.csv"  # 设置黑名单文件路径
        try:
            if os.path.exists(blacklist_path):  # 检查黑名单文件是否存在
                return set(pd.read_csv(blacklist_path)['ticker'].tolist())  # 读取黑名单并转换为集合
            self.logger.info("未检测到黑名单文件，跳过过滤")  # 记录未找到黑名单文件的日志
            return set()  # 返回空集合
        except Exception as e:
            self.logger.error(f"黑名单加载异常: {str(e)}")  # 记录黑名单加载失败的错误日志
            return set()  # 返回空集合


    def get_stable_positions(self) -> Dict[str, float]:
        """获取稳定的持仓信息（统一返回字典格式）"""
        try:
            positions = self.client.get_position_info()  # 获取当前持仓信息

            # 如果返回的是列表，转换为字典
            if isinstance(positions, list):  # 检查持仓数据是否为列表
                return {
                    item["symbol"]: float(item["positionAmt"])  # 将交易对和持仓数量转换为字典
                    for item in positions
                    if float(item["positionAmt"]) != 0  # 仅包含非零持仓
                }

            # 如果已经是字典格式，直接返回
            elif isinstance(positions, dict):  # 检查持仓数据是否为字典
                return {k: float(v) for k, v in positions.items()}  # 转换为浮点数并返回

            else:
                raise ValueError(f"未知的持仓数据格式: {type(positions)}")  # 抛出未知格式的异常

        except Exception as e:
            self.logger.error(f"获取稳定持仓失败: {str(e)}")  # 记录获取持仓失败的错误日志
            return {}  # 返回空字典


if __name__ == "__main__":
    logger = setup_logger("../logs/trading.log")  # 初始化日志记录器，设置日志文件路径
    from config_loader import ConfigLoader  # 导入配置加载器模块

    config_loader = ConfigLoader()  # 创建配置加载器实例
    api_config = config_loader.get_api_config()  # 获取API配置
    trading_config = config_loader.get_trading_config()  # 获取交易配置
    paths_config = config_loader.get_paths_config()  # 获取路径配置
    config = {**api_config, **trading_config, **paths_config}  # 合并所有配置
    client = BinanceFuturesClient(api_config["api_key"], api_config["api_secret"], api_config["test_net"] == "True",
                                  logger)  # 创建Binance期货客户端实例
    engine = TradingEngine(client, config, logger)  # 创建交易引擎实例
    engine.run("20250324")  # 运行交易引擎，指定日期为2025-03-24