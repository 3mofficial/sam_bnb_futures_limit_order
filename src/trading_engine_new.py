# 导入必要的模块
import xlsxwriter  # 用于创建和写入 Excel 文件
import json  # 用于处理 JSON 数据
import logging  # 用于日志记录
from datetime import datetime, timedelta  # 用于处理日期和时间
import time  # 用于时间相关的操作（如等待）
from typing import Dict, List, Any, Tuple  # 类型提示，用于增强代码可读性
import pandas as pd  # 用于数据处理和 Excel 操作
import os  # 用于文件和路径操作
from .binance_client import BinanceFuturesClient  # 导入自定义的 Binance 期货客户端模块
from .data_processor import DataProcessor  # 导入自定义的数据处理模块
from .logger import setup_logger  # 导入自定义的日志设置模块
from typing import Set, Dict

class TradingEngine:
    """交易引擎类，负责执行交易逻辑"""

    def __init__(self, client: BinanceFuturesClient, config: Dict[str, str], logger: logging.Logger):
        """初始化交易引擎"""
        self.client = client
        self.config = config
        self.logger = logger
        self.leverage = int(config["leverage"])
        self.basic_funds = float(config["basic_funds"])
        self.num_long_pos = int(config["num_long_pos"])
        self.num_short_pos = int(config["num_short_pos"])
        self.trade_time = config["trade_time"]
        self.max_wait_time = float(config.get("max_wait_time", 30))
        self.account_metrics = {}
        self.pending_orders = []
        self.failed_depth_tickers = set()
        self.error_reasons = {}  # 新增：记录交易失败原因，格式为 {ticker: reason}
        self.is_first_run = self._check_first_run()
        self.positions_file = 'data/positions_output.csv'
        if not os.path.exists(self.positions_file):
            os.makedirs(os.path.dirname(self.positions_file), exist_ok=True)
            df = pd.DataFrame(columns=['调仓日期', '交易对', '持仓数量', '入场价格'])
            df.to_csv(self.positions_file, index=False, encoding='utf-8')
            self.logger.info(f"初始化空持仓文件: {self.positions_file}")

    def _check_first_run(self) -> bool:
        """检查是否为首次运行"""
        account_metrics_file = "data/account_metrics.xlsx"
        if not os.path.exists(account_metrics_file):
            return True
        try:
            df = pd.read_excel(account_metrics_file, sheet_name='Account_Metrics')
            # 检查是否存在 before_trade_balance 或 after_trade_balance 数据
            has_balance_data = df['Metric'].isin(['before_trade_balance', 'after_trade_balance']).any()
            return df.empty or not has_balance_data
        except Exception as e:
            self.logger.warning(f"检查首次运行状态失败: {str(e)}，假设为首次运行")
            return True

    def get_postonly_price(self, symbol: str, side: str, depth_level: int = 0) -> float:
        """获取 postOnly 挂单价格，直接使用买一档或卖一档价格"""
        try:
            depth = self.client.client.futures_order_book(symbol=symbol, limit=20)
            price = float(depth['bids'][0][0]) if side == "BUY" else float(depth['asks'][0][0])
            exchange_info = self.client.client.futures_exchange_info()
            symbol_info = next(s for s in exchange_info["symbols"] if s["symbol"] == symbol)
            price_filter = next(f for f in symbol_info["filters"] if f["filterType"] == "PRICE_FILTER")
            tick_size = float(price_filter["tickSize"])
            adjusted_price = round(price / tick_size) * tick_size
            self.logger.debug(f"{symbol} {side} 挂单价格: 原始={price}, 调整后={adjusted_price}, tickSize={tick_size}")
            return adjusted_price
        except Exception as e:
            self.logger.error(f"获取 {symbol} postOnly 挂单价格失败: {str(e)}")
            self.error_reasons[symbol] = f"获取价格失败: {str(e)}"
            return 0.0

    def get_current_positions(self) -> Dict[str, float]:
        """获取当前持仓向量（适配修改后的get_stable_positions）"""
        positions = self.get_stable_positions()

        # 现在positions已经是字典，直接返回
        if not isinstance(positions, dict):
            self.logger.error(f"持仓数据格式错误，预期 dict，实际: {type(positions)}")
            return {}

        self.logger.info(f"当前持仓: {positions}")
        return positions

    def calculate_position_size(self, balance: float, price: float) -> float:
        """计算单笔交易的持仓数量"""
        per_share = balance / (self.num_long_pos + self.num_short_pos)  # 计算每笔交易的资金份额
        margin = per_share * self.leverage  # 计算杠杆后的保证金
        return margin / price  # 返回持仓数量（保证金除以价格）

    def adjust_quantity(self, symbol: str, quantity: float, price: float, is_close: bool = False) -> float:
        """根据交易所规则调整交易数量，平仓跳过最小名义价值检查"""
        try:
            exchange_info = self.client.client.futures_exchange_info()
            symbol_info = next(s for s in exchange_info["symbols"] if s["symbol"] == symbol)
            filters = {f["filterType"]: f for f in symbol_info["filters"]}
            quantity_precision = symbol_info["quantityPrecision"]
            step_size = float(filters["LOT_SIZE"]["stepSize"])
            min_qty = float(filters["LOT_SIZE"]["minQty"])
            max_qty = float(filters["LOT_SIZE"]["maxQty"])
            min_notional = float(filters["MIN_NOTIONAL"]["notional"]) if not is_close else 0.0

            adjusted_qty = round(round(quantity / step_size) * step_size, quantity_precision)
            if adjusted_qty < min_qty:
                adjusted_qty = 0
                self.logger.warning(f"{symbol} 数量调整为 0 (原值: {adjusted_qty} 小于最小值: {min_qty})")
                self.error_reasons[symbol] = f"数量过小: {quantity} 小于最小值 {min_qty}"
            elif adjusted_qty > max_qty:
                adjusted_qty = max_qty
                self.logger.warning(f"{symbol} 数量调整为最大值: {max_qty} (原值: {adjusted_qty} 大于最大值: {max_qty})")
                self.error_reasons[symbol] = f"数量过大: {quantity} 大于最大值 {max_qty}"

            if not is_close:
                notional = adjusted_qty * price
                if notional < min_notional:
                    adjusted_qty = 0
                    self.logger.warning(f"{symbol} 数量调整为 0 (因名义价值 {notional} 小于最小值 {min_notional})")
                    self.error_reasons[symbol] = f"名义价值过低: {notional} 小于最小值 {min_notional}"

            self.logger.info(
                f"{symbol} {'平仓' if is_close else ''}数量调整: {quantity} -> {adjusted_qty}, 最小值: {min_qty}, 最大值: {max_qty}, "
                f"名义价值={adjusted_qty * price}, 真实保证金={(adjusted_qty * price) / self.leverage:.6f}, 数量精度={quantity_precision}"
            )
            return adjusted_qty
        except Exception as e:
            self.logger.error(f"调整 {symbol} 数量失败: {str(e)}")
            self.error_reasons[symbol] = f"数量调整失败: {str(e)}"
            return 0.0

    def execute_trade(self, symbol: str, side: str, quantity: float, is_close: bool = False) -> bool:
        try:
            # 检查交易对状态
            exchange_info = self.client.get_exchange_info()
            symbol_info = next((s for s in exchange_info["symbols"] if s["symbol"] == symbol), None)
            if not symbol_info or symbol_info["status"] != "TRADING":
                error_msg = f"交易对 {symbol} 不可交易"
                self.logger.error(error_msg)
                self.error_reasons[symbol] = error_msg
                return False

            # 设置杠杆
            self.client.set_leverage(symbol, self.leverage)
            price = self.client.get_symbol_price(symbol)
            if price == 0.0:
                error_msg = f"{symbol} 获取价格失败"
                self.logger.error(error_msg)
                self.error_reasons[symbol] = error_msg
                return False

            # 检查账户余额
            account_info = self.client.get_account_info()
            available_balance = float(account_info["availableBalance"])
            margin_required = (quantity * price) / self.leverage
            if available_balance < margin_required:
                error_msg = f"{symbol} 余额不足: 可用 {available_balance} < 需求 {margin_required}"
                self.logger.error(error_msg)
                self.error_reasons[symbol] = error_msg
                return False

            # 检查是否为反向开仓
            try:
                current_positions = self.get_current_positions()
            except Exception as e:
                error_msg = f"获取持仓失败: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                self.error_reasons[symbol] = error_msg
                return False

            target_qty = self.final_target.get(symbol, 0)
            is_reverse_open = (
                    (side == "BUY" and current_positions.get(symbol, 0) <= 0 and target_qty > 0) or
                    (side == "SELL" and current_positions.get(symbol, 0) >= 0 and target_qty < 0)
            )
            adjusted_qty = (
                self.adjust_quantity(symbol, quantity, price, is_close=True)
                if (is_reverse_open or is_close) and price != 0.0
                else self.adjust_quantity(symbol, quantity, price, is_close=False) if price != 0.0 else 0
            )
            if adjusted_qty == 0:
                error_msg = f"{symbol} 调整数量为 0"
                self.logger.warning(error_msg)
                self.error_reasons[symbol] = error_msg
                return False

            success = False
            order_id = 0
            trade_details = {"total_quote": 0.0, "total_qty": 0.0, "price": 0.0}
            if adjusted_qty != 0:
                self.logger.debug(
                    f"准备下市价单: symbol={symbol}, side={side}, quantity={adjusted_qty}, reduce_only={is_close}")
                success, order, trade_details, order_id = self.client.place_market_order(
                    symbol, side, adjusted_qty, reduce_only=is_close
                )
                self.logger.debug(
                    f"市价单返回: success={success}, order={order}, trade_details={trade_details}, order_id={order_id}")

            if success:
                trade_key = f"trade_{symbol}_{side}_{datetime.now().strftime('%Y-%m-%d')}_{order_id}"
                self.account_metrics[trade_key] = {
                    "value": trade_details,
                    "description": (
                        f"{side} {symbol} 成交数量 {trade_details['total_qty']} "
                        f"成交均价 {trade_details['price']} "
                        f"杠杆后成交金额 {trade_details['total_quote']} "
                        f"真实成交金额 {trade_details['total_quote'] / self.leverage} "
                        f"订单ID {order_id}"
                    ),
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                }
                self.record_trade(0, symbol, side, order_id, adjusted_qty)
                self.logger.info(
                    f"交易 {symbol} {side} 成功: 订单ID={order_id}, 成交数量={trade_details['total_qty']}, "
                    f"成交均价={trade_details['price']}, 杠杆后成交金额={trade_details['total_quote']}, "
                    f"真实成交金额={trade_details['total_quote'] / self.leverage}, is_close={is_close}"
                )
                return True
            else:
                error_msg = (
                    order.get("error", str(order)) if isinstance(order, dict)
                    else str(order) if order else "未知错误"
                )
                self.logger.warning(f"交易 {symbol} {side} 下单失败，订单ID: {order_id}, 错误: {error_msg}")
                self.error_reasons[symbol] = f"下单失败: {error_msg}"
                return False
        except Exception as e:
            error_msg = f"交易 {symbol} {side} 失败: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            self.error_reasons[symbol] = error_msg
            return False

    def write_to_excel(self, filename="data/account_metrics.xlsx", run_id=None):
        """将账户指标写入 Excel 文件"""
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            current_date = datetime.now().strftime('%Y-%m-%d')
            required_metrics = [
                "position_file", "before_trade_balance", "before_available_balance",
                "after_trade_balance", "balance_loss", "balance_loss_rate",
                "after_available_balance", "btc_usdt_price",
                f"trade_commission_summary_{current_date}",
                f"trade_commission_summary_ratio_{current_date}",
                f"trade_realized_pnl_summary_{current_date}",
                f"trade_realized_pnl_summary_ratio_{current_date}",
                "pre_rebalance_return", "post_rebalance_return"
            ]

            # 获取账户信息以填充缺失指标
            try:
                account_info = self.client.get_account_info()
                total_balance = float(account_info["totalMarginBalance"])
                available_balance = float(account_info["availableBalance"])
            except Exception as e:
                self.logger.error(f"获取账户信息失败，无法填充缺失指标: {str(e)}")
                total_balance = 0.0
                available_balance = 0.0

            # 填充 after_trade_balance 和 after_available_balance
            if "after_trade_balance" not in self.account_metrics:
                self.account_metrics["after_trade_balance"] = {
                    "value": total_balance,
                    "description": "调仓后账户总保证金余额(totalMarginBalance)",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                }
            if "after_available_balance" not in self.account_metrics:
                self.account_metrics["after_available_balance"] = {
                    "value": available_balance,
                    "description": "调仓后可用保证金余额(available_balance)",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                }

            # 计算 balance_loss 和 balance_loss_rate
            if "before_trade_balance" in self.account_metrics and "after_trade_balance" in self.account_metrics:
                before_balance = float(self.account_metrics["before_trade_balance"]["value"])
                after_balance = float(self.account_metrics["after_trade_balance"]["value"])
                self.account_metrics["balance_loss"] = {
                    "value": before_balance - after_balance,
                    "description": "调仓前后账户余额变化",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                }
                self.account_metrics["balance_loss_rate"] = {
                    "value": f"{((before_balance - after_balance) / before_balance * 100) if before_balance != 0 else 0:.6f}%",
                    "description": "调仓前后账户余额变化率(%)",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                }

            # 获取 BTC/USDT 价格
            btc_usdt_price = self.client.get_symbol_price("BTCUSDT")
            if "btc_usdt_price" not in self.account_metrics:
                self.account_metrics["btc_usdt_price"] = {
                    "value": btc_usdt_price,
                    "description": f"当前 BTC/USDT 价格: {btc_usdt_price}",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                }

            # 初始化缺失的指标（首次运行或异常情况）
            for metric in required_metrics:
                if metric not in self.account_metrics:
                    if metric.startswith("trade_commission_summary_"):
                        self.account_metrics[metric] = {
                            "value": 0.0,
                            "description": f"{current_date} 买卖交易手续费总和（未计算）",
                            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                        }
                    elif metric.startswith("trade_commission_summary_ratio_"):
                        self.account_metrics[metric] = {
                            "value": "0.000000%",
                            "description": f"{current_date} 买卖交易总手续费占比（未计算）",
                            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                        }
                    elif metric.startswith("trade_realized_pnl_summary_"):
                        self.account_metrics[metric] = {
                            "value": 0.0,
                            "description": f"{current_date} 买卖交易已实现盈亏总和（未计算）",
                            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                        }
                    elif metric.startswith("trade_realized_pnl_summary_ratio_"):
                        self.account_metrics[metric] = {
                            "value": "0.000000%",
                            "description": f"{current_date} 买卖交易总盈亏占比（未计算）",
                            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                        }
                    elif metric == "pre_rebalance_return":
                        self.account_metrics[metric] = {
                            "value": "0.000000%",
                            "description": f"调仓前回报率: 首次运行或缺少历史数据",
                            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                        }
                    elif metric == "post_rebalance_return":
                        self.account_metrics[metric] = {
                            "value": "0.000000%",
                            "description": f"调仓后回报率: 首次运行或缺少历史数据",
                            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                        }

            # 检查缺失指标并记录
            missing_metrics = [m for m in required_metrics if m not in self.account_metrics]
            if missing_metrics:
                self.logger.warning(f"缺失指标: {missing_metrics}，已初始化默认值")

            data = []
            record_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            run_id = run_id or f"{record_time.replace(' ', '_').replace(':', '')}"
            for metric in required_metrics:
                if metric in self.account_metrics:
                    entry = {
                        "Metric": metric,
                        "Value": self.account_metrics[metric]["value"],
                        "Description": self.account_metrics[metric]["description"],
                        "Date": self.account_metrics[metric]["date"],
                        "Record_Time": record_time,
                        "Run_ID": run_id
                    }
                    data.append(entry)

            # 如果无数据，写入空文件
            if not data:
                self.logger.warning("无有效账户指标数据，生成空 account_metrics.xlsx")
                df_new = pd.DataFrame(columns=["Metric", "Value", "Description", "Date", "Record_Time", "Run_ID"])
            else:
                df_new = pd.DataFrame(data)

            # 写入 Excel
            if os.path.exists(filename):
                try:
                    df_existing = pd.read_excel(filename, sheet_name='Account_Metrics')
                    concat_list = [df for df in [df_existing, df_new] if not df.empty and not df.isna().all().all()]
                    if concat_list:
                        df_combined = pd.concat(concat_list, ignore_index=True)
                        df_combined = df_combined.drop_duplicates(subset=["Metric", "Run_ID"], keep="last")
                        with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
                            df_combined.to_excel(writer, index=False, sheet_name='Account_Metrics')
                    else:
                        self.logger.warning("无有效数据可拼接，写入空文件")
                        df_new.to_excel(filename, index=False, sheet_name='Account_Metrics')
                except ValueError as e:
                    self.logger.warning(f"工作表 'Account_Metrics' 不存在，创建新文件: {str(e)}")
                    df_new.to_excel(filename, index=False, sheet_name='Account_Metrics')
            else:
                df_new.to_excel(filename, index=False, sheet_name='Account_Metrics')

            self.logger.info(f"成功写入 {len(data)} 条指标到 {filename}，Run_ID={run_id}")
        except Exception as e:
            self.logger.error(f"写入 account_metrics.xlsx 失败: {str(e)}")

    def datetime_to_timestamp(self, date_time_str: str) -> int:
        """将日期时间字符串转换为Unix时间戳（毫秒）"""
        try:
            dt = datetime.strptime(date_time_str, "%Y-%m-%d_%H:%M:%S")
            return int(dt.timestamp() * 1000)
        except ValueError:
            try:
                dt = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")
                return int(dt.timestamp() * 1000)
            except ValueError as e:
                self.logger.error(f"日期时间格式错误: {str(e)}，请使用 YYYY-MM-DD_HH:MM:SS 或 YYYY-MM-DD HH:MM:SS 格式")
                raise

    def process_trade_commissions(self):
        """通过API获取指定日期的手续费总和"""
        current_date = datetime.now().strftime('%Y-%m-%d')
        commission_key = f"trade_commission_summary_{current_date}"

        # 获取调仓时间范围
        start_time_str = self.account_metrics.get("before_trade_balance", {}).get("date")
        end_time_str = self.account_metrics.get("after_trade_balance", {}).get("date")

        if not start_time_str or not end_time_str:
            self.logger.warning(f"缺少调仓时间信息，无法获取手续费: start={start_time_str}, end={end_time_str}")
            self.account_metrics[commission_key] = {
                "value": 0.0,
                "description": f"{current_date} 买卖交易手续费总和（缺少时间信息）",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }
            return self.account_metrics[commission_key]

        try:
            start_time = self.datetime_to_timestamp(start_time_str)
            end_time = self.datetime_to_timestamp(end_time_str)
            if start_time >= end_time:
                self.logger.warning(f"无效时间范围: 开始时间 {start_time_str} 不早于结束时间 {end_time_str}")
                self.account_metrics[commission_key] = {
                    "value": 0.0,
                    "description": f"{current_date} 买卖交易手续费总和（无效时间范围）",
                    "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                }
                return self.account_metrics[commission_key]

            records = self.client.get_commission_history(start_time, end_time)
            total_commission = self.client.calculate_total_commission(records)

            self.account_metrics[commission_key] = {
                "value": total_commission,
                "description": f"{current_date} 买卖交易手续费总和（API获取，{len(records)}条记录）",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }
            self.logger.info(f"手续费汇总 ({commission_key}): {total_commission} USDT, 记录数: {len(records)}")
        except Exception as e:
            self.logger.error(f"获取手续费失败: {str(e)}")
            self.account_metrics[commission_key] = {
                "value": 0.0,
                "description": f"{current_date} 买卖交易手续费总和（获取失败: {str(e)}）",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }
        return self.account_metrics[commission_key]

    def process_trade_realized_pnl(self):
        """计算特定日期买卖交易的已实现盈亏总和"""
        current_date = datetime.now().strftime('%Y-%m-%d')
        trade_keys = [key for key in self.account_metrics.keys() if
                      ("SELL" in key or "BUY" in key) and current_date in key]
        total_realized_pnl = 0.0
        for trade_key in trade_keys:
            trades = self.account_metrics[trade_key]["value"]  # value 是 trades 列表
            if not trades:  # 检查 trades 是否为空
                self.logger.warning(f"{trade_key} 无成交记录，跳过盈亏计算")
                continue
            try:
                total_realized_pnl += sum(float(trade["realizedPnl"]) for trade in trades)
            except Exception as e:
                self.logger.error(f"{trade_key} 计算盈亏失败: {str(e)}")
                continue
        pnl_key = f"trade_realized_pnl_summary_{current_date}"
        self.account_metrics[pnl_key] = {
            "value": total_realized_pnl,
            "description": f"{current_date} 买卖交易已实现盈亏总和",
            "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        }
        self.logger.info(f"盈亏汇总 ({pnl_key}): {total_realized_pnl}")
        return self.account_metrics[pnl_key]

    def cancel_all_open_orders(self):
        """撤消所有未成交挂单"""
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
            symbol_info = next(s for s in exchange_info["symbols"] if s["symbol"] == symbol)  # 找到指定交易对的信息
            return symbol_info["pricePrecision"]  # 返回价格精度
        except Exception as e:
            self.logger.error(f"获取 {symbol}  substituting_precision 失败: {str(e)}")  # 记录错误日志
            return 8  # 默认返回精度 8

    def adjust_or_open_positions(self, long_candidates: List[Dict], short_candidates: List[Dict], run_id: str,
                                 date_str: str):
        """持续限价单调仓，直到完成或超时后市价单补齐，非目标持仓通过限价单平仓"""
        try:
            # 获取账户信息并缓存
            account_info = self.client.get_account_info()
            total_balance = float(account_info["totalMarginBalance"]) - self.basic_funds
            available_balance = float(account_info["availableBalance"])
            self.logger.info(
                f"=====账户信息获取成功: totalMarginBalance = {total_balance + self.basic_funds}, "
                f"available_balance = {available_balance}====="
            )

            # 记录调仓前账户信息
            self.account_metrics["before_trade_balance"] = {
                "value": total_balance + self.basic_funds,
                "description": "调仓前账户总保证金余额(totalMarginBalance)",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }
            self.account_metrics["before_available_balance"] = {
                "value": available_balance,
                "description": "调仓前可用保证金余额(available_balance)",
                "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            }

            # 计算每笔交易的资金份额
            per_share = total_balance / (self.num_long_pos + self.num_short_pos)
            self.logger.info(f"每笔交易资金份额: per_share={per_share}")
            self.per_share = per_share

            # 初始化目标持仓
            self.final_target = {}
            current_positions = self.get_stable_positions()
            is_first_day = not bool(current_positions)
            non_zero_positions = {k: v for k, v in current_positions.items() if v != 0}
            self.logger.info(f"当前持仓: {non_zero_positions}，是否第一天: {is_first_day}")
            long_count, short_count = 0, 0

            # 处理多头候选
            for candidate in long_candidates:
                if long_count >= self.num_long_pos:
                    break
                ticker = candidate["ticker"]
                price = self.client.get_symbol_price(ticker)
                if price != 0.0:
                    qty = self.adjust_quantity(ticker, self.calculate_position_size(total_balance, price), price)
                    if qty > 0:
                        self.final_target[ticker] = qty
                        long_count += 1
                    else:
                        self.error_reasons[ticker] = self.error_reasons.get(ticker, "数量调整为 0")
                else:
                    self.error_reasons[ticker] = self.error_reasons.get(ticker, "获取价格失败")

            # 处理空头候选
            for candidate in short_candidates:
                if short_count >= self.num_short_pos:
                    break
                ticker = candidate["ticker"]
                price = self.client.get_symbol_price(ticker)
                if price != 0.0:
                    qty = self.adjust_quantity(ticker, self.calculate_position_size(total_balance, price), price)
                    if qty > 0:
                        self.final_target[ticker] = -qty
                        short_count += 1
                    else:
                        self.error_reasons[ticker] = self.error_reasons.get(ticker, "数量调整为 0")
                else:
                    self.error_reasons[ticker] = self.error_reasons.get(ticker, "获取价格失败")

            self.logger.info(f"初始化目标持仓: {self.final_target}")

            # 计算所需调整的持仓
            current_positions = self.get_stable_positions()
            if not isinstance(current_positions, dict):
                self.logger.error("get_stable_positions 返回格式错误，预期 Dict[str, float]")
                raise ValueError("持仓数据格式错误")
            require = {k: self.final_target.get(k, 0) - current_positions.get(k, 0) for k in
                       set(self.final_target) | set(current_positions)}
            all_orders = {ticker: qty for ticker, qty in require.items() if qty != 0}
            adjusted_to_zero = set()

            # 获取交易所信息中的 tick_size
            exchange_info = self.client.client.futures_exchange_info()
            tick_sizes = {
                s["symbol"]: float(next(f for f in s["filters"] if f["filterType"] == "PRICE_FILTER")["tickSize"])
                for s in exchange_info["symbols"]
            }

            self.logger.info("=" * 20 + " 开始持续限价单调仓 " + "=" * 20)
            round_num = 0
            start_time = time.time()
            max_wait_seconds = self.max_wait_time * 60

            while True:
                round_num += 1
                self.logger.info(f"开始第 {round_num} 轮调仓...")
                pending_orders = []
                # 实时获取账户信息以更新可用余额
                try:
                    account_info = self.client.get_account_info()
                    available_balance = float(account_info["availableBalance"])
                    self.logger.info(f"第 {round_num} 轮开始时可用余额: {available_balance}")
                except Exception as e:
                    self.logger.error(f"第 {round_num} 轮获取账户信息失败: {str(e)}，使用缓存余额: {available_balance}")

                # 检查未成交的限价单，包括部分成交订单
                open_orders_info = self.client.client.futures_get_open_orders()
                for order in open_orders_info:
                    ticker = order['symbol']
                    executed_qty = float(order['executedQty'])
                    if executed_qty > 0 and order['status'] == 'PARTIALLY_FILLED':
                        self.logger.info(
                            f"发现部分成交订单: {ticker}, 已成交数量: {executed_qty}, 订单ID: {order['orderId']}")
                        side = order['side']
                        current_qty = current_positions.get(ticker, 0)
                        if side == 'BUY':
                            current_positions[ticker] = current_qty + executed_qty
                        else:
                            current_positions[ticker] = current_qty - executed_qty

                # 检查是否超时
                elapsed_time = time.time() - start_time
                if elapsed_time >= max_wait_seconds:
                    self.logger.info(
                        f"限价单调仓已运行 {elapsed_time:.2f} 秒，超过最大等待时间 {max_wait_seconds} 秒，进入市价单补齐")
                    break

                processed_tickers = set()

                # 处理需要平仓的交易对
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
                for ticker, qty in close_orders.items():
                    if qty == 0 or ticker in adjusted_to_zero:
                        continue
                    side = "BUY" if qty > 0 or current_positions[ticker] < 0 else "SELL"
                    qty_to_close = abs(qty) if abs(qty) < abs(current_positions[ticker]) else abs(
                        current_positions[ticker])
                    price = self.get_postonly_price(ticker, side)
                    if price == 0.0:
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 获取一档价格失败，跳过")
                        self.error_reasons[ticker] = "获取价格失败"
                        continue
                    adjusted_qty = self.adjust_quantity(ticker, qty_to_close, price, is_close=True)
                    if adjusted_qty <= 0:
                        self.logger.warning(f"第 {round_num} 轮: {ticker} 调整数量为 0，跳过")
                        self.error_reasons[ticker] = "数量调整为 0"
                        continue
                    success, order, order_id, trade_details = self.client.place_postonly_order(ticker, side,
                                                                                               adjusted_qty,
                                                                                               price,
                                                                                               is_close=True)
                    if success:
                        pending_orders.append((ticker, order_id, side, adjusted_qty))
                        self.logger.info(
                            f"第 {round_num} 轮: {ticker} {side} postOnly 平仓挂单成功，订单ID={order_id}, is_close=True")
                        processed_tickers.add(ticker)
                        current_positions[ticker] = current_positions.get(ticker,
                                                                          0) + adjusted_qty if side == "BUY" else current_positions.get(
                            ticker, 0) - adjusted_qty
                        all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(ticker, 0)
                    else:
                        error_msg = str(order) if order else "未知错误"
                        self.error_reasons[ticker] = f"下单失败: {error_msg}"
                        self.handle_postonly_error(round_num, ticker, side, qty_to_close, error_msg, pending_orders)

                        # 处理开仓订单
                        open_orders = {ticker: qty for ticker, qty in all_orders.items() if
                                       ticker not in adjusted_to_zero}
                        self.logger.info(f"第 {round_num} 轮开仓前可用余额: {available_balance}")

                        for ticker, qty in open_orders.items():
                            if qty == 0 or ticker in processed_tickers:
                                continue
                            side = "BUY" if qty > 0 else "SELL"
                            price = self.get_postonly_price(ticker, side)
                            if price == 0.0:
                                self.logger.warning(f"第 {round_num} 轮: {ticker} 获取一档价格失败，跳过")
                                self.error_reasons[ticker] = "获取价格失败"
                                continue
                            is_reverse_open = (
                                    ticker in current_positions and (
                                    (qty > 0 and current_positions[ticker] <= 0 and self.final_target.get(ticker,
                                                                                                          0) > 0) or
                                    (qty < 0 and current_positions[ticker] >= 0 and self.final_target.get(ticker,
                                                                                                          0) < 0)
                            )
                            )
                            if is_reverse_open:
                                adjusted_qty = self.adjust_quantity(ticker, abs(qty), price, is_close=True)
                                if adjusted_qty <= 0:
                                    self.logger.warning(f"第 {round_num} 轮: {ticker} 反向开仓调整数量为 0，跳过")
                                    self.error_reasons[ticker] = "数量调整为 0"
                                    continue
                                success, order, order_id, trade_details = self.client.place_postonly_order(ticker, side,
                                                                                                           adjusted_qty,
                                                                                                           price,
                                                                                                           is_close=True)
                                if success:
                                    pending_orders.append((ticker, order_id, side, adjusted_qty))
                                    self.logger.info(
                                        f"第 {round_num} 轮: {ticker} {side} postOnly 反向开仓挂单成功，订单ID={order_id}, is_close=True")
                                    processed_tickers.add(ticker)
                                    current_positions[ticker] = current_positions.get(ticker,
                                                                                      0) + adjusted_qty if side == "BUY" else current_positions.get(
                                        ticker, 0) - adjusted_qty
                                    all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(
                                        ticker, 0)
                                    # 更新可用余额（已在循环开头更新，无需重复）
                                else:
                                    error_msg = str(order) if order else "未知错误"
                                    self.error_reasons[ticker] = f"下单失败: {error_msg}"
                                    self.handle_postonly_error(round_num, ticker, side, qty, error_msg, pending_orders)
                            else:
                                adjusted_qty = self.adjust_quantity(ticker, abs(qty), price, is_close=False)
                                if adjusted_qty <= 0:
                                    self.logger.warning(f"第 {round_num} 轮: {ticker} 调整数量为 0，跳过")
                                    self.error_reasons[ticker] = "数量调整为 0"
                                    continue
                                margin_required = (adjusted_qty * price) / self.leverage
                                if available_balance < margin_required:
                                    self.logger.info(
                                        f"第 {round_num} 轮: 可用余额 {available_balance} < 需求 {margin_required}，跳过 {ticker}")
                                    self.error_reasons[
                                        ticker] = f"余额不足: 可用 {available_balance} < 需求 {margin_required}"
                                    continue
                                success, order, order_id, trade_details = self.client.place_postonly_order(ticker, side,
                                                                                                           adjusted_qty,
                                                                                                           price,
                                                                                                           is_close=False)
                                if success:
                                    pending_orders.append((ticker, order_id, side, adjusted_qty))
                                    self.logger.info(
                                        f"第 {round_num} 轮: {ticker} {side} postOnly 挂单成功，订单ID={order_id}, is_close=False")
                                    processed_tickers.add(ticker)
                                    current_positions[ticker] = current_positions.get(ticker,
                                                                                      0) + adjusted_qty if side == "BUY" else current_positions.get(
                                        ticker, 0) - adjusted_qty
                                    all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(
                                        ticker, 0)
                                    # 更新可用余额（已在循环开头更新，无需重复）
                                else:
                                    error_msg = str(order) if order else "未知错误"
                                    self.error_reasons[ticker] = f"下单失败: {error_msg}"
                                    self.handle_postonly_error(round_num, ticker, side, qty, error_msg, pending_orders)

                # 处理挂单状态
                if pending_orders:
                    self.logger.info(f"第 {round_num} 轮挂单等待，最多 30 秒...")
                    wait_start = time.time()
                    while time.time() - wait_start < 30:
                        still_pending = []
                        for ticker, order_id, side, qty in pending_orders:
                            try:
                                order_info = self.client.client.futures_get_order(symbol=ticker, orderId=order_id)
                                if order_info["status"] == "NEW":
                                    still_pending.append((ticker, order_id, side, qty))
                                elif order_info["status"] == "FILLED":
                                    executed_qty = float(order_info["executedQty"])
                                    self.record_trade(round_num, ticker, side, order_id, executed_qty)
                                    self.logger.info(
                                        f"第 {round_num} 轮: {ticker} {side} 已完全成交，订单ID={order_id}, 成交数量={executed_qty}")
                                    current_positions[ticker] = current_positions.get(ticker,
                                                                                      0) + executed_qty if side == "BUY" else current_positions.get(
                                        ticker, 0) - executed_qty
                                    all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(
                                        ticker, 0)
                                elif order_info["status"] == "PARTIALLY_FILLED":
                                    executed_qty = float(order_info["executedQty"])
                                    self.record_trade(round_num, ticker, side, order_id, executed_qty)
                                    still_pending.append((ticker, order_id, side, qty - executed_qty))
                                    self.logger.info(
                                        f"第 {round_num} 轮: {ticker} {side} 部分成交，订单ID={order_id}, 已成交数量={executed_qty}")
                                    current_positions[ticker] = current_positions.get(ticker,
                                                                                      0) + executed_qty if side == "BUY" else current_positions.get(
                                        ticker, 0) - executed_qty
                                    all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(
                                        ticker, 0)
                                else:
                                    self.logger.info(
                                        f"第 {round_num} 轮: {ticker} {side} 订单已取消或失效，状态={order_info['status']}, 订单ID={order_id}")
                                    self.error_reasons[ticker] = f"订单取消或失效: 状态 {order_info['status']}"
                                    # 检查是否意外成交
                                    trades = self.client.client.futures_account_trades(symbol=ticker, orderId=order_id)
                                    if trades:
                                        executed_qty = sum(float(trade['qty']) for trade in trades)
                                        self.record_trade(round_num, ticker, side, order_id, executed_qty)
                                        self.logger.info(
                                            f"第 {round_num} 轮: {ticker} {side} 订单状态为 {order_info['status']}，但发现成交记录，记录成交数量={executed_qty}")
                                        current_positions[ticker] = current_positions.get(ticker,
                                                                                          0) + executed_qty if side == "BUY" else current_positions.get(
                                            ticker, 0) - executed_qty
                                        all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(
                                            ticker, 0)
                            except Exception as e:
                                self.logger.error(
                                    f"第 {round_num} 轮: 检查 {ticker} 订单 {order_id} 状态失败: {str(e)}")
                                if "APIError(code=-2011)" in str(e):
                                    # 订单不存在，可能已成交
                                    trades = self.client.client.futures_account_trades(symbol=ticker, orderId=order_id)
                                    if trades:
                                        executed_qty = sum(float(trade['qty']) for trade in trades)
                                        self.record_trade(round_num, ticker, side, order_id, executed_qty)
                                        self.logger.info(
                                            f"第 {round_num} 轮: {ticker} {side} 订单不存在但发现成交记录，记录成交数量={executed_qty}, 订单ID={order_id}")
                                        current_positions[ticker] = current_positions.get(ticker,
                                                                                          0) + executed_qty if side == "BUY" else current_positions.get(
                                            ticker, 0) - executed_qty
                                        all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(
                                            ticker, 0)
                                    else:
                                        self.logger.warning(
                                            f"第 {round_num} 轮: {ticker} {side} 订单不存在且无成交记录，订单ID={order_id}")
                                        self.error_reasons[ticker] = f"订单不存在且无成交记录: {str(e)}"
                                else:
                                    still_pending.append((ticker, order_id, side, qty))
                                    self.error_reasons[ticker] = f"检查订单状态失败: {str(e)}"
                        pending_orders = still_pending
                        if not pending_orders:
                            self.logger.info(f"第 {round_num} 轮所有挂单已处理完成，提前结束等待")
                            break
                        time.sleep(2)

                    if pending_orders:
                        self.logger.info(f"第 {round_num} 轮结束，取消所有未成交挂单...")
                        for ticker, order_id, side, qty in pending_orders:
                            try:
                                self.client.cancel_order(ticker, order_id)
                                self.logger.info(
                                    f"第 {round_num} 轮: 成功取消 {ticker} {side} 挂单，订单ID={order_id}")
                            except Exception as e:
                                self.logger.error(
                                    f"第 {round_num} 轮: 取消 {ticker} {side} 挂单失败，订单ID={order_id}，错误: {str(e)}")
                                if "APIError(code=-2011)" in str(e):
                                    # 订单不存在，可能已成交
                                    trades = self.client.client.futures_account_trades(symbol=ticker, orderId=order_id)
                                    if trades:
                                        executed_qty = sum(float(trade['qty']) for trade in trades)
                                        self.record_trade(round_num, ticker, side, order_id, executed_qty)
                                        self.logger.info(
                                            f"第 {round_num} 轮: {ticker} {side} 撤单失败但发现成交记录，记录成交数量={executed_qty}, 订单ID={order_id}")
                                        current_positions[ticker] = current_positions.get(ticker,
                                                                                          0) + executed_qty if side == "BUY" else current_positions.get(
                                            ticker, 0) - executed_qty
                                        all_orders[ticker] = self.final_target.get(ticker, 0) - current_positions.get(
                                            ticker, 0)
                                    else:
                                        self.logger.warning(
                                            f"第 {round_num} 轮: {ticker} {side} 撤单失败且无成交记录，订单ID={order_id}")
                                        self.error_reasons[ticker] = f"撤单失败且无成交记录: {str(e)}"
                                else:
                                    self.error_reasons[ticker] = f"取消订单失败: {str(e)}"
                        pending_orders = []

                # 更新当前持仓和订单需求
                current_positions = self.get_stable_positions()
                all_orders = {
                    k: self.final_target.get(k, 0) - current_positions.get(k, 0)
                    for k in set(self.final_target) | set(current_positions)
                    if self.final_target.get(k, 0) != current_positions.get(k, 0)
                }
                # 处理非首日的小额调整
                if not is_first_day:
                    for ticker, qty in all_orders.items():
                        price = self.client.get_symbol_price(ticker)
                        if price != 0.0:
                            adjusted_qty = self.adjust_quantity(ticker, abs(qty), price, is_close=True)
                            is_open = ticker in self.final_target and self.final_target[ticker] != 0
                            notional = adjusted_qty * price
                            if is_open and (adjusted_qty == 0 or notional < 5):
                                self.logger.info(
                                    f"第 {round_num} 轮: {ticker} 差值 {qty} 为小额调整（名义价值 {notional} < 5 或数量过小），跳过后续轮次"
                                )
                                self.final_target[ticker] = current_positions.get(ticker, 0)
                                adjusted_to_zero.add(ticker)
                                self.error_reasons[ticker] = f"小额调整忽略: 名义价值 {notional} < 5 或数量过小"
                    all_orders = {ticker: qty for ticker, qty in all_orders.items() if ticker not in adjusted_to_zero}

                self.logger.info(f"第 {round_num} 轮完成，剩余需求: {all_orders}")
                if not all_orders:
                    self.logger.info(f"第 {round_num} 轮后所有目标已达成，结束限价单调仓")
                    break

            # 市价单补齐逻辑
            self.logger.info("=" * 20 + f" 市价单补齐：{'第一天' if is_first_day else '后续调仓'} " + "=" * 20)
            current_positions = self.get_stable_positions()
            remaining_require = {k: self.final_target.get(k, 0) - current_positions.get(k, 0) for k in
                                 set(self.final_target) | set(current_positions)}
            if not any(qty != 0 for qty in remaining_require.values()):
                self.logger.info("市价单补齐：所有目标已达成，无需补齐")
            else:
                self.logger.info(f"市价单补齐前可用余额: {available_balance}")
                force_adjustments = remaining_require
                if force_adjustments:
                    if not is_first_day:
                        self.logger.info("市价单补齐：处理需要强制调整的交易对，先平仓释放资金...")
                        for ticker, qty in force_adjustments.items():
                            if ticker in current_positions and current_positions[ticker] != 0 and self.final_target.get(
                                    ticker, 0) == 0:
                                side = "BUY" if current_positions[ticker] < 0 else "SELL"
                                qty_to_close = abs(current_positions[ticker])
                                success = self.execute_trade(ticker, side, qty_to_close, is_close=True)
                                if success:
                                    self.logger.info(
                                        f"市价单补齐平仓: {ticker} {side} 数量={qty_to_close}, is_close=True")
                                else:
                                    self.logger.warning(f"市价单补齐平仓失败: {ticker} {side}")
                    # 更新可用余额
                    account_info = self.client.get_account_info()
                    available_balance = float(account_info["availableBalance"])
                    self.logger.info(f"市价单补齐开仓前可用余额: {available_balance}")
                    for ticker, qty in force_adjustments.items():
                        if qty == 0:
                            continue
                        # 验证交易对
                        exchange_info = self.client.get_exchange_info()
                        symbol_info = next((s for s in exchange_info["symbols"] if s["symbol"] == ticker), None)
                        if not symbol_info or symbol_info["status"] != "TRADING":
                            error_msg = f"交易对 {ticker} 不可交易"
                            self.logger.warning(f"市价单补齐: {error_msg}")
                            self.error_reasons[ticker] = error_msg
                            continue
                        side = "BUY" if qty > 0 else "SELL"
                        price = self.client.get_symbol_price(ticker)
                        if price == 0.0:
                            self.logger.warning(f"市价单补齐: {ticker} 获取价格失败，跳过")
                            self.error_reasons[ticker] = "获取价格失败"
                            continue
                        is_open = ticker in self.final_target and self.final_target[ticker] != 0
                        adjusted_qty = self.adjust_quantity(ticker, abs(qty), price, is_close=not is_open)
                        if adjusted_qty == 0:
                            self.logger.warning(f"市价单补齐: {ticker} 调整数量为 0，跳过")
                            self.error_reasons[ticker] = "数量调整为 0"
                            continue
                        margin_required = (adjusted_qty * price) / self.leverage
                        if available_balance < margin_required:
                            self.logger.info(
                                f"市价单补齐: 可用余额 {available_balance} < 需求 {margin_required}，跳过 {ticker}"
                            )
                            self.error_reasons[ticker] = f"余额不足: 可用 {available_balance} < 需求 {margin_required}"
                            continue
                        success = self.execute_trade(ticker, side, adjusted_qty, is_close=not is_open)
                        if success:
                            self.logger.info(
                                f"市价单补齐: {ticker} {side} 成功，数量={adjusted_qty}, is_close={not is_open}"
                            )
                            current_positions[ticker] = current_positions.get(ticker,
                                                                              0) + adjusted_qty if side == "BUY" else current_positions.get(
                                ticker, 0) - adjusted_qty
                            # 更新可用余额
                            account_info = self.client.get_account_info()
                            available_balance = float(account_info["availableBalance"])
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

            # 移除或调整 position_file 的设置，避免覆盖 run 方法中的值
            # self.account_metrics["position_file"] = {
            #     "value": self.positions_file,
            #     "description": f"持仓记录文件路径: {self.positions_file}",
            #     "date": datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            # }

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

            # 先记录所有指标（包括手续费、盈亏等）
            self.record_post_trade_metrics(total_balance=total_balance)
            # 计算回报率
            self.calculate_and_append_returns()
            # 再写入 Excel 和 JSON
            self.write_to_excel(run_id=run_id)
            self.save_to_json(date_str, run_id)
            self.logger.info("调仓完成，账户指标已记录")
        except Exception as e:
            self.logger.error(f"调仓失败: {str(e)}")
            self.error_reasons["global"] = f"调仓失败: {str(e)}"

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
        """记录成交订单到 account_metrics，使用文档1的统一格式"""
        trades = self.client.client.futures_account_trades(symbol=ticker, orderId=order_id)
        if not trades:
            self.logger.warning(f"第 {round_num} 轮: {ticker} {side} 订单ID={order_id} 无成交记录")
            return

        # 计算汇总信息
        total_qty = sum(float(trade['qty']) for trade in trades)
        total_quote = sum(float(trade['qty']) * float(trade['price']) for trade in trades)
        avg_price = total_quote / total_qty if total_qty > 0 else 0

        # 获取第一笔交易的详细信息作为模板
        first_trade = trades[0]

        # 构建统一格式的交易记录
        trade_key = f"trade_{ticker}_{side}_{datetime.now().strftime('%Y-%m-%d')}_{order_id}"

        self.account_metrics[trade_key] = {
            "value": [{
                "symbol": first_trade['symbol'],
                "id": int(first_trade['id']),
                "orderId": int(first_trade['orderId']),
                "side": first_trade['side'],
                "price": f"{float(first_trade['price']):.4f}",
                "qty": f"{float(first_trade['qty']):.4f}",
                "realizedPnl": first_trade.get('realizedPnl', '0'),
                "quoteQty": f"{float(first_trade['quoteQty']):.6f}",
                "commission": first_trade['commission'],
                "commissionAsset": first_trade['commissionAsset'],
                "time": int(first_trade['time']),
                "positionSide": first_trade.get('positionSide', 'BOTH'),
                "buyer": first_trade['buyer'],
                "maker": first_trade['maker']
            }],
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
                        if ticker in current_positions and current_positions[ticker] > 0 and closed < to_close:  # 如果有持仓且需平仓
                            qty = current_positions[ticker]  # 获取持仓数量
                            if self.execute_trade(ticker, "SELL", abs(qty)):  # 执行卖出平仓
                                closed += 1  # 已平仓计数器加 1

                short_candidates_sorted = sorted(short_candidates, key=lambda x: x['id'], reverse=True)  # 按 ID 降序排序空头候选
                current_shorts = {ticker for ticker, qty in current_positions.items() if qty < 0}  # 获取当前空头交易对
                opened = 0  # 初始化已开仓计数器
                for candidate in short_candidates_sorted:  # 遍历空头候选
                    if candidate['ticker'] not in current_shorts and opened < to_close:  # 如果不在当前空头且需开仓
                        price = self.client.get_symbol_price(candidate['ticker'])  # 获取价格
                        qty = self.adjust_quantity(candidate['ticker'], self.calculate_position_size(total_balance, price), price) if price != 0.0 else 0  # 计算并调整数量
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
                        if ticker in current_positions and current_positions[ticker] < 0 and closed < to_close:  # 如果有持仓且需平仓
                            qty = current_positions[ticker]  # 获取持仓数量
                            if self.execute_trade(ticker, "BUY", abs(qty)):  # 执行买入平仓
                                closed += 1  # 已平仓计数器加 1

                long_candidates_sorted = sorted(long_candidates, key=lambda x: x['id'])  # 按 ID 升序排序多头候选
                current_longs = {ticker for ticker, qty in current_positions.items() if qty > 0}  # 获取当前多头交易对
                opened = 0  # 初始化已开仓计数器
                for candidate in long_candidates_sorted:  # 遍历多头候选
                    if candidate['ticker'] not in current_longs and opened < to_close:  # 如果不在当前多头且需开仓
                        price = self.client.get_symbol_price(candidate['ticker'])  # 获取价格
                        qty = self.adjust_quantity(candidate['ticker'], self.calculate_position_size(total_balance, price), price) if price != 0.0 else 0  # 计算并调整数量
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

            # 添加交易记录
            for symbol, side, order_id, trade in trade_records:
                trade_key = f"trade_{symbol}_{side}_{order_id}_{current_date.replace('-', '-')}"
                account_info_data[trade_key] = {
                    'value': trade,
                    'description': f"{side} {symbol} 成交数量 {trade['total_qty']} 成交均价 {trade['price']} 杠杆后成交金额 {trade['total_quote']} 真实成交金额 {trade['real_quote']} 订单ID {trade['order_id']}",
                    'date': datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                }

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
            self.save_to_json(date_str, run_id)
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
            self.save_to_json(date_str, run_id)
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
    client = BinanceFuturesClient(api_config["api_key"], api_config["api_secret"], api_config["test_net"] == "True", logger)  # 创建 Binance 客户端实例
    engine = TradingEngine(client, config, logger)  # 创建交易引擎实例
    engine.run("20250324")  # 运行交易引擎，指定日期为 2025-03-24