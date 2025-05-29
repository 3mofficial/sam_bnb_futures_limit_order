# 从binance.client模块导入Client类，用于与Binance API交互
import time
from binance.client import Client
# 导入logging模块，用于记录日志
import logging
# 从typing模块导入类型提示，用于函数参数和返回值的类型注解
from typing import Dict, Any, List, Tuple
from .config_loader import ConfigLoader
import traceback


class BinanceFuturesClient:
    """Binance U 本位永续合约客户端封装类，用于封装Binance期货API操作"""

    def __init__(self, api_key: str, api_secret: str, testnet: bool, logger: logging.Logger):
        """初始化 Binance 客户端

        Args:
            api_key (str): Binance API 密钥，用于身份验证
            api_secret (str): Binance API 私钥，用于签名请求
            testnet (bool): 是否使用测试网络，True为测试网，False为实盘
            logger (logging.Logger): 日志记录器，用于记录操作日志
        """
        # 保存日志记录器实例
        self.logger = logger
        # 初始化 Binance 客户端，支持测试网络
        self.client = Client(api_key, api_secret, testnet=testnet)

        # 加载配置
        # 创建ConfigLoader实例以加载配置文件
        self.config_loader = ConfigLoader()

        # 获取交易相关配置（如交易时间、参数）
        self.trading_config = self.config_loader.get_trading_config()

        # 从配置中获取基础金额并转换为整数，赋值给实例变量
        self.basic_funds = float(self.trading_config["basic_funds"])

        self.leverage = int(self.trading_config["leverage"])
        # 记录初始化成功的日志
        self.logger.info(f"Binance 客户端初始化成功，测试网络: {testnet}")

        # 调用私有方法设置账户模式
        self._setup_account_modes()

    def _setup_account_modes(self):
        """设置账户模式为全仓、单币种、单向持仓，在设置前检查当前状态"""
        try:
            # 1. 检查并设置全仓模式（Cross Margin）
            # 获取期货账户信息，检查是否有逐仓模式的交易对
            account_info = self.client.futures_account()
            has_isolated = any(pos['isolated'] for pos in account_info.get('positions', []))

            if has_isolated:
                # 如果存在逐仓模式的交易对，逐个切换为全仓模式
                for pos in account_info['positions']:
                    if pos['isolated']:
                        try:
                            self.client.futures_change_margin_type(symbol=pos['symbol'], marginType="CROSSED")
                            self.logger.info(f"交易对 {pos['symbol']} 保证金模式设置为全仓 (CROSSED)")
                        except Exception as e:
                            if "No need to change margin type" in str(e):
                                self.logger.info(f"交易对 {pos['symbol']} 已是全仓模式 (CROSSED)，无需修改")
                            else:
                                raise  # 抛出其他意外错误
            else:
                self.logger.info("所有交易对已是全仓模式 (CROSSED)，无需修改")

            # 2. 检查并设置单资产模式（Single Currency）
            multi_asset_mode = self.client.futures_get_multi_assets_mode()['multiAssetsMargin']
            if multi_asset_mode:  # True 表示多资产模式
                self.client.futures_change_multi_assets_mode(multiAssetsMargin="false")
                self.logger.info("账户资产模式设置为单币种 (Single)")
            else:
                self.logger.info("账户已是单币种模式 (Single)，无需修改")

            # 3. 检查并设置单向持仓模式（One-way）
            position_mode = self.client.futures_get_position_mode()['dualSidePosition']
            if position_mode:  # True 表示双向持仓
                self.client.futures_change_position_mode(dualSidePosition="false")
                self.logger.info("账户仓位模式设置为单向 (One-way)")
            else:
                self.logger.info("账户已是单向模式 (One-way)，无需修改")

        except AttributeError as e:
            self.logger.error(f"设置账户模式失败: {str(e)}")
        except Exception as e:
            self.logger.error(f"设置账户模式时发生错误: {str(e)}")

    def get_account_info(self) -> Dict[str, Any]:
        retries = 3
        delay = 1.0
        for attempt in range(retries):
            try:
                info = self.client.futures_account()
                self.logger.debug(f"获取账户信息成功: {info}")
                return info
            except Exception as e:
                self.logger.warning(f"获取账户信息失败，尝试 {attempt + 1}/{retries}，错误: {str(e)}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    self.logger.error(f"获取账户信息失败: {str(e)}")
                    raise

    def get_symbol_price(self, symbol: str) -> float:
        """获取指定交易对的当前市价

        Args:
            symbol (str): 交易对符号（如 BTCUSDT）

        Returns:
            float: 当前市价，如果失败或交易对已下架则返回 0.0
        """
        try:
            # 先检查交易对是否处于 TRADING 状态
            exchange_info = self.client.futures_exchange_info()
            symbol_status = None
            for symbol_info in exchange_info["symbols"]:
                if symbol_info["symbol"] == symbol:
                    symbol_status = symbol_info["status"]
                    break

            if symbol_status != "TRADING":
                self.logger.info(f"交易对 {symbol} 不可交易，状态: {symbol_status}")
                return 0.0  # 交易对不可交易，返回默认值

            # 调用 API 获取指定交易对的最新价格
            ticker = self.client.futures_symbol_ticker(symbol=symbol)
            # 检查 ticker 是否为空或不含 "price" 键
            if not ticker or "price" not in ticker:
                self.logger.info(f"获取 {symbol} 市价失败: ticker 数据无效或为空 - {ticker}")
                return 0.0  # 返回 0.0 作为默认值

            # 将价格转换为浮点数
            price = float(ticker["price"])
            return price
        except Exception as e:
            # 如果获取失败，记录错误日志但不抛出异常
            self.logger.error(f"获取 {symbol} 市价失败: {str(e)}")
            return 0.0  # 返回 0.0 作为默认值

    def get_exchange_info(self) -> Dict[str, Any]:
        """获取交易所信息

        Returns:
            Dict[str, Any]: 交易所信息字典，包含交易对规则等
        """
        try:
            # 调用API获取交易所信息
            info = self.client.futures_exchange_info()
            # 记录成功获取信息的日志
            self.logger.info("交易所信息获取成功")
            return info
        except Exception as e:
            # 如果获取失败，记录错误日志并抛出异常
            self.logger.error(f"获取交易所信息失败: {str(e)}")
            raise

    def set_leverage(self, symbol: str, leverage: int):
        """设置指定交易对的杠杆

        Args:
            symbol (str): 交易对符号（如 BTCUSDT）
            leverage (int): 杠杆倍数（如 10 表示10倍杠杆）
        """
        try:
            # 调用API设置指定交易对的杠杆
            self.client.futures_change_leverage(symbol=symbol, leverage=leverage)
            # 记录成功设置杠杆的日志
            self.logger.info(f"{symbol} 杠杆设置为 {leverage}")
        except Exception as e:
            # 如果设置失败，记录错误日志并抛出异常
            self.logger.error(f"设置 {symbol} 杠杆失败: {str(e)}")
            raise

    def place_market_order(self, symbol: str, side: str, quantity: float, reduce_only: bool = False) -> Tuple[
        bool, Dict[str, Any], Dict[str, float], int]:
        order_id = 0
        default_trade_details = {"total_quote": 0.0, "total_qty": 0.0, "price": 0.0}
        try:
            # 验证 symbol
            if not symbol or not isinstance(symbol, str) or not symbol.isalnum():
                error_msg = f"无效的交易对: {symbol}"
                self.logger.error(error_msg)
                return False, {"error": error_msg}, default_trade_details, 0

            # 获取交易所信息以验证交易对和数量
            exchange_info = self.client.futures_exchange_info()
            symbol_info = next((s for s in exchange_info["symbols"] if s["symbol"] == symbol), None)
            if not symbol_info:
                error_msg = f"交易对 {symbol} 不存在"
                self.logger.error(error_msg)
                return False, {"error": error_msg}, default_trade_details, 0
            if symbol_info["status"] != "TRADING":
                error_msg = f"交易对 {symbol} 不可交易，状态: {symbol_info['status']}"
                self.logger.error(error_msg)
                return False, {"error": error_msg}, default_trade_details, 0

            # 验证并格式化 quantity
            filters = {f["filterType"]: f for f in symbol_info["filters"]}
            quantity_precision = symbol_info["quantityPrecision"]
            step_size = float(filters["LOT_SIZE"]["stepSize"])
            min_qty = float(filters["LOT_SIZE"]["minQty"])
            max_qty = float(filters["LOT_SIZE"]["maxQty"])
            if not isinstance(quantity, (int, float)) or quantity <= 0:
                error_msg = f"无效的数量: {quantity}"
                self.logger.error(error_msg)
                return False, {"error": error_msg}, default_trade_details, 0
            adjusted_qty = round(round(quantity / step_size) * step_size, quantity_precision)
            if adjusted_qty < min_qty:
                error_msg = f"数量 {adjusted_qty} 小于最小值 {min_qty}"
                self.logger.error(error_msg)
                return False, {"error": error_msg}, default_trade_details, 0
            if adjusted_qty > max_qty:
                error_msg = f"数量 {adjusted_qty} 大于最大值 {max_qty}"
                self.logger.error(error_msg)
                return False, {"error": error_msg}, default_trade_details, 0

            # 检查账户余额
            account_info = self.client.futures_account()
            available_balance = float(account_info["availableBalance"])
            price = self.get_symbol_price(symbol)
            if price == 0.0:
                error_msg = f"无法获取 {symbol} 价格"
                self.logger.error(error_msg)
                return False, {"error": error_msg}, default_trade_details, 0
            margin_required = (adjusted_qty * price) / self.leverage
            if available_balance < margin_required:
                error_msg = f"余额不足: 可用 {available_balance} < 需求 {margin_required}"
                self.logger.error(error_msg)
                return False, {"error": error_msg}, default_trade_details, 0

            # 下单
            order_params = {
                "symbol": symbol,
                "side": side.upper(),
                "type": "MARKET",
                "quantity": adjusted_qty
            }
            if reduce_only:
                order_params["reduceOnly"] = "true"
            self.logger.debug(f"市价单参数: {order_params}")
            order = self.client.futures_create_order(**order_params)
            order_id = order.get("orderId", 0)
            if not order_id:
                error_msg = f"订单创建失败，无效的 orderId: {order}"
                self.logger.error(error_msg)
                return False, {"error": error_msg}, default_trade_details, 0
            self.logger.info(f"市价单下单成功: {symbol} {side} 数量={adjusted_qty}, 订单ID={order_id}")

            # 检查订单状态
            order_status = None
            retries = 5
            for attempt in range(retries):
                try:
                    order_status = self.client.futures_get_order(symbol=symbol, orderId=order_id)
                    if order_status and isinstance(order_status, dict):
                        break
                except Exception as e:
                    self.logger.warning(
                        f"订单 {symbol} 获取状态失败，尝试 {attempt + 1}/{retries}，订单ID: {order_id}, 错误: {str(e)}"
                    )
                    time.sleep(0.5)
            if not order_status or not isinstance(order_status, dict):
                error_msg = f"无法获取订单状态或状态格式错误，订单ID: {order_id}, 返回值: {order_status}"
                self.logger.error(error_msg)
                return False, {"error": error_msg}, default_trade_details, order_id

            # 处理订单状态
            if order_status.get("status") == "FILLED":
                trades = []
                for attempt in range(retries):
                    try:
                        trades = self.client.futures_account_trades(symbol=symbol, orderId=order_id)
                        if trades and isinstance(trades, list):
                            break
                    except Exception as e:
                        self.logger.warning(
                            f"订单 {symbol} 获取成交记录失败，尝试 {attempt + 1}/{retries}，订单ID: {order_id}, 错误: {str(e)}"
                        )
                        time.sleep(0.5)
                if not trades:
                    self.logger.warning(f"订单 {symbol} 无成交记录，订单ID: {order_id}")
                    return True, order, default_trade_details, order_id

                total_qty = sum(float(trade["qty"]) for trade in trades)
                total_quote = sum(float(trade["qty"]) * float(trade["price"]) for trade in trades)
                price = total_quote / total_qty if total_qty > 0 else 0
                self.logger.info(
                    f"{symbol} 市价单 {side} 下单成功: 成交金额={total_quote}, 成交数量={total_qty}, "
                    f"成交均价={price}, 订单ID={order_id}, reduceOnly={reduce_only}"
                )
                return True, order, {"total_quote": total_quote, "total_qty": total_qty, "price": price}, order_id
            elif order_status.get("status") in ["CANCELED", "REJECTED", "EXPIRED"]:
                error_msg = f"订单未成交，状态: {order_status.get('status')}, 订单ID: {order_id}"
                self.logger.warning(error_msg)
                return False, {"error": error_msg}, default_trade_details, order_id
            else:
                error_msg = f"订单状态未知: {order_status.get('status')}, 订单ID: {order_id}"
                self.logger.warning(error_msg)
                return False, {"error": error_msg}, default_trade_details, order_id

        except Exception as e:
            error_msg = f"{symbol} 下单失败: {str(e)}, 订单ID: {order_id}"
            self.logger.error(error_msg)
            return False, {"error": error_msg}, default_trade_details, order_id

    def place_postonly_order(self, symbol: str, side: str, quantity: float, price: float, is_close: bool = False) -> \
            Tuple[bool, Any, int, Dict[str, float]]:
        """下 postonly 限价单，只做 maker，平仓时跳过最小名义价值检查并设置 reduceOnly

        Args:
            symbol (str): 交易对符号（如 BTCUSDT）
            side (str): 交易方向（BUY 或 SELL）
            quantity (float): 交易数量
            price (float): 限价价格
            is_close (bool): 是否为平仓订单，默认为 False

        Returns:
            Tuple[bool, Any, int, Dict[str, float]]: (是否成功, 订单信息或错误, 订单ID, 成交详情)
        """
        try:
            self.set_leverage(symbol, self.leverage)  # 确保杠杆一致
            self.logger.info(f"{symbol} 杠杆已设置为 {self.leverage}, is_close={is_close}")

            # 获取交易对规则
            exchange_info = self.client.futures_exchange_info()
            symbol_info = next(s for s in exchange_info["symbols"] if s["symbol"] == symbol)
            price_precision = symbol_info["pricePrecision"]
            quantity_precision = symbol_info["quantityPrecision"]
            min_notional = float(
                next(f for f in symbol_info["filters"] if f["filterType"] == "MIN_NOTIONAL")["notional"])
            price_filter = next(f for f in symbol_info["filters"] if f["filterType"] == "PRICE_FILTER")
            tick_size = float(price_filter["tickSize"])

            # 调整价格和数量
            adjusted_price = round(round(price / tick_size) * tick_size, price_precision)
            adjusted_quantity = round(quantity, quantity_precision)
            notional = adjusted_quantity * adjusted_price

            # 仅对开仓订单检查最小名义价值
            if not is_close and notional < min_notional:
                self.logger.error(f"{symbol} 名义价值 {notional} 小于最小要求 {min_notional}")
                return False, Exception(f"名义价值 {notional} 小于最小要求 {min_notional}"), 0, {
                    "total_quote": 0.0, "total_qty": 0.0, "price": 0.0}

            # 检查保证金，仅对非平仓订单
            if not is_close:
                account_info = self.client.futures_account()
                available_balance = float(account_info["availableBalance"])
                required_margin = notional / self.leverage
                if available_balance < required_margin:
                    self.logger.error(f"{symbol} 保证金不足: 可用 {available_balance}, 需求 {required_margin}")
                    return False, Exception(f"保证金不足: 可用 {available_balance}, 需求 {required_margin}"), 0, {
                        "total_quote": 0.0, "total_qty": 0.0, "price": 0.0}
                self.logger.debug(f"{symbol} 保证金检查通过: 可用 {available_balance}, 需求 {required_margin}")
            else:
                self.logger.info(f"{symbol} 为平仓订单 (reduceOnly=True)，跳过保证金检查")

            # 下单，平仓订单设置 reduceOnly=True
            order_params = {
                "symbol": symbol,
                "side": side.upper(),
                "type": "LIMIT",
                "timeInForce": "GTX",
                "quantity": adjusted_quantity,
                "price": adjusted_price
            }
            if is_close:
                order_params["reduceOnly"] = True

            self.logger.debug(f"{symbol} 下单参数: {order_params}")
            order = self.client.futures_create_order(**order_params)  # 仅调用一次
            order_id = order['orderId']
            self.logger.info(
                f"{symbol} postonly 挂单成功: 方向={side}, 数量={adjusted_quantity}, 价格={adjusted_price}, 订单ID={order_id}, is_close={is_close}, reduceOnly={is_close}"
            )

            # 检查订单状态并获取成交详情
            order_status = {}
            while True:
                try:
                    order_status = self.client.futures_get_order(symbol=symbol, orderId=order_id)
                    if order_status:  # 如果获取到订单状态，退出循环
                        break
                except Exception as e:
                    self.logger.warning(
                        f"订单 {symbol} 获取futures_get_order暂未同步，等待后重试 | order_id: {order_id}")
                    time.sleep(0.5)  # 等待0.5秒后重试

            if order_status['status'] == 'FILLED':
                trades = []
                while True:
                    try:
                        trades = self.client.futures_account_trades(symbol=symbol, orderId=order_id)
                        if trades:  # 如果获取到成交记录，退出循环
                            break
                    except Exception as e:
                        self.logger.warning(
                            f"订单 {symbol} 获取futures_account_trades暂未同步，等待后重试 | order_id: {order_id}")
                        time.sleep(0.5)  # 等待0.5秒后重试

                total_qty = sum(float(trade['qty']) for trade in trades)
                total_quote = sum(float(trade['qty']) * float(trade['price']) for trade in trades)
                avg_price = total_quote / total_qty if total_qty > 0 else 0
                self.logger.info(
                    f"{symbol} postonly 单立即成交: 方向={side}, 数量={total_qty}, 均价={avg_price}, 订单ID={order_id}, is_close={is_close}"
                )
                return True, order, order_id, {"total_quote": total_quote, "total_qty": total_qty, "price": avg_price}
            else:
                # 未立即成交，返回默认成交详情
                return True, order, order_id, {"total_quote": 0.0, "total_qty": 0.0, "price": 0.0}

        except Exception as e:
            self.logger.error(f"{symbol} postonly 挂单失败: {str(e)}, is_close={is_close}")
            return False, e, 0, {"total_quote": 0.0, "total_qty": 0.0, "price": 0.0}

    def place_limit_order(self, symbol: str, side: str, quantity: float, price: float) -> Tuple[bool, Any, int]:
        """下普通限价单（GTC），并检查是否为吃单或挂单

        Args:
            symbol (str): 交易对符号（如 BTCUSDT）
            side (str): 交易方向（BUY 或 SELL）
            quantity (float): 交易数量
            price (float): 限价价格

        Returns:
            Tuple[bool, Any, int]: (是否成功, 订单信息或错误, 订单ID)
        """
        try:
            self.set_leverage(symbol, self.leverage)
            self.logger.info(f"{symbol} 杠杆已设置为 {self.leverage}")

            # 获取交易对规则
            exchange_info = self.client.futures_exchange_info()
            symbol_info = next(s for s in exchange_info["symbols"] if s["symbol"] == symbol)
            price_precision = symbol_info["pricePrecision"]
            quantity_precision = symbol_info["quantityPrecision"]
            min_notional = float(
                next(f for f in symbol_info["filters"] if f["filterType"] == "MIN_NOTIONAL")["notional"])
            price_filter = next(f for f in symbol_info["filters"] if f["filterType"] == "PRICE_FILTER")
            tick_size = float(price_filter["tickSize"])

            # 调整价格和数量
            adjusted_price = round(round(price / tick_size) * tick_size, price_precision)
            adjusted_quantity = round(quantity, quantity_precision)
            notional = adjusted_quantity * adjusted_price
            if notional < min_notional:
                self.logger.error(f"{symbol} 名义价值 {notional} 小于最小要求 {min_notional}")
                return False, Exception(f"名义价值 {notional} 小于最小要求 {min_notional}"), 0

            # 检查保证金
            account_info = self.client.futures_account()
            available_balance = float(account_info["availableBalance"])
            required_margin = notional / self.leverage
            if available_balance < required_margin:
                self.logger.error(f"{symbol} 保证金不足: 可用 {available_balance}, 需求 {required_margin}")
                return False, Exception(f"保证金不足: 可用 {available_balance}, 需求 {required_margin}"), 0

            # 下普通限价单（GTC）
            order = self.client.futures_create_order(
                symbol=symbol,
                side=side.upper(),
                type="LIMIT",
                timeInForce="GTC",
                quantity=adjusted_quantity,
                price=adjusted_price
            )
            order_id = order['orderId']
            self.logger.info(
                f"{symbol} 普通限价单 {side} 下单成功: 数量={adjusted_quantity}, 价格={adjusted_price}, 订单ID={order_id}"
            )

            # 检查订单状态并获取成交详情（参考 place_market_order）
            order_status = {}
            while True:
                try:
                    order_status = self.client.futures_get_order(symbol=symbol, orderId=order_id)
                    if order_status:  # 如果获取到订单状态，退出循环
                        break
                except Exception as e:
                    self.logger.warning(
                        f"订单 {symbol} 获取futures_get_order暂未同步，等待后重试 | order_id: {order_id}")
                    time.sleep(0.5)  # 等待0.5秒后重试

            if order_status['status'] == 'FILLED':
                trades = []
                while True:
                    try:
                        trades = self.client.futures_account_trades(symbol=symbol, orderId=order_id)
                        if trades:  # 如果获取到成交记录，退出循环
                            break
                    except Exception as e:
                        self.logger.warning(
                            f"订单 {symbol} 获取futures_account_trades暂未同步，等待后重试 | order_id: {order_id}")
                        time.sleep(0.5)  # 等待0.5秒后重试

                total_qty = sum(float(trade['qty']) for trade in trades)
                total_quote = sum(float(trade['qty']) * float(trade['price']) for trade in trades)
                avg_price = total_quote / total_qty if total_qty > 0 else 0
                is_maker = all(trade['maker'] for trade in trades)  # 检查是否全为 Maker
                order_type = "挂单 (Maker)" if is_maker else "吃单 (Taker)"
                self.logger.info(
                    f"{symbol} 普通限价单 {side} 已成交: 类型={order_type}, 数量={total_qty}, 均价={avg_price}, 订单ID={order_id}"
                )
            elif order_status['status'] in ['CANCELED', 'REJECTED', 'EXPIRED']:
                self.logger.warning(f"{symbol} 订单未成交，状态: {order_status['status']}, 订单ID: {order_id}")
            else:
                self.logger.info(f"{symbol} 普通限价单 {side} 未立即成交，等待后续处理，订单ID: {order_id}")

            return True, order, order_id

        except Exception as e:
            self.logger.error(f"{symbol} 普通限价单 {side} 下单失败: {str(e)}")
            return False, e, 0

    def get_account_trades(self, symbol: str, order_id: int, retries: int = 3, delay: float = 1.0) -> list:
        for attempt in range(retries):
            try:
                trades = self.client.futures_account_trades(symbol=symbol, orderId=order_id)
                if trades:
                    return trades
                logger.warning(f"{symbol} 第{attempt + 1}次获取成交记录为空，订单ID={order_id}")
                time.sleep(delay)
            except Exception as e:
                logger.error(f"{symbol} 获取成交记录失败，订单ID={order_id}, 错误: {str(e)}")
                time.sleep(delay)
        logger.error(f"{symbol} 多次尝试后仍未获取到成交记录，订单ID={order_id}")
        return []

    def get_open_orders(self, symbol: str) -> List[Dict]:
        """
        查询指定交易对的未成交订单。

        :param symbol: 交易对符号（如 'FLMUSDT'）
        :return: 未成交订单列表，每个订单为字典格式
        """
        try:
            # 调用 Binance Futures API 获取未成交订单
            open_orders = self.client.futures_get_open_orders(symbol=symbol)
            self.logger.debug(f"成功获取 {symbol} 的未成交订单: {len(open_orders)} 条")
            return open_orders
        except Exception as e:
            self.logger.error(f"获取 {symbol} 未成交订单失败: {str(e)}")
            raise  # 抛出异常，交给上层处理

    def cancel_order(self, symbol: str, order_id: int) -> bool:
        try:
            self.client.futures_cancel_order(symbol=symbol, orderId=order_id)
            self.logger.info(f"成功取消 {symbol} 订单，订单ID={order_id}")
            return True
        except Exception as e:
            self.logger.error(f"{symbol} 撤单失败: 订单ID={order_id}, 错误: {str(e)}")
            # 检查订单状态
            try:
                order = self.client.futures_get_order(symbol=symbol, orderId=order_id)
                if order['status'] == 'FILLED':
                    self.logger.info(f"{symbol} 订单已成交，订单ID={order_id}, 状态={order['status']}")
                    # 获取成交记录
                    trades = self.client.futures_account_trades(symbol=symbol, orderId=order_id)
                    if trades:
                        self.logger.info(f"{symbol} 找到成交记录，订单ID={order_id}, 记录={trades}")
                        return False  # 订单已成交，不是取消成功
                    else:
                        self.logger.warning(f"{symbol} 订单已成交但无成交记录，订单ID={order_id}")
                        return False
                elif order['status'] == 'CANCELED':
                    self.logger.info(f"{symbol} 订单已取消，订单ID={order_id}, 状态={order['status']}")
                    return True  # 订单已取消
                else:
                    self.logger.warning(
                        f"{symbol} 订单状态非FILLED或CANCELED，状态={order['status']}, 订单ID={order_id}")
                    return False
            except Exception as order_error:
                self.logger.error(f"{symbol} 检查订单状态失败，订单ID={order_id}, 错误: {str(order_error)}")
                return False

    def get_account_info(self) -> Dict[str, Any]:
        """获取账户信息

        Returns:
            Dict[str, Any]: 包含账户详细信息的字典，结构如下：
            {
                "totalMarginBalance": float,  # 总保证金余额
                "availableBalance": float,    # 可用余额
                "totalWalletBalance": float,  # 钱包总余额
                "totalUnrealizedProfit": float,  # 总未实现盈亏
                "totalPositionInitialMargin": float,  # 持仓初始保证金总额
                "totalOpenOrderInitialMargin": float,  # 挂单初始保证金总额
                "assets": List[Dict],  # 资产列表
                "updateTime": int  # 最后更新时间戳
            }
        """
        try:
            # 调用API获取账户信息
            info = self.client.futures_account()

            # 提取关键信息并转换为适当类型
            account_info = {
                "totalMarginBalance": float(info.get("totalMarginBalance", 0)),
                "availableBalance": float(info.get("availableBalance", 0)),
                "totalWalletBalance": float(info.get("totalWalletBalance", 0)),
                "totalUnrealizedProfit": float(info.get("totalUnrealizedProfit", 0)),
                "totalPositionInitialMargin": float(info.get("totalPositionInitialMargin", 0)),
                "totalOpenOrderInitialMargin": float(info.get("totalOpenOrderInitialMargin", 0)),
                "assets": info.get("assets", []),
                "updateTime": info.get("updateTime", 0)
            }

            # 记录调试信息
            self.logger.debug(f"获取账户信息成功: {account_info}")
            return account_info

        except Exception as e:
            self.logger.error(f"获取账户信息失败: {str(e)}")
            raise

    def get_position_info(self) -> List[Dict[str, Any]]:
        """获取持仓信息

        Returns:
            List[Dict[str, Any]]: 非零持仓列表，每个持仓包含以下信息：
            [
                {
                    "symbol": str,  # 交易对
                    "positionAmt": float,  # 持仓数量
                    "entryPrice": float,  # 开仓均价
                    "markPrice": float,  # 标记价格
                    "unRealizedProfit": float,  # 未实现盈亏
                    "liquidationPrice": float,  # 强平价格
                    "leverage": int,  # 杠杆倍数
                    "positionSide": str,  # 持仓方向(LONG/SHORT)
                    "updateTime": int  # 更新时间戳
                },
                ...
            ]
        """
        try:
            # 获取原始持仓数据
            positions = self.client.futures_position_information()

            # 过滤非零持仓并转换数据类型
            non_zero_positions = []
            for pos in positions:
                position_amt = float(pos.get("positionAmt", 0))
                if position_amt != 0:
                    processed_pos = {
                        "symbol": pos.get("symbol"),
                        "positionAmt": position_amt,
                        "entryPrice": float(pos.get("entryPrice", 0)),
                        "markPrice": float(pos.get("markPrice", 0)),
                        "unRealizedProfit": float(pos.get("unRealizedProfit", 0)),
                        "liquidationPrice": float(pos.get("liquidationPrice", 0)),
                        "leverage": int(pos.get("leverage", 1)),
                        "positionSide": pos.get("positionSide", "BOTH"),
                        "updateTime": int(pos.get("updateTime", 0))
                    }
                    non_zero_positions.append(processed_pos)

            self.logger.info(f"获取到 {len(non_zero_positions)} 个非零持仓")
            return non_zero_positions

        except Exception as e:
            self.logger.error(f"获取持仓信息失败: {str(e)}")
            raise

    def get_commission_history(self, start_time: int, end_time: int, limit: int = 1000) -> List[Dict[str, Any]]:
        """获取指定时间段内的手续费记录，处理分页"""
        all_records = []
        current_start_time = start_time

        while current_start_time < end_time:
            try:
                records = self.client.futures_income_history(
                 incomeType="COMMISSION",
                    startTime=current_start_time,
                    endTime=end_time,
                    limit=limit
                )
                if not records:
                    break

                all_records.extend(records)
                self.logger.info(f"已获取 {len(records)} 条手续费记录，总计 {len(all_records)} 条")

                last_record_time = int(records[-1]['time'])
                if last_record_time >= end_time:
                  break
                current_start_time = last_record_time + 1

                if len(records) < limit:
                    break
            except Exception as e:
                self.logger.error(f"获取手续费记录失败: {str(e)}")
                break

        return all_records

    def calculate_total_commission(self, records: List[Dict[str, Any]]) -> float:
        """计算指定记录中用户支付的手续费总额"""
        total_fee = 0.0
        for record in records:
            income = float(record['income'])
            if income < 0:
                total_fee += abs(income)
                self.logger.debug(f"记录: {record['symbol']} - 支付金额: {abs(income)} USDT")
        return total_fee

    def get_trade_history(self, symbol, start_time, end_time):
        """
        获取指定交易对在时间范围内的交易历史记录。

        参数:
            symbol (str): 交易对符号（如 'BTCUSDT'）。
            start_time (int): 开始时间（毫秒）。
            end_time (int): 结束时间（毫秒）。

        返回:
            list: 包含交易记录的列表。
        """
        try:
            trades = self.client.get_my_trades(symbol=symbol, startTime=start_time, endTime=end_time)
            trade_details = []
            for trade in trades:
                trade_info = {
                    'symbol': trade['symbol'],
                    'side': trade['side'],  # BUY 或 SELL
                    'quantity': float(trade['qty']),
                    'price': float(trade['price']),
                    'quoteQty': float(trade['quoteQty']),  # 总报价资产金额
                    'realizedPnl': float(trade.get('realizedPnl', 0)),  # 已实现盈亏
                    'orderId': trade['orderId'],
                    'time': trade['time'],  # 交易执行时间
                    'commission': float(trade.get('commission', 0)),
                    'commissionAsset': trade.get('commissionAsset', '')
                }
                trade_details.append(trade_info)
            #self.logger.info(f"获取到 {symbol} 从 {start_time} 到 {end_time} 的 {len(trade_details)} 条交易记录")
            return trade_details
        except Exception as e:
            self.logger.error(f"获取 {symbol} 交易历史失败: {str(e)}")
            return []


# 示例使用，测试 Binance 客户端功能
if __name__ == "__main__":
    # 从logger模块导入设置日志的函数
    from logger import setup_logger

    # 设置日志文件路径并初始化日志记录器
    logger = setup_logger("../logs/trading.log")
    # 从config_loader模块导入配置加载类
    from config_loader import ConfigLoader

    # 加载API配置（如密钥和测试网设置）
    config = ConfigLoader().get_api_config()
    # 创建BinanceFuturesClient实例
    client = BinanceFuturesClient(config["api_key"], config["api_secret"], config["test_net"] == "True", logger)
    # 获取并打印账户信息
    print(client.get_account_info())