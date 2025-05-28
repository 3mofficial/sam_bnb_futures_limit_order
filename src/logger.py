# 导入logging模块，用于配置和记录日志
import logging
# 导入os模块，用于文件路径操作和创建目录
import os
# 从datetime模块导入datetime类，用于生成每日日志文件名
from datetime import datetime


def setup_logger(log_path: str) -> logging.Logger:
    """设置每日归档的日志记录器，同时输出到控制台和文件

    Args:
        log_path (str): 日志文件基础路径（如 'logs/trading.log'），实际文件名会附加日期

    Returns:
        logging.Logger: 配置好的日志记录器
    """
    # 获取当前日期，用于生成每日日志文件名
    current_date = datetime.now().strftime("%Y-%m-%d")
    # 分解基础路径，提取目录和文件名
    log_dir = os.path.dirname(log_path)
    log_filename = os.path.basename(log_path)
    # 构造每日归档日志文件路径，例如 'logs/trading_2025-03-31.log'
    daily_log_path = os.path.join(log_dir, f"{log_filename.rsplit('.', 1)[0]}_{current_date}.log")

    # 创建日志目录，确保路径存在
    os.makedirs(log_dir, exist_ok=True)

    # 创建日志记录器，命名为 'TradingBot'
    logger = logging.getLogger("TradingBot")
    # 设置日志级别为 INFO
    logger.setLevel(logging.INFO)

    # 创建文件处理器，将日志保存到每日文件
    file_handler = logging.FileHandler(daily_log_path, encoding="utf-8")
    # 设置文件处理器的日志级别为 INFO
    file_handler.setLevel(logging.INFO)

    # 创建控制台处理器，将日志输出到终端
    console_handler = logging.StreamHandler()
    # 设置控制台处理器的日志级别为 INFO
    console_handler.setLevel(logging.INFO)

    # 设置日志格式，包括时间、日志名称、级别和消息内容
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    # 为文件处理器设置格式
    file_handler.setFormatter(formatter)
    # 为控制台处理器设置格式
    console_handler.setFormatter(formatter)

    # 清空已有处理器，避免重复添加
    logger.handlers = []
    # 添加文件处理器到日志记录器
    logger.addHandler(file_handler)
    # 添加控制台处理器到日志记录器
    logger.addHandler(console_handler)

    # 记录日志初始化信息
    logger.info(f"日志初始化成功，当前日志文件: {daily_log_path}")

    # 返回配置好的日志记录器
    return logger


# 示例使用，测试日志功能
if __name__ == "__main__":
    # 调用 setup_logger 函数，设置日志基础路径
    logger = setup_logger("../logs/trading.log")
    # 记录测试日志
    logger.info("测试每日归档日志功能")