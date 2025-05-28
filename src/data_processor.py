# 导入pandas库，用于数据处理和操作CSV文件
import pandas as pd
# 从typing模块导入List, Tuple和Dict类型，用于类型提示
from typing import List, Dict, Tuple
# 导入logging模块，用于记录日志
import logging


class DataProcessor:
    """数据处理类，用于读取和处理 CSV 文件，并支持黑名单过滤"""

    def __init__(self, csv_path: str, blacklist_path: str, logger: logging.Logger):
        """初始化数据处理器

        Args:
            csv_path (str): CSV 文件路径
            blacklist_path (str): 黑名单 CSV 文件路径
            logger (logging.Logger): 日志记录器，用于记录操作日志
        """
        # 保存CSV文件路径
        self.csv_path = csv_path
        # 保存黑名单文件路径
        self.blacklist_path = blacklist_path
        # 保存日志记录器实例
        self.logger = logger
        # 加载黑名单
        self.blacklist = self.load_blacklist()

    def load_blacklist(self) -> List[str]:
        """加载黑名单 CSV 文件

        Returns:
            List[str]: 黑名单币种列表
        """
        try:
            # 使用pandas读取黑名单CSV文件，假设文件只有一列，列名为 'ticker'
            df = pd.read_csv(self.blacklist_path)
            # 提取 'ticker' 列并转换为列表
            blacklist = df['ticker'].tolist()
            # 记录成功加载黑名单的日志
            self.logger.info(f"成功加载黑名单文件: {self.blacklist_path}, 币种数量={len(blacklist)}")
            return blacklist
        except Exception as e:
            # 如果加载失败，记录错误日志并返回空列表
            self.logger.error(f"加载黑名单文件失败: {str(e)}")
            return []

    def load_csv(self) -> pd.DataFrame:
        """加载 CSV 文件

        Returns:
            pd.DataFrame: 加载的 CSV 数据，以DataFrame格式返回
        """
        try:
            # 使用pandas读取CSV文件
            df = pd.read_csv(self.csv_path)
            # 记录成功加载的日志，包括文件路径和行数
            self.logger.info(f"成功加载 CSV 文件: {self.csv_path}, 行数={len(df)}")
            return df
        except Exception as e:
            # 如果加载失败，记录错误日志并抛出异常
            self.logger.error(f"加载 CSV 文件失败: {str(e)}")
            raise

    def filter_tickers(self, df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
        """根据资金费率过滤多头和空头备选交易对，并过滤黑名单币种

        Args:
            df (pd.DataFrame): CSV 数据，包含交易对信息

        Returns:
            Tuple[List[Dict], List[Dict]]: 多头和空头备选列表，每个元素为字典
        """
        # 初始化多头和空头备选列表
        long_candidates = []
        short_candidates = []

        # 将数据对半分开，并按 id 排序
        mid = len(df) // 2  # 计算数据中间点
        long_df = df.iloc[:mid].sort_values("id")  # 前半部分用于多头，按 id 升序排序
        short_df = df.iloc[mid:].sort_values("id", ascending=False)  # 后半部分用于空头，按 id 降序排序

        # 过滤多头：fundingRate 为 -1 的交易对，且不在黑名单中
        for _, row in long_df.iterrows():
            if row["fundingRate"] == -1 and row["ticker"] not in self.blacklist:
                long_candidates.append({
                    "id": row["id"],
                    "ticker": row["ticker"],
                    "fundingRate": row["fundingRate"]
                })

        # 过滤空头：fundingRate 为 1 的交易对，且不在黑名单中
        for _, row in short_df.iterrows():
            if row["fundingRate"] == 1 and row["ticker"] not in self.blacklist:
                short_candidates.append({
                    "id": row["id"],
                    "ticker": row["ticker"],
                    "fundingRate": row["fundingRate"]
                })

        # 记录过滤结果的日志
        self.logger.info(f"多头备选: {len(long_candidates)} 个, 空头备选: {len(short_candidates)} 个")
        return long_candidates, short_candidates


# 示例使用，测试数据处理功能
if __name__ == "__main__":
    # 从logger模块导入设置日志的函数
    from logger import setup_logger

    # 设置日志文件路径并初始化日志记录器
    logger = setup_logger("../logs/trading.log")
    # 创建DataProcessor实例，指定CSV文件路径、黑名单文件路径和日志记录器
    processor = DataProcessor("../data/pos20250328_v3.csv", "../data/blacklist.csv", logger)
    # 加载CSV文件
    df = processor.load_csv()
    # 根据资金费率过滤多头和空头交易对，并过滤黑名单币种
    long, short = processor.filter_tickers(df)
    # 打印前5个多头备选交易对
    print("多头备选:", long[:5])
    # 打印前5个空头备选交易对
    print("空头备选:", short[:5])