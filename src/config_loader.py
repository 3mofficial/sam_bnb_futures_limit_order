# 导入configparser模块，用于解析INI格式的配置文件
import configparser
# 导入os模块，用于文件路径操作和检查文件是否存在
import os
# 从typing模块导入Dict类型，用于类型提示返回字典
from typing import Dict


class ConfigLoader:
    """配置文件加载类，用于读取 config.ini 文件并解析配置参数"""

    def __init__(self, config_path: str = "config/config.ini"):
        """初始化配置文件加载器

        Args:
            config_path (str): 配置文件路径，默认为 'config.ini'
        """
        # 保存配置文件路径
        self.config_path = config_path
        # 创建ConfigParser实例，用于解析INI文件
        self.config = configparser.ConfigParser()

        # 检查配置文件是否存在，若不存在则抛出异常
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"配置文件 {self.config_path} 不存在")

        # 读取配置文件，使用 UTF-8 编码以支持多语言字符
        self.config.read(self.config_path, encoding="utf-8")

    def get_section(self, section: str) -> Dict[str, str]:
        """获取指定 section 的配置参数

        Args:
            section (str): 配置文件的 section 名称（如 'API', 'TRADING'）

        Returns:
            Dict[str, str]: 该 section 的键值对字典
        """
        # 检查指定的section是否存在，若不存在则抛出异常
        if section not in self.config:
            raise KeyError(f"配置文件中不存在 section: {section}")
        # 将section中的配置项转换为字典并返回
        return dict(self.config[section])

    def get_api_config(self) -> Dict[str, str]:
        """获取 API 配置

        Returns:
            Dict[str, str]: API 相关的配置参数（如 API 密钥、密钥等）
        """
        # 调用 get_section 方法获取 'API' section 的配置
        return self.get_section("API")

    def get_trading_config(self) -> Dict[str, str]:
        """获取交易配置

        Returns:
            Dict[str, str]: 交易相关的配置参数（如交易对、数量等）
        """
        # 调用 get_section 方法获取 'TRADING' section 的配置
        return self.get_section("TRADING")

    def get_paths_config(self) -> Dict[str, str]:
        """获取路径配置

        Returns:
            Dict[str, str]: 路径相关的配置参数（如日志路径、数据路径等）
        """
        # 调用 get_section 方法获取 'PATHS' section 的配置
        return self.get_section("PATHS")


# 示例使用，测试配置文件加载功能
if __name__ == "__main__":
    # 创建 ConfigLoader 实例，加载 'custom_config/config.ini' 文件
    loader = ConfigLoader(config_path="../config/config.ini")
    # 打印 API 配置内容
    print("API 配置:", loader.get_api_config())
    # 打印交易配置内容
    print("交易配置:", loader.get_trading_config())
    # 打印路径配置内容
    print("路径配置:", loader.get_paths_config())