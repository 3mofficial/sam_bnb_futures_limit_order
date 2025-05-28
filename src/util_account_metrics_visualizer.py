# 导入必要的库
import pandas as pd  # 用于数据处理和分析
import matplotlib.pyplot as plt  # 用于数据可视化
from matplotlib.backends.backend_pdf import PdfPages  # 用于生成PDF文件
import datetime  # 用于处理日期和时间


class AccountMetricsVisualizer:
    """
    账户指标可视化工具类，用于从Excel文件中读取账户指标数据，
    计算持仓和BTC/USDT的收益率，并生成对比图表保存为PDF文件。
    """

    @staticmethod
    def _setup_plotting_environment():
        """
        设置绘图环境，配置全局字体和负号显示。

        Returns:
            None
        """
        # 设置全局字体为微软雅黑，确保中文显示正常
        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
        # 解决负号显示为方块的问题
        plt.rcParams['axes.unicode_minus'] = False

    @staticmethod
    def _load_data(file_path: str) -> pd.DataFrame:
        """
        从指定的Excel文件中加载账户指标数据。

        Args:
            file_path (str): Excel文件路径，包含账户指标数据

        Returns:
            pd.DataFrame: 加载的数据

        Raises:
            Exception: 如果读取Excel文件失败
        """
        try:
            return pd.read_excel(file_path)
        except Exception as e:
            raise Exception(f"读取Excel文件失败: {e}")

    @staticmethod
    def _validate_today_data(df: pd.DataFrame) -> bool:
        """
        验证当天日期的数据是否存在且包含post_rebalance_return指标。

        Args:
            df (pd.DataFrame): 账户指标数据

        Returns:
            bool: 如果数据有效返回True，否则返回False
        """
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        today_data = df[df['Date'].str.startswith(today)]

        if today_data.empty:
            print(f"警告：找不到{today}的数据，不生成PDF文件")
            return False

        has_post_rebalance_return = any(today_data['Metric'] == 'post_rebalance_return')
        if not has_post_rebalance_return:
            print(f"警告：{today}的数据中不包含post_rebalance_return指标，不生成PDF文件")
            return False

        return True

    @staticmethod
    def process_portfolio_data(df: pd.DataFrame) -> tuple:
        """
        处理持仓收益率数据，计算日收益率和累计收益率。

        Args:
            df (pd.DataFrame): 账户指标数据

        Returns:
            tuple: 包含以下内容：
                - balance_data (pd.DataFrame): 持仓余额数据
                - post_returns (pd.Series): 持仓日收益率
                - post_cumulative_returns (list): 持仓累计收益率
                - post_dates (pd.Series): 累计收益率日期
                - post_dates_daily (pd.Series): 日收益率日期
                - cumulative_post_return (float): 最终累计收益率

        Raises:
            ValueError: 如果数据不足两天
        """
        # 筛选after_trade_balance指标数据（用于计算累计收益率和日收益率的分子）
        balance_data = df[df['Metric'] == 'after_trade_balance'].copy()
        balance_data['Date'] = pd.to_datetime(balance_data['Date'].str.split('_').str[0])
        balance_data['Date'] = balance_data['Date'].dt.normalize()  # 规范化日期，去掉时间部分
        balance_data = balance_data.sort_values('Date')
        after_trade_balances = balance_data['Value'].astype(float)

        # 筛选before_trade_balance指标数据（用于初始资金和日收益率的分母）
        before_balance_data = df[df['Metric'] == 'before_trade_balance'].copy()
        before_balance_data['Date'] = pd.to_datetime(before_balance_data['Date'].str.split('_').str[0])
        before_balance_data['Date'] = before_balance_data['Date'].dt.normalize()
        before_balance_data = before_balance_data.sort_values('Date')
        before_trade_balances = before_balance_data['Value'].astype(float)

        # 获取初始资金（调仓前账户总保证金余额），使用第一天的值
        initial_balance = before_trade_balances.iloc[0]

        # 计算持仓日收益率：(当日调仓后账户总保证金余额 / 前日调仓前账户总保证金余额) - 1
        post_returns = []
        for i in range(1, len(after_trade_balances)):
            daily_return = (after_trade_balances.iloc[i] / before_trade_balances.iloc[i-1]) - 1
            post_returns.append(daily_return)
        post_returns = pd.Series(post_returns)
        post_dates_daily = balance_data['Date'].iloc[1:]  # 从第二天开始

        # 计算持仓累计收益率：(调仓后账户总保证金余额 / 初始资金) - 1
        post_cumulative_returns = []
        for balance in after_trade_balances:
            cumulative_return = (balance / initial_balance) - 1
            post_cumulative_returns.append(cumulative_return)

        # 创建一个DataFrame，包含日期和累计收益率
        cumulative_df = pd.DataFrame({
            'Date': balance_data['Date'],
            'Cumulative_Return': post_cumulative_returns
        })

        # 从数据中获取排序后的唯一日期
        unique_dates = balance_data['Date'].drop_duplicates().sort_values()
        if len(unique_dates) < 2:
            raise ValueError("数据中至少需要两天的数据来确定第二天日期")
        second_date = unique_dates.iloc[1]  # 取第二个日期

        # 筛选出第二天及之后的数据用于显示
        mask = cumulative_df['Date'] >= second_date
        cumulative_df = cumulative_df[mask]
        post_dates = cumulative_df['Date']
        post_cumulative_returns = cumulative_df['Cumulative_Return'].tolist()
        cumulative_post_return = post_cumulative_returns[-1]  # 最终累计收益率

        return balance_data, post_returns, post_cumulative_returns, post_dates, post_dates_daily, cumulative_post_return

    @staticmethod
    def process_btc_data(df: pd.DataFrame, post_dates: pd.Series, post_dates_daily: pd.Series) -> tuple:
        """
        处理BTC/USDT价格数据，计算日收益率和累计收益率，以持仓日期为基准。

        Args:
            df (pd.DataFrame): 账户指标数据
            post_dates (pd.Series): 持仓累计收益率日期
            post_dates_daily (pd.Series): 持仓日收益率日期

        Returns:
            tuple: 包含以下内容：
                - btc_data (pd.DataFrame): BTC价格数据
                - btc_daily_returns (pd.Series): BTC日收益率
                - btc_cumulative_returns (list): BTC累计收益率
                - btc_dates (pd.Series): BTC累计收益率日期
                - btc_dates_daily (pd.Series): BTC日收益率日期
                - cumulative_btc_return (float): BTC最终累计收益率

        Raises:
            ValueError: 如果数据不足两天
        """
        # 筛选btc_usdt_price指标数据
        btc_data = df[df['Metric'] == 'btc_usdt_price'].copy()
        btc_data['Date'] = pd.to_datetime(btc_data['Date'].str.split('_').str[0])
        btc_data['Date'] = btc_data['Date'].dt.normalize()
        btc_data = btc_data.sort_values('Date')

        # 计算BTC累计收益率：(当前价格 / 初始价格) - 1
        btc_prices = btc_data['Value'].astype(float)
        initial_price = btc_prices.iloc[0]  # 初始价格（第一天的价格）
        btc_cumulative_returns = []
        for price in btc_prices:
            cumulative_return = (price / initial_price) - 1
            btc_cumulative_returns.append(cumulative_return)

        # 创建一个DataFrame，包含日期和累计收益率
        btc_cumulative_df = pd.DataFrame({
            'Date': btc_data['Date'],
            'Cumulative_Return': btc_cumulative_returns
        })

        # 从数据中获取排序后的唯一日期
        unique_dates = btc_data['Date'].drop_duplicates().sort_values()
        if len(unique_dates) < 2:
            raise ValueError("数据中至少需要两天的数据来确定第二天日期")
        second_date = unique_dates.iloc[1]  # 取第二个日期

        # 筛选出第二天及之后的数据用于显示
        mask = btc_cumulative_df['Date'] >= second_date
        btc_cumulative_df = btc_cumulative_df[mask]
        btc_dates = btc_cumulative_df['Date']
        btc_cumulative_returns = btc_cumulative_df['Cumulative_Return'].tolist()
        cumulative_btc_return = btc_cumulative_returns[-1]  # 最终累计收益率

        # 计算BTC日收益率：(当日价格 / 前日价格) - 1
        btc_prices_all = btc_data['Value'].astype(float)
        btc_daily_returns_all = []
        for i in range(1, len(btc_prices_all)):
            daily_return = (btc_prices_all.iloc[i] / btc_prices_all.iloc[i-1]) - 1
            btc_daily_returns_all.append(daily_return)
        btc_dates_all = btc_data['Date'].iloc[1:]
        btc_daily_returns_all = pd.Series(btc_daily_returns_all, index=btc_dates_all)

        # 创建一个DataFrame，包含日期和日收益率
        btc_daily_df = pd.DataFrame({
            'Date': btc_dates_all,
            'Daily_Return': btc_daily_returns_all.values
        })

        # 筛选出与post_dates_daily相同的日期
        btc_daily_df = btc_daily_df[btc_daily_df['Date'].isin(post_dates_daily)]
        btc_dates_daily = btc_daily_df['Date']
        btc_daily_returns = btc_daily_df['Daily_Return']

        return btc_data, btc_daily_returns, btc_cumulative_returns, btc_dates, btc_dates_daily, cumulative_btc_return

    @staticmethod
    def _generate_pdf_report(pdf_path: str, post_dates: pd.Series, post_returns: pd.Series,
                             post_cumulative_returns: list, cumulative_post_return: float,
                             btc_dates: pd.Series, btc_daily_returns: pd.Series,
                             btc_cumulative_returns: list, cumulative_btc_return: float,
                             post_dates_daily: pd.Series, btc_dates_daily: pd.Series) -> None:
        """
        生成PDF报告，包含累计收益率和日收益率对比图表。

        Args:
            pdf_path (str): PDF文件保存路径
            post_dates (pd.Series): 持仓累计收益率日期
            post_returns (pd.Series): 持仓日收益率
            post_cumulative_returns (list): 持仓累计收益率
            cumulative_post_return (float): 持仓最终累计收益率
            btc_dates (pd.Series): BTC累计收益率日期
            btc_daily_returns (pd.Series): BTC日收益率
            btc_cumulative_returns (list): BTC累计收益率
            cumulative_btc_return (float): BTC最终累计收益率
            post_dates_daily (pd.Series): 持仓日收益率日期
            btc_dates_daily (pd.Series): BTC日收益率日期

        Returns:
            None

        Raises:
            Exception: 如果图表生成失败
        """
        with PdfPages(pdf_path, metadata={'Title': '收益率对比'}) as pdf:
            # 第一页：累计收益率图表
            try:
                AccountMetricsVisualizer._create_cumulative_returns_chart(
                    pdf, post_dates, post_cumulative_returns, cumulative_post_return,
                    btc_dates, btc_cumulative_returns, cumulative_btc_return
                )
                print("第一页累计收益率图表已保存")
            except Exception as e:
                print(f"第一页图表生成失败: {e}")

            # 第二页：日收益率图表
            try:
                AccountMetricsVisualizer._create_daily_returns_chart(
                    pdf, post_dates_daily, post_returns, btc_dates_daily, btc_daily_returns
                )
                print("第二页日收益率图表已保存")
            except Exception as e:
                print(f"第二页图表生成失败: {e}")

    @staticmethod
    def _create_cumulative_returns_chart(pdf: PdfPages, post_dates: pd.Series,
                                         post_cumulative_returns: list, cumulative_post_return: float,
                                         btc_dates: pd.Series, btc_cumulative_returns: list,
                                         cumulative_btc_return: float) -> None:
        """
        创建累计收益率对比图表并保存到PDF。

        Args:
            pdf (PdfPages): PDF文件对象
            post_dates (pd.Series): 持仓累计收益率日期
            post_cumulative_returns (list): 持仓累计收益率
            cumulative_post_return (float): 持仓最终累计收益率
            btc_dates (pd.Series): BTC累计收益率日期
            btc_cumulative_returns (list): BTC累计收益率
            cumulative_btc_return (float): BTC最终累计收益率

        Returns:
            None
        """
        fig, ax = plt.subplots(figsize=(14, 8), dpi=100)
        ax.plot(post_dates, [r * 100 for r in post_cumulative_returns],
                label=f'持仓累计收益率: {cumulative_post_return:.3%}',
                linestyle='-', linewidth=2.5, color='#1f77b4', marker='o')
        ax.plot(btc_dates, [r * 100 for r in btc_cumulative_returns],
                label=f'BTCUSDT累计收益率: {cumulative_btc_return:.3%}',
                linestyle='-', linewidth=2.5, color='#ff7f0e', marker='o')
        # 使用合并后的方法添加标签，累计收益率已经乘以 100
        AccountMetricsVisualizer._add_data_point_labels(
            ax, post_dates, pd.Series([r * 100 for r in post_cumulative_returns]),
            btc_dates, pd.Series([r * 100 for r in btc_cumulative_returns]),
            scale_to_percent=False  # 数据已经转换为百分比，无需再次转换
        )
        ax.set_title('持仓与BTCUSDT累计收益率对比', fontsize=18, pad=15)
        ax.set_ylabel('累计收益率 (%)', fontsize=14)
        ax.set_xticks(post_dates)
        ax.set_xticklabels([d.strftime('%Y-%m-%d') for d in post_dates],
                           rotation=45, ha='right', fontsize=12)
        plt.yticks(fontsize=12)
        ax.grid(True, linestyle='--', alpha=0.5, color='gray')
        ax.legend(loc='best', fontsize=12, frameon=True, framealpha=0.9, edgecolor='black')
        for spine in ax.spines.values():
            spine.set_linewidth(0.8)
            spine.set_color('gray')
        plt.tight_layout(pad=3.0)
        plt.subplots_adjust(bottom=0.17)
        start_date = post_dates.min()
        end_date = post_dates.max()
        time_range = f"{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}"
        fig.text(0.5, 0.01,
                 f"公式：累计收益率 = ( 调仓后账户总保证金余额 / 初始资金 ) - 1\n"
                 f"场景：本图表展示 {time_range} 内持仓与BTCUSDT的累计收益率对比。",
                 ha='center', va='bottom', fontsize=10, color='gray')
        pdf.savefig(fig, dpi=300)
        plt.close(fig)

    @staticmethod
    def _create_daily_returns_chart(pdf: PdfPages, post_dates: pd.Series, post_returns: pd.Series,
                                    btc_dates: pd.Series, btc_daily_returns: pd.Series) -> None:
        """
        创建日收益率对比图表并保存到PDF。

        Args:
            pdf (PdfPages): PDF文件对象
            post_dates (pd.Series): 持仓日收益率日期
            post_returns (pd.Series): 持仓日收益率
            btc_dates (pd.Series): BTC日收益率日期
            btc_daily_returns (pd.Series): BTC日收益率

        Returns:
            None
        """
        fig, ax = plt.subplots(figsize=(14, 8), dpi=100)
        ax.plot(post_dates, post_returns * 100,
                label='持仓日收益率',
                linestyle='-', linewidth=2.5, color='#1f77b4', marker='o')
        ax.plot(btc_dates, btc_daily_returns * 100,
                label='BTCUSDT 日收益率',
                linestyle='-', linewidth=2.5, color='#ff7f0e', marker='o')
        # 使用合并后的方法添加标签，日收益率已经乘以 100
        AccountMetricsVisualizer._add_data_point_labels(
            ax, post_dates, post_returns * 100,
            btc_dates, btc_daily_returns * 100,
            scale_to_percent=False  # 数据已经转换为百分比，无需再次转换
        )
        ax.set_title('持仓与BTCUSDT日收益率对比', fontsize=18, pad=15)
        ax.set_ylabel('日收益率 (%)', fontsize=14)
        ax.set_xticks(post_dates)
        ax.set_xticklabels([d.strftime('%Y-%m-%d') for d in post_dates],
                           rotation=45, ha='right', fontsize=12)
        plt.yticks(fontsize=12)
        ax.grid(True, linestyle='--', alpha=0.5, color='gray')
        ax.legend(loc='best', fontsize=12, frameon=True, framealpha=0.9, edgecolor='black')
        for spine in ax.spines.values():
            spine.set_linewidth(0.8)
            spine.set_color('gray')
        plt.tight_layout(pad=3.0)
        plt.subplots_adjust(bottom=0.17)
        start_date = post_dates.min()
        end_date = post_dates.max()
        time_range = f"{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}"
        fig.text(0.5, 0.01,
                 f"公式：日收益率 = ( 当日调仓后账户总保证金余额 / 前日调仓前账户总保证金余额 ) - 1\n"
                 f"场景：本图表展示 {time_range} 内持仓与BTCUSDT的日收益率对比。",
                 ha='center', va='bottom', fontsize=10, color='gray')
        pdf.savefig(fig, dpi=300)
        plt.close(fig)

    @staticmethod
    def _add_data_point_labels(ax, post_dates: pd.Series, post_values: pd.Series,
                               btc_dates: pd.Series, btc_values: pd.Series,
                               scale_to_percent: bool = True) -> None:
        """
        为图表的数据点添加数值标签，优化位置以避免重叠。

        Args:
            ax: Matplotlib 轴对象，用于绘制标签
            post_dates (pd.Series): 持仓数据的日期
            post_values (pd.Series): 持仓数据的收益率值（可能是日收益率或累计收益率）
            btc_dates (pd.Series): BTC 数据的日期
            btc_values (pd.Series): BTC 数据的收益率值（可能是日收益率或累计收益率）
            scale_to_percent (bool): 是否将收益率值乘以 100 转换为百分比，默认为 True

        Returns:
            None
        """
        # 基础偏移量，用于控制标签与数据点的距离
        base_offset = 0.22

        # 将数据点按日期分组，方便处理同一日期下的标签重叠
        # 首先将持仓数据和 BTC 数据合并到一个列表中，记录数据类型（持仓或 BTC）和值
        all_points = []
        for x, y in zip(post_dates, post_values):
            all_points.append((x, y, 'post'))
        for x, y in zip(btc_dates, btc_values):
            all_points.append((x, y, 'btc'))

        # 按日期排序，确保同一日期的数据点在一起
        all_points.sort(key=lambda p: p[0])

        # 按日期分组
        grouped_points = {}
        for x, y, data_type in all_points:
            if x not in grouped_points:
                grouped_points[x] = []
            grouped_points[x].append((y, data_type))

        # 为每个日期的数据点添加标签
        for date, points in grouped_points.items():
            # 按 y 值排序（从小到大），以便从下到上排列标签
            points.sort(key=lambda p: p[0])

            # 计算动态偏移量：如果同一日期有多个数据点，增加偏移以避免重叠
            num_points = len(points)
            if num_points > 1:
                offset = base_offset * 1.5  # 多个数据点时增加偏移量
            else:
                offset = base_offset

            # 为每个数据点添加标签
            for i, (y, data_type) in enumerate(points):
                # 如果需要转换为百分比，则乘以 100
                y_display = y * 100 if scale_to_percent else y

                # 根据数据类型设置颜色
                color = '#1f77b4' if data_type == 'post' else '#ff7f0e'

                # 计算标签的 y 位置
                # 如果是同一日期的第一个点（最下面的点），标签放在下方
                # 如果是最后一个点（最上面的点），标签放在上方
                # 中间的点根据相对位置调整
                if num_points == 1:
                    # 只有一个点时，根据 y 值正负决定标签位置
                    y_pos = y_display + offset if y_display >= 0 else y_display - offset
                    va = 'bottom' if y_display >= 0 else 'top'
                else:
                    # 多个点时，动态调整位置
                    if i == 0:
                        # 第一个点（最小的 y 值），标签放在下方
                        y_pos = y_display - offset
                        va = 'top'
                    elif i == num_points - 1:
                        # 最后一个点（最大的 y 值），标签放在上方
                        y_pos = y_display + offset
                        va = 'bottom'
                    else:
                        # 中间的点，稍微偏移
                        y_pos = y_display + (offset if y_display >= 0 else -offset)
                        va = 'bottom' if y_display >= 0 else 'top'

                # 添加标签
                ax.text(date, y_pos, f'{y_display:.3f}%', ha='center', va=va, fontsize=9, color=color)

    @staticmethod
    def visualize(file_path: str = "data/account_metrics.xlsx", pdf_output_path: str = None) -> None:
        """
        主方法：从Excel文件中读取账户指标数据，生成收益率对比图表，并保存为PDF文件。

        Args:
            file_path (str): Excel文件路径，包含账户指标数据，默认为"account_metrics.xlsx"
            pdf_output_path (str): 输出PDF文件的路径，若为None，则自动生成包含当前日期的文件名

        Returns:
            None，但会在控制台打印处理结果信息
        """
        # 如果未指定PDF输出路径，则生成包含当前日期的文件名
        if pdf_output_path is None:
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            pdf_output_path = f"data/{today}_account_metrics_data_visualization.pdf"

        # 设置绘图环境
        AccountMetricsVisualizer._setup_plotting_environment()

        # 加载数据
        try:
            df = AccountMetricsVisualizer._load_data(file_path)
        except Exception as e:
            print(e)
            return

        # 验证当天数据
        if not AccountMetricsVisualizer._validate_today_data(df):
            return

        # 处理数据并计算收益率
        try:
            post_data, post_returns, post_cumulative_returns, post_dates, post_dates_daily, cumulative_post_return = \
                AccountMetricsVisualizer.process_portfolio_data(df)
            btc_data, btc_daily_returns, btc_cumulative_returns, btc_dates, btc_dates_daily, cumulative_btc_return = \
                AccountMetricsVisualizer.process_btc_data(df, post_dates, post_dates_daily)
        except Exception as e:
            print(f"数据处理失败: {e}")
            return

        # 生成PDF报告
        try:
            AccountMetricsVisualizer._generate_pdf_report(
                pdf_output_path,
                post_dates, post_returns, post_cumulative_returns, cumulative_post_return,
                btc_dates, btc_daily_returns, btc_cumulative_returns, cumulative_btc_return,
                post_dates_daily, btc_dates_daily
            )
            print(f"图表已保存至: {pdf_output_path}")
        except Exception as e:
            print(f"PDF 文件生成失败: {e}")


# 主程序入口
if __name__ == "__main__":
    AccountMetricsVisualizer.visualize()