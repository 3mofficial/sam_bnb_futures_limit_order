# -*- coding: utf-8 -*-
# 指定文件编码为 UTF-8，确保支持中文字符

import os
# 导入 os 模块，用于文件和目录操作，如路径处理、文件存在性检查

import smtplib
# 导入 smtplib 模块，用于通过 SMTP 协议发送电子邮件

import imaplib
# 导入 imaplib 模块，用于通过 IMAP 协议接收和处理电子邮件

import email
# 导入 email 模块，用于解析和处理电子邮件内容

import time
# 导入 time 模块，用于处理时间相关操作，如延时等待

import hashlib
# 导入 hashlib 模块，用于计算文件的 MD5 哈希值以标识文件唯一性

from datetime import datetime, timedelta
# 从 datetime 模块导入 datetime 和 timedelta，用于处理日期和时间计算

from email import encoders
# 从 email 模块导入 encoders，用于对邮件附件进行 Base64 编码

from email.mime.base import MIMEBase
# 从 email.mime.base 导入 MIMEBase，用于创建邮件附件的 MIME 对象

from email.mime.multipart import MIMEMultipart
# 从 email.mime.multipart 导入 MIMEMultipart，用于创建多部分邮件对象

from email.mime.text import MIMEText
# 从 email.mime.text 导入 MIMEText，用于创建邮件正文的文本部分

import pandas as pd
# 导入 pandas 库并命名为 pd，用于数据处理和分析，如读取 CSV 和 Excel 文件

from src.binance_client import BinanceFuturesClient
# 从 src.binance_client 模块导入 BinanceFuturesClient 类，用于与 Binance 期货 API 交互

from src.config_loader import ConfigLoader
# 从 src.config_loader 模块导入 ConfigLoader 类，用于加载配置文件

from src.logger import setup_logger
# 从 src.logger 模块导入 setup_logger 函数，用于初始化日志记录器

from src.trading_engine_new import TradingEngine
# 从 src.trading_engine_new 模块导入 TradingEngine 类，用于执行交易逻辑

from src.util_account_metrics_visualizer import AccountMetricsVisualizer
# 从 src.util_account_metrics_visualizer 模块导入 AccountMetricsVisualizer 类，用于生成账户指标可视化

from typing import Dict
# 从 typing 模块导入 Dict，用于类型注解，表示字典类型

def receive_and_download_attachments(logger):
    # 定义函数 receive_and_download_attachments，接收 logger 参数，用于接收邮件并下载附件
    sender_email = "igrklo@163.com"
    # 定义发件人邮箱地址

    receiver_email = "yanzhiyuan3@gmail.com"
    # 定义收件人邮箱地址

    imap_server = "imap.gmail.com"
    # 定义 IMAP 服务器地址（Gmail 的 IMAP 服务器）

    imap_port = 993
    # 定义 IMAP 服务器端口（标准 SSL 端口）

    imap_user = receiver_email
    # 设置 IMAP 用户名，与收件人邮箱相同

    imap_password = "pyrf nykw jeai kgdf"
    # 设置 IMAP 密码（Gmail 应用专用密码）

    current_date = datetime.now().strftime("%Y%m%d")
    # 获取当前日期，格式为 YYYYMMDD（如 20250529）

    save_dir = 'data'
    # 定义附件保存目录为 data 文件夹

    subject = f"pos{current_date}_v3.csv"
    # 定义邮件主题，格式为 posYYYYMMDD_v3.csv，表示当天的资金费率文件

    max_retries = 10
    # 定义最大重试次数为 10 次

    retry_delay = 5
    # 定义重试间隔为 5 秒

    for attempt in range(max_retries):
        # 循环尝试连接和处理邮件，最多 max_retries 次
        try:
            logger.info(f"尝试连接到 {imap_server}:{imap_port} (尝试 {attempt + 1}/{max_retries})")
            # 记录日志，显示当前连接尝试的信息

            with imaplib.IMAP4_SSL(imap_server, imap_port) as mail:  # 移除 timeout
                # 使用 IMAP4_SSL 创建 IMAP 连接，使用 with 语句确保连接关闭
                mail.socket().settimeout(30)  # 手动设置 socket 超时
                # 设置 socket 超时时间为 30 秒，防止连接卡死

                logger.info("连接成功，开始登录")
                # 记录成功连接的日志

                mail.login(imap_user, imap_password)
                # 使用用户名和密码登录 IMAP 服务器

                logger.info("登录成功，开始扫描邮箱")
                # 记录成功登录的日志

                mail.select("INBOX")
                # 选择收件箱（INBOX）作为操作目标

                search_date = datetime.now().strftime("%d-%b-%Y").upper()
                # 获取当前日期，格式为 DD-MMM-YYYY（如 29-May-2025），并转换为大写

                status, messages = mail.search(None, f'SINCE {search_date}')
                # 搜索当天收到的邮件，返回状态和邮件 ID 列表

                if status != "OK" or not messages[0]:
                    # 检查搜索状态是否为 OK 且是否有邮件
                    logger.info("未找到符合条件的邮件")
                    # 记录未找到邮件的日志

                    return False, None
                    # 返回 False 和 None，表示未找到目标邮件

                email_ids = messages[0].split()
                # 将邮件 ID 列表分割为单独的 ID

                logger.info(f"找到 {len(email_ids)} 封邮件")
                # 记录找到的邮件数量

                for email_id in email_ids:
                    # 遍历每封邮件的 ID
                    status, msg_data = mail.fetch(email_id, "(RFC822)")
                    # 获取邮件的完整内容（RFC822 格式）

                    if status != "OK":
                        # 如果获取失败，跳过当前邮件
                        continue

                    msg = email.message_from_bytes(msg_data[0][1])
                    # 将邮件数据转换为 email 消息对象

                    from_header = msg.get("From", "")
                    # 获取邮件的发件人信息，默认为空字符串

                    expected_from = f"{datetime.now().strftime('%Y-%m-%d')}<{sender_email}>"
                    # 构造期望的发件人格式，包含日期和发件人邮箱

                    if msg["Subject"] == subject and from_header == expected_from:
                        # 检查邮件主题和发件人是否匹配目标
                        logger.info(f"找到目标邮件: {msg['Subject']}")
                        # 记录找到目标邮件的日志

                        run_id = datetime.now().strftime("%Y-%m-%d_%H%M%S")
                        # 生成运行 ID，格式为 YYYY-MM-DD_HHMMSS

                        for part in msg.walk():
                            # 遍历邮件的所有部分（如文本、附件等）
                            if part.get_content_maintype() == "multipart":
                                # 如果是 multipart 类型（容器），跳过
                                continue

                            if not part.get("Content-Disposition"):
                                # 如果没有 Content-Disposition（非附件），跳过
                                continue

                            filename = part.get_filename()
                            # 获取附件文件名

                            if filename and filename.endswith('.csv'):
                                # 检查是否为 CSV 文件
                                filepath = os.path.join(save_dir, filename)
                                # 构造附件保存路径

                                with open(filepath, "wb") as f:
                                    # 以二进制写模式打开文件
                                    f.write(part.get_payload(decode=True))
                                    # 写入解码后的附件内容

                                logger.info(f"附件下载成功: {filepath}")
                                # 记录附件下载成功的日志

                                return True, run_id
                                # 返回 True 和运行 ID，表示成功下载附件

                logger.info("未找到目标附件")
                # 记录未找到目标附件的日志

                return False, None
                # 返回 False 和 None，表示未找到目标附件

        except Exception as e:
            # 捕获所有异常
            logger.error(f"操作失败: {str(e)}")
            # 记录操作失败的错误日志

            if attempt < max_retries - 1:
                # 如果未达到最大重试次数
                logger.info(f"{retry_delay}秒后重试...")
                # 记录重试等待的日志

                time.sleep(retry_delay)
                # 等待指定的重试间隔

    logger.error("达到最大重试次数，操作终止")
    # 记录达到最大重试次数的错误日志

    return False, None
    # 返回 False 和 None，表示操作失败

def analyze_positions(logger, run_id: str, error_reasons: Dict[str, str]):
    # 定义函数 analyze_positions，分析持仓数据并添加错误原因列，接收 logger、run_id 和 error_reasons 参数
    """分析持仓数据，添加失败原因列"""
    # 函数文档字符串，描述函数功能

    date_str = datetime.now().strftime("%Y-%m-%d")
    # 获取当前日期，格式为 YYYY-MM-DD

    logger.info(f"当前日期: {date_str}, Run_ID: {run_id}")
    # 记录当前日期和运行 ID 的日志

    positions_file = 'data/positions_output.csv'
    # 定义持仓文件路径

    if not os.path.exists(positions_file):
        # 检查持仓文件是否存在
        logger.info(f"{positions_file} 不存在，创建空持仓记录")
        # 记录文件不存在的日志

        positions_df = pd.DataFrame(columns=['调仓日期', '交易对', '持仓数量', '入场价格', '运行时间', 'Run_ID'])
        # 创建空的持仓 DataFrame，包含指定列

    else:
        # 如果持仓文件存在
        try:
            positions_df = pd.read_csv(positions_file, index_col=None)
            # 读取持仓 CSV 文件，不使用索引列

            if 'Run_ID' not in positions_df.columns:
                # 检查是否存在 Run_ID 列
                logger.warning(f"{positions_file} 缺少 Run_ID 列，添加空列")
                # 记录缺少 Run_ID 列的警告日志

                positions_df['Run_ID'] = ''
                # 添加空的 Run_ID 列

            # 兼容 Run_ID 为空的情况，尝试匹配日期
            positions_df['Run_ID'] = positions_df['Run_ID'].fillna('')
            # 将 Run_ID 列的空值填充为空字符串

            positions_df = positions_df.drop_duplicates(subset=['交易对', '调仓日期', 'Run_ID'], keep='last')
            # 按交易对、调仓日期和 Run_ID 去重，保留最新记录

            logger.info(f"读取 {positions_file}，去重后包含 {len(positions_df)} 条记录")
            # 记录读取和去重后的记录数

        except Exception as e:
            # 捕获读取文件时的异常
            logger.error(f"读取 {positions_file} 失败: {str(e)}")
            # 记录读取失败的错误日志

            positions_df = pd.DataFrame(columns=['调仓日期', '交易对', '持仓数量', '入场价格', '运行时间', 'Run_ID'])
            # 创建空的持仓 DataFrame

    # 过滤当前 Run_ID 或当天数据
    current_positions = positions_df[
        (positions_df['Run_ID'] == run_id) |
        ((positions_df['Run_ID'] == '') & (positions_df['调仓日期'] == date_str))
    ]
    # 筛选 Run_ID 匹配或当天日期且 Run_ID 为空的记录

    logger.info(f"找到 {len(current_positions)} 条 Run_ID={run_id} 或日期={date_str} 的记录")
    # 记录筛选出的记录数

    if not current_positions.empty:
        # 如果筛选结果不为空
        current_positions = current_positions[['调仓日期', '交易对', '持仓数量', '入场价格', 'Run_ID']]
        # 提取需要的列

        logger.info("已提取所需字段的持仓数据")
        # 记录提取字段的日志

    else:
        # 如果筛选结果为空
        logger.info("当前 Run_ID 无持仓数据，创建空结果")
        # 记录无持仓数据的日志

        current_positions = pd.DataFrame(columns=['调仓日期', '交易对', '持仓数量', '入场价格', 'Run_ID'])
        # 创建空的持仓 DataFrame

    positive_positions = current_positions[current_positions['持仓数量'] > 0]
    # 筛选持仓数量大于 0 的记录（多头持仓）

    negative_positions = current_positions[current_positions['持仓数量'] < 0]
    # 筛选持仓数量小于 0 的记录（空头持仓）

    logger.info(f"正持仓数量: {len(positive_positions)}, 负持仓数量: {len(negative_positions)}")
    # 记录多头和空头持仓的数量

    funding_file = f'data/pos{datetime.now().strftime("%Y%m%d")}_v3.csv'
    # 构造资金费率文件路径，格式为 data/posYYYYMMDD_v3.csv

    if os.path.exists(funding_file):
        # 检查资金费率文件是否存在
        try:
            funding_df = pd.read_csv(funding_file, index_col=None)
            # 读取资金费率 CSV 文件

            funding_df = funding_df.drop_duplicates(subset=['ticker'], keep='last')
            # 按 ticker 列去重，保留最新记录

            logger.info(f"资金费率文件 {funding_file} 已加载，去重后包含 {len(funding_df)} 条记录")
            # 记录加载和去重后的记录数

        except Exception as e:
            # 捕获读取文件时的异常
            logger.error(f"读取 {funding_file} 失败: {str(e)}")
            # 记录读取失败的错误日志

            funding_df = pd.DataFrame(columns=['id', 'ticker', 'fundingRate'])
            # 创建空的资金费率 DataFrame

    else:
        # 如果资金费率文件不存在
        logger.info(f"资金费率文件 {funding_file} 不存在，创建空资金费率数据")
        # 记录文件不存在的日志

        funding_df = pd.DataFrame(columns=['id', 'ticker', 'fundingRate'])
        # 创建空的资金费率 DataFrame

    # 初始化 ErrorReason 列
    funding_df['ErrorReason'] = ''
    # 为资金费率 DataFrame 添加空的 ErrorReason 列

    # 处理黑名单过滤的币种
    if "blacklisted_tickers" in error_reasons:
        # 检查错误原因中是否包含黑名单过滤
        blacklisted_tickers_str = error_reasons["blacklisted_tickers"]
        # 获取黑名单交易对字符串

        if blacklisted_tickers_str:
            # 如果黑名单字符串不为空
            blacklisted_tickers = [ticker.strip() for ticker in blacklisted_tickers_str.split(",")]
            # 将黑名单字符串分割为列表并去除空格

            funding_df.loc[funding_df['ticker'].isin(blacklisted_tickers), 'ErrorReason'] = "黑名单过滤"
            # 为黑名单中的交易对设置 ErrorReason 为 "黑名单过滤"

            logger.info(f"已为 {len(blacklisted_tickers)} 个黑名单币种设置 ErrorReason: 黑名单过滤")
            # 记录设置黑名单错误原因的日志

    # 映射其他错误原因（优先级高于黑名单过滤）
    ticker_errors = {k: v for k, v in error_reasons.items() if
                     k not in ["blacklisted_tickers", "file_not_found", "position_adjustment_failed", "system_error"]}    # 筛选出非特定类型的错误原因（交易对特定错误）

    logger.error(f"已筛选出 {len(ticker_errors)} 个币种映射错误原因")
    if ticker_errors:
        # 如果存在其他错误记录
            funding_df['ErrorReason'] = funding_df['ErrorReason'].where(
                ~funding_df['ticker'].isin(ticker_errors),
                funding_df['ticker'].map(ticker_errors)
            )
            # 使用 ticker_errors 更新 ErrorReason 列
            logger.info(f"已为 {len(ticker_errors)} 个币种映射其他错误原因")
            # 记录映射错误原因的日志

    positive_tickers = positive_positions['交易对'].tolist()
    # 获取多头持仓的交易对列表

    positive_funding = funding_df[funding_df['ticker'].isin(positive_tickers)]
    # 筛选资金费率数据中包含多头交易对的记录

    positive_funding_sorted = positive_funding.sort_values('id')
    # 按 id 升序排序多头资金费率数据

    if not positive_funding_sorted.empty:
        # 如果多头资金费率数据不为空
        max_id = positive_funding_sorted['id'].max()
        # 获取多头资金费率的最大 ID

        positive_funding_filtered = funding_df[funding_df['id'] <= max_id][['id', 'ticker', 'fundingRate', 'ErrorReason']]
        # 筛选 ID 小于等于 max_id 的资金费率数据，保留指定列

        positive_funding_filtered = positive_funding_filtered.sort_values('id')
        # 按 ID 升序排序筛选后的数据

        logger.info(f"正持仓资金费率数据已筛选，最大 id 数据为: {max_id}")
        # 记录筛选多头资金费率数据的日志

        positive_result = positive_funding_filtered.merge(
            positive_positions, how='left', left_on='ticker', right_on='交易对'
        )[['调仓日期', '交易对', '持仓数量', '入场价格', 'id', 'ticker', 'fundingRate', 'ErrorReason', 'Run_ID']]
        # 将多头资金费率数据与多头持仓数据合并，保留指定列

        positive_result['调仓日期'] = positive_result['调仓日期'].fillna(date_str)
        # 填充缺失的调仓日期为当前日期

        positive_result['Run_ID'] = positive_result['Run_ID'].fillna(run_id)
        # 填充空的 Run_ID 列为当前运行 ID

        # 对于持仓数量非 NaN 的记录，清空 ErrorReason
        positive_result.loc[positive_result['持仓数量'].notna(), 'ErrorReason'] = ''
        # 清空有持仓数量的记录的 ErrorReason 列

        logger.info("正持仓数据已完成合并并处理空调仓日期、Run_ID 和 ErrorReason")
        # 记录多头持仓数据合并完成的日志

    else:
        # 如果多头资金费率数据为空
        positive_result = pd.DataFrame(
            columns=['调仓日期', '交易对', '持仓数量', '入场价格', 'id', 'ticker', 'fundingRate', 'ErrorReason', 'Run_ID'])
        # 创建空的正持仓结果 DataFrame

    negative_tickers = negative_positions['交易对'].tolist()
    # 获取空头持仓的交易对列表

    negative_funding = funding_df[funding_df['ticker'].isin(negative_tickers)]
    # 筛选资金费率数据中包含空头交易对的记录

    negative_funding_sorted = negative_funding.sort_values('id', ascending=False)
    # 按 id 降序排序空头资金费率数据

    if not negative_funding_sorted.empty:
        # 如果空头资金费率数据不为空
        min_id = negative_funding_sorted['id'].min()
        # 获取空头资金费率的最小 ID

        negative_funding_filtered = funding_df[funding_df['id'] >= min_id][['id', 'ticker', 'fundingRate', 'ErrorReason']]
        # 筛选 ID 大于等于 min_id 的资金费率数据，保留指定列

        negative_funding_filtered = negative_funding_filtered.sort_values('id', ascending=False)
        # 按 ID 降序排序筛选后的数据

        logger.info(f"负持仓资金费率数据已筛选，最小 id 为: {min_id}")
        # 记录筛选空头资金费率数据的日志

        negative_result = negative_funding_filtered.merge(
            negative_positions, how='left', left_on='ticker', right_on='交易对'
        )[['调仓日期', '交易对', '持仓数量', '入场价格', 'id', 'ticker', 'fundingRate', 'ErrorReason', 'Run_ID']]
        # 将空头资金费率数据与空头持仓数据合并，保留指定列
        negative_result['调仓日期'] = negative_result['调仓日期'].fillna(date_str)
        # 填充缺失的调仓日期为当前日期
        negative_result['Run_ID'] = negative_result['Run_ID'].fillna(run_id)
        # 填充空的 Run_ID 列为当前运行 ID

        negative_result.loc[negative_result['持仓数量'].notna(), 'ErrorReason'] = ''
        # 清空有持仓数量的记录的 ErrorReason 列

        logger.info("负持仓数据已完成合并并处理空调仓日期、Run_ID 和 ErrorReason")
        # 记录负持仓数据合并完成的日志

    else:
        # 如果空头资金费率数据为空
        negative_result = pd.DataFrame(
            columns=['调仓日期', '交易对', '持仓数量', '入场价格', 'id', 'ticker', 'fundingRate', 'ErrorReason', 'Run_ID'])
        # 创建空的负持仓结果 DataFrame

    non_empty_results = [df for df in [positive_result, negative_result] if not df.empty and not df.isna().all().all()]
    # 筛选非空且非全缺失的正负持仓结果

    if non_empty_results:
        # 如果存在有效结果
        final_result = pd.concat(non_empty_results, ignore_index=True)
        # 合并正负持仓结果，忽略索引

        logger.info("正负持仓结果已合并")
        # 记录正负持仓结果合并的日志

    else:
        # 如果无有效持仓数据
        final_result = pd.DataFrame(
            columns=['调仓日期', '交易对', '持仓数量', '入场价格', 'id', 'ticker', 'fundingRate', 'ErrorReason', 'Run_ID'])
        # 创建空的最终结果 DataFrame

        logger.info("无有效持仓数据，生成空结果")
        # 记录生成空结果的日志

    output_file = 'data/position_analysis.xlsx'
    # 定义分析结果的 Excel 文件路径

    if os.path.exists(output_file):
        # 检查输出文件是否存
        existing_df = pd.read_excel(output_file)
        # 读取现有 Excel 文件

        # 添加缺失的 ErrorReason 列
        if 'ErrorReason' not in existing_df.columns:
            # 检查是否缺少 ErrorReason 列
            existing_df['ErrorReason'] = ''
            # 添加空的 ErrorReason 列

        concat_list = [df for df in [existing_df, final_result] if not df.empty and not df.isna().all().all()]
        # 筛选非空的数据列表用于拼接

        if concat_list:
            # 如果存在有效数据列表
            updated_df = pd.concat(concat_list, ignore_index=True)
            # 合并现有数据和新数据

            updated_df = updated_df.drop_duplicates(subset=['Run_ID', '交易对', 'ID'], keep='last')
            # 按 Run_ID、交易对和 id 去重，保留最新记录

            updated_df.to_excel(output_file, index=False)
            # 保存更新后的数据到到 Excel 文件中

            logger.info(f"分析结果已去重并追加写入文件: {output_file}，记录数: {len(updated_df)}")
            # 记录保存成功的日志

            # 记录保存成功的日志，包含记录数

        else:
            # 如果无有效数据可拼接
            logger.error("警告: 无有效数据可拼接，跳过写入")
            logger.warning("无有效数据可拼接，跳过")
            # 返回记录警告日志，跳过写入
            return
            # 退出函数

    else:
        # 如果输出文件不存在
        final_result.to_excel(output_file, index=False)
        # 保存最终结果到 Excel 文件
        logger.info(f"分析结果已首次写入文件): {output_file}")
        # 记录首次写入文件的日志

def send_email_with_attachments(logger):
    """发送带附件的邮件"""
    # 定义函数 send_email_with，用于发送带附件的电子邮件，接收 logger 参数

    sender_email = "yanzhiyuan3@gmail.com"
    # 设置发件人邮箱地址

    receiver_email = "yanzhiyuan3@gmail.com"
    # 设置收件人邮箱地址

    cc_emails = ["yanzhiyuan3@gmail.com"]
    # 设置抄送邮箱列表

    subject = "test_sam_account_metrics"
    # 设置邮件主题为测试账户指标

    data_dir = "data"
    # 设置数据目录为 data 文件夹

    account_metrics_path = os.path.join(data_dir, "account_metrics.xlsx")
    # 构造账户指标 Excel 文件路径

    position_analysis_path = os.path.join(data_dir, "position_analysis.xlsx")
    # 构造持仓分析 Excel 文件路径

    today = datetime.now().strftime("%Y-%m-%d-%d")
    # 获取当天日期，格式为 YYYY-MM-DD-DD

    account_metrics_data_visualization_pdf = os.path.join(data_dir, f"{today}_account_metrics_data_visualization.pdf")
    # 构造账户指标可视化 PDF 文件路径

    if not os.path.exists(account_metrics_path):
        # 检查账户指标文件是否存在
        logger.info("account_metrics.xlsx 文件不存在，请检查 data 目录下的文件")
        # 记录文件不存在的错误信息

        return
        # 如果文件不存在，退出函数

    try:
        # 尝试块，处理文件读取操作
        df = pd.read_excel(account_metrics_path)
        # 读取账户指标 Excel 文件

        current_date = datetime.now().strftime("%Y-%m-%d")
        # 获取当前日期，格式为YYYY-MM-DD

        if 'Date' not in df.columns or current_date not in df['Date'].str.split('_').str[0].values():
            # 检查是否存在 Date 列且当前日期是否在 Date 列中
            logger.info(f"今天是 {current_date}，account_metrics.xlsx 中未找到匹配的日期，不发送邮件")
            # 记录未找到当天数据的日志

            return
            # 如果没有当天数据，退出函数

    except Exception as e:
        # 捕获读取文件时的错误
        logger.error(f"读取 {account_metrics_path} 时出现错误: {str(e)}")
        # 返回记录读取错误的错误日志

        return
        # 返回退出函数

    msg = MIMEMultipart()
    # 创建多部分邮件对象

    msg['From'] = sender_email
    # 设置邮件发件人

    msg['To'] = receiver_email
    # 设置邮件收件人

    msg['CC'] = ",".join(cc_emails)
    # 设置抄送收件人，多个邮箱以逗号分隔

    msg['Subject'] = subject
    # 设置邮件主题

    body = """Hi,

    请查看附件。

    祝好，"""
    # 定义邮件正文内容

    msg.attach(MIMEText(body, body, 'plain'))
    # 将纯文本正文添加到邮件

    attachment_files = [account_metrics_path]
    # 初始化附件文件列表，包含账户指标文件

    if os.path.exists(position_analysis_path):
        # 检查持仓分析文件是否存在
        attachment_files.append(position_analysis_path)
        # 将持仓分析文件添加到附件列表

        logger.info(f"添加附件: {position_analysis_path}")
        # 记录添加持仓分析文件的日志

    else:
        # 如果持仓分析文件不存在
        logger.info(f"position_analysis.xlsx 文件不存在，将跳过此附件跳过")
        # 记录跳过持仓分析附件的日志

    if os.path.exists(account_metrics_data_visualization_pdf):
        # 检查可视化 PDF 文件是否存在
        attachment_files.append(account_metrics_data_visualization_pdf)
        # 将添加可视化文件到 PDF 到附件列表

        logger.info(f"添加可视化文件 PDF 文件: {account_metrics_data_visualization_pdf}")
        # 记录添加可视化 PDF 文件的日志

    else:
        # 如果可视化 PDF 文件不存在
        logger.info(f"未找到字符可视化字符 PDF: {account_metrics_data_visualization_pdf}")
        # 记录未找到可视化 PDF 文件的日志

    for file_path in attachment_files:
        # 遍历附件文件列表
        with open(file_path, "rb") as attachment:
            # # 以二进制读模式打开附件文件

            part = MIMEBase("application", "octet-stream")
            # 创建 MIMEBase 对象，表示二进制附件

            part.set_payload(attachment.read())
            # 读取附件内容并设置到 MIMEBase 对象

            encoders.encode_base64(part)            # 对附件内容进行 Base64 编码

            part.add_header(
                "Content-Disposition",
                f"attachment; filename={os.path.basename(file_path)}",
            )
            # 设置附件头信息，包含文件名

            msg.attach(part)
            # 将附件添加到邮件对象

    smtp_server = "smtp.gmail.com"
    # 设置 SMTP 服务器地址（Gmail 的 SMTP 服务器）

    smtp_port = 465
    # 设置 SMTP 端口（SSL 465 端口）

    smtp_user = sender_email
    # 设置 SMTP 用户名，与发件人邮箱相同

    smtp_password = "pyrf nykw jeai kdgf"
    # 设置 SMTP 密码（Gmail 应用专用密码）

    max_retries = 5
    # 设置最大重试次数为 5 次

    retry_delay = 600
    # 设置重试间隔为600 秒（10分钟分钟）

    for attempt in range(max_retries):
        # 循环尝试连接和发送邮件，最多 max_retries 次
        try:
            logger.info(f"尝试连接到 {smtp_server}:{smtp_port} (第 {attempt + 1}/{max_retries} 次)")
            # 记录连接尝试的服务器信息和次数

            with smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=60) as server:
                # 使用 SMTP_SSL 创建连接，设置超时时间为 60秒
                logger.info("连接成功，开始登录")
                # 记录成功登录的日志

                server.login(smtp_user, smtp_password)
                # 使用用户名和密码登录 SMTP 服务器

                server.send_message(msg)
                # 发送邮件

                logger.info("邮件已发送成功！")
                # 记录邮件发送成功的日志

                return
                # 如果发送成功，退出函数

        except Exception as e:
            # 捕获发送邮件时的异常
            logger.error(f"发送邮件失败 (第 {attempt + 1}/{max_retries}): {str(e)}")
            # 记录发送邮件失败的错误日志

            if attempt < max_retries - 1:  # 如果未达到最大重试次数
                logger.info(f"等待 {retry_delay} 秒后重试...")
                # 记录等待重试的日志

                time.sleep(retry_delay)
                # 等待指定的重试间隔

            else:
                # 如果达到最大重试次数
                logger.error("达到最大重试次数，邮件发送失败")
                # 记录邮件发送最终失败的错误日志

def main():
    # 定义主函数 main()，实现交易程序的主逻辑
    config_loader = ConfigLoader()
    # 创建 ConfigLoader 实例，用于加载配置文件

    api_config = config_loader.get_api_config()
    # 获取 API 相关配置

    trading_config = config_loader.get_trading_config()
    # 获取交易相关配置

    paths_config = config_loader.get_paths_config()
    # 获取路径相关配置

    config = {**api_config, **trading_config, **paths_config}
    # 合并所有配置为一个字典

    logger = setup_logger(config["log_path"])
    # 根据配置中的日志路径初始化日志记录器

    logger.info("交易程序启动，进入日内多次换仓模式")
    # 记录交易程序启动的日志，进入日内交易模式

    analysis_file = 'data/position_analysis.xlsx'
    # 定义持仓分析结果文件路径

    if not os.path.exists(analysis_file):
        # 检查分析文件是否存
        os.makedirs(os.path.dirname(analysis_file), exist_ok=True)
        # 创建文件所在目录（如果不存在）

        df = pd.DataFrame(columns=['调仓日期', '交易对', '持仓数量', '入场价格', 'id', 'ticker', 'fundingRate', 'Run_ID'])
        # 创建空的分析结果 DataFrame，包含指定列

        df.to_excel(analysis_file, index=False)
        # 保存空 DataFrame 到 Excel 文件

        logger.info(f"初始化空分析数据文件: {analysis_file}")
        # 记录初始化分析文件的日志

    client = BinanceFuturesClient(config["api_key"], config["api_secret"],
                                 config["test_net"] == "True", logger)
        # 创建 Binance 期货客户端实例，使用配置中的 API 密钥和测试网络设置

    engine = TradingEngine(client, config, logger)
    # 创建交易引擎实例，传入客户端、配置和日志记录器

    processed_files = {}
    # 初始化已处理文件字典，存储已处理文件的哈希值和运行 ID

    while True:
        # 进入无限循环，持续监控和处理文件
        try:
            current_date = datetime.now().strftime("%Y%m%d")
            # 获取当前日期，格式为 YYYYMMDD

            run_id = datetime.now().strftime("%Y-%m-%d_%H%M:%S")
            # 生成运行 ID，格式为 YYYY-MM-DD_HHMMSS

            logger.info(f"检查 {current_date} 的文件或邮件，Run_ID: {run_id}")
            # 记录检查当前日期和运行 ID的日志

            csv_path_template = config["csv_path"]
            # 获取 CSV 文件路径模板

            if "{date}" in csv_path_template:
                # 检查路径模板中是否包含日期占位符
                csv_file = csv_path_template.format(date=current_date)
                # 替换日期占位符为当前日期

                date_str = current_date
                # 设置日期字符串为当前日期

            else:
                # 如果路径模板不包含日期占位符
                csv_file = csv_path_template
                # # 使用模板路径
                date_str = csv_file.split("pos")[1].split("_")[0]
                # 从文件名提取日期字符串

            logger.info(f"检查本地 {csv_file}：{csv_file}")
            # 记录检查本地文件的日志

            file_hash = None
            # 初始化文件哈希值为空值

            file_exists = False
            # 初始化文件存在标志为 False

            if os.path.exists(csv_file):
                # 检查本地文件是否存在
                with open(csv_file, 'rb') as f:
                    # 以二进制读模式打开文件
                    file_hash = hashlib.md5(f.read()).hexdigest()
                    # 计算文件的 MD5 哈希值
                if file_hash not in processed_files:
                    # 如果文件哈希值未被处理
                    logger.info(f"本地找到文件 {csv_file}，哈希值: {file_hash}，未处理，直接使用")
                    # 记录找到未处理文件的日志
                    file_exists = True
                    # 设置文件存在标志为 True
                else:
                    # 如果文件已被处理
                    logger.info(f"文件 {csv_file} 已处理（哈希值: {file_hash}），已跳过）")
                    # 记录跳过已处理文件的日志
            else:
                # 如果本地文件不存在
                logger.info(f"本地未找到文件 {csv_file}，开始扫描邮箱")
                # 记录未找到文件，尝试从邮箱获取的日志

                file_exists, _, email_run_id = receive_and_download_attachments(logger)
                # 调用函数从邮箱接收并下载附件

                if file_exists:
                    # 如果从邮箱获取到文件
                    with open(csv_file, 'rb') as f:
                        # 以二进制读模式打开文件
                        file_hash = hashlib.md5(f.read()).hexdigest()
                        # 计算文件的 MD5 哈希值

                    run_id = email_run_id or run_id
                    # 使用邮箱中的运行 ID 或原运行 ID
                    logger.info(f"从邮箱下载 {csv_file}，哈希值: {file_hash}")

                    # 记录从邮箱下载文件的日志

            if file_exists:
                # 如果文件存在（本地或从邮箱下载）
                logger.info(f"处理 {date_str} 的pos文件，Run_ID:： {run_id}")
                # 记录处理文件的日志

                error_reasons = engine.run(date_str, run_id)  # 错误捕获错误原因
                # 运行交易引擎，获取错误原因字典

                analyze_positions(logger, run_id, error_reasons)
                # 调用分析持仓函数，传递错误原因

                account_metrics_file_path = "data/account_metrics.xlsx"
                # 定义账户指标文件路径

                if os.path.exists(account_metrics_file_path):
                    # 检查账户指标文件是否存在
                    account_metrics_df = pd.read_excel(account_metrics_file_path)
                    # 读取账户指标 Excel 文件

                    account_metrics_df['Date'] = pd.to_datetime(account_metrics_df['Date'].str.split('_').str[0])
                    # 将 Date 列转换为日期格式，仅保留日期部分

                    unique_dates = account_metrics_df['Date'].dt.date.nunique()
                    # 计算唯一日期数量
                    if unique_dates >= 3:
                        # 如果唯一日期数量不少于3 天
                        logger.info(f"唯一日期数量为 {unique_dates}，执行执行交易数据可视化...")
                        # 记录执行可视化的日志

                        AccountMetricsVisualizer.visualize()
                        # 调用可视化方法生成图表
                    else:
                        # 如果唯一日期少于少于3 天
                        logger.info(f"唯一日期数量为 {unique_dates}，少于3天，不执行交易可视化")
                        # 记录日期不足的日志
                else:
                    # 如果账户指标文件不存在
                    logger.info("account_metrics.xlsx 文件不存在，无法生成图表")

                send_email_with_attachments(logger)
                # 调用函数发送带附件的邮件

                logger.info(f"{date_str} 交易完成，Run_ID: {run_id}")
                # 记录交易完成的日志
                if file_hash:
                    # 如果存在文件哈希值
                    processed_files[file_hash] = run_id
                    # 记录已处理文件的哈希值和运行 ID

                    logger.info(f"记录已处理文件 {csv_file}，哈希值: {file_hash}，Run_ID: {run_id}")
                    # 记录已处理文件的日志

                logger.info("交易完成后等待 10 分钟分钟")
                # 记录等待下一轮的日志
                time.sleep(600)
                # 等待 600 秒（10 分钟）

            else:
                # 如果未找到文件
                logger.info(f"未收到 {date_str} 的pos文件，等待 10 分钟后重试")
                # 记录未收到文件的日志

                time.sleep(600)
                # 等待 600 秒后重试

        except Exception as e:
            # 捕获主循环中的异常
            logger.error(f"交易程序发生异常: {str(e)}")
            logger.critical(f"交易程序发生异常: {str(e)}")
            # 记录严重异常的日志

            time.sleep(60)
            # 等待 60 秒后继续循环

if __name__ == "__main__":
    # 检查是否直接运行脚本
    main()
    # 调用主函数启动程序