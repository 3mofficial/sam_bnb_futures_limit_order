# -*- coding: utf-8 -*-
import os
import smtplib
import imaplib
import email
import time
import hashlib
from datetime import datetime, timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from src.binance_client import BinanceFuturesClient
from src.config_loader import ConfigLoader
from src.logger import setup_logger
from src.trading_engine_new import TradingEngine
from src.util_account_metrics_visualizer import AccountMetricsVisualizer
from typing import Dict

def receive_and_download_attachments(logger):
    sender_email = "igrklo@163.com"
    receiver_email = "yanzhiyuan3@gmail.com"
    imap_server = "imap.gmail.com"
    imap_port = 993
    imap_user = receiver_email
    imap_password = "pyrf nykw jeai kgdf"

    current_date = datetime.now().strftime("%Y%m%d")
    save_dir = 'data'
    subject = f"pos{current_date}_v3.csv"

    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            logger.info(f"尝试连接到 {imap_server}:{imap_port} (尝试 {attempt + 1}/{max_retries})")
            with imaplib.IMAP4_SSL(imap_server, imap_port) as mail:  # 移除 timeout
                mail.socket().settimeout(30)  # 手动设置 socket 超时
                logger.info("连接成功，开始登录")
                mail.login(imap_user, imap_password)
                logger.info("登录成功，开始扫描邮箱")

                mail.select("INBOX")
                search_date = datetime.now().strftime("%d-%b-%Y").upper()
                status, messages = mail.search(None, f'SINCE {search_date}')

                if status != "OK" or not messages[0]:
                    logger.info("未找到符合条件的邮件")
                    return False, None

                email_ids = messages[0].split()
                logger.info(f"找到 {len(email_ids)} 封邮件")

                for email_id in email_ids:
                    status, msg_data = mail.fetch(email_id, "(RFC822)")
                    if status != "OK":
                        continue

                    msg = email.message_from_bytes(msg_data[0][1])
                    from_header = msg.get("From", "")
                    expected_from = f"{datetime.now().strftime('%Y-%m-%d')}<{sender_email}>"

                    if msg["Subject"] == subject and from_header == expected_from:
                        logger.info(f"找到目标邮件: {msg['Subject']}")
                        run_id = datetime.now().strftime("%Y-%m-%d_%H%M%S")
                        for part in msg.walk():
                            if part.get_content_maintype() == "multipart":
                                continue
                            if not part.get("Content-Disposition"):
                                continue

                            filename = part.get_filename()
                            if filename and filename.endswith('.csv'):
                                filepath = os.path.join(save_dir, filename)
                                with open(filepath, "wb") as f:
                                    f.write(part.get_payload(decode=True))
                                logger.info(f"附件下载成功: {filepath}")
                                return True, run_id

                logger.info("未找到目标附件")
                return False, None

        except Exception as e:
            logger.error(f"操作失败: {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"{retry_delay}秒后重试...")
                time.sleep(retry_delay)

    logger.error("达到最大重试次数，操作终止")
    return False, None

def analyze_positions(logger, run_id: str, error_reasons: Dict[str, str]):
    """分析持仓数据，添加失败原因列"""
    date_str = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"当前日期: {date_str}, Run_ID: {run_id}")

    positions_file = 'data/positions_output.csv'
    if not os.path.exists(positions_file):
        logger.info(f"{positions_file} 不存在，创建空持仓记录")
        positions_df = pd.DataFrame(columns=['调仓日期', '交易对', '持仓数量', '入场价格', '运行时间', 'Run_ID'])
    else:
        try:
            positions_df = pd.read_csv(positions_file, index_col=None)
            if 'Run_ID' not in positions_df.columns:
                logger.warning(f"{positions_file} 缺少 Run_ID 列，添加空列")
                positions_df['Run_ID'] = ''
            # 兼容 Run_ID 为空的情况，尝试匹配日期
            positions_df['Run_ID'] = positions_df['Run_ID'].fillna('')
            positions_df = positions_df.drop_duplicates(subset=['交易对', '调仓日期', 'Run_ID'], keep='last')
            logger.info(f"读取 {positions_file}，去重后包含 {len(positions_df)} 条记录")
        except Exception as e:
            logger.error(f"读取 {positions_file} 失败: {str(e)}")
            positions_df = pd.DataFrame(columns=['调仓日期', '交易对', '持仓数量', '入场价格', '运行时间', 'Run_ID'])

    # 过滤当前 Run_ID 或当天数据
    current_positions = positions_df[
        (positions_df['Run_ID'] == run_id) |
        ((positions_df['Run_ID'] == '') & (positions_df['调仓日期'] == date_str))
    ]
    logger.info(f"找到 {len(current_positions)} 条 Run_ID={run_id} 或日期={date_str} 的记录")

    if not current_positions.empty:
        current_positions = current_positions[['调仓日期', '交易对', '持仓数量', '入场价格', 'Run_ID']]
        logger.info("已提取所需字段的持仓数据")
    else:
        logger.info("当前 Run_ID 无持仓数据，创建空结果")
        current_positions = pd.DataFrame(columns=['调仓日期', '交易对', '持仓数量', '入场价格', 'Run_ID'])

    positive_positions = current_positions[current_positions['持仓数量'] > 0]
    negative_positions = current_positions[current_positions['持仓数量'] < 0]
    logger.info(f"正持仓数量: {len(positive_positions)}, 负持仓数量: {len(negative_positions)}")
    funding_file = f'data/pos{datetime.now().strftime("%Y%m%d")}_v3.csv'
    if os.path.exists(funding_file):
        try:
            funding_df = pd.read_csv(funding_file, index_col=None)
            funding_df = funding_df.drop_duplicates(subset=['ticker'], keep='last')
            logger.info(f"资金费率文件 {funding_file} 已加载，去重后包含 {len(funding_df)} 条记录")
        except Exception as e:
            logger.error(f"读取 {funding_file} 失败: {str(e)}")
            funding_df = pd.DataFrame(columns=['id', 'ticker', 'fundingRate'])
    else:
        logger.info(f"资金费率文件 {funding_file} 不存在，创建空资金费率数据")
        funding_df = pd.DataFrame(columns=['id', 'ticker', 'fundingRate'])

    # 初始化 ErrorReason 列
    funding_df['ErrorReason'] = ''

    # 处理黑名单过滤的币种
    if "blacklisted_tickers" in error_reasons:
        blacklisted_tickers_str = error_reasons["blacklisted_tickers"]
        if blacklisted_tickers_str:
            blacklisted_tickers = [ticker.strip() for ticker in blacklisted_tickers_str.split(",")]
            funding_df.loc[funding_df['ticker'].isin(blacklisted_tickers), 'ErrorReason'] = "黑名单过滤"
            logger.info(f"已为 {len(blacklisted_tickers)} 个黑名单币种设置 ErrorReason: 黑名单过滤")

    # 映射其他错误原因（优先级高于黑名单过滤）
    ticker_errors = {k: v for k, v in error_reasons.items() if k not in ["blacklisted_tickers", "file_not_found", "position_adjustment_failed", "system_error"]}
    if ticker_errors:
        funding_df['ErrorReason'] = funding_df['ErrorReason'].where(
            ~funding_df['ticker'].isin(ticker_errors),
            funding_df['ticker'].map(ticker_errors)
        )
        logger.info(f"已为 {len(ticker_errors)} 个币种映射其他错误原因")

    positive_tickers = positive_positions['交易对'].tolist()
    positive_funding = funding_df[funding_df['ticker'].isin(positive_tickers)]
    positive_funding_sorted = positive_funding.sort_values('id')
    if not positive_funding_sorted.empty:
        max_id = positive_funding_sorted['id'].max()
        positive_funding_filtered = funding_df[funding_df['id'] <= max_id][['id', 'ticker', 'fundingRate', 'ErrorReason']]
        positive_funding_filtered = positive_funding_filtered.sort_values('id')
        logger.info(f"正持仓资金费率数据已筛选，最大 id: {max_id}")

        positive_result = positive_funding_filtered.merge(
            positive_positions, how='left', left_on='ticker', right_on='交易对'
        )[['调仓日期', '交易对', '持仓数量', '入场价格', 'id', 'ticker', 'fundingRate', 'ErrorReason', 'Run_ID']]
        positive_result['调仓日期'] = positive_result['调仓日期'].fillna(date_str)
        positive_result['Run_ID'] = positive_result['Run_ID'].fillna(run_id)
        # 对于持仓数量非 NaN 的记录，清空 ErrorReason
        positive_result.loc[positive_result['持仓数量'].notna(), 'ErrorReason'] = ''
        logger.info("正持仓数据已完成合并并处理空调仓日期、Run_ID 和 ErrorReason")
    else:
        positive_result = pd.DataFrame(
            columns=['调仓日期', '交易对', '持仓数量', '入场价格', 'id', 'ticker', 'fundingRate', 'ErrorReason', 'Run_ID'])

    negative_tickers = negative_positions['交易对'].tolist()
    negative_funding = funding_df[funding_df['ticker'].isin(negative_tickers)]
    negative_funding_sorted = negative_funding.sort_values('id', ascending=False)
    if not negative_funding_sorted.empty:
        min_id = negative_funding_sorted['id'].min()
        negative_funding_filtered = funding_df[funding_df['id'] >= min_id][['id', 'ticker', 'fundingRate', 'ErrorReason']]
        negative_funding_filtered = negative_funding_filtered.sort_values('id', ascending=False)
        logger.info(f"负持仓资金费率数据已筛选，最小 id: {min_id}")

        negative_result = negative_funding_filtered.merge(
            negative_positions, how='left', left_on='ticker', right_on='交易对'
        )[['调仓日期', '交易对', '持仓数量', '入场价格', 'id', 'ticker', 'fundingRate', 'ErrorReason', 'Run_ID']]
        negative_result['调仓日期'] = negative_result['调仓日期'].fillna(date_str)
        negative_result['Run_ID'] = negative_result['Run_ID'].fillna(run_id)
        # 对于持仓数量非 NaN 的记录，清空 ErrorReason
        negative_result.loc[negative_result['持仓数量'].notna(), 'ErrorReason'] = ''
        logger.info("负持仓数据已完成合并并处理空调仓日期、Run_ID 和 ErrorReason")
    else:
        negative_result = pd.DataFrame(
            columns=['调仓日期', '交易对', '持仓数量', '入场价格', 'id', 'ticker', 'fundingRate', 'ErrorReason', 'Run_ID'])

    non_empty_results = [df for df in [positive_result, negative_result] if not df.empty and not df.isna().all().all()]
    if non_empty_results:
        final_result = pd.concat(non_empty_results, ignore_index=True)
        logger.info("正负持仓结果已合并")
    else:
        final_result = pd.DataFrame(
            columns=['调仓日期', '交易对', '持仓数量', '入场价格', 'id', 'ticker', 'fundingRate', 'ErrorReason', 'Run_ID'])
        logger.info("无有效持仓数据，生成空结果")

    output_file = 'data/position_analysis.xlsx'
    if os.path.exists(output_file):
        existing_df = pd.read_excel(output_file)
        # 添加缺失的 ErrorReason 列
        if 'ErrorReason' not in existing_df.columns:
            existing_df['ErrorReason'] = ''
        concat_list = [df for df in [existing_df, final_result] if not df.empty and not df.isna().all().all()]
        if concat_list:
            updated_df = pd.concat(concat_list, ignore_index=True)
            updated_df = updated_df.drop_duplicates(subset=['Run_ID', '交易对', 'id'], keep='last')
            updated_df.to_excel(output_file, index=False)
            logger.info(f"分析结果已去重并追加写入文件: {output_file}，记录数: {len(updated_df)}")
        else:
            logger.warning("无有效数据可拼接，跳过写入")
            return
    else:
        final_result.to_excel(output_file, index=False)
        logger.info(f"分析结果已首次写入文件: {output_file}")

def send_email_with_attachments(logger):
    sender_email = "yanzhiyuan3@gmail.com"
    receiver_email = "yanzhiyuan3@gmail.com"
    cc_emails = ["1010555283@qq.com"]
    subject = "test_sam_account_metrics"

    data_dir = "data"
    account_metrics_path = os.path.join(data_dir, "account_metrics.xlsx")
    position_analysis_path = os.path.join(data_dir, "position_analysis.xlsx")
    today = datetime.now().strftime("%Y-%m-%d")
    account_metrics_data_visualization_pdf = os.path.join(data_dir, f"{today}_account_metrics_data_visualization.pdf")

    if not os.path.exists(account_metrics_path):
        logger.info("account_metrics.xlsx 文件不存在，请检查 data 目录下的文件")
        return
    try:
        df = pd.read_excel(account_metrics_path)
        current_date = datetime.now().strftime("%Y-%m-%d")
        if 'Date' not in df.columns or current_date not in df['Date'].str.split('_').str[0].values:
            logger.info(f"今天是 {current_date}，account_metrics.xlsx 中未找到匹配的日期，不发送邮件")
            return
    except Exception as e:
        logger.info(f"读取 account_metrics.xlsx 时出错: {str(e)}")
        return

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Cc'] = ",".join(cc_emails)
    msg['Subject'] = subject

    body = """Hi,

    Please see the attachment.

    Regards"""
    msg.attach(MIMEText(body, 'plain'))

    attachment_files = [account_metrics_path]
    if os.path.exists(position_analysis_path):
        attachment_files.append(position_analysis_path)
        logger.info(f"添加附件: {position_analysis_path}")
    else:
        logger.info(f"position_analysis.xlsx 文件不存在，将跳过此附件")

    if os.path.exists(account_metrics_data_visualization_pdf):
        attachment_files.append(account_metrics_data_visualization_pdf)
        logger.info(f"添加可视化 PDF 文件: {account_metrics_data_visualization_pdf}")
    else:
        logger.info(f"未找到可视化 PDF 文件: {account_metrics_data_visualization_pdf}")

    for file_path in attachment_files:
        with open(file_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={os.path.basename(file_path)}",
            )
            msg.attach(part)

    smtp_server = "smtp.gmail.com"
    smtp_port = 465
    smtp_user = sender_email
    smtp_password = "pyrf nykw jeai kgdf"
    max_retries = 5
    retry_delay = 10

    for attempt in range(max_retries):
        try:
            logger.info(f"尝试连接到 {smtp_server}:{smtp_port} (第 {attempt + 1}/{max_retries})")
            with smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=60) as server:
                logger.info("连接成功，开始登录")
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
                logger.info("邮件发送成功！")
                return
        except Exception as e:
            logger.error(f"发送邮件失败 (第 {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
            else:
                logger.error("达到最大重试次数，邮件发送失败")

def main():
    config_loader = ConfigLoader()
    api_config = config_loader.get_api_config()
    trading_config = config_loader.get_trading_config()
    paths_config = config_loader.get_paths_config()
    config = {**api_config, **trading_config, **paths_config}

    logger = setup_logger(config["log_path"])
    logger.info("交易程序启动，进入日内多次换仓模式")

    analysis_file = 'data/position_analysis.xlsx'
    if not os.path.exists(analysis_file):
        os.makedirs(os.path.dirname(analysis_file), exist_ok=True)
        df = pd.DataFrame(columns=['调仓日期', '交易对', '持仓数量', '入场价格', 'id', 'ticker', 'fundingRate', 'Run_ID'])
        df.to_excel(analysis_file, index=False)
        logger.info(f"初始化空分析文件: {analysis_file}")

    client = BinanceFuturesClient(api_config["api_key"], api_config["api_secret"],
                                 api_config["test_net"] == "True", logger)
    engine = TradingEngine(client, config, logger)

    processed_files = {}

    while True:
        try:
            current_date = datetime.now().strftime("%Y%m%d")
            run_id = datetime.now().strftime("%Y-%m-%d_%H%M%S")
            logger.info(f"检查 {current_date} 的文件或邮件，Run_ID: {run_id}")

            csv_path_template = config["csv_path"]
            if "{date}" in csv_path_template:
                csv_file = csv_path_template.format(date=current_date)
                date_str = current_date
            else:
                csv_file = csv_path_template
                date_str = csv_file.split("pos")[1].split("_")[0]

            logger.info(f"检查本地文件: {csv_file}")

            file_hash = None
            file_exists = False
            if os.path.exists(csv_file):
                with open(csv_file, 'rb') as f:
                    file_hash = hashlib.md5(f.read()).hexdigest()
                if file_hash not in processed_files:
                    logger.info(f"本地找到文件 {csv_file}，哈希: {file_hash}，未处理，直接使用")
                    file_exists = True
                else:
                    logger.info(f"文件 {csv_file} 已处理（哈希: {file_hash}），跳过")
            else:
                logger.info(f"本地未找到文件 {csv_file}，开始扫描邮箱")
                file_exists, email_run_id = receive_and_download_attachments(logger)
                if file_exists:
                    with open(csv_file, 'rb') as f:
                        file_hash = hashlib.md5(f.read()).hexdigest()
                    run_id = email_run_id or run_id
                    logger.info(f"从邮箱下载文件 {csv_file}，哈希: {file_hash}")

            if file_exists:
                logger.info(f"处理 {date_str} 的pos文件，Run_ID: {run_id}")
                error_reasons = engine.run(date_str, run_id)  # 捕获 error_reasons
                analyze_positions(logger, run_id, error_reasons)  # 传递 error_reasons

                account_metrics_file_path = "data/account_metrics.xlsx"
                if os.path.exists(account_metrics_file_path):
                    account_metrics_df = pd.read_excel(account_metrics_file_path)
                    account_metrics_df['Date'] = pd.to_datetime(account_metrics_df['Date'].str.split('_').str[0])
                    unique_dates = account_metrics_df['Date'].dt.date.nunique()
                    if unique_dates >= 3:
                        logger.info(f"唯一日期数量为 {unique_dates}，执行可视化...")
                        AccountMetricsVisualizer.visualize()
                    else:
                        logger.info(f"唯一日期数量为 {unique_dates}，少于3天，不执行可视化")
                else:
                    logger.info("account_metrics.xlsx 文件不存在，无法生成图表")

                send_email_with_attachments(logger)
                logger.info(f"{date_str} 交易完成，Run_ID: {run_id}")

                if file_hash:
                    processed_files[file_hash] = run_id
                    logger.info(f"记录已处理文件 {csv_file}，哈希: {file_hash}，Run_ID: {run_id}")

                logger.info("交易完成后等待 10 分钟")
                time.sleep(600)
            else:
                logger.info(f"未收到 {date_str} 的pos文件，等待 10 分钟后重试")
                time.sleep(600)

        except Exception as e:
            logger.critical(f"交易程序发生异常: {str(e)}")
            time.sleep(60)

if __name__ == "__main__":
    main()