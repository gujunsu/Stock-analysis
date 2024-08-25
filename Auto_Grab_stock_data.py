import os
import sqlite3
import json
import requests
import csv
import time
import numpy as np
import pandas as pd
import talib
from datetime import datetime
from prettytable import PrettyTable


# ==============================
# 收盘后获取A股所有股票的收盘数据
def getData(baseUrl, headers):
    response = requests.get(url=baseUrl, headers=headers)
    data = json.loads(response.text)['data']['diff']
    result = []
    for key, value in data.items():
        value['f2'] = '%.2f' % (value['f2']/100)
        value['f3'] = '%.2f' % (value['f3']/100) + '%'
        value['f4'] = '%.2f' % (value['f4']/100)
        value['f5'] = '%.2f' % (value['f5']/10000)
        #value['f5'] = '%.2f' % (value['f5']/10000) + '万'
        value['f6'] = '%.2f' % (value['f6']/100000000)
        #value['f6'] = '%.2f' % (value['f6']/100000000) + '亿'
        value['f7'] = '%.2f' % (value['f7']/100) + '%'
        value['f15'] = '%.2f' % (value['f15']/100)
        value['f16'] = '%.2f' % (value['f16']/100)
        value['f17'] = '%.2f' % (value['f17']/100)
        value['f18'] = '%.2f' % (value['f18']/100)
        value['f10'] = '%.2f' % (value['f10']/100)
        value['f8'] = '%.2f' % (value['f8']/100) + '%'
        value['f9'] = '%.2f' % (value['f9']/100)
        value['f23'] = '%.2f' % (value['f23']/100)
        result.append([key,value['f12'],value['f14'],value['f2'],value['f3'],value['f4'],value['f5'],value['f6'],value['f7'],value['f15'],value['f16'],value['f17'],value['f18'],value['f10'],value['f8'],value['f9'],value['f23']])
    return result

def printData(result):
    table = PrettyTable()
    table.field_names = ["序号", "代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交量(万手)", "成交额(亿)", "振幅", "最高", "最低", "今开", "昨收", "量比", "换手率", "市盈率（动态）", "市净率"]
    table.add_rows(result)
    print(table)

def saveData(result):
    # 获取当前日期并将其格式化为指定的日期字符串
    today_date = datetime.today().strftime('%Y%m%d')
    dir_path = './csv'
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    # 构造带有日期的文件名
    file_name = f'{dir_path}/A股股票数据_{today_date}.csv'
    # 使用UTF-8编码打开文件，确保支持中文等特殊字符
    with open(file_name, 'w', encoding='utf-8', newline='') as file:
        # 创建 CSV writer，设置 quoting 参数为 QUOTE_NONNUMERIC
        writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC)
        # 写入表头
        writer.writerow(["序号", "代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交量(万手)", "成交额(亿)", "振幅", "最高", "最低", "今开", "昨收", "量比", "换手率", "市盈率（动态）", "市净率"])
        # 写入数据
        writer.writerows(result)

def saveAsSQLite(result):
    # 获取当前日期并将其格式化为指定的日期字符串
    today_date = datetime.today().strftime('%Y%m%d')
    dir_path = './db'
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    # 构造带有日期的文件名
    db_name = f'{dir_path}/A股股票数据_{today_date}.db'
    if os.path.exists(db_name):
        # 删除文件
        os.remove(db_name)
    # 连接到 SQLite 数据库
    conn = sqlite3.connect(db_name)
    # 创建游标对象
    cursor = conn.cursor()
    # 创建表格（如果不存在）
    table_name = f'stocks'
    #table_name = f'stocks_{today_date}'
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER PRIMARY KEY,
                        code TEXT,
                        name TEXT,
                        price REAL,
                        change_rate TEXT,
                        change_amount REAL,
                        volume TEXT,
                        amount TEXT,
                        amplitude TEXT,
                        high REAL,
                        low REAL,
                        open REAL,
                        close REAL,
                        volume_ratio REAL,
                        turnover_rate TEXT,
                        pe_ratio REAL,
                        pb_ratio REAL
                    )''')
    # 插入数据
    cursor.executemany(f'''INSERT INTO {table_name} (code, name, price, change_rate, change_amount, volume, amount, amplitude, high, low, open, close, volume_ratio, turnover_rate, pe_ratio, pb_ratio)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', [row[1:] for row in result])
    # 提交事务
    conn.commit()
    # 关闭连接
    conn.close()

def saveAsJSON(result):
    # 获取当前日期并将其格式化为指定的日期字符串
    today_date = datetime.today().strftime('%Y%m%d')
    dir_path = './json'
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    # 构造带有日期的文件名
    file_name = f'{dir_path}/A股股票数据_{today_date}.json'
    # 写入 JSON 文件
    with open(file_name, 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent=4)
# ==============================

def a_all_date():
    baseUrl = 'https://22.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=6000&po=1&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    result = getData(baseUrl, headers)
    printData(result)
    saveData(result)
    saveAsJSON(result)
    saveAsSQLite(result)


# 获取股票数据
def http_get_stock_data(secid):
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "cb": "jQuery351043472769495360547_1716288724686",
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "end": "20500101",
        "lmt": "365",
        "_": "1716288724710"
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        json_data = response.text
        start_idx = json_data.index('(') + 1
        end_idx = json_data.rindex(')')
        json_data = json_data[start_idx:end_idx]
        stock_data = json.loads(json_data)

        if len(response.content) < 256:
            return None
        else:
            return stock_data
    else:
        return None


def print_stock_compre_score_info(data):
    if data["success"]:
        stock_info = data["result"]["data"][0]
        print(f"证券代码: {stock_info['SECURITY_CODE']}")
        print(f"证券简称: {stock_info['SECURITY_NAME_ABBR']}")
        print(f"综合评分: {stock_info['COMPRE_SCORE']}")
        print(f"市场排名: {stock_info['MARKET_RANK']}")
        print(f"市场评价数: {stock_info['EVALUATE_MARKET_NUM']}")
        print(f"市场最高评分: {stock_info['MARKET_SCORE_HIGH']}")
        print(f"市场最低评分: {stock_info['MARKET_SCORE_LOW']}")
        print(f"市场平均评分: {stock_info['MARKET_SCORE_AVG']}")
        print(f"股票排名比率: {stock_info['STOCK_RANK_RATIO']}")
        print(f"行业评价数: {stock_info['EVALUATE_INDUSTRY_NUM']}")
        print(f"行业股票数: {stock_info['INDUSTRY_STOCK_NUM']}")
        print(f"行业排名: {stock_info['INDUSTRY_RANK']}")
        print(f"行业最高评分: {stock_info['INDUSTRY_SCORE_HIGH']}")
        print(f"行业最低评分: {stock_info['INDUSTRY_SCORE_LOW']}")
        print(f"行业平均评分: {stock_info['INDUSTRY_SCORE_AVG']}")
        print(f"市场股票数: {stock_info['MARKET_STOCK_NUM']}")
        print(f"变动率: {stock_info['CHANGE_RATE']}")
        print(f"板块代码: {stock_info['BOARD_CODE']}")
        print(f"板块名称: {stock_info['BOARD_NAME']}")
    else:
        print("获取股票信息失败")


def autoplot_stock_data(file_path, use_sliding_window=True):
        # 6.13.5     /   12.23.10        /   20.46.15
        # 第一组MACD技术指标参数
        macd_1 = (6, 13, 5)
        macd_1_short, macd_1_long, macd_1_signal = macd_1
        # 第二组MACD技术指标参数
        macd_2 = (12, 23, 10)
        macd_2_short, macd_2_long, macd_2_signal = macd_2
        # 第三组MACD技术指标参数
        macd_3 = (15, 28, 13)
        macd_3_short, macd_3_long, macd_3_signal = macd_3

        #use_sliding_window = True      #表示全数据分析模式，分析所有历史数据
        use_sliding_window = False      #表示最新一天数据模式，分析最新一天的数据


        # print(f"file_path:{file_path}")
        stock_name, stock_code = get_stock_info(file_path)
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # 是否启用全部分析模式，还是每日数据分析模式做判断
        if not use_sliding_window:
            # 如果 use_sliding_window 是假（False），直接输出最后60行数据
            klines_all = data['data']['klines']
            klines_all = klines_all[-60:]
            #print(klines_all)
            #print("如果 use_sliding_window 是假（False），直接输出最后60行数据")

        else:
            # 如果 use_sliding_window 是真（True），使用滑动窗口模式输出数据
            klines_all = data['data']['klines']
            #print(klines_all)
            #print("如果 use_sliding_window 是真（True），全部数据逐一分析模式")

        # 从窗口界面上获取显示周期参数
        #period = int(self.period_var.get())
        period = 60     # 固定周期参数
        # 获取 klines 的总行数
        total_rows = len(klines_all)

        # 检查是否有足够的数据进行滑动窗口
        if total_rows >= period:
            # 从第 1 行开始到倒数第 60 行进行滑动窗口
            for i in range(total_rows - period + 1):
                klines = klines_all[i:i + period]
                #print(klines)

                # 获取最后一行的数据
                last_row = klines[-1]
                # 提取最后一行的日期字段
                last_date = last_row.split(',')[0]
                # 转换日期格式为 YYYYMMDD
                Stock_date_time = last_date.replace('-', '')
                # 输出最后一行的日期字段
                # print(f"最后一行的日期: {Stock_date_time}")

                dates = [entry.split(',')[0] for entry in klines]
                closing_prices = [float(entry.split(',')[2]) for entry in klines]
                volumes = [float(entry.split(',')[5]) for entry in klines]

                # 记录最近3天是否绘制了买入和卖出信号的标志
                buy_signal_days = []
                sell_signal_days = []
                Macd_trading_index_number = 0

                # MACD 技术指标方法一
                macd_line, signal_line, macd_histogram = calculate_macd(closing_prices, macd_1_short, macd_1_long, macd_1_signal)
                Macd_trading_number = 0
                # 标注MACD的买卖点
                for i in range(1, len(macd_histogram)):
                    if macd_histogram[i] > 0 and macd_histogram[i - 1] < 0:
                        Macd_trading_number = 30  # MACD买卖指标数值
                        if len(macd_histogram) - i < 3:  # 最近3天内又买入信号
                            buy_signal_days.append(
                                (dates[i], closing_prices[i], macd_histogram[i], macd_histogram[i - 1]))
                    elif macd_histogram[i] < 0 and macd_histogram[i - 1] > 0:
                        Macd_trading_number = 0  # MACD买卖指标数值
                        if len(macd_histogram) - i < 3:  # 最近3天内又卖出信号
                            sell_signal_days.append(
                                (dates[i], closing_prices[i], macd_histogram[i], macd_histogram[i - 1]))
                Macd_trading_index_number += Macd_trading_number  # macd 买卖指标数值

                # MACD 技术指标方法二

                macd_line, signal_line, macd_histogram = calculate_macd(closing_prices, macd_2_short, macd_2_long, macd_2_signal)
                Macd_trading_number = 0
                # 标注MACD的买卖点
                for i in range(1, len(macd_histogram)):
                    if macd_histogram[i] > 0 and macd_histogram[i - 1] < 0:
                        Macd_trading_number = 30  # MACD买卖指标数值
                        if len(macd_histogram) - i < 3:  # 最近3天内又买入信号
                            buy_signal_days.append(
                                (dates[i], closing_prices[i], macd_histogram[i], macd_histogram[i - 1]))
                    elif macd_histogram[i] < 0 and macd_histogram[i - 1] > 0:
                        Macd_trading_number = 0  # MACD买卖指标数值
                        if len(macd_histogram) - i < 3:  # 最近3天内又卖出信号
                            sell_signal_days.append(
                                (dates[i], closing_prices[i], macd_histogram[i], macd_histogram[i - 1]))
                Macd_trading_index_number += Macd_trading_number  # macd 买卖指标数值

                # MACD 技术指标方法三
                macd_line, signal_line, macd_histogram = calculate_macd(closing_prices, macd_3_short, macd_3_long, macd_3_signal)
                Macd_trading_number = 0
                # 标注MACD的买卖点
                for i in range(1, len(macd_histogram)):
                    if macd_histogram[i] > 0 and macd_histogram[i - 1] < 0:
                        Macd_trading_number = 30  # MACD买卖指标数值
                        if len(macd_histogram) - i < 3:  # 最近3天内又买入信号
                            buy_signal_days.append(
                                (dates[i], closing_prices[i], macd_histogram[i], macd_histogram[i - 1]))
                    elif macd_histogram[i] < 0 and macd_histogram[i - 1] > 0:
                        Macd_trading_number = 0  # MACD买卖指标数值
                        if len(macd_histogram) - i < 3:  # 最近3天内又卖出信号
                            sell_signal_days.append(
                                (dates[i], closing_prices[i], macd_histogram[i], macd_histogram[i - 1]))

                # 2024.07.07 增加对股票做DPO技术指标分析,检测最后是买入还是卖出策略
                df, stock_code, stock_name, dpo_last_signal, dpo_value = calculate_dpo_from_json(file_path, trend_days=[20])
                # 向数据库内添加DPO技术指标决策
                add_data_to_db(stock_code, stock_name, 'DPO', Stock_date_time, dpo_last_signal)


                # -----------------------------------------------------
                Macd_trading_index_number += Macd_trading_number  # macd 买卖指标数值
                if Macd_trading_index_number > 80:
                    # 获取最近5日的涨跌情况
                    # up_ratio, down_ratio, total_change, print_info = self.analyze_stock_klines(klines_all)
                    up_ratio, down_ratio, total_change, latest_sar_value, latest_sar_color, sar_latest_two_days_sar_color, latest_close_price, print_info = analyze_stock_klines(
                        klines_all)

                    # 向数据库内添加SAR技术指标决策
                    add_data_to_db(stock_code, stock_name, 'SAR', Stock_date_time, latest_sar_color)

                    # print(f"最新一天的SAR指标值:{latest_sar_value}")
                    # print(f"最新一天的SAR指标颜色:{latest_sar_color}")
                    # print(f"最新两天的SAR指标是否都为红色:{latest_two_days_sar_color}")
                    # print(f"涨比例:{up_ratio}")
                    # print(f"跌比例:{down_ratio}")
                    # print(f"综合涨跌幅度:{total_change}")
                    # print(f"最新收盘价:{latest_close_price}")

                    # print(f'自选股分析 {stock_name} ({stock_code})')
                    print(f'{stock_name}（股票代码：{stock_code}） 收盘价:{latest_close_price}   MACD买入指标:{Macd_trading_index_number}%   SAR趋势:{latest_sar_color}   DPO趋势:{dpo_last_signal}   涨跌比例:{up_ratio}/{down_ratio}   5日股价综合涨跌幅度:{total_change}')
                    # 写出买卖日志记录到文档
                    # content = f'买入信号 {Macd_trading_index_number}%：{stock_name} [{stock_code}]\n----------------------------------------------------------------------------------'
                    # write_to_text(content, "买入信号", Stock_date_time)

                    # if up_ratio > down_ratio and latest_two_days_sar_color == True and last_signal == '红':, dpo_last_signal<0 表示为负数
                    if sar_latest_two_days_sar_color == True and dpo_last_signal == '红':
                        content2 = f'{stock_name} [{stock_code}]  收盘价:{latest_close_price}   SAR趋势:{latest_sar_color}   DPO趋势:{dpo_last_signal}  {dpo_value}   涨跌比例:{up_ratio}/{down_ratio}   5日股价综合涨跌幅度:{total_change} \n----------------------------------------------------------------------------------'
                        write_to_text(content2, "全部趋势上涨", Stock_date_time)
                    else:
                        print(
                            f'{stock_name}（股票代码：{stock_code}）趋势不够明确')
                        # print(f"最新一天的SAR指标值:{latest_sar_value}")
                        print(f"最新两天的SAR指标是否都为红色:{sar_latest_two_days_sar_color}")
                        print(f"SAR指标 最后的策略:{latest_sar_color}")
                        print(f"DPO指标 最后的策略:{dpo_last_signal}     DPO指标的值:{dpo_value}")


                    # 2024.06.24 增加将涨跌信息写入到数据库中
                    # Stock_date_time = datetime.now().strftime("%Y%m%d")
                    create_or_update_stock_info('Stock_rise_and_fall_forecast.db', stock_code, stock_name,
                                                     Stock_date_time, '涨')
                    print(f"-------------------------------------------------------------------")

                elif Macd_trading_index_number == 0:
                    # 获取最近5日的涨跌情况
                    up_ratio, down_ratio, total_change, latest_sar_value, latest_sar_color, latest_two_days_sar_color, latest_close_price, print_info = analyze_stock_klines(
                        klines_all)

                    # 向数据库内添加SAR技术指标决策
                    add_data_to_db(stock_code, stock_name, 'SAR', Stock_date_time, latest_sar_color)

                    # print(f'{stock_name}（股票代码：{stock_code}）macd 卖出 指标数值:{Macd_trading_index_number}%   涨比例:{up_ratio}  跌比例:{down_ratio}  5日股价综合涨跌幅度:{total_change}')
                    # 写出买卖日志记录到文档
                    # content = f'卖出信号 {Macd_trading_index_number}%：{stock_name} [{stock_code}]\n-------------------------------------------------------------------'
                    # self.write_to_text(content, "卖出信号", Stock_date_time)


                    content2 = f'{stock_name} [{stock_code}]  收盘价:{latest_close_price}   SAR趋势:{latest_sar_color}  涨比例:{up_ratio}  跌比例:{down_ratio}  5日股价综合涨跌幅度:{total_change} \n{print_info}\n-------------------------------------------------------------------'

                    write_to_text(content2, "全部趋势下跌", Stock_date_time)

                    # 2024.06.24 增加将涨跌信息写入到数据库中
                    # Stock_date_time = datetime.now().strftime("%Y%m%d")
                    create_or_update_stock_info('Stock_rise_and_fall_forecast.db', stock_code, stock_name,
                                                     Stock_date_time, '跌')
                    # print("-" * 40)


        else:
            print(f'{stock_name}（股票代码：{stock_code}）数据分析完毕！')



# 保存数据到文件
def save_data_to_file(secid, data, exchange, directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

    file_path = os.path.join(directory, f"{exchange}{secid}.json")

    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    # print(f"数据已保存到 {file_path}")
    return file_path


def get_stock_info(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    stock_name = data['data']['name']
    stock_code = data['data']['code']
    return stock_name, stock_code


    # 特别注意：此处的参数是设置MACD指标参数10，23，8
def calculate_macd(closing_prices, short_window=10, long_window=23, signal_window=8):
    # 计算短周期的指数移动平均（EMA），周期为short_window
    short_ema = pd.Series(closing_prices).ewm(span=short_window, adjust=True).mean()
    # 计算长周期的指数移动平均（EMA），周期为long_window
    long_ema = pd.Series(closing_prices).ewm(span=long_window, adjust=True).mean()
    # 计算MACD线，即短周期EMA减去长周期EMA
    macd_line = short_ema - long_ema
    # 计算信号线，即MACD线的EMA，周期为signal_window
    signal_line = macd_line.ewm(span=signal_window, adjust=True).mean()
    # 计算MACD柱，即MACD线减去信号线
    macd_histogram = macd_line - signal_line
    # 返回MACD线，信号线和MACD柱
    return macd_line, signal_line, macd_histogram


def calculate_rsi(closing_prices, window=14):
        deltas = np.diff(closing_prices)
        gains = deltas[deltas >= 0]
        losses = -deltas[deltas < 0]

        avg_gain = np.mean(gains[:window])
        avg_loss = np.mean(losses[:window])

        rs = avg_gain / avg_loss
        rsi = np.zeros_like(closing_prices)
        rsi[:window] = 100.0 - (100.0 / (1.0 + rs))

        for i in range(window, len(closing_prices)):
            delta = deltas[i - 1]

            if delta > 0:
                avg_gain = (avg_gain * (window - 1) + delta) / window
                avg_loss = (avg_loss * (window - 1)) / window
            else:
                avg_gain = (avg_gain * (window - 1)) / window
                avg_loss = (avg_loss * (window - 1) - delta) / window

            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))

        return rsi

def add_data_to_db(stock_code, stock_name, table_name, column_name, column_value,
                       db_file='Stock_rise_and_fall_forecast.db'):
        # 检查传入的参数是否为空
        if not stock_code or not stock_name or not table_name or not column_name or not column_value:
            print("输入参数不能为空")
            return

        # 使用前缀来保证列名合法
        column_name_with_prefix = f"col_{column_name}"

        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # 检查表是否存在，如果不存在则创建表
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (stock_code TEXT, stock_name TEXT)")

        # 检查表中是否存在stock_code、stock_name列
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]

        if 'stock_code' not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN stock_code TEXT")
        if 'stock_name' not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN stock_name TEXT")
        if column_name_with_prefix not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name_with_prefix} TEXT")

        # 搜索表中是否存在指定的stock_code和stock_name的记录
        cursor.execute(f"SELECT * FROM {table_name} WHERE stock_code = ? AND stock_name = ?", (stock_code, stock_name))
        record = cursor.fetchone()

        if record:
            # 如果存在，更新对应的字段列名称
            cursor.execute(
                f"UPDATE {table_name} SET {column_name_with_prefix} = ? WHERE stock_code = ? AND stock_name = ?",
                (column_value, stock_code, stock_name))
        else:
            # 如果不存在，插入新的记录
            cursor.execute(
                f"INSERT INTO {table_name} (stock_code, stock_name, {column_name_with_prefix}) VALUES (?, ?, ?)",
                (stock_code, stock_name, column_value))

        conn.commit()
        conn.close()

def calculate_dpo_from_json(json_file, dpo_period=20, madpo_period=6, trend_days=[5, 10]):
        """
        从JSON文件中读取股票历史价格数据，并计算DPO和MADPO。

        参数：
        json_file: JSON文件路径
        dpo_period: DPO计算的周期（一般为20）
        madpo_period: MADPO计算的周期（一般为6）
        trend_days: 用于走势判断的天数列表

        返回：
        DataFrame，包含原始数据以及计算出的DPO和MADPO
        """
        # 读取JSON文件
        with open(json_file, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # 提取股票代码和名称
        stock_code = data['data']['code']
        stock_name = data['data']['name']

        # 提取历史价格数据
        klines = data['data']['klines']

        # 创建DataFrame
        df = pd.DataFrame([line.split(',') for line in klines], columns=[
            'Date', 'Open', 'Close', 'High', 'Low', 'Volume', 'Turnover',
            'Amplitude', 'Change', 'ChangePercent', 'TurnoverRate'
        ])

        # 转换数据类型
        df['Date'] = pd.to_datetime(df['Date'])
        df[['Open', 'Close', 'High', 'Low', 'Volume']] = df[['Open', 'Close', 'High', 'Low', 'Volume']].astype(float)

        # 计算DPO
        df['SMA'] = talib.SMA(df['Close'], timeperiod=dpo_period)
        df['DPO'] = df['Close'] - df['SMA'].shift(int(dpo_period / 2 + 1))

        # 计算MADPO
        df['MADPO'] = talib.SMA(df['DPO'], timeperiod=madpo_period)

        # 获取最近指定天数的DPO和MADPO数据
        result = {}
        for days in trend_days:
            df_last_days = df.tail(days)

            # 分析DPO数据的后市走势
            dpo_values = df_last_days['DPO'].tolist()
            madpo_values = df_last_days['MADPO'].tolist()

            if all(value > 0 for value in dpo_values):
                trend = "上升趋势"
            elif all(value < 0 for value in dpo_values):
                trend = "下降趋势"
            else:
                trend = "震荡市场"

            result[f'{days}天趋势'] = trend
            result[f'{days}天数据'] = df_last_days[['Date', 'DPO', 'MADPO']]

            # 判断买点和卖点
            signals = []

            for i in range(1, len(df_last_days)):
                if df_last_days.iloc[i]['DPO'] > df_last_days.iloc[i]['MADPO'] and \
                        df_last_days.iloc[i - 1]['DPO'] <= df_last_days.iloc[i - 1]['MADPO']:
                    signals.append((df_last_days.iloc[i]['Date'], '红'))
                elif df_last_days.iloc[i]['DPO'] < df_last_days.iloc[i]['MADPO'] and \
                        df_last_days.iloc[i - 1]['DPO'] >= df_last_days.iloc[i - 1]['MADPO']:
                    signals.append((df_last_days.iloc[i]['Date'], '绿'))

            # 按日期排序买卖点信号
            signals.sort()

            # 将买卖点信号加入结果
            signals_output = "\n".join([f"{date.strftime('%Y-%m-%d')} {signal}" for date, signal in signals])
            result[f'{days}天买卖点'] = signals_output

            # 判断强弱势
            if df_last_days.iloc[-1]['DPO'] > df_last_days.iloc[-1]['MADPO']:
                strength = "强势"
            else:
                strength = "弱势"

            result[f'{days}天强弱势'] = strength


        last_signal = None
        for days in trend_days:
            if f'{days}天买卖点' in result:
                signals = result[f'{days}天买卖点'].split("\n")
                if signals and signals[-1].split():  # 确保split()的结果非空
                    parts = signals[-1].split()
                    if len(parts) > 1:  # 确保至少有两个元素
                        last_signal = parts[1]
                        result['最后的买入或卖出决策'] = last_signal
                        break  # 如果你只想处理第一个匹配的days
                    else:
                        # 处理parts长度不足的情况（可选）
                        last_signal = None
                        # print(f"警告：最后一个信号的格式不正确，无法获取第二部分。")
                else:
                    # 处理signals为空的情况（可选）
                    last_signal = None
                    # print(f"警告：没有找到任何信号。")

        # # 输出趋势判断、买卖点信号和强弱势判断
        # for key, value in result.items():
        #     if '趋势' in key:
        #         print(f"根据最近{key}的DPO和MADPO数据分析，{stock_name} ({stock_code}) 处于{value}。")
        #     elif '买卖点' in key:
        #         if value:
        #             print(f"最近{key}：\n{value}")
        #     elif '强弱势' in key:
        #         print(f"最近{key}：{value}")
        #     elif '最后的买入或卖出决策' in key:
        #         print(f"{stock_name} ({stock_code}) 最后的买入或卖出决策：{value}")
        #     else:
        #         print(f"最近{key}的 {stock_name} ({stock_code}) DPO 和 MADPO 指标值：")
        #         print(value)
        #         print()

        dpo_value = df_last_days.iloc[-1]['DPO']
        # 返回结果
        return df, stock_code, stock_name, last_signal, dpo_value

def create_or_update_stock_info(db_file, stock_code, stock_name, date_str, prediction):
        # 连接到SQLite数据库(如果它不存在，将被创建)
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # 确保该表至少包含stock_code和stock_name列
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_info (
            stock_code TEXT PRIMARY KEY,
            stock_name TEXT
        )
        ''')

        # Check if the date column exists, and if not, add it
        try:
            cursor.execute(f"ALTER TABLE stock_info ADD COLUMN '{date_str}' TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Check if the record with the given stock_code exists
        cursor.execute('SELECT stock_code FROM stock_info WHERE stock_code = ?', (stock_code,))
        record_exists = cursor.fetchone()

        if record_exists:
            # If the record exists, update the prediction for the given date
            cursor.execute(f'''
            UPDATE stock_info
            SET '{date_str}' = ?
            WHERE stock_code = ?
            ''', (prediction, stock_code))
        else:
            # If the record does not exist, insert a new record
            cursor.execute(f'''
            INSERT INTO stock_info (stock_code, stock_name, '{date_str}')
            VALUES (?, ?, ?)
            ''', (stock_code, stock_name, prediction))

        # Commit the transaction and close the connection
        conn.commit()
        conn.close()

def write_to_text(content, prefix, current_date):
        # 获取当前日期
        #current_date = datetime.now().strftime("%Y-%m-%d")
        # 构造文件名:前缀+日期
        file_name = f"{prefix}{current_date}.txt"
        # 定义子目录名称
        subdirectory = "买卖信号"
        # 获取当前工作目录
        current_dir = os.getcwd()
        # 构造子目录的完整路径
        subdirectory_path = os.path.join(current_dir, subdirectory)
        # 如果子目录不存在，则创建它
        if not os.path.exists(subdirectory_path):
            os.makedirs(subdirectory_path)

        # 构造文件路径，包括子目录
        file_path = os.path.join(subdirectory_path, file_name)

        # 检查文件是否存在以及内容是否已经存在
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='gbk') as file:
                existing_contents = file.read()
            if content in existing_contents:
                # print("内容已经存在，跳过写入。")
                return

        # 写入文本文件，使用"a"模式追加内容,"w"模式写入模式
        with open(file_path, "a", encoding='gbk') as file:
            file.write(content + '\n')
            # print("内容已写入。")

def analyze_stock_klines(klines, recent_days=5):
        # 解析数据
        dates = []
        opens = []
        closes = []
        highs = []
        lows = []
        changes = []
        today_changes = []

        for kline in klines:
            parts = kline.split(',')
            dates.append(parts[0])
            opens.append(float(parts[1]))  # 开盘价信息
            closes.append(float(parts[2]))  # 收盘价信息
            highs.append(float(parts[3]))  # 最高价信息
            lows.append(float(parts[4]))  # 最低价信息

            # 计算单日涨跌幅度
            change = round((closes[-1] - opens[-1]) / opens[-1] * 100, 2)
            changes.append(change)

            # 今日涨跌
            today_change = '涨' if change > 0 else '跌'
            today_changes.append(today_change)

        # 创建DataFrame
        df = pd.DataFrame({
            '日期': pd.to_datetime(dates),  # 将日期文本转换为日期时间格式
            '开盘价': opens,
            '收盘价': closes,
            '最高价': highs,
            '最低价': lows,
            '涨跌幅(%)': changes,
            '今日涨跌': today_changes
        })

        # 将日期设置为索引
        df.set_index('日期', inplace=True)

        # 计算SAR指标
        df['SAR'] = talib.SAR(df['最高价'], df['最低价'], acceleration=0.02, maximum=0.2)

        # 判断SAR指标颜色
        df['SAR颜色'] = ['红' if close > sar else '绿' for close, sar in zip(df['收盘价'], df['SAR'])]

        # 判断最新一天的SAR指标颜色
        latest_sar_value = df['SAR'].iloc[-1]
        latest_close_price = df['收盘价'].iloc[-1]
        latest_sar_color = '红' if latest_close_price > latest_sar_value else '绿'

        # 判断最新的两天日期SAR指标是否都为红色
        latest_two_days_sar_color = all(df['收盘价'].iloc[-2:] > df['SAR'].iloc[-2:])

        # 计算最近5日的涨跌情况及比例
        recent_data = df.tail(recent_days)
        total_up_days = recent_data[recent_data['涨跌幅(%)'] > 0].shape[0]
        total_down_days = recent_data[recent_data['涨跌幅(%)'] < 0].shape[0]
        up_ratio = round(total_up_days / recent_days * 100, 2)
        down_ratio = round(total_down_days / recent_days * 100, 2)

        # 计算最近5天的综合涨跌幅度百分比
        total_change = round(recent_data['涨跌幅(%)'].sum(), 2)

        # 准备打印信息
        print_info = []
        print_info.append("最近5日的开盘价、收盘价、单日涨跌幅度和今日涨跌情况：")
        for index, row in recent_data.iterrows():
            print_info.append(
                f"{index.date()}  {row['开盘价']}  {row['收盘价']}    {row['涨跌幅(%)']}    {row['今日涨跌']}    {row['SAR']:.4f}    {row['SAR颜色']}")

        print_info.append(f"\n最近5日内总计涨天数: {total_up_days} 天，跌天数: {total_down_days} 天")
        print_info.append(f"最近5日内涨跌比例: 涨 {up_ratio:.2f}% ，跌 {down_ratio:.2f}%")
        print_info.append(f"最近5日内综合涨跌幅度: {total_change:.2f}%")
        print_info.append(f"最新一天的SAR指标值: {latest_sar_value:.2f}，颜色: {latest_sar_color}")
        print_info.append(f"最新两天的SAR指标是否都为红色: {'是' if latest_two_days_sar_color else '否'}")

        # 返回结果
        return up_ratio, down_ratio, total_change, latest_sar_value, latest_sar_color, latest_two_days_sar_color, latest_close_price, "\n".join(
            print_info)


def extract_last_date(data):
    # 确保输入是字典对象，如果是字符串则解析
    if isinstance(data, str):
        import json
        data = json.loads(data)
    # 提取 klines 列表
    klines = data['data']['klines']
    # 获取最后一行数据
    last_line = klines[-1]
    # 提取日期部分
    last_date = last_line.split(',')[0]
    return last_date



if __name__ == "__main__":

    print("脚本最后更新日期:2024-08-04\n5秒后启动执行程序")
    time.sleep(5)  # 等待15秒

    # 抓取A股所有股票的收盘数据
    a_all_date()
    # ======================================

    # 连接数据库
    conn = sqlite3.connect(f"./all_stocker_data.db")
    cursor = conn.cursor()
    # 查询股票代码
    cursor.execute("SELECT code,name,industry FROM stocks")
    stocks = cursor.fetchall()
    total_stocks = len(stocks)

    # 获取最后交易的日期
    Stock_date_json = http_get_stock_data(f"0.000001")
    json_last_date = extract_last_date(Stock_date_json)
    # 转换日期格式为 YYYYMMDD
    Stock_date_time = json_last_date.replace('-', '')
    directory = f"historical_data/{Stock_date_time}"
    print(directory)

    # 逐条获取股票数据
    for idx, stock in enumerate(stocks, start=1):
        stock_code = stock[0]  # 600000
        stock_name = stock[1]  # 浦发银行
        market = stock[2]  # 0 或 1
        # 构建出文件的前缀信息
        if market == "0":
            prefix = "SZ"
            prefix_secid = f"{prefix}{stock_code}"
        elif market == "1":
            prefix = "SH"
            prefix_secid = f"{prefix}{stock_code}"
        else:
            prefix = "SZ"
            prefix_secid = f"{prefix}{stock_code}"


        print(f"[{idx}/{total_stocks}] 正在获取股票 {stock_code} [{stock_name}] 的数据...")

        # 检查本地是否已有对应文件
        if os.path.exists(os.path.join(directory, f"{prefix_secid}.json")):
            print(f"股票 {prefix_secid} 的数据文件已存在，跳过...")
            continue

        try:
            # 尝试第一个接口

            if prefix == "SZ":
                print(f"处理深圳股票代码: {prefix_secid}")
                data = http_get_stock_data(f"0.{stock_code}")
                if data:
                    json_file_path = save_data_to_file(data, prefix_secid, directory)
                    # 对下载的股票json进行分析处理
                    # autoplot_stock_data(json_file_path, "全部趋势")
            elif prefix == "SH":
                print(f"处理上海股票代码: {prefix_secid}")
                data = http_get_stock_data(f"1.{stock_code}")
                if data:
                    json_file_path = save_data_to_file(data, prefix_secid, directory)
                    # 对下载的股票json进行分析处理
                    # autoplot_stock_data(json_file_path, "全部趋势")
            else:
                print(f"处理无前缀的股票代码: {prefix_secid}")
                # 尝试第一个接口
                data = http_get_stock_data(f"0.{stock_code}")
                if data:
                    json_file_path = save_data_to_file(data, prefix_secid, directory)
                    # 对下载的股票json进行分析处理
                    # autoplot_stock_data(json_file_path, "全部趋势")
                else:
                    # 尝试第二个接口
                    data = http_get_stock_data(f"1.{stock_code}")
                    if data:
                        json_file_path = save_data_to_file(data, prefix_secid, directory)
                        # 对下载的股票json进行分析处理
                        # autoplot_stock_data(json_file_path, "全部趋势")

        except Exception as e:
            print(f"股票 {prefix_secid} 获取数据时发生错误: {e}")


    # 关闭数据库连接
    conn.close()

    print("="*50)
    print("\n\n所有股票数据已经获取完成。30秒后自动退出该程序...\n\n")
    print("=" * 50)
    # 执行完所有任务后，30秒后自动退出程序
    time.sleep(30)


# 此脚本的功能是，每天抓取一次最近365个交易日的日K线数据,保存为json文件。
# 每天收盘以后运行一次。获取所有股票的收盘价格。
