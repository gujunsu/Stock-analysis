import os
import sqlite3
import json
import requests
import time
from datetime import datetime


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
    saveAsJSON(result)



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


# 保存数据到文件
def save_data_to_file(data, file_name, directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

    file_path = os.path.join(directory, f"{file_name}.json")

    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    # print(f"数据已保存到 {file_path}")
    return file_path


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
    cursor.execute("SELECT code,name,market FROM stocks")
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
