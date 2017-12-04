# _*_ coding:utf-8 _*_
# @Time    : 2017-12-04 11:15
# @Author  : liupan
# @Email   : liupan8910@163.com

import tushare as ts
import pymysql
from sqlalchemy import create_engine
import time
from retry import retry
import pandas as pd

# connect mysql
engine = create_engine('mysql+pymysql://root:caicai520@127.0.0.1/quantist?charset=utf8')


@retry(tries=5, delay=2)
def save_today():
    ts.get_today_all().to_sql("today_all", engine, if_exists='append')
    print("today_all over")


save_today()


@retry(tries=5, delay=2)
def save_index():
    ts.get_index().to_sql("index_stock", engine, if_exists='append')
    print('index....')


save_index()

from retry import retry


@retry(tries=5, delay=2)
def save_sh():
    basic_list = ["sh", "sz", "hs300", "sz50", "zxb", "cyb"]
    for basic in basic_list:
        ts.get_hist_data(basic).to_sql(basic, engine, if_exists='append')
        print(basic + " over")


save_sh()

from retry import retry


@retry(tries=5, delay=2)
def basic_information():
    ts.get_cashflow_data(2017, 1).to_sql('cash_flow', engine, if_exists='append')
    ts.get_debtpaying_data(2017, 1).to_sql('debtpaying', engine, if_exists='append')
    ts.get_growth_data(2017, 1).to_sql('growth', engine, if_exists='append')
    ts.get_operation_data(2017, 1).to_sql('operation', engine, if_exists='append')
    ts.get_profit_data(2017, 1).to_sql('profit', engine, if_exists='append')
    ts.get_report_data(2017, 2).to_sql('report', engine, if_exists='append')
    print('basic information over ....')


basic_information()


@retry(tries=5, delay=2)
def get_today():
    today_all = ts.get_today_all()
    print(today_all[:5])


get_today()

realtime = ts.get_realtime_quotes("603618")
realtime2 = ts.get_realtime_quotes(["sh", "zs", "hs300", "sz50", "zxb", "cyb"])

# get news
# ts.guba_sina().to_sql('guba_sina',engine,if_exists='append')
print('guba_sina')
# ts.get_notices().to_sql('notices',engine,if_exists='append')
print('notices')
ts.get_latest_news().to_sql('latest_news', engine, if_exists='append')
print('latest_news')

# save all stock to mysql
#  open     high    close      low      volume  p_change
# conn = pymysql.connect('localhost','root','caicai520','quantist')
# cursor = conn.cursor()
# cursor.execute("select distinct good1 from t_good limit 5")
# cursor.execute("select distinct code from today_all limit 10");
stocks = pd.read_sql_query('select distinct code from today_all', engine)
print("----------------------")


@retry(tries=10, delay=3)
def save_all_stock():
    n = 0
    open = []
    high = []
    close = []
    low = []
    volume = []
    change = []
    for stock in stocks['code']:
        # stock = list(stock)[0]
        # print(stock)
        s = ts.get_hist_data(stock)
        print("***********************")
        # open = s['open']
        # high = s['high']
        close = s['close']
        # low = s['low']
        # volume = s['volume']
        change = s['p_change']
        # t_stock = {'open':open,'high':high,'close':close,'low':low,'volume':volume,'change':change}
        t_stock = {'close': close, 'change': change}
        s_stock = pd.DataFrame(data=t_stock)
        # print("ok,,,,,,,")
        # time.sleep(5)
        s_stock.to_sql("t_" + stock, engine, if_exists='append')
        n += 1;
        print("t_" + stock + "--" + str(n))


save_all_stock()
# conn.close()


# get analysis1 to mysql
# the diffirent between conn and engine
conn = pymysql.connect('localhost', 'root', 'caicai520', 'quantist')
cursor = conn.cursor()

cursor.execute("select distinct code from t_good")
t_sh = ts.get_hist_data('sh')
# t_sh_change20 = t_sh['p_change'][0:30]
t_sh_change60 = t_sh['p_change'][0:60]

t = [10, 20, 60, 120, 180, 240]


@retry(tries=10, delay=3)
def get_corr_with_sh():
    n = 0
    code = []
    sum = []
    max = []
    time = []
    corr10 = []
    corr20 = []
    corr60 = []
    corr120 = []
    corr180 = []
    corr240 = []

    for stock in cursor.fetchall()[0:3]:
        stock = list(stock)[0]
        stock_change = ts.get_hist_data(stock)['p_change']
        s60 = stock_change[0:60]

        code.append(stock)
        sum.append(s60.sum())
        max.append(s60.max())
        time.append(datetime.datetime.now().strftime("%Y-%m-%d"))

        corr60.append(t_sh_change60.corr(s60))
        corr10.append(t_sh_change60.corr(stock_change[0:10]))
        corr20.append(t_sh_change60.corr(stock_change[0:20]))
        corr120.append(t_sh_change60.corr(stock_change[0:120]))
        corr180.append(t_sh_change60.corr(stock_change[0:180]))
        corr240.append(t_sh_change60.corr(stock_change[0:240]))

        n += 1
        print(stock + "--" + str(n))

    analysis1 = {"code": code, "sum": sum, "max": max, "time": time, "corr10": corr10
        , "corr20": corr20, "corr120": corr120, "corr180": corr180, "corr60": corr60,
                 "corr240": corr240}

    t_analysis1 = pd.DataFrame(data=analysis1)
    print(analysis1)
    t_analysis1.to_sql("t_analysis1", engine, flavor='mysql', if_exists='append')


get_corr_with_sh()

conn.close()
