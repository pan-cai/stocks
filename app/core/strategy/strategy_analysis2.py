# _*_ coding:utf-8 _*_
# @Time    : 2017-12-04 11:26
# @Author  : liupan
# @Email   : liupan8910@163.com

# _*_ coding:utf-8 _*_
import tushare as ts
import pandas as pd
import matplotlib.pylab as plt
import seaborn as sns
import numpy as np
import pymysql
import datetime
from retry import retry

# connect mysql
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:caicai520@127.0.0.1/quantist?charset=utf8')

stocks = ['300220', '600807', '603659', '300408', '603289']


# Get the stocks data
# code is the stock you want to get
def get_stock(code):
    s = ts.get_hist_data(str(code))
    s.reset_index(inplace=True)
    s['date'] = pd.to_datetime(s['date'])
    s = s.set_index('date')
    return s


# Plot single stock
# Stock is the code of sigle stock
def plot_single_stock(code, parameter):
    data = get_stock(code)
    data[parameter].plot(label=code)
    plt.legend(loc='best')
    plt.title(code)


plot_single_stock('300220', 'close')

for s in stocks:
    plot_single_stock(s, 'close')

for s in stocks:
    plot_single_stock(s, 'volume')


# Plot the MA in diffrent days
def plot_sigle_stock_MA(code):
    ma_day = [5, 10, 20, 60, 120, 180, 240]
    for ma in ma_day:
        column = "MA for %s days" % (str(ma))
        get_stock(str(code))['close'].rolling(window=ma).mean().plot(label='MA_' + str(ma))
        plt.legend(loc='best')
        plt.title(code)


plot_sigle_stock_MA('300220')

plot_single_stock('300220', 'p_change')

data = get_stock('300220')
data['p_change'].hist(bins=100)
