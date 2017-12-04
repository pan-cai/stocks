# _*_ coding:utf-8 _*_
# @Time    : 2017-12-04 12:36
# @Author  : liupan
# @Email   : liupan8910@163.com
# Get the stocks data
# code is the stock you want to get

import tushare as ts
import pandas as pd
import matplotlib.pylab as plt
import seaborn as sns
import numpy as np
import pymysql
import datetime
from retry import retry
from sqlalchemy import create_engine


def get_stock(code):
    s = ts.get_hist_data(str(code))
    s.reset_index(inplace=True)
    s['date'] = pd.to_datetime(s['date'])
    s = s.set_index('date')
    return s


s = get_stock('600848')
print(s[:3])
