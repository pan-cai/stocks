# _*_ coding:utf-8 _*_
# @Time    : 2017-12-04 10:55
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
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:123/stock?charset=utf8')


class GetDataFromNet:
    @staticmethod
    def get_sh():
        return ts.get_hist_data('sh')

    # code必须为string格式
    @staticmethod
    def get_stock_data(self, code):
        return ts.get_hist_data(str(code))


class GetDataFromDB:
    # 从数据库获取单个股票所有数据
    def get_sigle_stock_data(self, code):
        sql = "selcet * from t_" + code
        return pd.read_sql_query(sql, engine, index_col='data', parse_dates=True)
