# _*_ coding:utf-8 _*_
# @Time    : 2017-12-04 11:51
# @Author  : liupan
# @Email   : liupan8910@163.com

from app.core.data.get_data import GetDataFromNet

sh = GetDataFromNet.get_sh()
print(sh[:3])