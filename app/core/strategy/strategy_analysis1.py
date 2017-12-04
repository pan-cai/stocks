# _*_ coding:utf-8 _*_
# @Time    : 2017-12-04 11:21
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


#connect mysql
from sqlalchemy import create_engine
engine = create_engine('mysql+pymysql://root:caicai520@127.0.0.1/quantist?charset=utf8')

# get data
get_data_sql = 'select distinct corr,code from t_analysis2'
analysis_data = pd.read_sql_query(get_data_sql,engine)
print(analysis_data.head(5))


sort_data = analysis_data.sort_values(by='corr',ascending=False)
sort_data[0:5]


# Save high corr stocks to mysql

@retry(tries=10, delay=3)
def save_all_stock():
    n = 1
    open = []
    high = []
    close = []
    low = []
    volume = []
    change = []
    for stock in sort_data['code'][0:5]:
        print(stock)
        s = ts.get_hist_data(stock)
        print("***********************")
        open = s['open']
        high = s['high']
        close = s['close']
        low = s['low']
        volume = s['volume']
        change = s['p_change']
        t_stock = {'open': open, 'high': high, 'close': close, 'low': low, 'volume': volume, 'change': change}
        s_stock = pd.DataFrame(data=t_stock)
        print("ok,,,,,,,")
        # time.sleep(2)
        s_stock.to_sql("t_" + stock, engine, flavor='mysql', if_exists='append')
        print("t_" + stock)


save_all_stock()
# conn.close()

# Plot them
sh_close = ts.get_hist_data('sh')['close'][0:120]
plt.plot(sh_close)
plt.show()
for stock in sort_data['code'][0:5]:
        print(stock)
        get_close_sql = "select  distinct date,close from t_" + stock;
        close_data = pd.read_sql_query(get_close_sql,engine)[0:120]
        plt.plot(close_data['close'])
        plt.show()


s600919 = ts.get_hist_data('600919')
plt.plot(s600919['close'])
print('600919')
plt.show()


t_600919_close = pd.read_sql_query('select close from t_600919',engine)
plt.plot(t_600919_close)
plt.show()

t_600919_close = pd.read_sql_query('select distinct date,close from t_600919',engine)
plt.plot(t_600919_close['close'])
plt.show()

t_600919_close = pd.read_sql_query('select distinct date,close from t_600919',engine)
data = t_600919_close['close']
if not data.empty:
    data.tolist().reverse()
    print(type(data))
    #print(data)
    plt.plot(data)
    plt.show()
else:
    print('Data is None!')

t_600919_close = pd.read_sql_query('select distinct date,close from t_600919',engine)
data = t_600919_close['close']
if not data.empty:
    data.tolist().reverse()
    print(type(data))
    #print(data)
    plt.plot(data)
    plt.show()
else:
    print('Data is None!')

# Get the corr of diffirent time
t = [5, 10, 30, 60, 120, 180]
sh_change = ts.get_hist_data('sh')
s600919_change = ts.get_hist_data('600919')
#print(sh_change[0:3])
#print(s600919_change[0:3])
corr_list = []
for tx in t:
    print('tx = ' + str(tx))
    t_sh_change = sh_change['p_change'][0:tx]
    print('len = ' + str(len(t_sh_cahange)))
    t_s600919_change = s600919_change['p_change'][0:tx]
    print(len(t_s600919_change))
    t_corr = t_sh_change.corr(t_s600919_change)
    print('corr = ' + str(t_corr))
    print('------------------------------')
    corr_list.append(str(t_corr))
d = pd.DataFrame({'t':t,'corr':corr_list})
print(d)


get_analysis1_sql = "select distinct * from t_analysis1"
analysis1_data = pd.read_sql_query(get_analysis1_sql,engine)
print(len(analysis1_data))
print(analysis1_data[0:5])

num = [10, 20, 60, 120, 180, 240]
for n in num:
    corr_num = "corr" + str(n)
    corr = analysis1_data[corr_num].sort_values()
    #print("std = " + str(corr.std()))
    plt.scatter([x for x in range(len(corr))],corr)
    plt.legend(corr_num)
plt.show()

print(analysis1_data.describe())

def normfun(x,mu,sigma):
    pdf = np.exp(-((x-mu)**2)/(2*sigma**2))/(sigma*np.sqrt(2*np.pi))
    return pdf
data = analysis1_data['corr120']
mean = data.mean()
std = data.std()
x = np.arange(-0.4,1,0.1)
y = normfun(x,mean,std)
plt.plot(x,y,color='g',linewidth=3)
plt.hist(data)
plt.title('kk')
plt.xlabel('xx')
plt.ylabel('yy')
plt.show()


max = analysis1_data['max']
x = [x for x in range(len(max))]
plt.scatter(x,max)
plt.title('max')
plt.show()

max = [m for m in analysis1_data['max'] if m < 20]
x = [x for x in range(len(max))]
plt.scatter(x,max)
plt.title('max')
plt.show()

sum = analysis1_data['sum']
x = [x for x in range(len(sum))]
plt.scatter(x,max)
plt.title('sum')
plt.show()

sum = [m for m in analysis1_data['sum'] if m < 15]
x = [x for x in range(len(sum))]
plt.scatter(x,sum)
plt.title('sum')
plt.show()

co = ['index', 'code', 'time']
for c in analysis1_data.columns:
    if c not in co:
        data = analysis1_data[c]
        plt.hist(data)
        plt.title(c)
        plt.show()

good4_sql = 'select distinct code,corr240,max,sum from t_analysis1 where corr240 > 0.6 and max < 15'
good4 = pd.read_sql_query(good4_sql,engine)
good4_data = good4.sort_values(by='corr240',ascending=False)[:5]
print(good4_data)
print("-----------------------")
print(good4.sort_values(by='max',ascending=False)[:5])
print("-----------------------")
print(good4.sort_values(by='sum',ascending=False)[:5])

#Plot sh and good4
sh = ts.get_hist_data('sh')
plt.plot(sh['open'])
plt.title('sh')
plt.show()

for stock in good4_data['code']:
    #print(type(stock))
    stock_data = ts.get_hist_data(stock)
    stock_data.to_sql('t_' + stock,engine,if_exists='append')
    open_data = stock_data['open']
    plt.plot(open_data)
    plt.title(stock)
    plt.show()

#Plot sh and good4
sh = ts.get_hist_data('sh')
plt.plot(sh['open'])
plt.title('sh')
plt.show()

for stock in good4_data['code']:
    #print(type(stock))
    stock_data = ts.get_hist_data(stock)
    stock_data.to_sql('t_' + stock,engine,if_exists='append')
    open_data = stock_data['open']
    plt.plot(open_data)
    plt.title(stock)
    plt.show()

stocks = ['600790','600269','600017','002029','603729']
stock = '600790'
data = ts.get_hist_data(stock)
data.to_sql('t_' + stock,engine,if_exists='append')