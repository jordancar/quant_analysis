#coding:utf8
import datetime
import tushare as ts
import pymysql
import stock_list
from para_set import back_test as bt 
import sys  
reload(sys)
sys.setdefaultencoding('utf8')

if __name__ == '__main__':

    # 设置tushare pro的token并获取连接
    ts.set_token('9479832d9c2b7719809d7a39f606172d37d27af2f1713f6814493dfc')
    pro = ts.pro_api()
    # 设定获取日线行情的初始日期和终止日期，其中终止日期设定为昨天。
    start_dt = '20181204'
    time_temp = datetime.datetime.now() - datetime.timedelta(days=1)
    end_dt = time_temp.strftime('%Y%m%d')
    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='root', db='stock', charset='utf8')
    cursor = db.cursor()
    # 设定需要获取数据的股票池
    # stock_pool = ['603912.SH','300666.SZ','300618.SZ','002049.SZ','300672.SZ']
    # stock_pool=['601398.SH','601857.SH','601288.SH','601988.SH','601318.SH','600036.SH','600028.SH','601628.SH','601088.SH']
    stock_pool=bt.stock_pool_9
    index_pool=['000001.SH']
    #直接获取指数行情 
    df_index=pro.index_daily(ts_code='000001.SH', start_date='20100101', end_date=end_dt)

    # df_index=df_index[[['ts_code', 'trade_date', 'close', 'open', 'high', 'low','pre_close', 'change', 'pct_chg', 'vol', 'amount']]]
    # df_index_2['date']=df_index_2.index
    df_index['code']='sh'
    # df_index_2['amount']=0
    df_index=df_index[['trade_date','code','open', 'close','high', 'low','vol','amount','pct_chg']]
    def init_stock_index(df):
        sql_init='truncate stock_index;'
        cursor.execute(sql_init)
        c_len=df.shape[0]
        for j in range(c_len):
                resu0 = list(df.ix[c_len-1-j])
                resu = []
                for k in range(len(resu0)):
                    if str(resu0[k]) == 'nan':
                        resu.append(-1)
                    else:
                        resu.append(resu0[k])
    #             state_dt = (datetime.datetime.strptime(resu[0], "%Y%m%d")).strftime('%Y-%m-%d')
                # state_dt=resu[0]
                state_dt = (datetime.datetime.strptime(resu[0], "%Y%m%d")).strftime('%Y-%m-%d')
                try:
                    sql_insert = "INSERT INTO stock_index(state_dt,stock_code,open,close,high,low,vol,amount,p_change) VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f')" % (state_dt,str(resu[1]),float(resu[2]),float(resu[3]),float(resu[4]),float(resu[5]),float(resu[6]),float(resu[7]),float(resu[8]))
                    print sql_insert
                    cursor.execute(sql_insert)
                    print 'inserted'
                    db.commit()
                except Exception as err:
                    print err
                    continue
    # init_stock_index(df_index) # 初始化上证指数

    df_dim=stock_list.stock_all()  #获取上证股票维表
    stock_big_pool=list(df_dim.ts_code) #A 股票 3565 股票 list

    # aaa=raw_input('index initialized ')
    def init_stock_all_plus(stock_pool):
        total = len(stock_pool)
        for i in range(len(stock_pool)):
            try:
                # df = pro.daily(ts_code=stock_pool[i], start_date=start_dt, end_date=end_dt)
                #使用前复权数据插入
                df=ts.pro_bar(pro_api=pro, ts_code=stock_pool[i], adj='qfq', start_date=start_dt, end_date=end_dt)
                df_new=df.merge(df_dim,on='ts_code')
                df=df_new
                # print df_new.columns 
                # aaa=raw_input('aaaa...')
                print('Seq: ' + str(i+1) + ' of ' + str(total) + '   Code: ' + str(stock_pool[i]))
                c_len = df.shape[0]
            except Exception as aa:
                print(aa)
                print('No DATA Code: ' + str(i))
                continue
            for j in range(c_len):
                resu0 = list(df.ix[c_len-1-j])
                resu = []
                for k in range(len(resu0)):
                    if str(resu0[k]) == 'nan':
                        resu.append(-1)
                    else:
                        resu.append(resu0[k])
                state_dt = (datetime.datetime.strptime(resu[1], "%Y%m%d")).strftime('%Y-%m-%d')
                try:
                    sql_insert = "INSERT INTO stock_all_plus(state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change,name,area,industry) VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f','%s','%s','%s')" \
                     % (state_dt,str(resu[0]),float(resu[2]),float(resu[5]),float(resu[3]),float(resu[4]),float(resu[9]),float(resu[10]),float(resu[6]),float(resu[7]),float(resu[8]),str(resu[12]),str(resu[13]),str(resu[14]))
                    cursor.execute(sql_insert)
                    db.commit()
                except Exception as err:
                    print err
                    print 'already exists!'
                    continue
    # init_stock_all_plus(stock_big_pool)
    # aaa=raw_input('aaaa...')
    total = len(stock_pool)
    # 循环获取单个股票的日线行情
    for i in range(len(stock_pool)):
        try:
            # df = pro.daily(ts_code=stock_pool[i], start_date=start_dt, end_date=end_dt)
            #使用前复权数据插入
            df=ts.pro_bar(pro_api=pro, ts_code=stock_pool[i], adj='qfq', start_date=start_dt, end_date=end_dt)
            print('Seq: ' + str(i+1) + ' of ' + str(total) + '   Code: ' + str(stock_pool[i]))
            c_len = df.shape[0]
        except Exception as aa:
            print(aa)
            print('No DATA Code: ' + str(i))
            continue
        for j in range(c_len):
            resu0 = list(df.ix[c_len-1-j])
            resu = []
            for k in range(len(resu0)):
                if str(resu0[k]) == 'nan':
                    resu.append(-1)
                else:
                    resu.append(resu0[k])
            state_dt = (datetime.datetime.strptime(resu[1], "%Y%m%d")).strftime('%Y-%m-%d')
            try:
                sql_insert = "INSERT INTO stock_all(state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change) VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f')" % (state_dt,str(resu[0]),float(resu[2]),float(resu[5]),float(resu[3]),float(resu[4]),float(resu[9]),float(resu[10]),float(resu[6]),float(resu[7]),float(resu[8]))
                cursor.execute(sql_insert)
                db.commit()
            except Exception as err:
                print 'already exists!'
                continue
    cursor.close()
    db.close()
    print('All Finished!')
