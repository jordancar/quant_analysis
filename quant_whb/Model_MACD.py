#coding:utf8 
import pandas as pd 
from sklearn import svm
import pymysql.cursors
import datetime
import DC,math
import tushare as ts
'''
策略规范：
参数：1.stock_code  2.state_dt   3.回测时间长度 4.回测 所需时间窗口长度
返回值:1 可以买 0 不买
 
'''
def collectDataAsDF(in_code):
        db = pymysql.connect(host='127.0.0.1', user='root', passwd='root', db='stock', charset='utf8')
        cursor = db.cursor()
        sql_done_set = "SELECT * FROM stock_all a where stock_code = '%s'  order by state_dt asc" % (in_code)
        cursor.execute(sql_done_set)
        done_set = cursor.fetchall()
        if len(done_set) == 0:
            raise Exception
        date_seq = []
        code=[] #stockcode list
        open_list = []
        close_list = []
        high_list = []
        low_list = []
        vol_list = []
        amount_list = []
        for i in range(len(done_set)):
            date_seq.append(done_set[i][0])
            code.append(done_set[i][1])
            open_list.append(float(done_set[i][2]))
            close_list.append(float(done_set[i][3]))
            high_list.append(float(done_set[i][4]))
            low_list.append(float(done_set[i][5]))
            vol_list.append(float(done_set[i][6]))
            amount_list.append(float(done_set[i][7]))
        df=pd.DataFrame({'code':code,'dt':date_seq,'open':open_list,'end_price':close_list,'high':high_list,'low':low_list,'amount':amount_list})
        return df 
def calc_macd(df,code,shortday=12,longday=26,M=9,min_apx_days=200): #基于复权数据计算macd值，计算进入点
    apx_days=min_apx_days #缓存计算天数
    df=df[(df.code==code)]
    short_a=float(shortday-1)/(shortday+1) 
    short_b=float(2)/(shortday+1)

    long_a=float(longday-1)/(longday+1)
    long_b=float(2)/(longday+1)
    
    dea_a=float(M-1)/(M+1)
    dea_b=float(2)/(M+1)

    #计算ema和diff
    dt_list=df.sort_values('dt',ascending=0).dt.tolist()

    
    ema_list=[]
    for dt in dt_list:
        data= df[(df.code==code)&(df.dt<=dt)].sort_values('dt',ascending=0)
        # aa=raw_input('waiting..')
        min_apx_days= min_apx_days if min_apx_days < len(data.end_price.tolist()) else len(data.end_price.tolist())
        ema_short=0
        ema_long=0
        for i in range(0,min_apx_days):
            end_price_n=data.end_price.tolist()[i] # 当日end price 
            ema_short+= float(short_b*math.pow(short_a,i)*end_price_n)
            ema_long+= float(long_b*math.pow(long_a,i)*end_price_n)
        ema_list.append([dt,ema_short,ema_long,ema_short-ema_long])    

    df_diff=pd.DataFrame(ema_list)
    df_diff.columns=['dt','ema_short','ema_long','diff_macd']
    df_m=df.merge(df_diff,how='left',on='dt')
    df_m=df_m.sort_values('dt',ascending=False)
    df_diff=df_m #缓存 df_diff 用于merge dea 形成最终macd计算
    df_diff[df_diff.dt>='2018-01-01'].to_csv('/Users/wanghongbo8/Desktop/ema.csv')

    #计算dea和macd值
    macd_list=[]
    min_apx_days=apx_days
    print "minapxdays:",min_apx_days
    for dt in df_diff.dt.tolist():
        dea_macd=0
        df_m= df_m[(df_m.code==code)&(df_m.dt<=dt)].sort_values('dt',ascending=0)
        diff_list=df_m.diff_macd.tolist()
        min_apx_days= min_apx_days if min_apx_days < len(data.end_price.tolist()) else len(data.end_price.tolist())
        for i in range(0,apx_days):
            diff_s=diff_list[i]
            dea_macd+=float(dea_b*math.pow(dea_a,i)*diff_s)  
            # print dea_macd      
        print dt,'  diff: ',round(diff_list[0],2),'dea:'," ",round(dea_macd,2),"macd: ",2*(round(diff_list[0],2)-round(dea_macd,2))
        # aa=raw_input('dea calc...')
        macd_list.append([dt,round(dea_macd,2),2*(round(diff_list[0],2)-round(dea_macd,2))])
    df_dea=pd.DataFrame(macd_list)
    df_dea.columns=['dt','dea','macd']
    df_macd=df_diff.merge(df_dea,how='left',on='dt')
    df_macd=df_macd.sort_values('dt',ascending=0)
    # print df_macd.head(100)
    return df_macd
df=collectDataAsDF('600030.SH')
print df.tail()
df=calc_macd(df,'600030.SH')
print df.head()

def predict_buy(df,predict_dt): #基于当日和前2日的macd来预测是否趋势翻转（正变负 ),把第二天标记为买点1，非买点0
    df['macd_1']=df.macd.shift(-1) #对齐前一天macd
    df['macd_2']=df.macd.shift(-2) #对齐前二天macd
    print df[df.macd.isnull().values==True].head() #筛选出所有为空的日期
    df['predict_macd']=0
    df.loc[(df.macd>0) & (df.macd_1<0) & (df.macd_2<0),'predict_macd']=1
    df['predict']=df.predict_macd.shift(1) #预测第二日是否买入
    print df[df.predict_macd==1]
    print df.head(10)

# predict_buy(df,'111')
