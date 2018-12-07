#coding:utf8 
import pandas as pd 
from sklearn import svm
import pymysql.cursors
import datetime
import DC
import tushare as ts
'''
策略规范：
参数：1.stock_code  2.state_dt   3.回测时间长度 4.回测 所需时间窗口长度
返回值:1 可以买 0 不买
 
'''
def calc_macd(df,code,shortday=12,longday=26,M=9,min_apx_days=100): #基于复权数据计算macd值，计算进入点
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
    for dt in dt_list[0:400]:
        data= df[(df.code==code)&(df.dt<=dt)].sort_values('dt',ascending=0)
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


    #计算dea和macd值
    macd_list=[]
    for dt in df_m.dt.tolist()[0:212]:
        dea_macd=0
        df_m= df_m[(df_m.code==code)&(df_m.dt<=dt)].sort_values('dt',ascending=0)
        diff_list=df_m.diff_macd.tolist()
       
        for i in range(0,min_apx_days):
            diff_s=diff_list[i]
            dea_macd+=float(dea_b*math.pow(dea_a,i)*diff_s)       
        # print dt,'  diff: ',round(diff_list[0],2),'dea:'," ",round(dea_macd,2),"macd: ",2*(round(diff_list[0],2)-round(dea_macd,2))
        macd_list.append([dt,round(dea_macd,2),2*(round(diff_list[0],2)-round(dea_macd,2))])
    df_dea=pd.DataFrame(macd_list)
    df_dea.columns=['dt','dea','macd']
    
    df_macd=df_diff.merge(df_dea,how='left',on='dt')
    df_macd=df_macd.sort_values('dt',ascending=0)
    # print df_macd.head(100)
    return df_macd