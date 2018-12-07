#coding:utf8
import numpy as np
import datetime
import pymysql
import copy
import tushare as ts


# 返回的resu中 特征值按由小到大排列，对应的是其特征向量
def get_portfolio(stock_list,state_dt,para_window):
    print "stock_list,state_dt,para_window: ",stock_list,state_dt,para_window
    # 建数据库连接，设置Tushare的token
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='root', db='stock', charset='utf8')
    cursor = db.cursor()
    ts.set_token('9479832d9c2b7719809d7a39f606172d37d27af2f1713f6814493dfc')
    pro = ts.pro_api()

    portfilio = stock_list

    # 建评估时间序列, para_window参数代表回测窗口长度
    model_test_date_start = (datetime.datetime.strptime(state_dt, '%Y-%m-%d') - datetime.timedelta(days=para_window)).strftime(
        '%Y%m%d')
    model_test_date_end = (datetime.datetime.strptime(state_dt, "%Y-%m-%d")).strftime('%Y%m%d')
    df = pro.trade_cal(exchange_id='', is_open=1, start_date=model_test_date_start, end_date=model_test_date_end)
    date_temp = list(df.iloc[:, 1])
    model_test_date_seq = [(datetime.datetime.strptime(x, "%Y%m%d")).strftime('%Y-%m-%d') for x in date_temp]

    list_return = []
    for i in range(len(model_test_date_seq)-4):
        ti = model_test_date_seq[i]
        ri = []
        for j in range(len(portfilio)):
            sql_select = "select * from stock_all a where a.stock_code = '%s' and a.state_dt >= '%s' and a.state_dt <= '%s' order by state_dt asc" % (portfilio[j], model_test_date_seq[i], model_test_date_seq[i + 4])
            cursor.execute(sql_select)
            done_set = cursor.fetchall()
            db.commit()
            temp = [x[3] for x in done_set]
            base_price = 0.00
            after_mean_price = 0.00
            if len(temp) <= 1:
                r = 0.00
            else:
                base_price = temp[0]
                after_mean_price = np.array(temp[1:]).mean()
                r = (float(after_mean_price/base_price)-1.00)*100.00
            ri.append(r)
            del done_set
            del temp
            del base_price
            del after_mean_price
        list_return.append(ri)

    # 求协方差矩阵
    cov = np.cov(np.array(list_return).T)
    # 求特征值和其对应的特征向量
    ans = np.linalg.eig(cov)
    # 排序，特征向量中负数置0，非负数归一
    ans_index = copy.copy(ans[0])
    ans_index.sort()
    resu = []
    # print ans 
    for k in range(len(ans_index)):
        con_temp = []
        con_temp.append(ans_index[k])
        content_temp1 = ans[1][np.argwhere(ans[0] == ans_index[k])[0][0]]
        content_temp2 = []
        content_sum = np.array([x for x in content_temp1 if x >= 0.00]).sum()
        for m in range(len(content_temp1)):
            if content_temp1[m] >= 0 and content_sum > 0:
                content_temp2.append(content_temp1[m]/content_sum)
            else:
                content_temp2.append(0.00)
        con_temp.append(content_temp2)
        # 计算夏普率
        sharp_temp = np.array(copy.copy(list_return)) * content_temp2
        sharp_exp = sharp_temp.mean()
        sharp_base = 0.04
        sharp_std = np.std(sharp_temp)
        if sharp_std == 0.00:
            sharp = 0.00
        else:
            sharp = (sharp_exp - sharp_base) / sharp_std

        con_temp.append(sharp)
        resu.append(con_temp)

    return resu

if __name__ == '__main__':
    # pf = ['603912.SH', '300666.SZ', '300618.SZ', '002049.SZ', '300672.SZ']
    pf=['601398.SH','601857.SH','601288.SH','601988.SH','601318.SH','600036.SH','600028.SH','601628.SH','601088.SH']
    pf=['000002.SZ', '000007.SZ', '000040.SZ', '000056.SZ', '000063.SZ', '000069.SZ', '000333.SZ', '000401.SZ', '000507.SZ',  \
     '000576.SZ', '000582.SZ', '000592.SZ', '000603.SZ', '000610.SZ', '000622.SZ', '000638.SZ', '000651.SZ', '000666.SZ', '000722.SZ',  \
     '000725.SZ', '000735.SZ', '000768.SZ', '000778.SZ', '000786.SZ', '000793.SZ', '000830.SZ', '000856.SZ', '000858.SZ', '000876.SZ', \
      '000887.SZ', '000895.SZ', '000913.SZ', '000917.SZ', '000936.SZ', '000975.SZ', '000977.SZ', '000979.SZ', '000996.SZ','002007.SZ',  \
      '002011.SZ', '002024.SZ', '002027.SZ', '002075.SZ', '002086.SZ', '002094.SZ', '002103.SZ', '002108.SZ', '002110.SZ', '002210.SZ',  \
      '002221.SZ', '002236.SZ', '002264.SZ', '002271.SZ', '002285.SZ', '002288.SZ', '002320.SZ', '002337.SZ', '002344.SZ', '002353.SZ',  \
      '002366.SZ', '002367.SZ', '002371.SZ', '002400.SZ', '002415.SZ', '002416.SZ', '002440.SZ', '002441.SZ', '002450.SZ', '002451.SZ',  \
      '002466.SZ', '002472.SZ', '002504.SZ', '002549.SZ', '002568.SZ', '002575.SZ', '002594.SZ','002607.SZ', '002611.SZ', '002679.SZ',  \
      '002696.SZ', '002708.SZ', '002711.SZ', '002714.SZ', '002719.SZ', '002761.SZ', '002856.SZ', '002903.SZ', '300003.SZ', '300017.SZ',  \
      '300059.SZ', '300084.SZ', '300104.SZ', '300122.SZ', '300144.SZ', '300146.SZ', '300159.SZ', '300173.SZ', '300181.SZ', '300221.SZ',  \
      '300285.SZ', '300324.SZ', '300343.SZ', '300409.SZ', '300441.SZ', '300674.SZ', '300750.SZ', '300760.SZ', '600004.SH', '600009.SH',  \
      '600011.SH', '600019.SH', '600020.SH', '600026.SH', '600027.SH','600028.SH', '600029.SH', '600030.SH', '600031.SH', '600036.SH',  \
      '600048.SH', '600050.SH', '600053.SH', '600056.SH', '600107.SH', '600122.SH', '600125.SH', '600128.SH', '600132.SH', '600150.SH',  \
      '600165.SH', '600171.SH', '600176.SH', '600187.SH', '600196.SH', '600210.SH', '600219.SH', '600226.SH', '600235.SH', '600271.SH', \
       '600276.SH', '600283.SH', '600309.SH', '600339.SH', '600340.SH', '600352.SH', '600362.SH', '600365.SH', '600368.SH', '600387.SH',  \
       '600438.SH', '600487.SH', '600490.SH','600506.SH', '600507.SH', '600516.SH', '600518.SH', '600519.SH', '600538.SH', '600547.SH',  \
       '600552.SH', '600585.SH', '600598.SH', '600600.SH', '600604.SH', '600624.SH', '600635.SH', '600662.SH', '600689.SH', '600695.SH',  \
       '600729.SH', '600733.SH', '600740.SH', '600754.SH', '600755.SH', '600760.SH', '600783.SH', '600790.SH', '600825.SH', '600874.SH',  \
       '600884.SH', '600887.SH', '600895.SH', '600900.SH', '600903.SH', '601006.SH', '601011.SH', '601012.SH', '601088.SH', '601111.SH',  \
       '601118.SH','601155.SH', '601166.SH', '601186.SH', '601225.SH', '601233.SH', '601318.SH', '601319.SH', '601600.SH', '601668.SH',  \
       '601766.SH', '601828.SH', '601857.SH', '601888.SH', '601899.SH', '601933.SH', '601989.SH', '603032.SH', '603069.SH', '603101.SH', \
        '603156.SH', '603180.SH', '603196.SH', '603389.SH', '603630.SH', '603693.SH', '603711.SH', '603776.SH', '603787.SH', '603789.SH', '603799.SH', '603933.SH']

    ans = get_portfolio(pf,'2018-12-04',10)
    print "eigz num:",len(ans)
    print('**************  Market Trend  ****************')
    print('Risk : ' + str(round(ans[0][0], 2)))
    print('Sharp ratio : ' + str(round(ans[0][2], 2)))

    for i in range(len(pf)):
        print('----------------------------------------------')
        print('Stock_code : ' + str(pf[i]) + '  Position : ' + str(round(ans[0][1][i] * 100, 2)) + '%')
    print('----------------------------------------------')

    print('**************  Best Return  *****************')
    print('Risk : ' + str(round(ans[1][0], 2)))
    print('Sharp ratio : ' + str(round(ans[1][2], 2)))
    for j in range(len(pf)):
        print('----------------------------------------------')
        print('Stock_code : ' + str(pf[j]) + '  Position : ' + str(
            round(ans[1][1][j] * 100, 2)) + '%')
    print('----------------------------------------------')
