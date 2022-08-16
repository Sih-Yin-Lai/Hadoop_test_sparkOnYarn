#!/usr/bin/python3
# 匯入套件
import pandas as pd
import numpy as np
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.pandas.config import set_option, reset_option
import time
import sys

#test_loop=input(f'Input number of loops: ')
#sys.argv[1]
test_loop=sys.argv[1]
test_loop=int(test_loop)
#yarn mode
spark = SparkSession.builder.appName("clusterTest").getOrCreate()

# 優化
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", True)
ps.set_option("compute.default_index_type", "distributed")

# 讓不同表格可以做結合
ps.set_option('compute.ops_on_diff_frames', True)
#mean_time=ps.Series(np.zeros(test_loop))
# 讀檔
start_t=time.time()
start_p=time.process_time()
print('Now reading file ...')
df_raw=ps.read_csv('hdfs://nncluster/user/hadoop/installments_payments.csv')
print('Input dataframe shape: ', df_raw.shape)
end_t=time.time()
end_p=time.process_time()
readingtime = end_t - start_t
readingCPUtime = end_p - start_p

print('reading time :    ',readingtime,' sec')
print('reading CPU time :',readingCPUtime,' sec')

#test_loop=input(f'Input number of loops: ')
#print('Set loop: ',test_loop)
# 開始
start_t=time.time()
start_p=time.process_time()
for loop in range(test_loop):
    print('loop:',loop+1)
    df_ori=df_raw
    # 創新NAN欄位
    df_ori['NAN']=df_ori['AMT_PAYMENT'].apply(lambda x:0 if x>=0 else 1)
    df_ori['NAN']=df_ori['NAN'].mask((df_ori['AMT_INSTALMENT']==0)&(df_ori['AMT_PAYMENT'].isnull()),0)
    # DAYS_ENTRY_PAYMENT & AMT_PAYMENT :填補原本NAN =>0供後續計算使用
    df_ori=df_ori.fillna(0)

    # Days 合併為一個欄位
    df_ori['DAYS_LATE_PAYMENT']=df_ori["DAYS_INSTALMENT"] - df_ori["DAYS_ENTRY_PAYMENT"]
    df_ori['DAYS_LATE_PAYMENT']=df_ori['DAYS_LATE_PAYMENT'].mask(df_ori['DAYS_LATE_PAYMENT']<0,0)

    # AMT_ARREARS = (AMT_INSTALMENT - AMT_PAYMENT)
    df_ori['AMT_ARREARS']=df_ori["AMT_INSTALMENT"] - df_ori["AMT_PAYMENT"]
    df_ori['AMT_ARREARS']=df_ori['AMT_ARREARS'].apply(lambda x :1 if x>0 else 0)
    # 去除原本欄位
    df=df_ori.drop(['AMT_INSTALMENT','AMT_PAYMENT''DAYS_INSTALMENT','DAYS_ENTRY_PAYMENT'],axis=1)

    # part1 切分資料(欄位)
    df2_0=df[['SK_ID_PREV','SK_ID_CURR']]
    # 'SK_ID_PREV','SK_ID_CURR','NUM_INSTALMENT_NUMBER' 欄位處裡
    df_ID=df2_0.groupby('SK_ID_PREV').aggregate({'SK_ID_PREV':'max' ,'SK_ID_CURR':'max'})
    df_ins=df[['SK_ID_PREV','NUM_INSTALMENT_NUMBER']]
    # instalment 處理:要不重複的紀錄
    df_instalment=df_ins.groupby('SK_ID_PREV')['NUM_INSTALMENT_NUMBER'].nunique().to_frame()
    df_instalment= df_instalment.sort_index()
    df_ver=df[['SK_ID_PREV','NUM_INSTALMENT_VERSION']]
    # version 處理 要不重複的
    df_version=df_ver.groupby('SK_ID_PREV')['NUM_INSTALMENT_VERSION'].nunique()
    df_version=df_version.to_frame()
    df_version =df_version.sort_index()
    df_part1=ps.concat([df_ID,df_version,df_instalment],axis=1)

    # part2
    df_day=df[['SK_ID_PREV','NUM_INSTALMENT_NUMBER','DAYS_LATE_PAYMENT']]
    # last_day 處裡
    df_day2=df_day.groupby(['SK_ID_PREV','NUM_INSTALMENT_NUMBER']).max()
    df_day2=df_day2.reset_index()
    df_lastday=df_day2.groupby(['SK_ID_PREV']).sum()
    df_lastday=df_lastday.sort_index()
    df_p2=df[['SK_ID_PREV','NAN','AMT_ARREARS']]
    df_p21=df_p2.groupby('SK_ID_PREV').sum().sort_index()
    df_part2=ps.concat([df_lastday,df_p21],axis=1)

    # p1,p2整理
    df_part2= df_part2.drop('NUM_INSTALMENT_NUMBER',axis=1)
    df_final=ps.concat([df_part1,df_part2],axis=1)
    # 將late day轉成每期平均遲交天數
    df_final['DAYS_LATE_PAYMENT']=df_final['DAYS_LATE_PAYMENT']/df_final['NUM_INSTALMENT_NUMBER']

    # 一個客戶有多少申請
    sk=df_final.groupby('SK_ID_CURR').count().sort_index()
    sk=sk['SK_ID_PREV']

    # final df
    df_final=df_final.groupby('SK_ID_CURR').mean().round(3).sort_index()
    df_final.insert(0,'NUM_PREV',sk)
    df_final=df_final.reset_index().drop('SK_ID_PREV',axis=1)

end_t=time.time()
end_p=time.process_time()
loopingtime = (end_t - start_t)/test_loop
loopingCPUtime = (end_p - start_p)/test_loop

# 恢復
reset_option("compute.ops_on_diff_frames")

#print('reading time :    ',readingtime,' sec')
#print('reading CPU time :',readingCPUtime,' sec')
print('Mean time per loops :    ',loopingtime,' sec')
print('Mean CPU time per loops :',loopingCPUtime,' sec')
print('Output dataframe shape:', df_final.shape)
print('Now showing df ...')
print(df_final.head(10))
print('SUCESS & FINISH')
spark.stop()

