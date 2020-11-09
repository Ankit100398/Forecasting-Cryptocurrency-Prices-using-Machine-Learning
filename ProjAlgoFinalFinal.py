#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 11:05:18 2020

@author: student
"""

import os
import numpy as np 
import pandas as pd
from fbprophet import Prophet as proph
import matplotlib.pyplot as plt
from pyhive import hive


class Prophet:
    
    def __init__(self):
        
        self.host_name = "localhost"
        self.port = 10000
        self.database = "default"
        self.auth = 'NOSASL'
    
    def _getConnection(self):
        
        conn = hive.Connection(host = self.host_name, port = self.port, database = self.database, auth=self.auth)
        return conn;
        
    def ProphetBTC(self):
        conn=self._getConnection()
        df = pd.read_sql('select * from btc1',conn)
        #df.to_csv("/home/student/Pied Piper/btcfinal101.csv")

        conn.close()
        print(df)
        lst=['Open','High','Low','Close']

        #df.drop(df.index[0],inplace=True)
        df.insert(0, 'index', np.arange(1, 1931), True)

        df.columns=['index','Date','Open','High','Low','Close','Volume']

        result_BTC=[]

        for i in lst:
            df_BTC= pd.DataFrame(df)
    
            df_BTC.rename(columns={'Date':'ds', i:'y'},inplace=True)
    
            mod_BTC = proph(interval_width=0.95)
            mod_BTC.fit(df_BTC)
    
            future_BTC = mod_BTC.make_future_dataframe(periods=360, freq='D')
    
            forecast_BTC = mod_BTC.predict(future_BTC)
            result_BTC.append(forecast_BTC)

            for jB in result_BTC:
                df_BTC.predict=pd.DataFrame(jB)
                df_BTC.predict.to_csv(str(i)+'BTC'+'.csv')     
            mod_BTC.plot(forecast_BTC, uncertainty=True,xlabel='Date',ylabel=i).savefig('/home/student/Pied Piper/static/'+str(i)+'BTC'+'.png')
            mod_BTC.plot_components(forecast_BTC,uncertainty=True).savefig('/home/student/Pied Piper/static/'+str(i)+'BTCComponent'+'.png')
            plt.title('Facebook Prophet Forecast and Fitting'+(str(i)+'BTC'))  
            plt.suptitle(str(i)+ ' '+ 'BTC', fontsize=20)
            #plt.show()
            
            
            
            #mod_BTC.plot(forecast_BTC).savefig('/home/student/Pied Piper/WebDev (Flask)/static/'+str(i)+'BTC'+'.png')
    
    
    def ProphetETH(self):
        conn=self._getConnection()
        df = pd.read_sql('select * from eth1',conn)
        #df.to_csv("/home/student/Pied Piper/ethfinal101.csv")

        conn.close()

        lst=['Open','High','Low','Close']

        #df.drop(df.index[0],inplace=True)
        df.insert(0, 'index', np.arange(1, 1607), True)

        df.columns=['index','Date','Open','High','Low','Close','Volume']

        result_ETH=[]

        for i in lst:
            df_ETH= pd.DataFrame(df)
    
            df_ETH.rename(columns={'Date':'ds', i:'y'},inplace=True)
    
            mod_ETH = proph(interval_width=0.95)
            mod_ETH.fit(df_ETH)
    
            future_ETH = mod_ETH.make_future_dataframe(periods=360, freq='D')
    
            forecast_ETH = mod_ETH.predict(future_ETH)
            result_ETH.append(forecast_ETH)

            for jB in result_ETH:
                df_ETH.predict=pd.DataFrame(jB)
                df_ETH.predict.to_csv(str(i)+'ETH'+'.csv')     
            mod_ETH.plot(forecast_ETH, uncertainty=True,xlabel='Date',ylabel=i).savefig('/home/student/Pied Piper/static/'+str(i)+'ETH'+'.png')
            mod_ETH.plot_components(forecast_ETH,uncertainty=True).savefig('/home/student/Pied Piper/static/'+str(i)+'ETHComponent'+'.png')
            plt.title('Facebook Prophet Forecast and Fitting'+(str(i)+'ETH'))  
            plt.suptitle(str(i)+ ' '+ 'ETH', fontsize=20)
            #plt.show()
    
    
    def ProphetLTC(self):
        conn=self._getConnection()
        df = pd.read_sql('select * from ltc1',conn)
        #df.to_csv("/home/student/Pied Piper/ltcfinal101.csv")

        conn.close()

        lst=['Open','High','Low','Close']

        #df.drop(df.index[0],inplace=True)
        df.insert(0, 'index', np.arange(1, 1931),True) 
          
        df.columns=['index','Date','Open','High','Low','Close','Volume']

        result_LTC=[]

        for i in lst:
            df_LTC= pd.DataFrame(df)
    
            df_LTC.rename(columns={'Date':'ds', i:'y'},inplace=True)
    
            mod_LTC = proph(interval_width=0.95)
            mod_LTC.fit(df_LTC)
    
            future_LTC = mod_LTC.make_future_dataframe(periods=360, freq='D')
    
            forecast_LTC = mod_LTC.predict(future_LTC)
            result_LTC.append(forecast_LTC)

            for jB in result_LTC:
                df_LTC.predict=pd.DataFrame(jB)
                df_LTC.predict.to_csv(str(i)+'LTC'+'.csv')     
            mod_LTC.plot(forecast_LTC, uncertainty=True,xlabel='Date',ylabel=i).savefig('/home/student/Pied Piper/static/'+str(i)+'LTC'+'.png')
            mod_LTC.plot_components(forecast_LTC,uncertainty=True).savefig('/home/student/Pied Piper/static/'+str(i)+'LTCComponent'+'.png')
            plt.title('Facebook Prophet Forecast and Fitting'+(str(i)+'LTC'))  
            plt.suptitle(str(i)+ ' '+ 'LTC', fontsize=20)
            #plt.show()

if __name__=="__main__":
    p=Prophet()
    p.ProphetBTC()
    p.ProphetETH()
    p.ProphetLTC()