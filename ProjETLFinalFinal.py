#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 22 18:30:28 2020

@author: student
"""

from hdfs import InsecureClient
from pyhive import hive
import sasl
import os
import yfinance as yf



class Project:
    def __init__(self):
        
        self.host_name = "localhost"
        self.port = 10000
        self.database = "default"
        self.auth = 'NOSASL'
    
    def _getConnection(self):
        
        conn = hive.Connection(host = self.host_name, port = self.port, database = self.database, auth=self.auth)
        return conn;
       
    def download(self):
       
        #Downloading Historical Data for Ethereum and converting it to csv file
        ethereumDownload = yf.download("ETH-USD", start="2014-01-01", end="2019-12-30")
        print(ethereumDownload)
        ethereumDownload.reset_index(level=0, inplace=True)
        ethereumDownload.columns=['Dat','Open', 'High', 'Low', 'Close', 'AdjClose', 'Volume']
        ethereumDownload.to_csv("/home/student/Pied Piper/ethfinal68.csv",index=False)

        
        #Downloading Historical Data for Bitcoin and converting it to csv file
        bitcoinDownload = yf.download("BTC-USD", start="2014-01-01", end="2019-12-30")
        print(bitcoinDownload)
        bitcoinDownload.reset_index(level=0, inplace=True)
        bitcoinDownload.columns=['Dat','Open', 'High', 'Low', 'Close', 'AdjClose', 'Volume']
        bitcoinDownload.to_csv("/home/student/Pied Piper/btcfinal68.csv",index=False)
        
        #Downloading Historical Data for Litecoin and converting it to csv file    
        litecoinDownload = yf.download("LTC-USD", start="2014-01-01", end="2019-12-30")
        print(litecoinDownload)
        litecoinDownload.reset_index(level=0, inplace=True)
        litecoinDownload.columns=['Dat','Open', 'High', 'Low', 'Close', 'AdjClose', 'Volume']
        litecoinDownload.to_csv("/home/student/Pied Piper/ltcfinal68.csv",index=False)
#        
   
    def hadoop_load(self):
        
        #Dumping data from local file system to hadoop ecosystem
        client_hdfs=InsecureClient('http://localhost:50070',user="hduser")
        try:
            client_hdfs.upload('/',"/home/student/Pied Piper/ethfinal68.csv")
        except:
            client_hdfs.delete(hdfs_path='/'+ 'ethfinal68.csv' + '/',recursive=True)
            client_hdfs.upload('/',"/home/student/Pied Piper/ethfinal68.csv")
        
        try:  
            client_hdfs.upload('/',"/home/student/Pied Piper/btcfinal68.csv")
        except:
            client_hdfs.delete(hdfs_path='/'+ 'btcfinal68.csv' + '/',recursive=True)
            client_hdfs.upload('/',"/home/student/Pied Piper/btcfinal68.csv")
            
        try:
            client_hdfs.upload('/',"/home/student/Pied Piper/ltcfinal68.csv")  
        except:
            client_hdfs.delete(hdfs_path='/'+ 'ltcfinal68.csv' + '/',recursive=True)
            client_hdfs.upload('/',"/home/student/Pied Piper/ltcfinal68.csv")
    
        
    def hive_load(self):
        
        #Connecting to Hive to create tables and perform data preprocessing / data cleaning
        conn = self._getConnection()
        cur = conn.cursor()
        
        # drop tables  
        drop_btc = "drop table btc"
        drop_eth = "drop table eth"
        drop_ltc = "drop table ltc"
        
        cur.execute(drop_btc)
#        print("Table droped: aisles")
        cur.execute(drop_eth)
#        print("Table droped: departments")
        cur.execute(drop_ltc)
        
        drop_btc1 = "drop table btc1"
        drop_eth1 = "drop table eth1"
        drop_ltc1 = "drop table ltc1"
        
#        print("Table droped: order_products__prior")
        cur.execute(drop_btc1)
#        print("Table droped: order_products__train")
        cur.execute(drop_eth1)
#        print("Table droped: orders")
        cur.execute(drop_ltc1)
#        print("Table droped: products")
      
        query_create1 = "Create table btc(Dat string,Open float,High float,Low float,Close float,Adjclose float,Volume bigint) row format delimited fields terminated by ',' stored as textfile"
        cur.execute(query_create1)
        query_create2 = "Create table eth(Dat string,Open float,High float,Low float,Close float,Adjclose float,Volume bigint) row format delimited fields terminated by ',' stored as textfile"
        cur.execute(query_create2)
        query_create3 = "Create table ltc(Dat string,Open float,High float,Low float,Close float,Adjclose float,Volume bigint) row format delimited fields terminated by ',' stored as textfile"
        cur.execute(query_create3)
        
        query_load1 = "load data inpath '/btcfinal68.csv' OVERWRITE INTO TABLE btc"
        cur.execute(query_load1)
        query_load2 = "load data inpath '/ethfinal68.csv' OVERWRITE INTO TABLE eth"
        cur.execute(query_load2)
        query_load3 = "load data inpath '/ltcfinal68.csv' OVERWRITE INTO TABLE ltc"
        cur.execute(query_load3)
                
        query_drop1 = "create table btc1 as select Dat,Open,High,Low,Close,Volume from btc where Open is not Null"
        cur.execute(query_drop1)
        query_drop2 = "create table eth1 as select Dat,Open,High,Low,Close,Volume from eth where Open is not Null"
        cur.execute(query_drop2)
        query_drop3 = "create table ltc1 as select Dat,Open,High,Low,Close,Volume from ltc where Open is not Null"
        cur.execute(query_drop3)
                
        cur.close()
        
        
if __name__=="__main__":
    p=Project()
    p.download()
    p.hadoop_load()
    p.hive_load()       
        
        
        
        