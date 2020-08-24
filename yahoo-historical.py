#!/usr/bin/env python
# coding: utf-8

# In[8]:


#collect historical stock data and store in MongoDB 

import time
import csv
from pymongo import MongoClient
#pip install yahoo-historical
#pip install fetcher
from yahoo_historical import Fetcher
import pandas as pd
#connect to MongoDB using pymongo
client = MongoClient('localhost', 27017)
db = client['historical_stock_data']
collection = db['yahoo_historical']

#path to csv containing ticker symbols
with open('path_to_csv' , newline= '', encoding='utf-8-sig') as csv_file:
    symbol_reader = csv.reader(csv_file)
    for x in symbol_reader:
        y = ''.join(x)    
        #Select start/end dates for stock data
        data = Fetcher(y, [2009,1,1], [2019,1,1])
        data = data.getHistorical()
        df = pd.DataFrame(data)
        try:
            df = df[["Date","Close"]]
            col_length = (len(df[["Close"]])) 
            #print(col_length)
            symbol = []
            for x in range(col_length):
                symbol.append(y) 
            df['Symbol'] = symbol
            df.to_csv('/Users/a/Desktop/stocks alpha csv/stocks_u_z.csv', mode='a', columns=['Date', 'Close', 'Symbol'], header=False)
        except:
            print(y)
