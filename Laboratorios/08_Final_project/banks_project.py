


# Importing the required libraries
import requests
import pandas as pd
import numpy as np
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime


# Code for ETL operations on Country-GDP data
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'

table_attribs = ["Rank","Bank name","Market cap"]
#table_attribs = ["Bank name","Market cap"]

#table_attribs = ["Rank","Market cap"]
#table_attribs = ["Bank name"]
#table_attribs = ["Market cap"]

def extract(url, table_attribs):

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    #print(tables)
    print('*************************************************')
    rows = tables[0].find_all('tr')
    #print(rows)

  
    for row in rows:
        col = row.find_all('td')
        #print(col)
        if len(col)!=0:
            if col[1].find('a') is not None:
                #bank_a = col[1].text
                #rank_a = col[1].a
                #rank_b = col[1].a.contents[0]
                data_dict = {
                            "Rank": col[0].contents[0],
                            "Bank name": col[1].text,
                            "Market cap": col[2].contents[0] 
                            }
                df1 = pd.DataFrame(data_dict, index = [0])
                df = pd.concat([df,df1],ignore_index=True)
        
    return df

data = extract(url,table_attribs)





