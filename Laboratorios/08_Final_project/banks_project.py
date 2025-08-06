


# Importing the required libraries
import requests
import pandas as pd
import numpy as np
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime


# Code for ETL operations on Country-GDP data
#Extracción de datos
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name","MC_USD_Billion"]

#table_attribs = ["Rank","Bank name","Market cap"]
#table_attribs = ["Rank","Market cap"]
#table_attribs = ["Bank name"]
#table_attribs = ["Market cap"]

#transform CSV
csv_path = r'./exchange_rate.csv'

#carga database
db_name = 'Banks.db'
table_name = 'Largest_banks'

#carga CSV
output_path = './Largest_banks_data.csv'



def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f:
        f.write(timestamp + ' : '+message + '\n')


def extract(url, table_attribs):

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    #print(tables)
    #print('*************************************************')
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
                            #"Rank": col[0].contents[0].strip(),
                            "Name": col[1].text.strip(),
                            "MC_USD_Billion": float(col[2].contents[0].strip())
                            }
                
                df1 = pd.DataFrame(data_dict, index = [0])
                df = pd.concat([df,df1],ignore_index=True)
    #print(data_dict)    
    return df


def transform(df,csv_path):

    df1 = pd.read_csv(csv_path)
    #print(df1)
    #print('***********************************************')
    exchange_rate = df1.set_index('Currency').to_dict()['Rate']
    #print(dict)
   # print(df)
    #print('***********************************************')
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    #print(df)

    print(df['MC_EUR_Billion'][4])
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists ='replace', index=False)
    
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


log_progress('Preliminaries complete. Initiating ETL process')
#extracción de datos:
df = extract(url,table_attribs)
#print(data)

log_progress('Data extraction complete. Initiating Transformation process')
#Transformación de datos:
data_trans = transform(df,csv_path)

log_progress('Data transformation complete. Initiating loading process')
#Carga de datos:
    #Carga a Csv:
load_to_csv(data_trans, output_path)
log_progress('Data saved to CSV file')

    #Carga a Base de datos SQLite3:
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')
load_to_db(data_trans,sql_connection,table_name)
log_progress('Data loaded to Database as table. Running the query')

#Consulta a base de datos:
query_statement1 = f"SELECT * from {table_name}"
query_statement2 = f"SELECT AVG(MC_GBP_Billion) from {table_name}"
query_statement3 = f"SELECT Name from {table_name} LIMIT 5"

run_query(query_statement1, sql_connection)
run_query(query_statement2, sql_connection)
run_query(query_statement3, sql_connection)

log_progress('Process Complete.')
sql_connection.close()