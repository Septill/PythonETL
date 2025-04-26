# Data URL 
# https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks
# CSV path https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv

import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime
import os

log_file = "code_log.txt" 
conn = sqlite3.connect('Banks.db')
target_file = "Largest_banks_data.csv"
url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
df = pd.DataFrame(columns=["Name","MC_USD_Billion"])

table_name = 'Largest_banks'
attribute_list = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    try:
        log_path = os.path.join(os.getcwd(), "code_log.txt")
        
        with open(log_path, "a+", buffering=1) as f:  
            timestamp = datetime.now().strftime('%Y-%h-%d-%H:%M:%S')
            f.write(f"{timestamp},{message}\n")
            os.fsync(f.fileno()) 
        
        print(f"[LOG] {message}")  
    except Exception as e:
        print(f"Log is failed（context：{message}）: {str(e)}")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    log_progress("Starting data extraction")
    
    # get context from html
    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, 'html.parser')
    
    # find tables
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    # retrieve data
    data = []
    for row in rows[1:]:  # jump the header
        cols = row.find_all('td')
        if len(cols) >= 2:
            name = cols[1].get_text(strip=True)
            market_cap = cols[2].get_text(strip=True)
            name = name.split('[')[0]
            market_cap = float(market_cap.split('[')[0])
            data.append({"Name": name, "MC_USD_Billion": market_cap})
    
    df = pd.DataFrame(data, columns=table_attribs)
    log_progress("Data extraction complete")
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    log_progress("Starting data transformation")
    
    # read the exchages
    exchange_rates = pd.read_csv(csv_path)
    
    rates = {
        'GBP': exchange_rates.loc[exchange_rates['Currency'] == 'GBP', 'Rate'].values[0],
        'EUR': exchange_rates.loc[exchange_rates['Currency'] == 'EUR', 'Rate'].values[0],
        'INR': exchange_rates.loc[exchange_rates['Currency'] == 'INR', 'Rate'].values[0]
    }
    
    # add new colums
    df['MC_GBP_Billion'] = [np.round(x * rates['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * rates['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * rates['INR'], 2) for x in df['MC_USD_Billion']]
    
    log_progress("Data transformation complete")
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    log_progress("Saving data to CSV")
    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    log_progress("Loading data to database")
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress("Data loaded to database")

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    log_progress(f"Running query: {query_statement}")
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    rows = cursor.fetchall()
    
    for row in rows:
        print(row)
    
    log_progress("Query execution complete")

# run main process
log_progress("Preliminaries complete. Starting ETL process")

# run ETL process
df = extract(url, ["Name", "MC_USD_Billion"])
df = transform(df, "exchange_rate.csv")
load_to_csv(df, target_file)
load_to_db(df, conn, table_name)

# queries
query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, conn)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, conn)

query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, conn)

# deconnect from DB
conn.close()
log_progress("Process complete")