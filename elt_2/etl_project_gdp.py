# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db_name = 'World_Economies.db'
table_attributes = ['Country', 'GDP_USD_millions']
table_name = 'Countries_by_GDP'
json_path = './Countries_by_GDP.json'
log_file = "etl_project_log.txt"

connect = sqlite3.connect(db_name)


def extract(url, table_attributes):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    # Get all tbody in html
    tables = data.find_all('tbody')
    # data table is 3rd => index is 2. Get all rows from the 3rd table
    rows = tables[2].find_all('tr')
    df = pd.DataFrame(columns=table_attributes)
    # start from 4th row because 1st and 2nd row is header and title and 3rd is World row
    for row in rows[3:]:
        # scraping all columns from row
        col = row.find_all('td')
        if len(col) != 0:
            if col[2].contents[0] != "—":
                data_dict = {
                    "Country": col[0].a.contents[0],
                    "GDP_USD_millions": col[2].contents[0]
                }

            # Create a dataframe from dictionary
            df1 = pd.DataFrame(data_dict, index=[0])
            # Add dictionary (df1) to df
            df = pd.concat([df, df1], ignore_index=True)
    # dataframe will be ['Country','GPD_USD_millions']
    return df


def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [round(x/1000, 2) for x in GDP_list]
    # override GDP_USD_millions data by transformed GDP_USD_billions
    df["GDP_USD_millions"] = GDP_list
    # rename the header
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})

    return df


def load_to_json(df, json_path):
    print(df)
    df.to_json(json_path)


def load_to_db(df, connect, table_name):
    df.to_sql(table_name, connect, if_exists='append', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
# df = extract(url, table_attributes)

# df = transform(df)


def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''

    ''' Here, you define the required entities and call the relevant 
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


# Log the initialization of the ETL process
log_progress("ETL Job Started")

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
extracted_data = extract(url, table_attributes)

# Log the completion of the Extraction process
log_progress("Extract phase Ended")

# Log the beginning of the Transformation process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Loading process to SQL
log_progress("Load phase Started")
load_to_db(transformed_data, connect, table_name)

# Log the completion of the Loading process to SQL
log_progress("Load phase Ended")

# Log the beginning of the Loading process to JSON
log_progress("Load phase Started")
load_to_json(transformed_data, json_path)

# Log the completion of the Loading process to JSON
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, connect)
connect.close()
log_progress('Process Complete.')
