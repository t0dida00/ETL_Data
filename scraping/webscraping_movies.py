import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './top_50_films.csv'

log_file = "etl_project_log.txt"
target_file = "transformed_data.csv"

df = pd.DataFrame(columns=["Film", "Year", "Rotten Tomatoes' Top 100"])
count = 0

html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

for row in rows:
    if count < 25:
        col = row.find_all('td')
        if len(col) != 0:
            print(col)
            year_value = col[2].contents[0].strip()
            if year_value.isdigit():  # Check if the year is numeric
                year = int(year_value)  # Convert the year to an integer
                if 2000 <= year <= 2009:  # Check if the year is in the 2000s
                    data_dict = {
                        "Film": col[1].contents[0],
                        "Year": year,
                        "Rotten Tomatoes' Top 100": col[3].contents[0]
                    }
                    df1 = pd.DataFrame(data_dict, index=[1])
                    df = pd.concat([df, df1], ignore_index=True)
                    count += 1
    else:
        break

df.to_csv(csv_path)

conn = sqlite3.connect(db_name)

df.to_sql(table_name, conn, if_exists='replace', index=False)

conn.close()
