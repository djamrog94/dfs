from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime
from datetime import timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

def batch_import(table, df):
    conn = psycopg2.connect(f'dbname=dfs user=postgres password=draftday')
    if len(df) > 0:
        df_columns = list(df)
        # create (col1,col2,...)
        columns = ",".join(df_columns)

        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 

        #create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {}".format(table,columns,values)

        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
        conn.commit()
        cur.close()

def get_data(start, end):
    driver = webdriver.Chrome()
    start = datetime.strptime(start, '%Y-%m-%d')
    end = datetime.strptime(end, '%Y-%m-%d')
    
    data = []
    while start < end:
        date = datetime.strftime(start, '%Y-%m-%d')
        url = f'https://rotogrinders.com/resultsdb/site/draftkings/date/{date}/sport/mlb/'
        driver.get(url)
        
        try:
            frame = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//h2"))).text
            items = frame.split(' - ')
            time = items[0]
            hi = items[-1].split(' ')
            games = hi[0]
            id = hi[-1][1:-1]
            data.append([int(id), date, time, int(games)])
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,  "//div[@class=' ant-tabs-tab' and text()='Contests']"))).click()
            links = WebDriverWait(driver, 5).until(EC.presence_of_all_elements_located((By.XPATH, "//a")))
            bad = ['', 'Close', 'Home', 'Contest']
            # we now have the links for all contests
            sorted = [x for x in links if x.text not in bad]
            # once we navigate to contest page
            data = WebDriverWait(driver, 5).until(EC.presence_of_all_elements_located((By.XPATH, "//tbody[@class='ant-table-tbody']")))
            #data[0] gets us the top x winners
            #data[1] gets us the line up for given player
            # gets us links for each person
            ppl = WebDriverWait(driver, 5).until(EC.presence_of_all_elements_located((By.XPATH, "//a")))
        except:
            print(f'No games on {date}')

        start = start + timedelta(days=1)
        print(f'moving on to next day {date}')
    df = pd.DataFrame(data, columns=['slate_id', 'slate_date', 'slate_time', 'num_games'])
    df.to_csv('slates.csv')

get_data('2018-03-29', '2020-11-29')
