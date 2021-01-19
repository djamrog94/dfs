from rdb_parser import Parser
import os
import pandas as pd
from datetime import datetime
import time
import random

class Controller():
    base = 'datav1'

    def get_dates(self):
        df = pd.read_csv('1920.TXT', header=None)
        df['day'] = df.apply(self.date, axis=1)
        return df['day'].drop_duplicates().values
        
    def date(self, r):
        string = str(r[0])
        return f'{string[4:6]}/{string[-2:]}/{string[:4]}'

    def scrape(self):
        dates = self.get_dates()
        for idx, date in enumerate(dates):
            fdate = date.replace('/', '-')
            # try:
            #     os.mkdir(f'{self.base}/{fdate}')
            #     os.mkdir(f'{self.base}/{fdate}/entry')
            #     os.mkdir(f'{self.base}/{fdate}/slate')
            #     os.mkdir(f'{self.base}/{fdate}/ownership')
            # except:
            #     continue

            #run scraper
            # print(f'started scraping {fdate} | {idx}/{len(dates)}')
            # os.system(f'scrapy crawl rdb -a date={date}')

            print(f'started parsing {fdate} | {idx}/{len(dates)}')
            # # parse scraped data
            pp = Parser()
            pp.run(fdate)
            now = datetime.now()

            current_time = now.strftime("%H:%M:%S")
            print(f'finished day {fdate} | {idx}/{len(dates)}: {current_time}')
            #time.sleep(random.randint(5,15))

def main():
    control = Controller()
    control.scrape()

main()