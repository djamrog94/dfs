from rdb_parser import Parser
from threading import Thread, Lock
import os
import pandas as pd
from datetime import datetime
import time
import random

class Controller():
    base = 'datav1'
    dates = []

    def __init__(self):
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print(f'Starting Program | {current_time}')
        df = pd.read_csv('2020t.TXT', header=None)
        df['day'] = df.apply(self.date, axis=1)
        self.dates = list(df['day'].drop_duplicates().values)
        self.lock = Lock()
        
    def date(self, r):
        string = str(r[0])
        return f'{string[4:6]}/{string[-2:]}/{string[:4]}'

    def scrape(self, thread_num):
        while len(self.dates) != 0:
            with self.lock:
                date = self.dates.pop()
            fdate = date.replace('/', '-')

            print(f'THREAD NO: {thread_num} | started parsing {fdate} | Remaining: {len(self.dates)}')
            # parse scraped data
            pp = Parser()
            pp.run(fdate)
            now = datetime.now()

            current_time = now.strftime("%H:%M:%S")
            print(f'THREAD NO: {thread_num} | finished day {fdate}: {current_time}')
        print(f'THREAD NO: {thread_num} FINISHED')
 

    def run(self):
        threads = []
        for i in range(os.cpu_count()):
            threads.append(Thread(target=self.scrape, kwargs=dict(thread_num=i)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
            

def main():
    control = Controller()
    control.run()

main()