import scrapy
import json
import random

class RdbSpider(scrapy.Spider):
    idx = 0
    name = 'rdb'
    start_urls = ['https://rotogrinders.com/resultsdb/']

    def __init__(self, date):
        self.date = date
        self.fdate = date.replace('/', '-')
        self.base = 'datav1'
        self.headers = {
            'authority': 'resultsdb-api.rotogrinders.com',
            'method': 'get',
            'path': f'/api/slates?start={date}&site=20',
            'scheme': 'https',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'origin': 'https://rotogrinders.com',
            'pragma': 'no-cache',
            'referer': 'https://rotogrinders.com/',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': self.pick_agent()
            }
            
    def pick_agent(self):
        agents =  ['Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36']
        idx = random.randint(0, len(agents) - 1)
        return agents[idx]

    def parse(self, response):
        '''
        this function starts by grabbing the main screen and passing along on each slate. this is not sport specific, so we need to determine
        which is baseball data and what is not.

        yields: each slate containing sports, types id, slate name
        '''

        url = f'https://resultsdb-api.rotogrinders.com/api/slates?start={self.date}&site=20'

        request = scrapy.Request(url, callback=self.parse_slate, headers=self.headers)

        yield request

    def parse_slate(self, response):
        '''
        this function starts by grabbing the main screen and passing along on each slate. this is not sport specific, so we need to determine
        which is baseball data and what is not.

        yields: only the baseball contests from the slates
        '''
        data = json.loads(response.body)
        filename = f'slate.json'
        with open(f'{self.base}/{self.fdate}/slate/{filename}', 'w') as f:
            json.dump(data, f)
        self.log(f'Saved file {filename}')
        # baseball is sport 2
        slates = [i['_id'] for i in data if i['sport'] == 2]

        path = f'https://resultsdb-api.rotogrinders.com/api/contests?slates={",".join(slates)}&lean=true'
        request = scrapy.Request(path, callback=self.parse_contest, headers=self.headers)

        yield request

    def parse_contest(self, response):
        '''
        this function is given all the contests, so needs to iterate through each one

        yields: only the baseball contests from the slates
        '''
        data = json.loads(response.body)

        for i in data:
            id = i['_id']

            url1 = f'https://resultsdb-api.rotogrinders.com/api/contests?_id={id}&ownership=True'
            request = scrapy.Request(url1, callback=self.ownership, headers=self.headers)
            yield request

            url = f'https://resultsdb-api.rotogrinders.com/api/entries?_contestId={id}&sortBy=points&order=desc&index=0&maxFinish=3855&players=&users=false&username=&isLive=false&minPoints=&maxSalaryUsed=&incomplete=false&positionsRemaining=&requiredStartingPositions='
            request = scrapy.Request(url, callback=self.parse_entry, headers=self.headers)
            yield request


    def ownership(self, response):
        data = json.loads(response.body)
        id = data[0]['_id']
        filename = f'{id}_ownership.json'
        with open(f'{self.base}/{self.fdate}/ownership/{filename}', 'w') as f:
            json.dump(data, f)
        self.log(f'Saved file {filename}')

    def parse_entry(self, response):
        data = json.loads(response.body)

        if int(data['count']) != 0:
            page = data['page']
            if page is None:
                page = 0
            id = data['entries'][0]['_contestId']
            url = f'https://resultsdb-api.rotogrinders.com/api/entries?_contestId={id}&sortBy=points&order=desc&index={page}'
            request = scrapy.Request(url, self.parse_entry, headers=self.headers)
            yield request

        output = json.loads(response.body)
        o_page = output['page']
        o_id = output['entries'][0]['_contestId']
        filename = f'{o_id}_{o_page}.json'
        with open(f'{self.base}/{self.fdate}/entry/{filename}', 'w') as f:
            json.dump(output, f)
        self.log(f'Saved file {filename}')
