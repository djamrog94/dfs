import psycopg2
import psycopg2.extras
import json
import pandas as pd
import db_import
import os
import logging

class Parser():
    year = 0
    raw_slate = []
    raw_game = []
    raw_player = []
    raw_contest = []
    raw_entries = []
    slate_ids = {}
    contest_ids = {}
    player_ids = {}
    slate_df = pd.DataFrame()
    player_df = pd.DataFrame()

    def __init__(self):
        self.conn = psycopg2.connect(f'dbname=dfsv1 user=postgres password=draftday')
        self.cur = self.conn.cursor()
        self.log = logging.getLogger("my-logger")
        self.player_ids = self.get_player_ids()

    def reset(self):

        self.raw_slate = []
        self.raw_game = []
        self.raw_player = []
        self.raw_contest = []
        self.raw_entries = []

    def parse_slate(self, data, date):
        '''
        slate: the slate file, json
        '''
        slates = [i for i in data if i['sport'] == 2]

        for idx, slate in enumerate(slates):
            self.slate_id = slate['_id']
            self.year = int(slate['start'][:4])
            s_id = f"s{date.replace('-', '')}{idx}"
            self.slate_ids[self.slate_id] = s_id
            self.raw_slate.append({'id': s_id,'slate_date': date})
        
            games = slate['slateGames']
            for game in games:
                res = []
                items= [
                'movement',
                'line',
                'total',
                'o/u',
                'team_away',
                'team_home',
                'slate_id']
                for item in items[:-3]:
                    try:
                        res.append(game['vegas'][item])
                    except:
                        res.append(None)

                try:
                    home = self.convert_roto_to_retro(game['teamHome']['hashtag'])
                    away = self.convert_roto_to_retro(game['teamAway']['hashtag'])
                except:
                    continue
                items[3] = 'o_u'
                res.extend([home, away, s_id])

                self.raw_game.append(dict(zip(items,res)))
    
    def parse_contest(self, data, date):
        for idx, dd in enumerate(data):
            d = dd[0]
            slate_id = d['_slateId']
            s_id = self.slate_ids[slate_id]
            res = []
            c_id = f"c{date.replace('-', '')}{idx}"
            res.append(c_id)
            self.contest_ids[d['_id']] = c_id
            res.append(s_id)
            items = ['id','slate_id',
            'name',
            'prizePool',
            'entryFee',
            'maxEntriesPerUser',
            'maxEntries',
            'entryCount',
            'rgPrizePool',
            'rgPrizeWinnerCount',
            'prizes']
            for item in items[2:-1]:
                try:
                   res.append(d[item])
                except:
                    res.append(None)

            res.append(json.dumps(d['prizes']))

            self.raw_contest.append(dict(zip(items,res)))

            players = d['playerOwnership']
            items = ['player_id', 'slate_id', 'salary', 'actualOwnership', 'actualFpts']
            
            for player in players:
                player_data = []
                try:
                    p_id = self.get_player_id(player['name'])
                except:
                    continue
                player_data.append(p_id)
                player_data.append(self.slate_ids[d['_slateId']])
                try:
                    int(player['salary'])
                except:
                    continue
                for item in items[2:]:
                    try:
                        player_data.append(player[item])          
                    except:
                        player_data.append(None) 

                self.raw_player.append(dict(zip(items,player_data)))


    def parse_entry(self, data, date):
        '''
        pass all json files into here
        '''
        for idx, file in enumerate(data):
            for entry in file['entries']:
                if entry['lineup'] == '' \
                 or not self.check_length(entry):
                    continue
                lineup = [['P', 0], ['P', 1], ['C', 0], ['1B', 0], ['2B', 0], ['SS', 0], ['3B', 0],
                ['OF',0], ['OF', 1], ['OF', 2]]

                res = []
                for l in lineup:
                    try:
                        name = entry['lineup'][l[0]][l[1]]['name']
                        res.append(name)
                    except:
                        res.append(None)

                keys = ['contest_id', 'entry_date', 'user_entry_count', 'username', 'entry_rank', 'points', 'salary_used',
                'pitcher_1', 'pitcher_2', 'catcher', 'first_base', 'second_base', 'short_stop', 'third_base',
                'outfield_1', 'outfield_2', 'outfield_3']
                ans = []
                ans.append(self.contest_ids[entry['_contestId']])
                ans.append(date)
                ans.append(entry['userEntryCount'])
                ans.append(entry['siteScreenName'])
                ans.append(entry['rank'])
                ans.append(entry['points'])
                ans.append(entry['salaryUsed'])
                ans.extend(res)
                
                self.raw_entries.append(dict(zip(keys, ans)))

    def check_player(self, id):
        if id in self.player_df['slate_player_id'].values:
           return id
        return None 
            
    
    def check_length(self, d):
        try:
            if len(d['lineup']['P']) != 2 or len(d['lineup']['OF']) != 3 or \
                len(d['lineup']['C']) != 1 or len(d['lineup']['1B']) != 1 or \
                    len(d['lineup']['2B']) != 1 or len(d['lineup']['3B']) != 1 or \
                        len(d['lineup']['SS']) != 1:
                        return False
        except:
            return False

        return True

    def get_player_id(self, name):
        '''
        convert team roto to retro
        '''
        first = name.split(' ')[0]
        last = name.split(' ')[1]
        self.cur.callproc("get_player_id", [first, last])
        return self.cur.fetchone()[0]

    def convert_roto_to_retro(self, team):
        df = pd.read_csv('team_map.csv')
        row = df[df['BBREFTEAM']==team]
        return row['RETROSHEET'].values[0]

    def get_player_ids(self):
        self.cur.execute("SELECT player_id, last_name, first_name FROM player")
        data = self.cur.fetchall()
        df = pd.DataFrame(data, columns=['player_id', 'last_name', 'first_name'])
        keys = tuple(zip(df['first_name'], df['last_name']))
        values = df['player_id']
        return dict(zip(keys, values))

    
    def check_slate(self, slate_id):
        self.cur.execute("SELECT site_slate_id FROM slate WHERE site_slate_id = %s", (slate_id,))
        return self.cur.fetchone() is not None

    def run(self, date):
        self.reset()
        self.year = int(date[-4:])
        base = 'datav1'
        # parse slate
        with open(f'{base}/{date}/slate/slate.json') as f:
            data = json.load(f)
        self.parse_slate(data, date)
        db_import.batch_import('slate', pd.DataFrame(self.raw_slate))
        db_import.batch_import('slate_game', pd.DataFrame(self.raw_game))
        # print('done with slate / game')
        #slate_df = pd.DataFrame(self.raw_slate)
        #game_df = pd.DataFrame(self.raw_game)
        # parse contests
        all_contests = os.listdir(f'{base}/{date}/ownership')
        data = []
        for contest in all_contests:
            with open(f'{base}/{date}/ownership/{contest}') as f:
                data.append(json.load(f))
        self.parse_contest(data, date)
        # print('done with contest')
        #contest_df = pd.DataFrame(self.raw_contest)
        #self.player_df = pd.DataFrame(self.raw_player)
        db_import.batch_import('contest', pd.DataFrame(self.raw_contest))
        db_import.batch_import('slate_player', pd.DataFrame(self.raw_player))
        # parse entries
        all_entries = os.listdir(f'{base}/{date}/entry')
        data = []
        for entry in all_entries:
            with open(f'{base}/{date}/entry/{entry}') as f:
                data.append(json.load(f))
        self.parse_entry(data, date)
        db_import.batch_import('entry', pd.DataFrame(self.raw_entries))
        # entry_df = pd.DataFrame(self.raw_entries)
        # commit data to db
        # items = {'slate': slate_df,
        # 'slate_game': game_df,
        # 'contest': contest_df,
        # 'slate_player': self.player_df,
        # 'entry': entry_df}

        # for table in items.keys():
        #     db_import.batch_import(table, items[table])


def main():
    p = Parser()
    # date = '07-29-2020'
    # base = 'datav1'
    # all_entries = os.listdir(f'{base}/{date}/entry')
    # data = []
    # for entry in all_entries:
    #     with open(f'{base}/{date}/entry/{entry}') as f:
    #         data.append(json.load(f))
    # p.parse_entry(data)
    # db_import.batch_import('entry', pd.DataFrame(p.raw_entries))
    p.run('03-20-2019')

if __name__ == "__main__":
    main()
