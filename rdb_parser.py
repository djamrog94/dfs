import psycopg2
import psycopg2.extras
import json
import pandas as pd
import db_import
import os

class Parser():
    year = 0
    raw_slate = []
    raw_game = []
    raw_player = []
    raw_contest = []
    raw_entries = []
    slate_df = pd.DataFrame()
    player_df = pd.DataFrame()

    def __init__(self):
        self.conn = psycopg2.connect(f'dbname=dfsv1 user=postgres password=draftday')
        self.cur = self.conn.cursor()

    def reset(self):
        self.raw_slate = []
        self.raw_game = []
        self.raw_player = []
        self.raw_contest = []
        self.raw_entries = []

    def parse_slate(self, data):
        '''
        slate: the slate file, json
        '''
        slates = [i for i in data if i['sport'] == 2]

        for slate in slates:
            slate_id = slate['_id']
            # check in db if slate already exists!
            # if self.check_slate(slate_id):
            #     continue

            self.year = int(slate['start'][:4])
            self.raw_slate.append({'site_slate_id': slate_id,
            'slate_date': slate['start'][:10]})
        
            games = slate['slateGames']
            for game in games:
                home = self.convert_roto_to_retro(game['teamHome']['hashtag'])
                away = self.convert_roto_to_retro(game['teamAway']['hashtag'])
                opp_total = None
                try:
                    movement = game['vegas']['movement']
                    line = game['vegas']['line']
                    total = game['vegas']['total']
                    opp_total: opp_total
                    over_under = game['vegas']['o/u']
                except:
                   continue

                self.raw_game.append({'game_id': game['id'], 
                'site_slate_id': slate_id,
                'movement': movement,
                'line': line,
                'total': total,
                    'over_under': over_under,
                    'team_away': away,
                    'team_home': home})
    
    def parse_contest(self, data):
        for dd in data:
            d = dd[0]
            max1 = None
            try:
                max1 = d['maxEntries']
            except:
                pass
            self.raw_contest.append({'contest_id': d['_id'],
            'slate_id': d['_slateId'],
            'contest_name': d['name'],
            'prize_pool': d['prizePool'],
            'buy_in': d['entryFee'],
            'max_entries_per_user': d['maxEntriesPerUser'],
            'max_entries': max1,
            'entries': d['entryCount'],
            'rg_prize_pool': d['rgPrizePool'],
            'rg_prize_winner': d['rgPrizeWinnerCount'],
            'prize': json.dumps(d['prizes']) 
            })

            players = d['playerOwnership']
            for player in players:
                try:
                    ownership = player['actualOwnership']
                    salary = int(player['salary'])
                    player_id = self.get_player_id(player['name'], player['team'])
                except:
                    continue
                self.raw_player.append({'slate_player_id': player['_id'],
                'site_slate_id': d['_slateId'],
                'player_id': player_id,
                'salary': salary,
                    'ownership': ownership,
                    'fantasy_points': player['actualFpts']})



    def parse_entry(self, data):
        '''
        pass all json files into here
        '''
        for idx, file in enumerate(data):
            for entry in file['entries']:
                if entry['lineup'] == '' or len(entry['lineup']['summary']) != 10 \
                 or not self.check_length(entry):
                    continue

                self.raw_entries.append({'entry_id': entry['_id'],
                'contest_id': entry['_contestId'],
                'user_entry_count': entry['userEntryCount'],
                'username': entry['siteScreenName'],
                'entry_rank': entry['rank'],
                'points': entry['points'],
                'salary_used': entry['salaryUsed'],
                'pitcher_1': self.check_player(entry['lineup']['P'][0]['_slatePlayerId']),
                'pitcher_2': self.check_player(entry['lineup']['P'][1]['_slatePlayerId']),
                'catcher': self.check_player(entry['lineup']['C'][0]['_slatePlayerId']),
                'first_base': self.check_player(entry['lineup']['1B'][0]['_slatePlayerId']),
                'second_base': self.check_player(entry['lineup']['2B'][0]['_slatePlayerId']),
                'short_stop': self.check_player(entry['lineup']['SS'][0]['_slatePlayerId']),
                'third_base': self.check_player(entry['lineup']['3B'][0]['_slatePlayerId']),
                'outfield_1': self.check_player(entry['lineup']['OF'][0]['_slatePlayerId']),
                'outfield_2': self.check_player(entry['lineup']['OF'][1]['_slatePlayerId']),
                'outfield_3': self.check_player(entry['lineup']['OF'][2]['_slatePlayerId']),
                })
            if idx % 1000 == 0:
                print(f'{idx} file out of {len(data)}')

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

    def get_player_id(self, name, team):
        '''
        convert team roto to retro
        '''
        converted_team = self.convert_roto_to_retro(team)
        first = name.split(' ')[0]
        last = name.split(' ')[1]
        self.cur.callproc("get_player_id", [first, last, converted_team, self.year])
        return self.cur.fetchone()[0]

    def convert_roto_to_retro(self, team):
        df = pd.read_csv('team_map.csv')
        row = df[df['BBREFTEAM']==team]
        return row['RETROSHEET'].values[0]
    
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
        self.parse_slate(data)
        print('done with slate')
        slate_df = pd.DataFrame(self.raw_slate)
        game_df = pd.DataFrame(self.raw_game)
        # parse contests
        all_contests = os.listdir(f'{base}/{date}/ownership')
        data = []
        for contest in all_contests:
            with open(f'{base}/{date}/ownership/{contest}') as f:
                data.append(json.load(f))
        self.parse_contest(data)
        print('done with contest')
        contest_df = pd.DataFrame(self.raw_contest)
        self.player_df = pd.DataFrame(self.raw_player)
        # parse entries
        all_entries = os.listdir(f'{base}/{date}/entry')
        data = []
        for entry in all_entries:
            with open(f'{base}/{date}/entry/{entry}') as f:
                data.append(json.load(f))
        print('start with entry')
        self.parse_entry(data)
        print('done with entry')
        entry_df = pd.DataFrame(self.raw_entries)
        # commit data to db
        items = {'slate': slate_df,
        'slate_game': game_df,
        'contest': contest_df,
        'slate_player': self.player_df,
        'entry': entry_df}

        for table in items.keys():
            db_import.batch_import(table, items[table])


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
    p.run('07-29-2020')

if __name__ == "__main__":
    main()
   