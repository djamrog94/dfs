from rdb_models import *
from collections import Counter
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import psycopg2
import pandas as pd

class Player():
    def __init__(self, player):
        self.player = player
        self.session = self.create_session()

    def create_session(self):
        engine = create_engine('postgresql://postgres:draftday@localhost:5432/dfsv1')
        Session = sessionmaker(bind=engine)
        Session.configure(bind=engine)
        session = Session()
        return session
    
    def get_name(self):
        return self.player

    def get_last(self):
        return self.player.split(' ')[1]

    def get_first(self):
        return self.player.split(' ')[0]

    def get_position(self):
        pass

    def get_era(self, date, num_games=None, start_date=None):
        '''
        date in string form yyyy-mm-dd
        num_game: last x games included
        start_date: games in range starting from here
        '''
        year = date[:4]
        d = date.replace('-', '')[2:]
        p = self.session.query(Roster).filter(Roster.last_name==self.get_last()) \
                                    .filter(Roster.first_name==self.get_first()) \
                                    .filter(Roster.year==year).first()
        pid = p.player_id

        conn = psycopg2.connect("dbname='dfsv1' user='postgres' host='localhost' password='draftday'")
        cur.execute(f"SELECT pitch_seq_tx, event_tx from event where game_id= \
            (SELECT game_id from game where game_dt={d} and (away_start_pit_id='{pid}' or home_start_pit_id='{pid}')) ")
        print('hi')


    


class At_bat():
    pitch_map = {'+': 'following pickoff throw by the catcher',
                '*': 'indicates the following pitch was blocked by the catcher',
                '.': 'marker for play not involving the batter',
                '1': 'pickoff throw to first',
                '2': 'pickoff throw to second',
                '3': 'pickoff throw to third',
                '>': 'Indicates a runner going on the pitch',
                'B': 'ball',
                'C': 'called strike',
                'F': 'foul',
                'H': 'hit batter',
                'I': 'intentional ball',
                'K': 'strike (unknown type)',
                'L': 'foul bunt',
                'M': 'missed bunt attempt',
                'N': 'no pitch (on balks and interference calls)',
                'O': 'foul tip on bunt',
                'P': 'pitchout',
                'Q': 'swinging on pitchout',
                'R': 'foul ball on pitchout',
                'S': 'swinging strike',
                'T': 'foul tip',
                'U': 'unknown or missed pitch',
                'V': 'called ball because pitcher went to his mouth',
                'X': 'ball put into play by batter',
                'Y': 'ball put into play on pitchout'
                }
    hit_map = {'S': 'single',
                    'D': 'double',
                    'T': 'triple',
                    'H': 'home run'}
    hit_modfier_map = {'+': 'hard', '-': 'soft'}
    position_map = {'1': 'P', '2': 'C', '3': '1B',
                    '4': '2B', '5': '3B', '6': 'SS',
                    '7': 'LF', '8': 'CF', '9': 'RF'}
    location_map = {}


    def __init__(self, game_id, bat_id):

        # self.type_hit = 0
        # self.location = 0
        # self.rbi = 0
        # # true if L/R or R/L
        # self.split = False
        # self.event = # get the id
        # self.pitches = # get the pitches
        # self.
        # self.num_pitches = len(self.pitches)
        self.game_id = game_id
        self.bat_id = bat_id
        self.pitches = ''

    def _parse(self):
        pass




    def is_gb(self):
        pass







class Slate_player():
    def __init__(self, player):
        assert isinstance(player, Player), "Input variable should be Player object"
        self.player = player


def main():
    hi = Player('Aaron Nola')
    hi.get_era('2020-09-27', 5)
    print('test')

main()