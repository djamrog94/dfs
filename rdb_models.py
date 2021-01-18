from sqlalchemy import Column, Integer, Date, String, ForeignKey, Float, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()
class Slate(Base):
    __tablename__ = 'slate'
    site_slate_id = Column(String, primary_key=True)
    slate_date = Column(Date)
    game = relationship('Slate_game')
    contest = relationship('Contest')
    player = relationship('Slate_player')
    # relationship function

class Contest(Base):
    __tablename__ = 'contest'
    contest_id = Column(String, primary_key=True)
    slate_id = Column(String, ForeignKey('slate.site_slate_id'))
    contest_name = Column(String)
    prize_pool = Column(Float)
    buy_in = Column(Float)
    max_entries = Column(Integer)
    max_entries_per_user = Column(Integer)
    entries = Column(Float)
    rg_prize_pool = Column(Float)
    rg_prize_winner = Column(Float)
    prize = Column(JSON)

    entry = relationship('Entry')

class Slate_game(Base):
    __tablename__ = 'slate_game'
    game_id = Column(Integer, primary_key=True)
    movement = Column(Float)
    line = Column(Float)
    total = Column(Float)
    over_under = Column(Float)
    team_away = Column(String, ForeignKey('team.team_abbr'))
    team_home = Column(String, ForeignKey('team.team_abbr'))
    site_slate_id = Column(String, ForeignKey('slate.site_slate_id'))
    # relationship function


class Slate_player(Base):
    __tablename__ = 'slate_player'
    slate_player_id = Column(String, primary_key=True)
    site_slate_id = Column(String, ForeignKey('slate.site_slate_id'))
    player_id = Column(String, ForeignKey('player.player_id'))
    salary = Column(Integer)
    ownership = Column(Float)
    fpts = Column(Float)

    pitcher_1 = relationship('Entry')
    pitcher_2 = relationship('Entry')
    catcher = relationship('Entry')
    first_base = relationship('Entry')
    second_base = relationship('Entry')
    short_stop = relationship('Entry')
    third_base = relationship('Entry')
    outfield_1 = relationship('Entry')
    outfield_2 = relationship('Entry')
    outfield_3 = relationship('Entry')

class Entry(Base):
    __tablename__ = 'entry'
    entry_id = Column(String, primary_key=True)
    contest_id = Column(String, ForeignKey('contest.contest_id'))
    user_entry_count = Column(Integer)
    username = Column(String)
    entry_rank = Column(Integer)
    points = Column(Float)
    salary_used = Column(Integer)
    pitcher_1 = Column(String, ForeignKey('slate_player.slate_player_id'))

    pitcher_2 = Column(String, ForeignKey('Slate_player.slate_player_id'))
    catcher = Column(String, ForeignKey('Slate_player.slate_player_id'))
    first_base = Column(String, ForeignKey('Slate_player.slate_player_id'))
    second_base = Column(String, ForeignKey('Slate_player.slate_player_id'))
    short_stop = Column(String, ForeignKey('Slate_player.slate_player_id'))
    third_base = Column(String, ForeignKey('Slate_player.slate_player_id'))
    outfield_1 = Column(String, ForeignKey('Slate_player.slate_player_id'))
    outfield_2 = Column(String, ForeignKey('Slate_player.slate_player_id'))
    outfield_3 = Column(String, ForeignKey('Slate_player.slate_player_id'))

class Player(Base):
    __tablename__ = 'player'
    player_id = Column(String, primary_key=True)
    last_name = Column(String)
    first_name = Column(String)
    batting_hand = Column(String)
    throwing_hand = Column(String)
    position = Column(String)
    player = relationship('Slate_player')
    roster = relationship('Roster')

class Team(Base):
    __tablename__ = 'team'
    team_abbr = Column(String, primary_key=True)
    league = Column(String)
    city = Column(String)
    team_name = Column(String)
    # team_away = relationship('Slate_game', foreign_keys=['team_abbr'])
    # team_home = relationship('Slate_game', foreign_keys=['team_abbr'])

    roster = relationship('Roster')

class Roster(Base):
    __tablename__ = 'roster'
    player_id = Column(String, ForeignKey('player.player_id'), primary_key=True)
    team_abbr = Column(String, ForeignKey('team.team_abbr'), primary_key=True)
    team_year = Column(Date, primary_key=True)

    

def main():
    engine = create_engine('postgresql://postgres:draftday@localhost:5432/dfsv1')
    Session = sessionmaker(bind=engine)
    Session.configure(bind=engine) 
    session = Session()
    # data = session.query(Slate).filter(Slate.slate_date=='2020-07-23').first()
    contest = session.query(Contest).filter(Contest.slate_id=='5f1a18ccc607b259cef6f2ab').first()
    data = session.query(Entry).filter(Entry.contest_id==contest.contest_id).first()
    print(data.pitcher_1)
main()