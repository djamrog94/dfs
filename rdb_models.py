from sqlalchemy import Column, Integer, Date, String, ForeignKey, Float, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy import extract
from sqlalchemy.sql.sqltypes import Boolean

Base = declarative_base()
class Slate(Base):
    __tablename__ = 'slate'
    id = Column(String, primary_key=True)
    slate_date = Column(Date)

    @hybrid_property
    def slate_year(self):
        return self.slate_date.year
    
    @slate_year.expression
    def slate_year(cls):
        return extract('year', cls.slate_date)


    # relationship function

class Contest(Base):
    __tablename__ = 'contest'
    id = Column(String, primary_key=True)
    slate_id = Column(String, ForeignKey('slate.id'))
    name = Column(String)
    prizepool = Column(Float)
    entryfee = Column(Float)
    maxentriesperuser = Column(Integer)
    maxentries = Column(Integer)
    entrycount = Column(Float)
    rgprizepool = Column(Float)
    rgprizewinnercount = Column(Float)
    prizes = Column(JSON)
    #gpp = Column(Boolean)

    slate = relationship('Slate', foreign_keys=[slate_id])

class Slate_game(Base):
    __tablename__ = 'slate_game'
    id = Column(Integer, primary_key=True)
    movement = Column(Float)
    line = Column(Float)
    total = Column(Float)
    o_u = Column(Float)
    team_away = Column(String, ForeignKey('team.team_abbr'))
    team_home = Column(String, ForeignKey('team.team_abbr'))
    slate_id = Column(String, ForeignKey('slate.id'))
    # relationship function

    team_a = relationship("Team", foreign_keys=[team_away])
    team_h = relationship("Team", foreign_keys=[team_home])
    slate = relationship("Slate", foreign_keys=[slate_id])


class Slate_player(Base):
    __tablename__ = 'slate_player'
    id = Column(Integer, primary_key=True)
    slate_id = Column(String, ForeignKey('slate.id'))
    player_id = Column(String, ForeignKey('player.player_id'))
    salary = Column(Integer)
    actualownership = Column(Float)
    actualfpts = Column(Float)

    slate = relationship("Slate", foreign_keys=[slate_id])
    player = relationship("Player", foreign_keys=[player_id])

class Player(Base):
    __tablename__ = 'player'
    player_id = Column(String, primary_key=True)
    last_name = Column(String)
    first_name = Column(String)
    batting_hand = Column(String)
    throwing_hand = Column(String)
    position = Column(String)

class Event(Base):
    __tablename__ = 'event'
    id = Column(Integer, primary_key=True)
    game_id = Column(String, ForeignKey('game.game_id'))

    game = relationship("Game", foreign_keys=[game_id])


class Game(Base):
    __tablename__ = 'game'
    game_id = Column(String, primary_key=True)



class Entry(Base):
    __tablename__ = 'entry'
    entry_id = Column(String, primary_key=True)
    contest_id = Column(String, ForeignKey('contest.id'))
    entry_date = Column(Date)
    user_entry_count = Column(Integer)
    username = Column(String)
    entry_rank = Column(Integer)
    points = Column(Float)
    salary_used = Column(Integer)
    pitcher_1 = Column(String)
    pitcher_2 = Column(String)
    catcher = Column(String)
    first_base = Column(String)
    second_base = Column(String)
    short_stop = Column(String)
    third_base = Column(String)
    outfield_1 = Column(String)
    outfield_2 = Column(String)
    outfield_3 = Column(String)

    contest = relationship("Contest", foreign_keys=[contest_id])

class Entry_y2020m07(Base):
    __tablename__ = 'entry_y2020m07'
    entry_id = Column(String, primary_key=True)
    contest_id = Column(String, ForeignKey('contest.id'))
    entry_date = Column(Date)
    user_entry_count = Column(Integer)
    username = Column(String)
    entry_rank = Column(Integer)
    points = Column(Float)
    salary_used = Column(Integer)
    pitcher_1 = Column(String)
    pitcher_2 = Column(String)
    catcher = Column(String)
    first_base = Column(String)
    second_base = Column(String)
    short_stop = Column(String)
    third_base = Column(String)
    outfield_1 = Column(String)
    outfield_2 = Column(String)
    outfield_3 = Column(String)

    contest = relationship("Contest", foreign_keys=[contest_id])

class Entry_y2020m08(Base):
    __tablename__ = 'entry_y2020m08'
    entry_id = Column(String, primary_key=True)
    contest_id = Column(String, ForeignKey('contest.id'))
    entry_date = Column(Date)
    user_entry_count = Column(Integer)
    username = Column(String)
    entry_rank = Column(Integer)
    points = Column(Float)
    salary_used = Column(Integer)
    pitcher_1 = Column(String)
    pitcher_2 = Column(String)
    catcher = Column(String)
    first_base = Column(String)
    second_base = Column(String)
    short_stop = Column(String)
    third_base = Column(String)
    outfield_1 = Column(String)
    outfield_2 = Column(String)
    outfield_3 = Column(String)

    contest = relationship("Contest", foreign_keys=[contest_id])

class Entry_y2020m09(Base):
    __tablename__ = 'entry_y2020m09'
    entry_id = Column(String, primary_key=True)
    contest_id = Column(String, ForeignKey('contest.id'))
    entry_date = Column(Date)
    user_entry_count = Column(Integer)
    username = Column(String)
    entry_rank = Column(Integer)
    points = Column(Float)
    salary_used = Column(Integer)
    pitcher_1 = Column(String)
    pitcher_2 = Column(String)
    catcher = Column(String)
    first_base = Column(String)
    second_base = Column(String)
    short_stop = Column(String)
    third_base = Column(String)
    outfield_1 = Column(String)
    outfield_2 = Column(String)
    outfield_3 = Column(String)

    contest = relationship("Contest", foreign_keys=[contest_id])


class Team(Base):
    __tablename__ = 'team'
    team_abbr = Column(String, primary_key=True)
    league = Column(String)
    city = Column(String)
    team_name = Column(String)

class Roster(Base):
    __tablename__ = 'roster'
    player_id = Column(String, ForeignKey('player.player_id'), primary_key=True)
    team_abbr = Column(String, ForeignKey('team.team_abbr'), primary_key=True)
    team_year = Column(Date, primary_key=True)

    player = relationship("Player", foreign_keys=[player_id])
    team = relationship("Team", foreign_keys=[team_abbr])

    

def main():
    engine = create_engine('postgresql://postgres:draftday@localhost:5432/dfsv1')
    Session = sessionmaker(bind=engine)
    Session.configure(bind=engine) 
    session = Session()
    from datetime import datetime
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print(f'Started: {current_time}')
    slates = session.query(Slate).filter(Slate.slate_year == 2020).all()
    slate = slates[0]
    players = session.query(Contest).join(Slate, Contest.slate_id==Slate.id).first()
    for row in tqdm(entries):
        rank = row.entry_rank
        prizes = row.prizes
        for prize in prizes:
            if rank >= prize['minFinish'] and rank <= prize['maxFinish']:
                row.winnings = prize['cash']
                break
        
    session.commit()
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print(f'Ended: {current_time}')
    # for slate in slates:
    #     entries = session.query(Entry).filter(Entry.entry_rank==1).join(Contest, Entry.contest_id==Contest.id).filter(Contest.slate_id==slate.id).all()
    # pos = {
    # 'P': [],
    # '1B': [],
    # '2B': [],
    # '3B': [],
    # 'SS': [],
    # 'OF': [],
    # 'C': []}

    # slates = session.query(Slate).filter(Slate.slate_year == 2018).all()
    # for idx, slate in enumerate(slates):
    #     print(f'slate {idx} | {len(slates)}')
    #     players = session.query(Slate_player).join(Player, Slate_player.player_id==Player.player_id).filter(Slate_player.slate_id==slate.id).all()
    #     for idxp, p in enumerate(players):
    #         if idxp % 50_000 == 0:
    #             print(f'player {idxp} | {len(players)}')
    #         if p.player.position not in pos.keys():
    #             continue
    #         if p.salary == 0 or p.actualfpts == 0:
    #             ptsper = 0
    #         else:
    #             ptsper = p.salary / p.actualfpts
    #         pos[p.Player.position].append(ptsper)
    #     data = session.query(Slate).filter(Slate.slate_year==2018).first()
    #players = session.query(Slate_player).filter(Slate_player.slate_id==data.id).all()
    # players = session.query(Slate_player).join(Player, Slate_player.player_id==Player.player_id).filter(Slate_player.slate_id==data.id).first()
    # entry = session.query(Entry).filter(Entry.contest_id==contest.id).first()
    
    print(entry.pitcher_1)

if __name__ == "__main__":
    main()