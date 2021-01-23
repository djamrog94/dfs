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
    gpp = Column(Boolean)

    entry = relationship('Slate', foreign_keys=[slate_id])

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
    user_entry_count = Column(Integer)
    username = Column(String)
    entry_rank = Column(Integer)
    points = Column(Float)
    salary_used = Column(Integer)
    pitcher_1 = Column(String, ForeignKey('player.player_id'))
    pitcher_2 = Column(String, ForeignKey('player.player_id'))
    catcher = Column(String, ForeignKey('player.player_id'))
    first_base = Column(String, ForeignKey('player.player_id'))
    second_base = Column(String, ForeignKey('player.player_id'))
    short_stop = Column(String, ForeignKey('player.player_id'))
    third_base = Column(String, ForeignKey('player.player_id'))
    outfield_1 = Column(String, ForeignKey('player.player_id'))
    outfield_2 = Column(String, ForeignKey('player.player_id'))
    outfield_3 = Column(String, ForeignKey('player.player_id'))
    year = Column(Integer)

    p1 = relationship("Player", foreign_keys=[pitcher_1])
    p2 = relationship("Player", foreign_keys=[pitcher_2])
    c = relationship("Player", foreign_keys=[catcher])
    b1 = relationship("Player", foreign_keys=[first_base])
    b2 = relationship("Player", foreign_keys=[second_base])
    b3 = relationship("Player", foreign_keys=[short_stop])
    ss = relationship("Player", foreign_keys=[third_base])
    of1 = relationship("Player", foreign_keys=[outfield_1])
    of2 = relationship("Player", foreign_keys=[outfield_2])
    of3 = relationship("Player", foreign_keys=[outfield_3])




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
    slates = session.query(Slate).filter(Slate.slate_year == 2018).all()
    for slate in slates:
        entries = session.query(Entry).filter(Entry.entry_rank==1).join(Contest, Entry.contest_id==Contest.id).filter(Contest.slate_id==slate.id).all()
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