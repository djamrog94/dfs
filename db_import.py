import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime
from datetime import timedelta

def batch_import(table, df):
    conn = psycopg2.connect(f'dbname=dfsv1 user=postgres password=draftday')
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

def main():
    # add data for team table
    # df = pd.read_csv('rs_data/teams.csv')
    # df = df[df['year'] == 2020]
    # df = df.drop('year', axis=1)
    # df.columns = ['team_abbr', 'league', 'city', 'team_name']
    # batch_import('team', df)

    # # #add data for player table
    # df = pd.read_csv('rs_data/rosters.csv')
    # df = df.drop_duplicates(subset='player_id', keep="first")
    # df = df.drop(['year', 'team_abbr_1'], axis=1)
    # batch_import('player', df)

    # add data for roster table
    df = pd.read_csv('rs_data/rosters.csv')
    df.columns = ['year', 'player_id', 'last_name', 'first_name', 'batting_hand', 'throwing_hand', 'team_abbr', 'position']
    df = df.drop_duplicates(subset=['year', 'team_abbr', 'player_id'], keep="first")
    batch_import('roster', df)
    



if __name__ == "__main__":
    main()














