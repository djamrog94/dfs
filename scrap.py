import json
import pandas as pd
import os

def clean_entries(files):
    master = []
    for file in files:
        with open(file) as f:
            data = json.load(f)
        df = pd.DataFrame(data['entries'])

        def get_summary(r):
            return r['lineup']['summary']
        df['summary'] = df.apply(get_summary,axis=1)
        master.append(df)


    return pd.concat(master)


# def get_files():
#     repair_folders = os.listdir('data/critical')
#     repair_folders = [x[:-5] for x in repair_folders]
#     for folder in repair_folders:
#         repair(folder)
