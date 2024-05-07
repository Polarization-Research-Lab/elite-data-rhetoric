'''

# Todo:
- this script scans the db table for each date from now until around the first date we have date. EVERY TIME. there should be a better way to do this (start from current date... if any in classif, stop)
    - ^ or actually, just set the "until" date (beginning_date)
'''
# Python Standard Library
import sys, json, urllib, datetime, os

# External Dependencies
import dotenv
import numpy as np 
import pandas as pd
import sqlalchemy as sql
import dataset as database
import ibis
from ibis import _

# Internal Dependencies
import text

# Setup
dotenv.load_dotenv('../secrets')

## DB Credentials
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
conn = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite',
)
classifications = conn.table('classifications')

# Chunk text data, then load it into the Database
today = datetime.date.today()
# beginning_date = datetime.date(year=2023,month=12,day=30)
beginning_date = datetime.date(year=2024,month=4,day=19)
# beginning_date = datetime.date(year=2022,month=8,day=1) # <-- go back to the beginning

print('start')
for d, day in enumerate(range((today - beginning_date).days + 1)):
    target_date = today - datetime.timedelta(days = day) # <-- start from lastest date (go backward)
    print(target_date)

    # source_classifications = classifications.filter(classifications.source == source)
    for source in ['floor', 'tweets', 'statements', 'newsletters']:
        source_table = conn.table(source)
        
        ids_for_date = (
            source_table
            .select([source_table.id])
            .filter(source_table.date == target_date)
            .execute()
        )

        if not ids_for_date.empty:

            clsf_in_ids_for_date = (
                classifications
                .filter(classifications.source == source)
                .group_by(_.source_id)
                .aggregate()
                .filter(_.source_id.isin(ids_for_date['id'].values))
                .execute()
            )

            missing_ids = ids_for_date[~ids_for_date['id'].isin(clsf_in_ids_for_date['source_id'])]

            if not missing_ids.empty:

                items = (
                    source_table
                    .select([source_table.id, source_table.bioguide_id, source_table.text, source_table.date])
                    .filter([source_table.date == target_date, source_table.id.isin(missing_ids['id'].values)])
                    .execute()
                )

                # split into chunks
                items['text'] = items['text'].apply(lambda x: text.process[source](x))

                # expand dataframe so each chunk gets their own row
                items = items.explode('text', ignore_index = True)

                items['source'] = source
                items.rename(columns = {'id': 'source_id'}, inplace = True)
                items['errors'] = items.apply(lambda row: {}, axis = 1).astype(object)
                items = items.fillna(np.nan).replace([np.nan], [None])

                with database.connect(params) as dbx:
                    dbx['classifications'].insert_many(
                        items.to_dict(orient = 'records'),
                    )
                print(f'---pushed {items.shape[0]} items from {source} for date {target_date}---')



print('end')


