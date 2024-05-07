'''
Actually classify the data
'''
# Python Standard Library
import sys, json, urllib, datetime, os, time
import concurrent.futures

# External Dependencies
import dotenv
import numpy as np 
import pandas as pd
import dask.dataframe as dd  

import sqlalchemy as sql
import dataset as database
import ibis
from ibis import _

import json5

# Internal Dependencies
import llms
from prompts import attack, outcomes, policy

# Setup
prompts = {
    'attack': attack,
    'outcomes': outcomes,
    'policy': policy,
}

dotenv.load_dotenv('../secrets') # <-- in addition to db creds, this also sets the OPENAI_API_KEY value

## DB Credentials
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
conn = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite',
)
classifications = conn.table('classifications')

debug = False

## Process Reponse Function
def process_response(item, prompt):
    try:
        if '{' in item['response']:
            item['response'] = item['response'][item['response'].find('{'):item['response'].rfind('}')+1]
        else: 
            raise ValueError('Bad JSON detected')

        item['response'] = json5.loads(item['response'].replace("“",'"').replace("”", '"'))
        for key in prompts[prompt].column_map:
            col = prompts[prompt].column_map[key]['name'] # <-- what we actually name the column
            item[col] = prompts[prompt].column_map[key]['filter'](
                item['response'][key]
            )
        return item

    except Exception as exception:
        item['errors'][prompt] = f"Error in processing {item['source']}-{item['source_id']}.\n===\nResponse: {item['response']}\n===\nException: {exception}"
        # raise(exception)
        return item

if __name__ == '__main__':
    print('---starting---')
    count = classifications.count().execute()

    total_processed = {
        'attack': 0,
        'policy': 0,
        'outcomes': 0,
    }

    today = datetime.date.today()
    # today = datetime.date(year=2023,month=7,day=3)
    beginning_date = datetime.date(year=2024, month=1, day=1)
    # beginning_date = datetime.date(year=2022,month=8,day=1) # <-- go back to the beginning


    for d, day in enumerate(range((today - beginning_date).days + 1)):
        target_date = today - datetime.timedelta(days = day) # <-- start from lastest date (go backward)
       
        print(target_date)

        chunk = (
            classifications 
            .filter(classifications.date == target_date)
        )
        for prompt in prompts:

            combined_filter = ibis.literal(False)
            for col in prompts[prompt].column_map.values(): # Chain OR conditions for each column
                combined_filter = combined_filter | chunk[col['name']].isnull()

            num_items_that_need_classifying = (
                chunk 
                .filter(combined_filter)
                .count()
                .execute()
            )

            print('\t', prompt, '|', num_items_that_need_classifying)

            if num_items_that_need_classifying > 0:
                total_processed[prompt] += num_items_that_need_classifying

                items_that_need_classifying = (
                    chunk
                    .select(['id','source','source_id','bioguide_id','text', 'errors'] + [col['name'] for col in prompts[prompt].column_map.values()])
                    .filter(combined_filter)
                    .execute()
                )
                items_that_need_classifying['errors'] = items_that_need_classifying['errors'].apply(lambda x: {} if x is None else x)

                items_that_need_classifying['response'] = None

                ddf = dd.from_pandas(items_that_need_classifying[['text']], npartitions = 16) # <-- convert to dask for parallelization

                ## Do the actual classification
                res = ddf['text'].apply( # <-- build apply
                    lambda text: llms.chatgpt(
                        prompts[prompt].prompt.format(target = text)
                    ),
                    meta = ('str')
                ).compute()

                ## Process
                items_that_need_classifying['response'] = res # <-- add classifications to the df

                for col in prompts[prompt].column_map.values():
                    items_that_need_classifying[col['name']] = None
                    items_that_need_classifying[col['name']] = items_that_need_classifying[col['name']].astype(object)

                # format results
                items_that_need_classifying = items_that_need_classifying.apply(lambda item: process_response(item, prompt), axis = 1)

                new_cols = [col['name'] for col in prompts[prompt].column_map.values()]
                results = items_that_need_classifying[['id','source','source_id', 'bioguide_id', 'text', 'errors'] + new_cols]
                results = results.fillna(np.nan).replace([np.nan], [None])

                # upload results
                if debug == False:
                    with database.connect(params) as dbx:
                        dbx['classifications'].upsert_many(
                            results.to_dict(orient = 'records'),
                            'id'
                        )
                else:
                    print('not pushing data; debug mode')

    print('done!\n======\nprocessed:\n')
    print(total_processed)



