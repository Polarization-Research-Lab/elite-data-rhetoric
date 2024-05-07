'''
---
title: Legislator Newsletter Ingester
---
'''
# Python Standard Library
import os, json, datetime, tempfile, time, csv
import urllib

# External Resources
import dataset
import requests
import newspaper # <-- api for getting the speech


tablename = 'statements'
def init(db):
    with dataset.connect(db) as dbx:
        table = dbx.create_table(tablename, primary_id = 'id', primary_type = dbx.types.integer, primary_increment = True)
        table.create_column('date', dbx.types.datetime)
        table.create_column('bioguide_id', dbx.types.string(50))
        table.create_column('text', dbx.types.text)
        table.create_column('url', dbx.types.text)
        table.create_column('title', dbx.types.text)
        table.create_column('type', dbx.types.text)
        table.create_column('congress', dbx.types.string(50))
        table.create_column('chamber', dbx.types.string(50))
        table.create_column('name', dbx.types.text)
        table.create_column('state', dbx.types.string(20))
        table.create_column('party', dbx.types.string(11))

def ingest(start_date, end_date, db, config):
    '''
    Ingest the Data
    '''
    for i in range((end_date - start_date).days + 1):
        try:
            entries = []
            date = start_date + datetime.timedelta(days = i)

            pagination = True
            statement_count = 0
            offset = 0

            while(pagination):
            
                # ## Structure Request
                url = f"https://api.propublica.org/congress/v1/statements/date/{date.strftime('%Y-%m-%d')}.json" # <-- url to send a request to
                response = requests.get(
                    url, 
                    params = {'offset': offset},
                    headers = {'x-api-key': config['propublica api key']}
                )
                response.raise_for_status()
                statements = response.json()

                if statements['status'] != '500':

                    if statements['num_results'] > 0 and statements['num_results'] >= offset:

                        for statement in statements['results']:
                            try:
                                article = newspaper.Article(statement['url']); article.download(); article.parse()
                                statement_text = article.text
                            except Exception as e:
                                statement_text = ''
                                print(e)
                                # logging.warning(f'[{datetime.datetime.now().date()}] Article from {statement["url"]} for {date}')

                            entries.append({
                                'date': datetime.datetime.strptime(statement['date'], '%Y-%m-%d').date(),
                                'bioguide_id': statement['member_id'],
                                'text': statement_text,
                                'url': statement['url'],
                                'title': statement['title'],
                                'type': statement['statement_type'],
                                'congress': statement['congress'],
                                'chamber': statement['chamber'],
                                'name': statement['name'],
                                'state': statement['state'],
                                'party': statement['party'],
                            }) 

                        offset += 20
                    else:
                        pagination = False
                else:
                    pagination = False

            # send it
            with dataset.connect(db) as dbx:
                dbx[tablename].insert_many(entries)

        except Exception as e:
            print(e) # <-- sends error to log file