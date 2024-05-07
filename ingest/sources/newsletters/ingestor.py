'''
title: Legislator Newsletter Ingester
source: www.dcinbox.com

The source of the newsletter data comes from DCInbox, by [Prof. Lindsey Cormack](http://personal.stevens.edu/~lcormack/). They do all the heavy lifting; we just download using their API.
'''
# Python Standard Library
import os, json, datetime, tempfile, time, csv

# External Resources
import dataset
import requests

# Internal Resources
tablename = 'newsletters'

def init(db):
    with dataset.connect(db) as dbx:
        table = dbx.create_table(tablename, primary_id = 'id', primary_type = dbx.types.integer, primary_increment = True)
        table.create_column('date', dbx.types.date)
        table.create_column('bioguide_id', dbx.types.string(50))
        table.create_column('text', dbx.types.text)
        table.create_column('subject', dbx.types.text)
        table.create_column('unix_timestamp', dbx.types.integer)

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Origin': 'https://www.dcinbox.com',
    'Referer': 'https://www.dcinbox.com/',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}

def ingest(start_date, end_date, db, config):
    '''
    Ingest the Data
    '''
    for i in range((end_date - start_date).days + 1):

        entries = []

        date = start_date + datetime.timedelta(days = i)
        date = datetime.datetime(date.year, date.month, date.day)

        start_timestamp = time.mktime(date.replace(hour = 0, minute =0, second=0).timetuple()) * 1000
        end_timestamp = (start_timestamp + 86400000 - 1) 

        data = {'data': '{"start":' + str(int(start_timestamp)) + ',"end":' + str(int(end_timestamp)) + '}',}

        # Request Data
        response = requests.post('https://www.dcinbox.com/api/csv.php', headers=headers, data=data)

        text = response.text

        # # Parse Resposne Data & Push to model
        lines = text.splitlines()
        parsed_csv = csv.DictReader(lines)
        
        for source_id, row in enumerate(parsed_csv):
            entries.append({
                'date': datetime.datetime.fromtimestamp(int(row['Unix Timestamp']) / 1000).date(),
                'bioguide_id': row["BioGuide ID"],
                'text': row['Body'],
                'subject': row['Subject'],
                'unix_timestamp': row['Unix Timestamp'],
            })

        with dataset.connect(db) as dbx:
            dbx[tablename].insert_many(entries)
