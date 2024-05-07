
import os, argparse
parser = argparse.ArgumentParser(description = 'Elite Data Harvest')
parser.add_argument('source', type = str, help = f'Which source to you want to pull from? Options: {[folder for folder in os.listdir("sources") if (~folder.startswith(".")) & (os.path.isdir(os.path.join("sources", folder)))]}')
parser.add_argument('-d', '--debug', action = 'store_true', help = 'Whether you want to run in debug mode')
args = parser.parse_args()

# Python Standard Library
import json, urllib, datetime, argparse

# External Resources
import dotenv
import dataset
import sqlalchemy as sql
import dataset

# Internal Resources
import sources.floor.ingestor
import sources.newsletters.ingestor
import sources.statements.ingestor
import sources.tv.ingestor
import sources.twitter.ingestor

sources = {
    'floor': sources.floor.ingestor,
    'newsletters': sources.newsletters.ingestor,
    'statements': sources.statements.ingestor,
    'tv': sources.tv.ingestor,
    'tweets': sources.twitter.ingestor,
}


# Setup
dotenv.load_dotenv('/prl/.secrets/admin')
with open('config.json') as file: config = json.load(file)

config['congress.gov api key'] = os.environ['CONGRESS_API']
config['propublica api key'] = os.environ['PROPUBLICA_API']
config['twitter bearer token'] = os.environ['TWITTER_API']



## Connect to DB
db = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

### Make table if it doesnt exist
sources[args.source].init(db)

## Get Date Ranges
start_date = datetime.datetime.strptime(config['start-date'], '%Y-%m-%d').date()
with dataset.connect(db) as dbx:
    max_date = sql.select(sql.func.max(dbx[args.source].table.c.date)).execute().first()[0]
if max_date: start_date = max_date + datetime.timedelta(days=1)


end_date = datetime.datetime.now().date()

with dataset.connect(db) as dbx: init_count = dbx[args.source].count()

# Execute Harvester
for d, day in enumerate(range((end_date - start_date).days)):
    date = start_date + datetime.timedelta(days = day)
    
    print('collecting for:', date)
    with dataset.connect(db) as dbx:
        existing = dbx[args.source].find_one(date = date)

    if existing:
        print(f'Skipping {date} since there are already existing entries for that date')

    else:
        if not args.debug:
            sources[args.source].ingest(date, date, db, config)

with dataset.connect(db) as dbx: end_count = dbx[args.source].count()

print(f'\titems processed: {end_count - init_count}')
