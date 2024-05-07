# Python Standard Library
import sys, json, urllib, datetime, os

# External Deps
import dotenv
import dataset as database

# Setup
dotenv.load_dotenv('../secrets')

## DB Credentials
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
with database.connect(params) as dbx:
    table = dbx.create_table('classifications', primary_id = 'id', primary_type = dbx.types.integer, primary_increment = True)
    table.create_column('source', dbx.types.string(50))
    table.create_column('source_id', dbx.types.integer)
    table.create_column('errors', dbx.types.json)
