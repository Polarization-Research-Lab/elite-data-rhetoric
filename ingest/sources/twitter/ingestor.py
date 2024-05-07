'''
---
title: House and Senate Floor Speech Ingester
---
'''
# Python Standard Library
import os, json, datetime, tempfile, time, requests, zipfile, io

# External Resources
import dataset
import pandas as pd
import requests

tablename = 'tweets'

def init(db):
    with dataset.connect(db) as dbx:
        table = dbx.create_table(tablename, primary_id = 'id', primary_type = dbx.types.integer, primary_increment = True)
        table.create_column('date', dbx.types.date)
        table.create_column('bioguide_id', dbx.types.string(50))
        table.create_column('text', dbx.types.text)
        table.create_column('created_at', dbx.types.datetime)
        table.create_column('tweet_id', dbx.types.text)
        table.create_column('url', dbx.types.text)
        table.create_column('public_metrics', dbx.types.json)


def get_tweets_by_user(user_id, start_date, end_date, bearer_token):

    # Convert start_date and end_date to ISO 8601 format
    start_time = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"https://api.twitter.com/2/users/{user_id}/tweets"

    headers = {
        'Authorization': f'Bearer {bearer_token}',
    }

    params = {
        'start_time': start_time,
        'end_time': end_time,
        'max_results': 10,  
        'tweet.fields': 'created_at,public_metrics',  # Add this line
        'exclude': 'retweets',  # Exclude retweets
    }

    # prep = requests.Request('GET', url, headers=headers, params=params).prepare()
    # print(prep)
    # print(prep.url)
    # exit()
    all_tweets = []

    retries = 0
    while retries < 4:  # Limit the number of retries to avoid infinite loops
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            tweets = response.json().get('data', [])
            if not tweets:
                break
            all_tweets.extend(tweets)

            # Pagination: get the next token for the next page of results
            next_token = response.json().get('meta', {}).get('next_token')
            if next_token:
                params['pagination_token'] = next_token
            else:
                break
        elif response.status_code == 429:  # Rate limit exceeded
            print(f"Rate limit exceeded. Waiting for 15 minutes...")
            time.sleep(15 * 60 + 10)  # Wait for 15 minutes
        else:
            print(f"Failed to fetch tweets. Status code: {response.status_code}")
            print(response.text)
            retries += 1
            # Exponential backoff: sleep for 2^retries seconds
            time.sleep(2**retries)
    
    return all_tweets

def ingest(start_date, end_date, db, config, legislator_id = None):
    '''
    Ingest the Data

    If legislator_id is provided, only pull data for that legislator
    '''
    if legislator_id:
        with dataset.connect(db) as dbx: legislators = pd.DataFrame(dbx['legislators'].find(bioguide_id = legislator_id))
    else:
        with dataset.connect(db) as dbx: legislators = pd.DataFrame(dbx['legislators'])

    # for i in range(start_date, end_date, datetime.timedelta(days = 1)):
    for i in range((end_date - start_date).days + 1):
        count = 0
        date = start_date + datetime.timedelta(days = i)

        start_of_day = datetime.datetime.combine(date, datetime.datetime.min.time())
        end_of_day = datetime.datetime.combine(date, datetime.datetime.max.time())

        with dataset.connect(db) as dbx:

            entries = []
            for l, legislator in legislators.iterrows():
                if legislator['twitter_id']:
                    for twitter_id in legislator['twitter_id'].split(','):
                        tweets = get_tweets_by_user(
                            legislator['twitter_id'], 
                            start_of_day, 
                            end_of_day, 
                            bearer_token = config['twitter bearer token']
                        )
                        
                        for tweet in tweets:
                            count += 1
                            entries.append({
                                'date': date,
                                'bioguide_id': legislator['bioguide_id'],
                                'text': tweet['text'],
                                'tweet_id': tweet['id'],
                                'created_at': datetime.datetime.strptime(tweet['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ"),
                                'public_metrics': tweet['public_metrics']
                            }) 

            dbx[tablename].upsert_many(entries, ['tweet_id'])
        print('count:', count)