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
import internetarchive # <-- api for getting the tv data

# Internal Resources


tablename = 'tv'

def init(db):
    with dataset.connect(db) as dbx:
        table = dbx.create_table(tablename, primary_id = 'id', primary_type = dbx.types.integer, primary_increment = True)
        table.create_column('date', dbx.types.date)
        table.create_column('time', dbx.types.text)
        table.create_column('text', dbx.types.text)
        table.create_column('network', dbx.types.text)
        table.create_column('show', dbx.types.text)


def tokenize_speaker_segments(file_obj):
    '''
    NOTE: We stopped using this in the most recent version; figured it's better to get the whole piece of data and then segment it later

    DESCRIPTION: Break closed captioning text into a list of segments (assuming consistent format of `[0HH:MM:SS;fff] >>`) <-- it will break if that format is broken
    - also remove commercials (which are defined as any text with long blocks of whitespace)

    Cons of this approach: we lose a small amount of actual news text right before commercial breaks. however, this text is usually something along the lines of "we'll be right back", and also doesn't have any clear indicator of when they stop and when the commercial begins
    '''
    segments = []
    segment = {'commercial_lines_guess': 0, 'text': []}
    for line in file_obj.readlines():

        # Check if we want to make a new segment (which seem to occur when >> are called)
        if line[16:18] == '>>':

            if (segment['commercial_lines_guess'] < 3) & (len(segment['text']) > 0): # if there's a lot of lines that we guess are commercials, dont add them to the list; here we could probaly even get away with < 2 or even 1 (worth testing) | we also check if there's even any lines to parse, because if the first line of the entire document starts with >>> it'll trigger this even condition if we don't check (and throw an error when we try to index the lines)
                segment['start_time'] = datetime.datetime.strptime(segment['text'][0][1:10], '0%H:%M:%S') # <-- pull time from first line added
                segment['end_time'] = datetime.datetime.strptime(segment['text'][-1][1:10], '0%H:%M:%S') # <-- pull time from last line added
                segment['time_length_in_seconds'] = (segment['end_time'] - segment['start_time']).seconds # we honestly dont have to keep start time and end time in memory like this; once theyve hit "length" we're done with them. im keeping them because: (a) its not like it's that much memory; we'll be fine, and (b) we may want it in the future so good to remind ourselves that we have the option (it's also fun to print when debugging)
                segment['text'] = ' '.join([segment_line[16:].replace('\n','').replace('>>> ','').replace('>> ','') for segment_line in segment['text']]) # <-- sometimes the stopgap is >>> (which I think indicates -- albeit inconsistently -- that there's a new speaker); we want to remove >> and >>>
                segments.append(segment)

            segment = {'commercial_lines_guess': 0, 'text': []} # <-- either way, make the new empty segment object

        segment['text'].append(line)
        if line[16:20] == '    ': # <-- if there's 4 empty whitespaces after the time stamp, it's indicative that we're in a commercial (we can probably even get away with 2 spaces but 4 is just being careful)
            segment['commercial_lines_guess'] += 1

        # Fun ways to watch what's going in (only do one at a time):
        # print('Num Segments:', len(segments)); print('Commercial lines (guess):', segment['commercial_lines_guess']); print(line); print(' | '.join(segment['text'])); time.sleep(.1); os.system('clear')
        # print('\n>>'.join([b['text'] for b in segments])); print(line); print('Num Segments:', len(segments)); print('Commercial lines (guess):', segment['commercial_lines_guess']); time.sleep(.1); os.system('clear')
        # [print(s['start_time'].time(), '|', s['end_time'].time(), '|', s['time_length'], '|', s['text'][:100], '...') for s in segments]; print(line); print('Num Segments:', len(segments)); time.sleep(.1); os.system('clear')
    return segments


def ingest(start_date, end_date, db, config):
    '''
    Ingest the Data
    '''
    for i in range((end_date - start_date).days + 1):
        entries = []
        date = start_date + datetime.timedelta(days = i)
        networks = ['CNNW', "MSNBCW", "FOXNEWSW"]

        for network in networks: # <-- iterate through each network
            search_query = f'collection:(TV-{network}) date:[{date}]' # <-- this is the format internetarchive is expecting; you can test it here: https://archive.org/advancedsearch.php#raw

            for item in internetarchive.search_items(query = search_query): # <-- request list of closed-caption transcript identifiers for the network at the date

                with tempfile.TemporaryDirectory() as temp_dir: # <-- this directory gets deleted once the context is left
                    # temp_dir = '.tmp/'
                    internetarchive.download(item['identifier'], verbose = False, formats = 'Closed Caption Text', destdir = temp_dir) # <-- downloades transcripts for the particular show we're looking at

                    splitname = item['identifier'].split('_') # example of what the identifier typically looks like: "CNNW_20221115_000000_Erin_Burnett_OutFront"
                    time = datetime.datetime.strptime(splitname[2], '%H%M00') # ignore microseconds
                    show = ' '.join(splitname[3:])

                    filepath = os.path.join(temp_dir, item['identifier'], item['identifier'] + '.cc5.txt')

                    if os.path.exists(filepath):
                        with open(filepath, 'r') as file:
                            text = file.read()

                        entries.append({
                            'date': date,
                            'text': text,
                            'network': network,
                            'show': show,
                            'time': time,
                        })

        with dataset.connect(db) as dbx:
            dbx[tablename].insert_many(entries)
