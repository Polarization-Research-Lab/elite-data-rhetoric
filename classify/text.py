# Python Standard Library
import re, urllib, string

# External Dependencies
import nltk

filters = [
    lambda text: text == '[[url]]',
]

cleaners = [
    lambda text: text.replace('\n', ' '), # <-- remove newlines
    lambda text: re.sub(' +', ' ', text), # <-- remove long stretches of whitespace
    # lambda text: re.sub(r'(^|[^@\w])@(\w{1,15})\b', ' [[user]]', text), # <-- regex version of preprocessor from: https://huggingface.co/cardiffnlp/twitter-roberta-base?text=The+goal+of+life+is+%3Cmask%3E.
    lambda text: re.sub(r'https?://\S+', ' ', text), # <-- url detector; thanks to hwnd @ https://stackoverflow.com/a/25298012/6794367
    lambda text: text.replace(' ', ' '),
    lambda text: text.replace('&amp;', '&'),
    lambda text: text.replace('\n', ' '), # <-- remove newlines
    lambda text: text.replace('? ? ? ? ? ? ? ?', ''), # <-- remove random question strings
    lambda text: text.replace('? ? ? ?', ''), # <-- remove random question strings
    lambda text: text.replace('? ?', ''), # <-- remove random question strings
    lambda text: text.replace('[ ]', ' '),
    lambda text: re.sub(' +', ' ', text), # <-- remove long stretches of whitespace
    lambda text: re.sub('_+', ' ', text), # <-- remove long stretches of underscores (happens in newsletters)
    lambda text: re.sub(r'(?<!\w)([A-Z])\.', r'\1', text), # replace abbreviations (all caps) ; e.g.: M.A.C. becomes MAC. Though a downside is something like "S." becomes "S"; not sure if this is gunna be an issue. Credit to Moses Koledoye @ https://stackoverflow.com/a/40197005/6794367
]

# # Setup
punkt_param = nltk.tokenize.punkt.PunktParameters()
tokenizer = nltk.tokenize.punkt.PunktSentenceTokenizer(punkt_param)

# set a list of exceptions to use for the sentence tokenizer (might have to make these )
punkt_param.abbrev_types = set([
    # titles
    'mr', 'mrs', 'ms', 'jr', 'dr', 'prof', 'st', 'hon', 'sen', 'rep', 'sens', 'reps',

    # government speech related
    'h.r', # <-- hourse bill?
    'no', # <-- proposition number?
    'u.s',
    'lt', # <-- lieutenant
    's', # <-- senate bill?
    'res', # <-- resolution?
    'hjres', # <-- ? (showed up in the statements page)

    # normal speech appreviations
    'b.s', 'm.s','ph.d', # degrees
    'i.e', 'inc', 'vs', 
    'mt', # mountain
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', # <-- U.S. states
    'wva', 'ind', 'okla', 'mont', 'ark', 'wyo', 'miss', 'tenn', 'va', 'ala', 'mo', 'penn',  # <-- more state abbreviations commonly used in `statements`
    'jan', 'feb', 'aug', 'sept', 'oct', 'nov', 'dec', # <-- months

    # Initials (this one might be risky)
    *string.ascii_lowercase,

    # abbreviations
    # re.compile(r'(?:(?<=\.|\s)[A-Z]\.)+'), # thanks to Ro Yo Mi @ https://stackoverflow.com/a/17779796/6794367
    # re.compile(r'')
])

def chunk(text, sentences, size = 2):
    chunks = []
    for i in range(0, len(sentences), size):
        chunks.append(
            ' '.join(sentences[i:i+size]),
        )
    return chunks

def general_tokenizer(text):
    for cleaner in cleaners: text = cleaner(text)
    return tokenizer.tokenize(text)

process = {
    'floor': lambda text: chunk(text, general_tokenizer(text)[1:], size = 2),
    'tweets': lambda text: chunk(text, general_tokenizer(text), size = 100),
    'newsletters': lambda text: chunk(text, general_tokenizer(text), size = 2),
    'statements': lambda text: chunk(text, general_tokenizer(text), size = 2),
}

import tiktoken
def get_num_tokens(x):
    enc = tiktoken.encoding_for_model('gpt-4')
    try:
        return len(enc.encode(x))
    except:
        print('err:', x)
        return 1