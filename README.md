---
title: PRL Elite Data
---



# Why can't I run this?

This repo contains code that's meant to run from our internal server. We publish it so that our work can be audited by the scientific community. We keep no secrets as to how our public data is acquired and curated.

So, this code isn't meant to be a grab-and-go tool for anyone to use. Maintaining this as a grab-and-go tool would also be a little burdensome for our small dev team.

That said, if you'd like to run this code yourself, this is what you'll have to do:

- create an `env` file with the following line:

```bash
py3={/your/path/to/python3}
```

- create a `secrets` file with the following lines (filling in required values):
```bash
DB_DIALECT="*************"
DB_USER="*************"
DB_PASSWORD="*************"
DB_HOST="*************"
DB_PORT="*************"
TWITTER_API="*************"
PROPUBLICA_API="*************"
CONGRESS_API="*************"
OPENAI_API_KEY="*************"
```

- run the `init` file in the head dir
```bash
./init
```
- which will install all the python dependencies

After all that, the `classify` and `ingest` scripts should work correctly
