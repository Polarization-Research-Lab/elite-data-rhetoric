---
title: Elite Data Harvest
version: 3
comments: Aggressively simplified from V2
---

# Setup

1. Install python3; you should then make a virtual env
2. Run `python3 -m pip install -r .python/requirements.txt`
3. Route secrets to the `harvest.py` script
    - if you want this to work for you, you'll have to provide database credentials (or path if using sqlite) and API keys
    - for the `tv` data harvester, we use the `internetarchive` python module. this requires having an ia.ini file with credentials stored (on ubuntu) @ `.config/internetarchive/ia.ini` (in the future we should probably find a way to pass that info like we do the other API keys)

# Run

- Execute the `run` python script, passing the source you want to harvest from. E.g.:

```bash
path/to/python3 ingest.py floor
```
^ you might have to make it executable with `chmod +x ./run`

# Dependencies

- python3
    - numpy
    - pandas
    - dataset
    - internetarchive
    - newspaper3k


# How it works

- the `sources/` folder contains scripts that (a) specify the structure of the data table for a given source, (b) have an `ingest(...)` function that collects data for that source given the specified date range
    - the `ingest(...)` function also manages adding new rows to the appropriate table in the database
- the `harvest.py` script finds out which dates it needs to collect data for, and calls the ingest function for the specified source, one day at a time

