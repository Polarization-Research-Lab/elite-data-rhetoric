
import os, json, urllib, datetime

# External Resources
import dotenv
import dataset
import sqlalchemy as sql
import pandas as pd
import numpy as np
import ibis
from ibis import _

# Setup
categories = ['attack_personal', 'attack_policy', 'outcome_creditclaiming', 'policy', 'policy_legislative_discussion', 'outcome_bipartisanship']

## Connect to DB
dotenv.load_dotenv('/prl/.secrets/admin')
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
conn = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite',
)
classifications = conn.table('classifications')
legislators = conn.table('legislators').filter(_.is_active == 1).select([_.bioguide_id, _.party])

# Transform
classifications = classifications.mutate(
    attack_personal = ibis.case()
        .when((classifications.attack == 0) & (classifications.attack_personal == 1), 0)
        .else_(classifications.attack_personal)
        .end(),
)

# Aggregate (across all sources)
aggregate_classifications = (
    classifications
    .group_by('bioguide_id')
    .aggregate(
        count = _.count(),
        **{col + '_count': _[f'{col}'].count() for col in categories},
        **{col + '_sum': _[f'{col}'].sum() for col in categories},
        **{col + '_mean': (_[f'{col}'].mean() * 100).round(2) for col in categories},
    )
    .join(
        legislators,
        _.bioguide_id == legislators.bioguide_id
    )
    .execute()
)

### Get Ranking based on MEAN values ()
for col in categories:
    aggregate_classifications[f"{col}_rank"] = None
    aggregate_classifications.loc[aggregate_classifications['count'] > 300, f"{col}_rank"] = aggregate_classifications.loc[aggregate_classifications['count'] > 300, f"{col}_mean"].rank(ascending=False, method = 'dense') 
    aggregate_classifications = aggregate_classifications.astype({col: 'int' for col in aggregate_classifications.select_dtypes(include=['int64']).columns}) # <-- convert to basic int

aggregate_classifications['source'] = 'all'

# # Aggregate (by source)
aggregate_by_source = (
    classifications
    .group_by(['bioguide_id', 'source'])
    .aggregate(
        count = classifications['bioguide_id'].count(),
        **{col + '_count': classifications[f'{col}'].count() for col in categories},
        **{col + '_sum': classifications[f'{col}'].sum() for col in categories},
        **{col + '_mean': (classifications[f'{col}'].mean() * 100).round(2) for col in categories},
    )
    .execute()
)

agg = pd.concat(
    [aggregate_classifications, aggregate_by_source],
)

agg = agg.replace(np.nan, None)

with dataset.connect(params) as dbx:
    dbx['rhetoric'].upsert_many(
        agg.to_dict(orient = 'records'),
        ['bioguide_id', 'source']
    )

