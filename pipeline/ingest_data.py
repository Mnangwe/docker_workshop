#!/usr/bin/env python
# coding: utf-8

import pandas as pd

year = 2021
month = 1

prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
url_2021 = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

df = pd.read_csv(url_2021, nrows=100)

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    url_2021,
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)

pg_user = 'root'
pg_password = 'root'
pg_host = 'localhost'
pg_db = 'ny_taxi'

from sqlalchemy import create_engine
engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:5432/{pg_db}')

print(pd.io.sql.get_schema(df,name='yellow_taxi_data', con=engine))

# Schema Creation
df.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

df_iter = pd.read_csv(
    url_2021,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000,
)


# In[20]:


get_ipython().system('uv add tqdm')


# In[21]:


from tqdm.auto import tqdm


# In[22]:


for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[ ]:




