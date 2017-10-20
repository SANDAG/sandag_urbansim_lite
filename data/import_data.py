from __future__ import print_function
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd

postgres_engine = create_engine(get_connection_string("config.yml", 'postgres_database'))

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

parcels_sql = '''
SELECT  parcel_id
      ,jurisdiction_id
      ,jurisdiction_name
      ,building_type_id
      ,capacity
      ,residential_units
      ,total_cap
      ,num_of_bldgs
      ,distance_to_coast
  FROM urbansim.urbansim.input_residential_capacity
'''


households_sql = '''
    SELECT
        yr AS year
        ,sum(households) AS hh
    FROM urbansim.urbansim.household_control
    GROUP BY yr
    ORDER BY yr
'''


## Missing building_sqft, residential sales price, non_residential rent
buildings_sql = '''
    SELECT
        building_id
        ,parcel_id
        ,COALESCE(development_type_id,0) AS building_type_id
        ,COALESCE(residential_units, 0) AS residential_units
        ,COALESCE(year_built, 0) AS year_built, 0 as random_prob, 0 as random
    FROM urbansim.urbansim.building
'''


households_df = pd.read_sql(households_sql, mssql_engine, index_col='year')
buildings_df = pd.read_sql(buildings_sql, mssql_engine, index_col='building_id')
parcels_df = pd.read_sql(parcels_sql, mssql_engine, index_col='parcel_id')

# convert unicode 'name' to str (needed for HDFStore in python 2)
parcels_df['jurisdiction_name'] = parcels_df['jurisdiction_name'].astype(str)

print(len(parcels_df))
parcels_df = parcels_df.fillna(1000)
print(len(parcels_df))


with pd.HDFStore('urbansim.h5', mode='w') as store:
    store.put('households',households_df,format='table')
    store.put('buildings',buildings_df,format='table')
    store.put('parcels',parcels_df,format='table')

# notes on pd.HDFStore:
# DataFrame column names are unicode, e.g. households_df.columns -> Index([u'hh'], dtype='object')
# for fixed format cannot store unicode in a HDFStore in python 2. (works correctly in python 3 however).
# workaround is setting format='table' which does work correctly in python 2.
# see https://github.com/pandas-dev/pandas/issues/12016
