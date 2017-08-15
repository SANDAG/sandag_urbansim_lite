from __future__ import print_function
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd

postgres_engine = create_engine(get_connection_string("config.yml", 'postgres_database'))

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

parcels_sql = '''
        with temp as (SELECT [ludu2015_parcel_id]
	  ,p.jurisdiction_id
      ,[sr13_cap_hs_growth_adjusted] as sr13_cap
  FROM [urbansim].[ref].[sr13_capacity] s
  Left join [urbansim].[urbansim].[parcel] p on s.ludu2015_parcel_id = p.parcel_id
  where p.jurisdiction_id in (14, 19) and sr13_cap_hs_growth_adjusted > 0)

SELECT         p.parcel_id
			  ,p.jurisdiction_id
              ,p.[development_type_id] as building_type_id
              ,coalesce(cast(c.sr14_cap as int), 0) + coalesce(cast(sr13_cap as int), 0) + coalesce(sum(b.residential_units), 0) as total_cap
              ,coalesce(sum(b.residential_units), 0) as residential_units
			  ,sr13_cap
			  ,c.sr14_cap as sr14_cap
             -- ,[land_value]/ [parcel_acres] as land_value_per_acre
              FROM [urbansim].[urbansim].[parcel] as p
              LEFT JOIN [urbansim].[urbansim].[building] as b
              ON b.parcel_id = p.parcel_id
              LEFT JOIN staging.sr14_capacity as c
              ON c.parcel_id = p.parcel_id
			  LEFT JOIN temp as t
              ON t.ludu2015_parcel_id = p.parcel_id
			  where sr13_cap > 0 or sr14_cap > 0
              GROUP BY  p.[parcel_id], p.jurisdiction_id, c.sr14_cap, sr13_cap, p.[development_type_id]'''

parcels_distance_to_coast_sql = '''
SELECT parcel_id, distance_to_coast
  FROM urbansim.parcels;
'''

households_sql = '''
          SELECT yr as year, sum(households) as hh
            FROM urbansim.household_controls
            GROUP BY yr
            ORDER BY yr'''


## Missing building_sqft, residential sales price, non_residential rent
buildings_sql = '''SELECT building_id, parcel_id,
                          COALESCE(development_type_id,0) as building_type_id,
                          COALESCE(residential_units, 0) as residential_units,
                          COALESCE(year_built, 0) year_built
                     FROM [urbansim].[urbansim].[building]'''


households_df = pd.read_sql(households_sql, postgres_engine, index_col='year')
buildings_df = pd.read_sql(buildings_sql, mssql_engine, index_col='building_id')
parcels_df = pd.read_sql(parcels_sql, mssql_engine, index_col='parcel_id')
parcels_distance_to_coast_df = pd.read_sql(parcels_distance_to_coast_sql, postgres_engine, index_col='parcel_id')
print(len(parcels_df))
parcels_df = parcels_df.join(parcels_distance_to_coast_df)
parcels_df = parcels_df.fillna(1000)
print(len(parcels_df))


with pd.HDFStore('urbansim.h5', mode='w') as store:
    store.put('households', households_df)
    store.put('buildings', buildings_df)
    store.put('parcels', parcels_df)
