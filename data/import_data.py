from __future__ import print_function
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd

postgres_engine = create_engine(get_connection_string("config.yml", 'postgres_database'))

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

parcels_sql = '''
    WITH temp AS (
        SELECT
            ludu2015_parcel_id
            ,p.jurisdiction_id
            ,sr13_cap_hs_growth_adjusted AS sr13_cap
        FROM urbansim.ref.sr13_capacity AS s
        LEFT JOIN urbansim.urbansim.parcel p ON s.ludu2015_parcel_id = p.parcel_id
        WHERE p.jurisdiction_id IN (14, 19) AND sr13_cap_hs_growth_adjusted > 0
    )
    SELECT
        p.parcel_id
        ,p.jurisdiction_id
        ,p.development_type_id AS building_type_id
        ,COALESCE(CAST(c.sr14_cap AS int), 0) + COALESCE(CAST(sr13_cap AS int), 0) + COALESCE(b.residential_units, 0) AS total_cap
        ,COALESCE(b.residential_units, 0) as residential_units
        ,sr13_cap
        ,c.sr14_cap as sr14_cap
        ,distance_to_coast
        -- ,land_value/ parcel_acres as land_value_per_acre
    FROM urbansim.urbansim.parcel AS p
    LEFT JOIN (SELECT parcel_id, SUM(residential_units) AS residential_units FROM urbansim.urbansim.building GROUP BY parcel_id) AS b
        ON b.parcel_id = p.parcel_id
    LEFT JOIN spacecore.staging.sr14_capacity AS c
        ON c.parcel_id = p.parcel_id
    LEFT JOIN temp AS t
        ON t.ludu2015_parcel_id = p.parcel_id
    WHERE sr13_cap > 0 OR sr14_cap > 0
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
        ,COALESCE(year_built, 0) AS year_built
    FROM urbansim.urbansim.building
'''


households_df = pd.read_sql(households_sql, mssql_engine, index_col='year')
buildings_df = pd.read_sql(buildings_sql, mssql_engine, index_col='building_id')
parcels_df = pd.read_sql(parcels_sql, mssql_engine, index_col='parcel_id')
print(len(parcels_df))
parcels_df = parcels_df.fillna(1000)
print(len(parcels_df))


with pd.HDFStore('urbansim.h5', mode='w') as store:
    store.put('households', households_df)
    store.put('buildings', buildings_df)
    store.put('parcels', parcels_df)
