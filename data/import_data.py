from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd

db_connection_string = get_connection_string('config.yml', 'mssql_db')
mssql_engine = create_engine(db_connection_string)

zoning_sql = '''
            SELECT  zoning_id as id,
                    zone as name,
                    max_building_height as max_height,
                    max_far as max_far
                    FROM urbansim.zoning
                    WHERE zoning_schedule_id = 1'''

zoning_for_parcels_sql = '''
            SELECT  p.parcel_id as parcel, zp.zoning_id as zoning
                    FROM urbansim.parcels p
                    JOIN urbansim.zoning_parcels zp
                       ON p.parcel_id = zp.parcel_id
                    WHERE zp.zoning_schedule_id = 1 '''

households_sql = '''
            SELECT household_id,
                   b.building_id,
                   development_type_id as building_type_id,
                   income,
                   persons,
                   tenure
                   FROM urbansim.households as h
                   JOIN urbansim.buildings as b
                   ON h.building_id = b.building_id'''

## Missing job_category
jobs_sql = '''
            SELECT job_id,
                   building_id,
                   'service' as job_category
                   --sector_id
                   FROM urbansim.jobs'''

## Missing building_sqft, residential sales price, non_residential rent
buildings_sql = '''SELECT building_id, parcel_id,
                          COALESCE(residential_units, 0) as residential_units,
                          COALESCE(non_residential_sqft,0) as non_residential_sqft,
                          0 as building_sqft,
                          COALESCE(stories, 1) as stories,
                          COALESCE(development_type_id,0) as building_type_id,
                          COALESCE(year_built, 0) year_built,
                          0 as residential_sales_price,
                          0 as non_residential_rent
                     FROM urbansim.buildings'''


zoning_df = pd.read_sql(zoning_sql, mssql_engine, index_col='id')
zoning_for_parcels_df = pd.read_sql(zoning_for_parcels_sql, mssql_engine, index_col='parcel')
households_df = pd.read_sql(households_sql, mssql_engine, index_col='household_id')
jobs_df = pd.read_sql(jobs_sql, mssql_engine, index_col='job_id')
buildings_df = pd.read_sql(buildings_sql, mssql_engine, index_col='building_id')

with pd.HDFStore('urbansim.h5', mode='w') as store:
    store.put('zoning', zoning_df)
    store.put('zoning_for_parcels', zoning_for_parcels_df)
    store.put('households', households_df)
    store.put('jobs', jobs_df)
    store.put('buildings', buildings_df)
